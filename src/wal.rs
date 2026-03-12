//! Write-Ahead Log (WAL) — guarantees crash safety for FlowDB.
//!
//! Design:
//! - Every write (INSERT / UPDATE / DELETE) is appended to the WAL before
//!   being applied to the in-memory table.
//! - On crash recovery, the WAL is replayed in order to reconstruct the
//!   last committed state.
//! - Records are newline-delimited JSON for debuggability.
//! - BEGIN / COMMIT / ROLLBACK fence transaction boundaries.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::FlowError;
use crate::types::Value;

// ── WAL record types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WalRecord {
    /// Start of an explicit transaction.
    Begin { tx_id: u64 },
    /// INSERT: a new row was inserted.
    Insert {
        tx_id: u64,
        table: String,
        row_id: Uuid,
        fields: std::collections::HashMap<String, Value>,
        timestamp: DateTime<Utc>,
    },
    /// UPDATE: specific fields were updated on an existing row.
    Update {
        tx_id: u64,
        table: String,
        row_id: Uuid,
        updates: std::collections::HashMap<String, Value>,
        timestamp: DateTime<Utc>,
    },
    /// DELETE: a row was soft-deleted.
    Delete {
        tx_id: u64,
        table: String,
        row_id: Uuid,
        timestamp: DateTime<Utc>,
    },
    /// Successful commit of a transaction.
    Commit { tx_id: u64, timestamp: DateTime<Utc> },
    /// Transaction was rolled back — discard all records with this tx_id.
    Rollback { tx_id: u64 },
    /// DDL: a table was created.
    CreateTable { tx_id: u64, table: String },
    /// DDL: a table was dropped.
    DropTable { tx_id: u64, table: String },
}

impl WalRecord {
    pub fn tx_id(&self) -> u64 {
        match self {
            WalRecord::Begin { tx_id } => *tx_id,
            WalRecord::Insert { tx_id, .. } => *tx_id,
            WalRecord::Update { tx_id, .. } => *tx_id,
            WalRecord::Delete { tx_id, .. } => *tx_id,
            WalRecord::Commit { tx_id, .. } => *tx_id,
            WalRecord::Rollback { tx_id } => *tx_id,
            WalRecord::CreateTable { tx_id, .. } => *tx_id,
            WalRecord::DropTable { tx_id, .. } => *tx_id,
        }
    }
}

// ── Wal writer ────────────────────────────────────────────────────────────

/// Thread-safe append-only WAL writer.
#[allow(dead_code)]
pub struct WalWriter {
    writer: Mutex<BufWriter<File>>,
    pub path: std::path::PathBuf,
}

impl WalWriter {
    /// Open (or create) the WAL file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FlowError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| FlowError::Wal(format!("cannot open WAL at {}: {e}", path.display())))?;
        info!("WAL opened at `{}`", path.display());
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            path,
        })
    }

    /// Append a single record to the WAL. Calls `flush()` to guarantee persistence.
    pub fn append(&self, record: &WalRecord) -> Result<(), FlowError> {
        let line = serde_json::to_string(record)
            .map_err(|e| FlowError::Wal(format!("serialization failed: {e}")))?;

        let mut w = self.writer.lock().unwrap();
        writeln!(w, "{line}")
            .map_err(|e| FlowError::Wal(format!("write failed: {e}")))?;
        w.flush()
            .map_err(|e| FlowError::Wal(format!("flush failed: {e}")))?;
        debug!("WAL appended op={}", record_op_name(record));
        Ok(())
    }
}

fn record_op_name(r: &WalRecord) -> &'static str {
    match r {
        WalRecord::Begin { .. }       => "BEGIN",
        WalRecord::Insert { .. }      => "INSERT",
        WalRecord::Update { .. }      => "UPDATE",
        WalRecord::Delete { .. }      => "DELETE",
        WalRecord::Commit { .. }      => "COMMIT",
        WalRecord::Rollback { .. }    => "ROLLBACK",
        WalRecord::CreateTable { .. } => "CREATE_TABLE",
        WalRecord::DropTable { .. }   => "DROP_TABLE",
    }
}

// ── Wal reader / recovery ─────────────────────────────────────────────────

/// Read all WAL records from `path` for crash recovery.
pub fn read_wal(path: impl AsRef<Path>) -> Result<Vec<WalRecord>, FlowError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(path)
        .map_err(|e| FlowError::Wal(format!("cannot read WAL: {e}")))?;
    let mut reader = BufReader::new(file);
    let mut contents = String::new();
    reader
        .read_to_string(&mut contents)
        .map_err(|e| FlowError::Wal(format!("WAL read error: {e}")))?;

    let mut records = Vec::new();
    for (line_no, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<WalRecord>(line) {
            Ok(r) => records.push(r),
            Err(e) => {
                warn!("WAL line {line_no}: parse error — skipping: {e}");
            }
        }
    }
    Ok(records)
}

/// Given a complete WAL record list, return only those records belonging to
/// committed transactions (discard incomplete and rolled-back transactions).
pub fn committed_records(records: Vec<WalRecord>) -> Vec<WalRecord> {
    use std::collections::HashSet;

    // Collect all committed and rolled-back tx_ids
    let committed: HashSet<u64> = records
        .iter()
        .filter_map(|r| if matches!(r, WalRecord::Commit { .. }) { Some(r.tx_id()) } else { None })
        .collect();

    let rolled_back: HashSet<u64> = records
        .iter()
        .filter_map(|r| if matches!(r, WalRecord::Rollback { .. }) { Some(r.tx_id()) } else { None })
        .collect();

    records
        .into_iter()
        .filter(|r| {
            // Keep only data records from committed transactions
            let is_data = !matches!(r, WalRecord::Begin { .. } | WalRecord::Commit { .. } | WalRecord::Rollback { .. });
            is_data && committed.contains(&r.tx_id()) && !rolled_back.contains(&r.tx_id())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_and_read_wal() {
        let tmp = NamedTempFile::new().unwrap();
        let wal = WalWriter::open(tmp.path()).unwrap();

        let rec = WalRecord::Insert {
            tx_id: 1,
            table: "users".into(),
            row_id: Uuid::new_v4(),
            fields: HashMap::new(),
            timestamp: Utc::now(),
        };
        wal.append(&rec).unwrap();
        wal.append(&WalRecord::Commit { tx_id: 1, timestamp: Utc::now() }).unwrap();

        let all = read_wal(tmp.path()).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_committed_records_filters_rollback() {
        let records = vec![
            WalRecord::Insert {
                tx_id: 1,
                table: "t".into(),
                row_id: Uuid::new_v4(),
                fields: HashMap::new(),
                timestamp: Utc::now(),
            },
            WalRecord::Rollback { tx_id: 1 },
            WalRecord::Insert {
                tx_id: 2,
                table: "t".into(),
                row_id: Uuid::new_v4(),
                fields: HashMap::new(),
                timestamp: Utc::now(),
            },
            WalRecord::Commit { tx_id: 2, timestamp: Utc::now() },
        ];
        let committed = committed_records(records);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].tx_id(), 2);
    }
}
