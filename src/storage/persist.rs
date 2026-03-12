//! Disk persistence for FlowDB.
//!
//! Design
//!  - **Catalog** (`<data_dir>/flowdb.catalog`): compact JSON file that stores
//!    every table's schema and which non-PK columns have indexes.  Re-written
//!    atomically after every DDL statement.
//!  - **Snapshots** (`<data_dir>/<table>.snap`): newline-delimited JSON rows for
//!    one table.  Written by the `CHECKPOINT` command (or on clean shutdown).
//!  - **Startup recovery**: load catalog → recreate empty tables & indexes →
//!    replay snapshot rows.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::FlowError;
use crate::storage::table::Database;
use crate::types::Row;

// ── Catalog ───────────────────────────────────────────────────────────────

/// Persisted metadata for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub schema: crate::types::TableSchema,
    /// Non-PK columns with explicit indexes (PK indexes are auto-created by Table::new).
    pub extra_indexes: Vec<String>,
}

/// The full on-disk catalog — serialized to `<data_dir>/flowdb.catalog`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub tables: HashMap<String, CatalogEntry>,
}

impl Catalog {
    fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("flowdb.catalog")
    }

    /// Load catalog from disk. Returns an empty default when the file is absent.
    pub fn load(data_dir: &Path) -> Result<Self, FlowError> {
        let path = Self::path(data_dir);
        if !path.exists() {
            return Ok(Catalog::default());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text)
            .map_err(|e| FlowError::Io(format!("catalog parse: {e}")))
    }

    /// Build a fresh catalog snapshot from the live `Database`.
    pub fn build(db: &Database) -> Self {
        let guard = db.tables.read().unwrap();
        let tables = guard
            .iter()
            .map(|(name, tbl_lock)| {
                let tbl = tbl_lock.read().unwrap();
                let pk_cols: std::collections::HashSet<String> = tbl
                    .schema
                    .columns
                    .iter()
                    .filter(|c| c.primary_key)
                    .map(|c| c.name.clone())
                    .collect();
                let extra_indexes = tbl
                    .indexes
                    .keys()
                    .filter(|col| !pk_cols.contains(*col))
                    .cloned()
                    .collect();
                (name.clone(), CatalogEntry { schema: tbl.schema.clone(), extra_indexes })
            })
            .collect();
        Catalog { tables }
    }

    /// Atomically write to `<data_dir>/flowdb.catalog`.
    pub fn save(&self, data_dir: &Path) -> Result<(), FlowError> {
        fs::create_dir_all(data_dir)?;
        let path = Self::path(data_dir);
        let tmp = path.with_extension("catalog.tmp");
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| FlowError::Io(format!("catalog serialize: {e}")))?;
        fs::write(&tmp, text)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

// ── Snapshots ─────────────────────────────────────────────────────────────

fn snap_path(data_dir: &Path, table: &str) -> PathBuf {
    data_dir.join(format!("{table}.snap"))
}

/// Flush every live row in `db` to `<data_dir>/<table>.snap`, then re-save
/// the catalog. Returns the total number of rows written across all tables.
pub fn checkpoint(db: &Database, data_dir: &Path) -> Result<usize, FlowError> {
    fs::create_dir_all(data_dir)?;
    let mut total = 0usize;

    let guard = db.tables.read().unwrap();
    for (name, tbl_lock) in guard.iter() {
        let tbl = tbl_lock.read().unwrap();
        let path = snap_path(data_dir, name);
        let tmp = path.with_extension("snap.tmp");
        let file = fs::File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        let mut count = 0usize;
        for row in tbl.rows.values().filter(|r| !r.deleted) {
            let line = serde_json::to_string(row)
                .map_err(|e| FlowError::Io(format!("snapshot serialize row: {e}")))?;
            writeln!(w, "{line}")?;
            count += 1;
        }
        w.flush()?;
        drop(w);
        fs::rename(&tmp, &path)?;
        total += count;
        info!("checkpoint: `{name}` → {count} rows");
    }
    drop(guard);

    Catalog::build(db).save(data_dir)?;
    info!("checkpoint complete: {total} rows total");
    Ok(total)
}

/// Reconstruct the full database state from the catalog + snapshots at startup.
pub fn recover(db: &Database, data_dir: &Path) -> Result<(), FlowError> {
    let catalog = Catalog::load(data_dir)?;
    if catalog.tables.is_empty() {
        info!("no catalog found — starting with empty database");
        return Ok(());
    }

    let mut table_count = 0usize;
    let mut row_count = 0usize;

    for (name, entry) in &catalog.tables {
        db.create_table(entry.schema.clone())?;

        let tbl_lock = db.get_table(name)?;
        {
            let mut tbl = tbl_lock.write().unwrap();
            for col in &entry.extra_indexes {
                let _ = tbl.create_index(col); // ignore "already exists"
            }
        }

        let snap = snap_path(data_dir, name);
        if snap.exists() {
            let file = fs::File::open(&snap)?;
            let reader = BufReader::new(file);
            let mut tbl = tbl_lock.write().unwrap();
            for (i, line_res) in reader.lines().enumerate() {
                let line = line_res?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Row>(line) {
                    Ok(row) => {
                        tbl.replay_row(row);
                        row_count += 1;
                    }
                    Err(e) => {
                        tracing::warn!("snapshot `{name}` line {i}: corrupt row — {e}");
                    }
                }
            }
        }

        table_count += 1;
        info!("recovered table `{name}`");
    }

    info!("recovery complete: {table_count} tables, {row_count} rows");
    Ok(())
}
