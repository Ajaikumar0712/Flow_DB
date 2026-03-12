//! Transaction manager — wraps the executor with BEGIN/COMMIT/ROLLBACK semantics.
//!
//! FlowDB uses a simple single-writer, auto-commit-or-explicit transaction model:
//!   - Without BEGIN: every statement is its own transaction (auto-commit).
//!   - With BEGIN … COMMIT: all statements run inside that transaction.
//!   - With BEGIN … ROLLBACK: all buffered WAL records are discarded.
//!
//! Concurrency model: one active transaction at a time per connection (connection is
//! represented by a session). The executor itself is protected by RwLock internally.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::engine::executor::{Executor, QueryResult};
use crate::error::FlowError;
use crate::sql::ast::Stmt;
use crate::wal::{WalRecord, WalWriter};

// ── Global transaction ID counter ─────────────────────────────────────────

static TX_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_tx_id() -> u64 {
    TX_COUNTER.fetch_add(1, Ordering::SeqCst)
}

// ── Transaction state ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum TxState {
    /// No open transaction; each operation auto-commits.
    AutoCommit,
    /// Inside an explicit BEGIN … COMMIT/ROLLBACK block.
    Active { tx_id: u64 },
}

// ── TransactionManager ────────────────────────────────────────────────────

/// Sits above the `Executor` and adds WAL-backed transaction support.
pub struct TransactionManager {
    executor: Arc<Executor>,
    wal: Arc<WalWriter>,
    state: TxState,
    /// Buffered WAL records for the current explicit transaction.
    buffer: Vec<WalRecord>,
}

impl TransactionManager {
    pub fn new(executor: Arc<Executor>, wal: Arc<WalWriter>) -> Self {
        Self {
            executor,
            wal,
            state: TxState::AutoCommit,
            buffer: Vec::new(),
        }
    }

    /// Execute a single statement, handling transaction control transparently.
    pub fn execute(&mut self, stmt: Stmt) -> Result<QueryResult, FlowError> {
        match stmt {
            Stmt::Begin => self.begin(),
            Stmt::Commit => self.commit(),
            Stmt::Rollback => self.rollback(),
            other => self.execute_dml(other),
        }
    }

    // ── Transaction control ───────────────────────────────────────────────

    fn begin(&mut self) -> Result<QueryResult, FlowError> {
        if matches!(self.state, TxState::Active { .. }) {
            return Err(FlowError::TxAlreadyActive);
        }
        let tx_id = next_tx_id();
        self.state = TxState::Active { tx_id };
        self.buffer.clear();
        let rec = WalRecord::Begin { tx_id };
        // BEGIN is written immediately even for explicit transactions
        self.wal.append(&rec)?;
        info!("transaction {tx_id} started");
        Ok(QueryResult::ok(format!("BEGIN (tx_id={tx_id})"), std::time::Duration::ZERO))
    }

    fn commit(&mut self) -> Result<QueryResult, FlowError> {
        match self.state {
            TxState::AutoCommit => {
                // Nothing to commit; auto-commit is already done.
                Ok(QueryResult::ok("COMMIT (auto-commit mode)", std::time::Duration::ZERO))
            }
            TxState::Active { tx_id } => {
                // Flush all buffered records to WAL
                let buf_len = self.buffer.len() as u64;
                for rec in &self.buffer {
                    self.wal.append(rec)?;
                }
                self.wal.append(&WalRecord::Commit {
                    tx_id,
                    timestamp: chrono::Utc::now(),
                })?;
                self.executor.metrics.wal_records_written.fetch_add(buf_len + 1, Ordering::Relaxed);
                self.buffer.clear();
                self.state = TxState::AutoCommit;
                self.executor.metrics.transactions_committed.fetch_add(1, Ordering::Relaxed);
                info!("transaction {tx_id} committed");
                Ok(QueryResult::ok(format!("COMMIT (tx_id={tx_id})"), std::time::Duration::ZERO))
            }
        }
    }

    fn rollback(&mut self) -> Result<QueryResult, FlowError> {
        match self.state {
            TxState::AutoCommit => {
                warn!("ROLLBACK called outside of an active transaction");
                Ok(QueryResult::ok("ROLLBACK (nothing to roll back)", std::time::Duration::ZERO))
            }
            TxState::Active { tx_id } => {
                // Write ROLLBACK to WAL so recovery knows to discard these records
                self.wal.append(&WalRecord::Rollback { tx_id })?;
                self.buffer.clear();
                self.state = TxState::AutoCommit;
                self.executor.metrics.transactions_rolled_back.fetch_add(1, Ordering::Relaxed);
                // NOTE: In Lite mode, in-memory changes already applied cannot be
                // automatically un-applied here. This is a known Lite limitation —
                // full MVCC rollback support is in Phase 7 of the full plan.
                warn!("ROLLBACK tx_id={tx_id}: in-memory state may reflect partial writes in Lite mode");
                Ok(QueryResult::ok(format!("ROLLBACK (tx_id={tx_id})"), std::time::Duration::ZERO))
            }
        }
    }

    // ── DML execution with WAL logging ────────────────────────────────────

    fn execute_dml(&mut self, stmt: Stmt) -> Result<QueryResult, FlowError> {
        let tx_id = match self.state {
            TxState::AutoCommit => {
                // Auto-commit: assign a fresh tx_id, write BEGIN, execute, write COMMIT
                let id = next_tx_id();
                self.wal.append(&WalRecord::Begin { tx_id: id })?;
                id
            }
            TxState::Active { tx_id } => tx_id,
        };

        // Build and buffer the WAL record(s) for this statement
        let wal_records = wal_records_for_stmt(&stmt, tx_id);

        // Execute the statement
        let result = self.executor.execute(stmt)?;

        // Write WAL records after successful execution
        // Count WAL records written: data records + COMMIT+BEGIN (auto-commit) or buffered
        let is_auto = matches!(self.state, TxState::AutoCommit);
        let wal_written = if is_auto { wal_records.len() as u64 + 2 } else { wal_records.len() as u64 };

        match self.state {
            TxState::AutoCommit => {
                // Write all records + COMMIT immediately
                for rec in &wal_records {
                    self.wal.append(rec)?;
                }
                self.wal.append(&WalRecord::Commit {
                    tx_id,
                    timestamp: chrono::Utc::now(),
                })?;
            }
            TxState::Active { .. } => {
                // Buffer records; commit will flush them later
                self.buffer.extend(wal_records);
            }
        }

        self.executor.metrics.wal_records_written.fetch_add(wal_written, Ordering::Relaxed);
        debug!("tx_id={tx_id} statement executed successfully");
        Ok(result)
    }
}

/// Convert a statement into the WAL records it would produce.
/// For Lite, we log the statement as a simple meta-record.
/// (Full recovery replays the statement, not raw byte diffs.)
fn wal_records_for_stmt(stmt: &Stmt, tx_id: u64) -> Vec<WalRecord> {
    // For Lite we create a lightweight record for each mutation type.
    // Real row-level diffs are added in the full version.
    match stmt {
        Stmt::Put { table, .. } => vec![WalRecord::Insert {
            tx_id,
            table: table.clone(),
            row_id: Uuid::new_v4(), // placeholder; full version uses actual row ID
            fields: Default::default(),
            timestamp: chrono::Utc::now(),
        }],
        Stmt::Set { table, .. } => vec![WalRecord::Update {
            tx_id,
            table: table.clone(),
            row_id: Uuid::nil(),
            updates: Default::default(),
            timestamp: chrono::Utc::now(),
        }],
        Stmt::Del { table, .. } => vec![WalRecord::Delete {
            tx_id,
            table: table.clone(),
            row_id: Uuid::nil(),
            timestamp: chrono::Utc::now(),
        }],
        Stmt::MakeTable { name, .. } => vec![WalRecord::CreateTable {
            tx_id,
            table: name.clone(),
        }],
        Stmt::DropTable { name } => vec![WalRecord::DropTable {
            tx_id,
            table: name.clone(),
        }],
        _ => vec![], // DDL indexes, reads, etc. — no WAL record needed
    }
}
