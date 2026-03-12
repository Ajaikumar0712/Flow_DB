//! Push-based WATCH subscriptions for FlowDB.
//!
//! When a client issues `WATCH <table> [WHERE <expr>]`, the server registers
//! a subscription in this global registry.  After every PUT, SET, or DEL
//! that touches a matching row, the mutated row is pushed over the open TCP
//! connection as a `{"status":"watch",...}` JSON event.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;

use crate::engine::evaluator::Evaluator;
use crate::sql::ast::Expr;
use crate::types::Row;

pub type WatchId = u64;

static WATCH_COUNTER: AtomicU64 = AtomicU64::new(1);

/// The kind of mutation that triggered the event.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchOp {
    Insert,
    Update,
    Delete,
}

/// A single event pushed to a subscriber.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub watch_id: WatchId,
    pub table: String,
    pub op: WatchOp,
    pub row: Row,
}

// ── Internal entry ────────────────────────────────────────────────────────

struct Entry {
    table: String,
    filter: Option<Expr>,
    sender: UnboundedSender<WatchEvent>,
}

// ── Public registry ───────────────────────────────────────────────────────

/// Global watch registry shared across all server connections.
///
/// Multiple connections can subscribe; any mutation from any connection
/// will notify all matching watchers.
pub struct WatchRegistry {
    entries: Mutex<HashMap<WatchId, Entry>>,
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
    }

    /// Register a new watcher. Returns the assigned subscription ID.
    pub fn subscribe(
        &self,
        table: String,
        filter: Option<Expr>,
        sender: UnboundedSender<WatchEvent>,
    ) -> WatchId {
        let id = WATCH_COUNTER.fetch_add(1, Ordering::SeqCst);
        self.entries.lock().unwrap().insert(id, Entry { table, filter, sender });
        id
    }

    /// Remove a watcher by subscription ID.
    pub fn unsubscribe(&self, id: WatchId) {
        self.entries.lock().unwrap().remove(&id);
    }

    /// Push a mutation event to all watchers that match `table` and whose
    /// filter (if any) is satisfied by `row`.  Dead senders (client
    /// disconnected) are silently pruned.
    pub fn notify(&self, table: &str, op: WatchOp, row: &Row) {
        let mut dead: Vec<WatchId> = Vec::new();

        {
            let entries = self.entries.lock().unwrap();
            for (&id, entry) in entries.iter() {
                if entry.table != table {
                    continue;
                }
                if let Some(ref f) = entry.filter {
                    if !Evaluator::matches_filter(f, row).unwrap_or(false) {
                        continue;
                    }
                }
                let evt = WatchEvent {
                    watch_id: id,
                    table: table.to_string(),
                    op: op.clone(),
                    row: row.clone(),
                };
                if entry.sender.send(evt).is_err() {
                    dead.push(id);
                }
            }
        }

        if !dead.is_empty() {
            let mut entries = self.entries.lock().unwrap();
            for id in dead {
                entries.remove(&id);
            }
        }
    }
}

impl Default for WatchRegistry {
    fn default() -> Self { Self::new() }
}
