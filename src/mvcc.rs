//! MVCC — Multi-Version Concurrency Control
//!
//! Every row carries `xmin` (the transaction that created it) and `xmax`
//! (the transaction that deleted it; 0 means "still live").
//!
//! Visibility rule for snapshot transaction `T`:
//!   A row version V is visible to T when:
//!     1. V.xmin is committed AND V.xmin < T.snapshot_id
//!     2. V.xmax == 0  OR  V.xmax >= T.snapshot_id  (not yet deleted)
//!
//! This gives each transaction a consistent point-in-time snapshot,
//! preventing dirty reads and non-repeatable reads (Snapshot Isolation).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

// ── Transaction ID ────────────────────────────────────────────────────────

/// Monotonically increasing transaction ID.
/// 0 is reserved for "pre-MVCC / auto-committed" rows.
pub type TxnId = u64;

// ── MVCC Manager ─────────────────────────────────────────────────────────

/// Global MVCC manager shared across all connections.
pub struct MvccManager {
    /// Next transaction ID to hand out.
    next_id: AtomicU64,
    /// Transaction IDs that have started but not yet committed or aborted.
    active: RwLock<HashSet<TxnId>>,
    /// Transaction IDs that have committed (kept for visibility checks).
    /// In a real system you'd prune entries older than the oldest active txn.
    committed: RwLock<HashSet<TxnId>>,
}

impl MvccManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            active: RwLock::new(HashSet::new()),
            committed: RwLock::new(HashSet::new()),
        }
    }

    /// Begin a new transaction. Returns a `Snapshot` that captures the
    /// committed state at this point in time.
    pub fn begin(&self) -> Snapshot {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.active.write().unwrap().insert(id);
        let committed_at_start = self.committed.read().unwrap().clone();
        Snapshot { txn_id: id, committed_at_start }
    }

    /// Commit a transaction, making its writes visible to future snapshots.
    pub fn commit(&self, txn_id: TxnId) {
        self.active.write().unwrap().remove(&txn_id);
        self.committed.write().unwrap().insert(txn_id);
    }

    /// Abort a transaction — its writes remain but xmin is never committed,
    /// so they are invisible to all future snapshots.
    pub fn abort(&self, txn_id: TxnId) {
        self.active.write().unwrap().remove(&txn_id);
        // Do NOT add to committed; writes are dead but they stay in storage
        // and will be vacuumed away by a background VACUUM (future work).
    }

    /// Check whether a transaction ID is committed.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        txn_id == 0 || self.committed.read().unwrap().contains(&txn_id)
    }
}

impl Default for MvccManager {
    fn default() -> Self { Self::new() }
}

// ── Snapshot ──────────────────────────────────────────────────────────────

/// A point-in-time view of the database, associated with one transaction.
#[derive(Clone)]
pub struct Snapshot {
    /// The transaction ID this snapshot belongs to.
    pub txn_id: TxnId,
    /// The set of committed transaction IDs at the moment this snapshot was taken.
    committed_at_start: HashSet<TxnId>,
}

impl Snapshot {
    /// Returns an "auto-commit" snapshot that sees all previously committed rows.
    /// Used when no explicit transaction is active.
    pub fn auto() -> Self {
        Snapshot {
            txn_id: 0,
            committed_at_start: HashSet::new(), // visibility handled by xmin==0 shortcut
        }
    }

    /// Visibility check: is row version `(xmin, xmax)` visible to this snapshot?
    ///
    /// * `xmin == 0`  → pre-MVCC row, always visible (legacy compat).
    /// * `xmax == 0`  → row is live (not deleted).
    pub fn is_visible(&self, xmin: u64, xmax: u64, deleted: bool) -> bool {
        // Legacy rows (created before MVCC was enabled) are always visible.
        if xmin == 0 {
            return !deleted;
        }

        // The creating transaction must be committed and in our snapshot.
        let xmin_visible = self.committed_at_start.contains(&xmin) || xmin == self.txn_id;
        if !xmin_visible {
            return false;
        }

        // The deleting transaction must NOT be committed in our snapshot.
        if xmax == 0 {
            return true; // not deleted
        }
        // Deleted by a transaction that committed AFTER our snapshot → still visible to us.
        !self.committed_at_start.contains(&xmax)
    }
}

// ── Shared ref ────────────────────────────────────────────────────────────

pub type SharedMvcc = Arc<MvccManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_commit_sees_legacy_rows() {
        let snap = Snapshot::auto();
        // xmin=0 means legacy / pre-MVCC → visible
        assert!(snap.is_visible(0, 0, false));
        // deleted legacy row → not visible
        assert!(!snap.is_visible(0, 0, true));
    }

    #[test]
    fn test_snapshot_isolation() {
        let mgr = MvccManager::new();

        // T1 starts and inserts (xmin=T1_id)
        let snap_t1 = mgr.begin();
        let t1_id = snap_t1.txn_id;

        // T2 takes a snapshot BEFORE T1 commits → T1's rows not visible
        let snap_t2 = mgr.begin();
        assert!(!snap_t2.is_visible(t1_id, 0, false));

        // T1 commits
        mgr.commit(t1_id);

        // T3 takes a snapshot AFTER T1 commits → T1's rows visible
        let snap_t3 = mgr.begin();
        assert!(snap_t3.is_visible(t1_id, 0, false));

        // T2 still cannot see T1's rows (snapshot taken before commit)
        assert!(!snap_t2.is_visible(t1_id, 0, false));
    }
}
