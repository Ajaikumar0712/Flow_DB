//! Resource control for FlowDB.
//!
//! Enforces:
//!   - Maximum simultaneous connections
//!   - Per-query row limits
//!   - Default query timeouts
//!   - Disk and memory quota reporting

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::error::FlowError;

// ── Limits ────────────────────────────────────────────────────────────────

/// Configurable resource limits applied server-wide.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum simultaneous TCP connections (0 = unlimited).
    pub max_connections: usize,
    /// Maximum rows returned by a single query (0 = unlimited).
    pub max_rows_per_query: usize,
    /// Default query timeout in milliseconds (0 = no timeout).
    pub default_timeout_ms: u64,
    /// Soft memory cap in MB for in-memory data (0 = unlimited; informational).
    pub max_memory_mb: usize,
    /// Disk quota in MB for WAL + snapshots (0 = unlimited; informational).
    pub disk_quota_mb: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_connections:    0,
            max_rows_per_query: 100_000,
            default_timeout_ms: 30_000,
            max_memory_mb:      0,
            disk_quota_mb:      0,
        }
    }
}

// ── Resource tracker ─────────────────────────────────────────────────────

/// Runtime resource tracker shared across all connections.
pub struct ResourceTracker {
    pub limits: ResourceLimits,
    active_connections: Arc<AtomicUsize>,
    pub total_connections: AtomicU64,
    pub total_queries: AtomicU64,
    pub rejected_queries: AtomicU64,
}

impl ResourceTracker {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            active_connections: Arc::new(AtomicUsize::new(0)),
            total_connections: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            rejected_queries: AtomicU64::new(0),
        }
    }

    /// Try to acquire a connection slot. Returns a `ConnectionGuard` on success.
    /// The slot is released automatically when the guard is dropped.
    pub fn acquire_connection(&self) -> Result<ConnectionGuard, FlowError> {
        let counter = Arc::clone(&self.active_connections);
        let max = self.limits.max_connections;

        if max > 0 {
            // CAS loop to avoid races
            loop {
                let current = counter.load(Ordering::SeqCst);
                if current >= max {
                    self.rejected_queries.fetch_add(1, Ordering::Relaxed);
                    return Err(FlowError::resource(format!(
                        "connection limit ({max}) reached; try again later"
                    )));
                }
                if counter
                    .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
        } else {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        self.total_connections.fetch_add(1, Ordering::Relaxed);
        Ok(ConnectionGuard { counter })
    }

    /// Return the number of currently active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Check whether a result set exceeds the configured row limit.
    pub fn check_row_limit(&self, rows: usize) -> Result<(), FlowError> {
        let max = self.limits.max_rows_per_query;
        if max > 0 && rows > max {
            self.rejected_queries.fetch_add(1, Ordering::Relaxed);
            Err(FlowError::resource(format!(
                "result set ({rows} rows) exceeds max_rows_per_query ({max})"
            )))
        } else {
            Ok(())
        }
    }

    /// Build a query deadline from the limits' default timeout (or an override).
    pub fn query_deadline(&self, override_ms: Option<u64>) -> Option<Instant> {
        let ms = override_ms.unwrap_or(self.limits.default_timeout_ms);
        if ms > 0 {
            Some(Instant::now() + Duration::from_millis(ms))
        } else {
            None
        }
    }

    /// Snapshot of runtime statistics.
    pub fn summary(&self) -> ResourceSummary {
        ResourceSummary {
            active_connections:  self.active_connections.load(Ordering::Relaxed),
            total_connections:   self.total_connections.load(Ordering::Relaxed),
            total_queries:       self.total_queries.load(Ordering::Relaxed),
            rejected_queries:    self.rejected_queries.load(Ordering::Relaxed),
            max_connections:     self.limits.max_connections,
            max_rows_per_query:  self.limits.max_rows_per_query,
            default_timeout_ms:  self.limits.default_timeout_ms,
        }
    }
}

// ── Connection guard ──────────────────────────────────────────────────────

/// RAII guard that decrements the active-connection counter when dropped.
pub struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

// Safe to send across threads (Arc<AtomicUsize> is Send + Sync).
unsafe impl Send for ConnectionGuard {}
unsafe impl Sync for ConnectionGuard {}

// ── Resource summary ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct ResourceSummary {
    pub active_connections: usize,
    pub total_connections: u64,
    pub total_queries: u64,
    pub rejected_queries: u64,
    pub max_connections: usize,
    pub max_rows_per_query: usize,
    pub default_timeout_ms: u64,
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_limit() {
        let mut limits = ResourceLimits::default();
        limits.max_connections = 2;
        let tracker = ResourceTracker::new(limits);

        let g1 = tracker.acquire_connection().expect("1st ok");
        let g2 = tracker.acquire_connection().expect("2nd ok");
        let err = tracker.acquire_connection();
        assert!(err.is_err(), "3rd should fail");

        // Release one slot
        drop(g1);
        let g3 = tracker.acquire_connection().expect("4th ok after release");
        drop(g2);
        drop(g3);

        assert_eq!(tracker.active_connections(), 0);
    }

    #[test]
    fn test_row_limit() {
        let mut limits = ResourceLimits::default();
        limits.max_rows_per_query = 5;
        let tracker = ResourceTracker::new(limits);
        assert!(tracker.check_row_limit(3).is_ok());
        assert!(tracker.check_row_limit(6).is_err());
    }

    #[test]
    fn test_summary() {
        let tracker = ResourceTracker::new(ResourceLimits::default());
        let _g = tracker.acquire_connection().unwrap();
        let s = tracker.summary();
        assert_eq!(s.active_connections, 1);
    }
}
