//! Monitoring & metrics for FlowDB.
//!
//! Tracks:
//! - Total queries executed
//! - Query latency (min / max / avg / p95 / p99)
//! - Error count per error type
//! - Currently running queries (for SHOW RUNNING QUERIES / KILL QUERY)
//! - Table row counts (snapshot)
//!
//! All fields are updated lock-free where possible (atomics) or behind a
//! lightweight Mutex for histogram data.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::FlowError;
use crate::types::Value;

// ── Running query registry ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningQuery {
    pub id: u64,
    pub query_text: String,
    pub started_at: DateTime<Utc>,
    pub elapsed_ms: u64,
}

// ── Latency histogram (approximate) ──────────────────────────────────────

/// Simple online latency tracker — stores last N samples for percentile estimation.
struct LatencyTracker {
    samples: Vec<u64>, // ms
    /// Keep at most this many samples in memory
    cap: usize,
    total: u64,
    count: u64,
    min: u64,
    max: u64,
}

impl LatencyTracker {
    fn new(cap: usize) -> Self {
        Self {
            samples: Vec::with_capacity(cap),
            cap,
            total: 0,
            count: 0,
            min: u64::MAX,
            max: 0,
        }
    }

    fn record(&mut self, ms: u64) {
        self.total += ms;
        self.count += 1;
        if ms < self.min { self.min = ms; }
        if ms > self.max { self.max = ms; }
        if self.samples.len() >= self.cap {
            // Drop oldest sample (ring-buffer style — keeps it simple)
            self.samples.remove(0);
        }
        self.samples.push(ms);
    }

    fn avg_ms(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.total as f64 / self.count as f64 }
    }

    fn percentile(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() { return 0; }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

// ── Metrics ───────────────────────────────────────────────────────────────

pub struct Metrics {
    // ── Counters (atomic — no lock needed) ──────────────────────────────
    pub queries_total: AtomicU64,
    pub queries_errored: AtomicU64,
    pub rows_inserted: AtomicU64,
    pub rows_updated: AtomicU64,
    pub rows_deleted: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_rolled_back: AtomicU64,
    pub wal_records_written: AtomicU64,

    // ── Latency tracker (mutex — histogram needs mutation) ───────────────
    latency: Mutex<LatencyTracker>,

    // ── Error breakdown ───────────────────────────────────────────────────
    error_counts: Mutex<HashMap<String, u64>>,

    // ── Running queries ───────────────────────────────────────────────────
    running: Mutex<HashMap<u64, RunningQuery>>,
    query_id_counter: AtomicU64,

    // ── Server start time ─────────────────────────────────────────────────
    pub started_at: DateTime<Utc>,
}

impl Metrics {
    pub fn new() -> Self {
        info!("FlowDB metrics subsystem initialized");
        Self {
            queries_total: AtomicU64::new(0),
            queries_errored: AtomicU64::new(0),
            rows_inserted: AtomicU64::new(0),
            rows_updated: AtomicU64::new(0),
            rows_deleted: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_rolled_back: AtomicU64::new(0),
            wal_records_written: AtomicU64::new(0),
            latency: Mutex::new(LatencyTracker::new(10_000)),
            error_counts: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            query_id_counter: AtomicU64::new(1),
            started_at: Utc::now(),
        }
    }

    // ── Query lifecycle ───────────────────────────────────────────────────

    /// Register a query as running. Returns its unique query ID.
    pub fn start_query(&self, query_text: impl Into<String>) -> u64 {
        let id = self.query_id_counter.fetch_add(1, Ordering::SeqCst);
        let entry = RunningQuery {
            id,
            query_text: query_text.into(),
            started_at: Utc::now(),
            elapsed_ms: 0,
        };
        self.running.lock().unwrap().insert(id, entry);
        id
    }

    /// Mark a query as finished and record its latency.
    pub fn finish_query(&self, query_id: u64, elapsed: Duration) {
        self.running.lock().unwrap().remove(&query_id);
        let ms = elapsed.as_millis() as u64;
        self.latency.lock().unwrap().record(ms);
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        debug!("query {query_id} finished in {ms}ms");
    }

    /// Record a completed query (simplified — no running-query tracking).
    pub fn record_query(&self, elapsed: Duration) {
        let ms = elapsed.as_millis() as u64;
        self.latency.lock().unwrap().record(ms);
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error, categorized by error type name.
    pub fn record_error(&self, err: &FlowError) {
        self.queries_errored.fetch_add(1, Ordering::Relaxed);
        let kind = error_kind(err);
        *self.error_counts.lock().unwrap().entry(kind).or_default() += 1;
    }

    // ── Admin ─────────────────────────────────────────────────────────────

    /// Return running queries as table rows for `SHOW RUNNING QUERIES`.
    pub fn running_queries_snapshot(&self) -> Vec<Vec<Value>> {
        let map = self.running.lock().unwrap();
        map.values()
            .map(|q| {
                let elapsed = (Utc::now() - q.started_at).num_milliseconds().max(0) as u64;
                vec![
                    Value::Int(q.id as i64),
                    Value::Text(q.query_text.clone()),
                    Value::Int(elapsed as i64),
                ]
            })
            .collect()
    }

    /// Kill (cancel) a running query by ID.
    pub fn kill_query(&self, id: u64) -> Result<(), FlowError> {
        let mut map = self.running.lock().unwrap();
        if map.remove(&id).is_some() {
            info!("query {id} killed by admin");
            Ok(())
        } else {
            Err(FlowError::QueryNotFound(id))
        }
    }

    // ── Summary snapshot ───────────────────────────────────────────────────

    /// Return a human-readable metrics snapshot as a formatted string.
    pub fn summary(&self) -> String {
        let uptime_secs = (Utc::now() - self.started_at).num_seconds();
        let mut lat = self.latency.lock().unwrap();
        let p95 = lat.percentile(95.0);
        let p99 = lat.percentile(99.0);
        let avg = lat.avg_ms();
        let min = if lat.count == 0 { 0 } else { lat.min };
        let max = lat.max;

        let errors = self.error_counts.lock().unwrap().clone();
        let running_count = self.running.lock().unwrap().len();

        format!(
            r#"== FlowDB — Metrics Snapshot ==
Uptime               : {uptime_secs}s
Queries total        : {}
Queries errored      : {}
  Error breakdown    : {errors:?}
Rows inserted        : {}
Rows updated         : {}
Rows deleted         : {}
Transactions committed  : {}
Transactions rolled back: {}
WAL records written  : {}
Latency (ms)
  avg  : {avg:.2}
  min  : {min}
  max  : {max}
  p95  : {p95}
  p99  : {p99}
Running queries      : {running_count}
"#,
            self.queries_total.load(Ordering::Relaxed),
            self.queries_errored.load(Ordering::Relaxed),
            self.rows_inserted.load(Ordering::Relaxed),
            self.rows_updated.load(Ordering::Relaxed),
            self.rows_deleted.load(Ordering::Relaxed),
            self.transactions_committed.load(Ordering::Relaxed),
            self.transactions_rolled_back.load(Ordering::Relaxed),
            self.wal_records_written.load(Ordering::Relaxed),
        )
    }
}

impl Default for Metrics {
    fn default() -> Self { Self::new() }
}

fn error_kind(err: &FlowError) -> String {
    match err {
        FlowError::Parse(_)            => "ParseError",
        FlowError::Type(_)             => "TypeError",
        FlowError::TableNotFound(_)    => "TableNotFound",
        FlowError::TableAlreadyExists(_) => "TableAlreadyExists",
        FlowError::ColumnNotFound(_, _) => "ColumnNotFound",
        FlowError::RowNotFound         => "RowNotFound",
        FlowError::IndexAlreadyExists { .. } => "IndexAlreadyExists",
        FlowError::NoActiveTx          => "NoActiveTx",
        FlowError::TxAlreadyActive     => "TxAlreadyActive",
        FlowError::TxNotFound(_)       => "TxNotFound",
        FlowError::Wal(_)              => "WalError",
        FlowError::Io(_)               => "IoError",
        FlowError::Timeout { .. }      => "Timeout",
        FlowError::ResultTooLarge(_)   => "ResultTooLarge",
        FlowError::QueryNotFound(_)    => "QueryNotFound",
        FlowError::Internal(_)         => "InternalError",
        FlowError::Auth(_)             => "AuthError",
        FlowError::ResourceLimit(_)    => "ResourceLimitError",
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_summary() {
        let m = Metrics::new();
        m.record_query(Duration::from_millis(10));
        m.record_query(Duration::from_millis(50));
        m.record_query(Duration::from_millis(100));
        let s = m.summary();
        assert!(s.contains("Queries total        : 3"));
        assert!(s.contains("avg"));
    }

    #[test]
    fn test_running_queries() {
        let m = Metrics::new();
        let id = m.start_query("GET users");
        assert_eq!(m.running_queries_snapshot().len(), 1);
        m.finish_query(id, Duration::from_millis(5));
        assert_eq!(m.running_queries_snapshot().len(), 0);
    }

    #[test]
    fn test_kill_query() {
        let m = Metrics::new();
        let id = m.start_query("GET orders");
        m.kill_query(id).unwrap();
        assert!(m.kill_query(id).is_err());
    }

    #[test]
    fn test_percentile() {
        let mut t = LatencyTracker::new(100);
        for ms in 1..=100u64 { t.record(ms); }
        let p95 = t.percentile(95.0);
        assert!(p95 >= 94 && p95 <= 96);
    }
}
