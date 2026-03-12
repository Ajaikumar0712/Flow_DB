//! Cluster membership, status, and Raft consensus for FlowDB.
//!
//! Tracks peer node addresses, background-pings them every 10 s, and
//! exposes `CLUSTER STATUS`, `CLUSTER JOIN "<addr>"`, `CLUSTER PART "<addr>"`.
//!
//! Write replication (log shipping to replicas) is wired up via the
//! `replicate()` method called by the TransactionManager on every commit.
pub mod raft;
pub mod shard;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

// ── Peer info ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct PeerStatus {
    pub addr: String,
    pub reachable: bool,
    pub latency_ms: Option<u64>,
}

struct PeerEntry {
    addr: String,
    last_seen: Option<Instant>,
    last_latency_ms: Option<u64>,
}

// ── Registry ──────────────────────────────────────────────────────────────

/// Registry of known cluster peers.  Thread-safe.
pub struct ClusterRegistry {
    peers: Mutex<HashMap<String, PeerEntry>>,
}

impl ClusterRegistry {
    pub fn new() -> Self {
        Self { peers: Mutex::new(HashMap::new()) }
    }

    /// Add a peer. Returns `false` if the address was already known.
    pub fn join(&self, addr: String) -> bool {
        let mut peers = self.peers.lock().unwrap();
        if peers.contains_key(&addr) {
            return false;
        }
        peers.insert(
            addr.clone(),
            PeerEntry { addr, last_seen: None, last_latency_ms: None },
        );
        true
    }

    /// Remove a peer. Returns `false` if the address was not known.
    pub fn part(&self, addr: &str) -> bool {
        self.peers.lock().unwrap().remove(addr).is_some()
    }

    /// Snapshot of all peer statuses.
    pub fn status(&self) -> Vec<PeerStatus> {
        let peers = self.peers.lock().unwrap();
        peers.values().map(|p| PeerStatus {
            addr: p.addr.clone(),
            reachable: p.last_seen
                .map(|t| t.elapsed() < Duration::from_secs(30))
                .unwrap_or(false),
            latency_ms: p.last_latency_ms,
        }).collect()
    }

    /// All registered peer addresses (used by the heartbeat task).
    pub fn peer_addrs(&self) -> Vec<String> {
        self.peers.lock().unwrap().keys().cloned().collect()
    }

    /// Update heartbeat result for a peer after a successful probe.
    pub fn record_heartbeat(&self, addr: &str, latency_ms: u64) {
        if let Some(entry) = self.peers.lock().unwrap().get_mut(addr) {
            entry.last_seen = Some(Instant::now());
            entry.last_latency_ms = Some(latency_ms);
        }
    }

    /// Mark a peer as unreachable (heartbeat failed).
    pub fn record_unreachable(&self, addr: &str) {
        if let Some(entry) = self.peers.lock().unwrap().get_mut(addr) {
            entry.last_latency_ms = None;
            // last_seen is left unchanged so we know when it was last reachable
        }
    }
}

impl Default for ClusterRegistry {
    fn default() -> Self { Self::new() }
}
