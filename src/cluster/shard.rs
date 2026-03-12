//! Shard management for FlowDB distributed mode.
//!
//! Implements hash-based sharding with configurable replication:
//!   - Assign a table to N shards spread across cluster nodes
//!   - Route write/read keys to the responsible primary node
//!   - Track replica nodes for each shard

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::FlowError;

// ── Shard types ───────────────────────────────────────────────────────────

/// A single logical shard owning a contiguous hash-range of rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub id: u32,
    pub table: String,
    /// Address of the node that owns writes to this shard.
    pub primary_node: String,
    /// Addresses of nodes that hold read-only replicas.
    pub replica_nodes: Vec<String>,
    /// Inclusive lower bound of the hash range.
    pub range_start: u64,
    /// Exclusive upper bound of the hash range (u64::MAX for the last shard).
    pub range_end: u64,
}

/// Shard assignment for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSharding {
    pub table: String,
    pub shard_count: u32,
    pub replication_factor: u32,
    pub shards: Vec<Shard>,
}

impl TableSharding {
    /// Find the shard whose hash range contains `key_hash`.
    pub fn find_shard(&self, key_hash: u64) -> Option<&Shard> {
        self.shards.iter().find(|s| {
            key_hash >= s.range_start
                && (s.range_end == u64::MAX || key_hash < s.range_end)
        })
    }

    /// Return all unique node addresses involved in this table's sharding.
    pub fn all_nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self
            .shards
            .iter()
            .flat_map(|s| {
                let mut n = vec![s.primary_node.clone()];
                n.extend_from_slice(&s.replica_nodes);
                n
            })
            .collect();
        nodes.dedup();
        nodes
    }
}

// ── Shard manager ─────────────────────────────────────────────────────────

/// Thread-safe manager for all table shard assignments.
pub struct ShardManager {
    assignments: RwLock<HashMap<String, TableSharding>>,
}

impl ShardManager {
    pub fn new() -> Self {
        Self { assignments: RwLock::new(HashMap::new()) }
    }

    /// Create or replace the shard assignment for `table`.
    ///
    /// Shards are distributed across `nodes` in round-robin order.
    /// `replication_factor` additional replicas are assigned from subsequent nodes.
    pub fn create_shards(
        &self,
        table: &str,
        shard_count: u32,
        replication_factor: u32,
        nodes: &[String],
    ) -> Result<(), FlowError> {
        if nodes.is_empty() {
            return Err(FlowError::internal("no nodes available for sharding"));
        }
        if shard_count == 0 {
            return Err(FlowError::internal("shard_count must be at least 1"));
        }

        let range_per_shard = u64::MAX / shard_count as u64;
        let mut shards = Vec::with_capacity(shard_count as usize);

        for i in 0..shard_count {
            let range_start = i as u64 * range_per_shard;
            let range_end = if i == shard_count - 1 {
                u64::MAX
            } else {
                range_start + range_per_shard
            };

            let primary_idx = i as usize % nodes.len();
            let primary_node = nodes[primary_idx].clone();

            let replica_nodes: Vec<String> = (1..=(replication_factor as usize).min(nodes.len() - 1))
                .map(|r| nodes[(primary_idx + r) % nodes.len()].clone())
                .collect();

            shards.push(Shard {
                id: i,
                table: table.to_string(),
                primary_node,
                replica_nodes,
                range_start,
                range_end,
            });
        }

        self.assignments.write().unwrap().insert(
            table.to_string(),
            TableSharding {
                table: table.to_string(),
                shard_count,
                replication_factor,
                shards,
            },
        );
        Ok(())
    }

    /// Route a key to its primary node address.
    /// Returns `None` if the table has no shard assignment.
    pub fn route(&self, table: &str, key: &str) -> Option<String> {
        let hash = fnv1a(key);
        let assignments = self.assignments.read().unwrap();
        let sharding = assignments.get(table)?;
        let shard = sharding.find_shard(hash)?;
        Some(shard.primary_node.clone())
    }

    /// Return sharding status for all assigned tables.
    pub fn status(&self) -> Vec<TableSharding> {
        let mut list: Vec<TableSharding> =
            self.assignments.read().unwrap().values().cloned().collect();
        list.sort_by(|a, b| a.table.cmp(&b.table));
        list
    }

    /// Return sharding details for one table.
    pub fn table_status(&self, table: &str) -> Option<TableSharding> {
        self.assignments.read().unwrap().get(table).cloned()
    }

    /// Remove shard assignment for a table.
    pub fn remove(&self, table: &str) -> bool {
        self.assignments.write().unwrap().remove(table).is_some()
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── FNV-1a hash ───────────────────────────────────────────────────────────

/// FNV-1a hash of a byte string (64-bit).
fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_route() {
        let mgr = ShardManager::new();
        let nodes = vec!["node1:7878".into(), "node2:7878".into()];
        mgr.create_shards("orders", 4, 1, &nodes).unwrap();

        // Every key should route somewhere
        for key in &["key1", "key2", "key3", "key4", "key5"] {
            assert!(mgr.route("orders", key).is_some());
        }
    }

    #[test]
    fn test_status() {
        let mgr = ShardManager::new();
        let nodes = vec!["n1".into()];
        mgr.create_shards("t1", 2, 0, &nodes).unwrap();
        let status = mgr.status();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].shard_count, 2);
    }

    #[test]
    fn test_remove() {
        let mgr = ShardManager::new();
        let nodes = vec!["n1".into()];
        mgr.create_shards("t1", 1, 0, &nodes).unwrap();
        assert!(mgr.remove("t1"));
        assert!(!mgr.remove("t1")); // second remove returns false
    }
}
