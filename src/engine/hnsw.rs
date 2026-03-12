//! HNSW — Hierarchical Navigable Small World graph.
//!
//! Provides approximate nearest-neighbour (ANN) search for dense f32 vectors,
//! replacing the O(n) cosine scan used by SIMILAR.
//!
//! Algorithm parameters:
//! * M          — max connections per node per layer (default 16)
//! * ef_search  — candidate set size during greedy search (default 50)
//! * ef_construction — candidate set size while building (default 200)
//!
//! Each vector is stored in a multi-layer graph.  During build, a random layer
//! is drawn for each node using `floor(-ln(uniform) * mL)` where `mL = 1/ln(M)`.
//! Greedy beam-search from the top layer descends to layer 0, collecting the
//! `ef` nearest candidates, then connecting the incoming node to the best M of
//! them at every layer it participates in.
//!
//! Search follows the same descent to produce the top-K results in sub-linear
//! time compared to a full scan.

use std::collections::{BinaryHeap, HashMap, HashSet};

// ── Distance ──────────────────────────────────────────────────────────────

/// Cosine similarity: 1.0 = identical direction, 0.0 = orthogonal, -1.0 = opposite.
fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() { return 0.0; }
    let dot: f32   = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 { 0.0 } else { dot / (mag_a * mag_b) }
}

// ── Ordered-float wrapper ─────────────────────────────────────────────────

/// Newtype wrapping (score, node_id) for use in a max-heap.
/// f32 NaN is excluded by our construction (cosine_sim never returns NaN for
/// non-empty inputs).
#[derive(Clone, Copy, PartialEq)]
struct OrderedScore(f32, usize);

impl Eq for OrderedScore {}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.1.cmp(&other.1))
    }
}

// ── Node ─────────────────────────────────────────────────────────────────

struct HnswNode {
    /// The actual embedding vector.
    vector: Vec<f32>,
    /// Adjacency lists, one per layer (index 0 = base layer).
    /// `neighbors[layer]` is the list of node IDs this node is connected to.
    neighbors: Vec<Vec<usize>>,
    /// The highest layer this node participates in (its "level").
    level: usize,
}

// ── HNSW index ───────────────────────────────────────────────────────────

/// Per-table HNSW index for one vector column.
pub struct HnswIndex {
    nodes: Vec<HnswNode>,
    /// Index into `nodes` for the current entry point (highest-level node).
    entry_point: Option<usize>,
    /// Maximum layer index seen.
    max_layer: usize,
    // hyper-parameters
    m: usize,              // max neighbours per layer (16 typical)
    m0: usize,             // max neighbours on layer 0 (2*M typical)
    ef_construction: usize, // candidate pool size during build
    ml: f64,               // normalisation factor = 1.0 / ln(M)
}

impl HnswIndex {
    pub fn new(m: usize, ef_construction: usize) -> Self {
        let m = m.max(2);
        Self {
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            m,
            m0: m * 2,
            ef_construction,
            ml: 1.0 / (m as f64).ln(),
        }
    }

    /// Default parameters suitable for most workloads.
    pub fn default() -> Self { Self::new(16, 200) }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize { self.nodes.len() }

    // ── Insert ───────────────────────────────────────────────────────────

    /// Add a vector to the index, tagging it with `payload` (e.g. a row UUID).
    /// The `payload` is the position the caller can map back to a row.
    pub fn insert(&mut self, vector: Vec<f32>) -> usize {
        let new_id = self.nodes.len();
        let level  = self.random_level();
        let layers = level + 1;

        self.nodes.push(HnswNode {
            vector,
            neighbors: vec![Vec::new(); layers],
            level,
        });

        let Some(ep) = self.entry_point else {
            // First node — becomes the entry point.
            self.entry_point = Some(new_id);
            self.max_layer   = level;
            return new_id;
        };

        let mut cur_ep = ep;

        // Phase 1: greedy descent from max_layer down to level+1 (1 neighbour).
        for lc in (level + 1..=self.max_layer).rev() {
            cur_ep = self.greedy_search_layer(new_id, cur_ep, 1, lc)[0];
        }

        // Phase 2: descend from min(level, max_layer) to 0, building connections.
        for lc in (0..=level.min(self.max_layer)).rev() {
            let m_at_layer = if lc == 0 { self.m0 } else { self.m };
            let candidates = self.search_layer(new_id, cur_ep, self.ef_construction, lc);
            let neighbours = self.select_neighbours(&candidates, m_at_layer);

            self.nodes[new_id].neighbors[lc] = neighbours.clone();
            for &nb in &neighbours {
                let nb_m = if lc == 0 { self.m0 } else { self.m };
                let nb_node = &mut self.nodes[nb];
                if nb_node.neighbors.len() <= lc {
                    nb_node.neighbors.resize(lc + 1, Vec::new());
                }
                nb_node.neighbors[lc].push(new_id);
                if nb_node.neighbors[lc].len() > nb_m {
                    // Trim back to nb_m — keep the closest ones.
                    let vec_clone = nb_node.vector.clone();
                    let nb_nbrs = nb_node.neighbors[lc].clone();
                    let mut scored: Vec<(f32, usize)> = nb_nbrs
                        .iter()
                        .map(|&x| (cosine_sim(&vec_clone, &self.nodes[x].vector), x))
                        .collect();
                    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                    self.nodes[nb].neighbors[lc] =
                        scored.into_iter().take(nb_m).map(|(_, id)| id).collect();
                }
            }
            // Update entry point for next layer descent.
            if !candidates.is_empty() {
                cur_ep = candidates[0].1;
            }
        }

        if level > self.max_layer {
            self.entry_point = Some(new_id);
            self.max_layer   = level;
        }

        new_id
    }

    // ── Search ───────────────────────────────────────────────────────────

    /// Return the top-`k` node indices sorted by descending cosine similarity.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(f32, usize)> {
        let k = k.max(1);
        let ef = ef.max(k);

        let Some(mut ep) = self.entry_point else { return Vec::new(); };

        // Phase 1: greedy descent to layer 1.
        for lc in (1..=self.max_layer).rev() {
            ep = self.greedy_search_layer_query(query, ep, 1, lc)[0];
        }

        // Phase 2: beam search on layer 0 with ef candidates.
        let mut results = self.search_layer_query(query, ep, ef, 0);
        results.truncate(k);
        results
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    /// Draw a random level using the formula `floor(-ln(uniform) * mL)`.
    fn random_level(&self) -> usize {
        // Simple deterministic pseudo-random based on current node count.
        // A real impl would use a proper RNG; using linear congruential here
        // to avoid pulling in rand crate just for this formula.
        let seed = self.nodes.len() as u64;
        let u = lcg_uniform(seed);
        (-u.ln() * self.ml) as usize
    }

    /// Greedy single-step search: given entry point, walk greedily toward query node.
    fn greedy_search_layer(&self, query_id: usize, ep: usize, ef: usize, layer: usize) -> Vec<usize> {
        let query_vec = self.nodes[query_id].vector.clone();
        let results = self.search_layer_query(&query_vec, ep, ef, layer);
        results.into_iter().map(|(_, id)| id).collect()
    }

    /// Greedy single-step search toward a raw query vector.
    fn greedy_search_layer_query(&self, query: &[f32], ep: usize, ef: usize, layer: usize) -> Vec<usize> {
        let results = self.search_layer_query(query, ep, ef, layer);
        results.into_iter().map(|(_, id)| id).collect()
    }

    /// Beam search on `layer` starting from `ep`, collecting `ef` candidates.
    /// Returns `(score_desc, node_id)` pairs sorted by descending cosine sim.
    fn search_layer(&self, query_id: usize, ep: usize, ef: usize, layer: usize) -> Vec<(f32, usize)> {
        let query_vec = self.nodes[query_id].vector.clone();
        self.search_layer_query(&query_vec, ep, ef, layer)
    }

    fn search_layer_query(&self, query: &[f32], ep: usize, ef: usize, layer: usize) -> Vec<(f32, usize)> {
        let ep_score = cosine_sim(query, &self.nodes[ep].vector);
        // candidates = max-heap (highest score first)
        let mut candidates: BinaryHeap<OrderedScore> = BinaryHeap::new();
        // W = working set of best ef candidates (min-heap to evict worst quickly)
        let mut w: BinaryHeap<std::cmp::Reverse<OrderedScore>> = BinaryHeap::new();
        let mut visited: HashSet<usize> = HashSet::new();

        candidates.push(OrderedScore(ep_score, ep));
        w.push(std::cmp::Reverse(OrderedScore(ep_score, ep)));
        visited.insert(ep);

        while let Some(OrderedScore(c_score, c_id)) = candidates.pop() {
            let worst_in_w = w.peek().map(|r| r.0.0).unwrap_or(f32::NEG_INFINITY);
            if c_score < worst_in_w {
                break; // all remaining candidates are farther than our current best
            }

            let nbrs = if layer < self.nodes[c_id].neighbors.len() {
                self.nodes[c_id].neighbors[layer].clone()
            } else {
                Vec::new()
            };

            for nb in nbrs {
                if visited.insert(nb) {
                    let nb_score = cosine_sim(query, &self.nodes[nb].vector);
                    let worst = w.peek().map(|r| r.0.0).unwrap_or(f32::NEG_INFINITY);
                    if w.len() < ef || nb_score > worst {
                        candidates.push(OrderedScore(nb_score, nb));
                        w.push(std::cmp::Reverse(OrderedScore(nb_score, nb)));
                        if w.len() > ef {
                            w.pop(); // discard the worst
                        }
                    }
                }
            }
        }

        // Collect and sort descending
        let mut results: Vec<(f32, usize)> = w
            .into_iter()
            .map(|std::cmp::Reverse(OrderedScore(s, id))| (s, id))
            .collect();
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Simple greedy neighbour selection — take the `m` closest.
    fn select_neighbours(&self, candidates: &[(f32, usize)], m: usize) -> Vec<usize> {
        candidates.iter().take(m).map(|(_, id)| *id).collect()
    }
}

// ── LCG pseudo-random ─────────────────────────────────────────────────────

/// Simple LCG pseudo-random float in (0, 1). Not suitable for crypto.
/// Good enough for level-drawing in HNSW.
fn lcg_uniform(seed: u64) -> f64 {
    const A: u64 = 6_364_136_223_846_793_005;
    const C: u64 = 1_442_695_040_888_963_407;
    let x = A.wrapping_mul(seed).wrapping_add(C);
    // Map to (0, 1) — avoid exact 0 to prevent -ln(0) = inf.
    (x >> 11) as f64 / (1u64 << 53) as f64 + 1e-10
}

// ── Per-table HNSW registry ───────────────────────────────────────────────

/// Holds HNSW indexes keyed by `(table_name, column_name)`.
///
/// Wrapped in the `Database` so the executor can call it during SIMILAR queries.
pub struct HnswRegistry {
    indexes: HashMap<(String, String), HnswIndex>,
    /// Maps `(table, column, node_id)` → row UUID for result lookup.
    node_row_map: HashMap<(String, String, usize), uuid::Uuid>,
}

impl HnswRegistry {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            node_row_map: HashMap::new(),
        }
    }

    /// Add or replace an index for a `(table, column)` pair.
    pub fn build(
        &mut self,
        table: &str,
        column: &str,
        rows: &[(uuid::Uuid, Vec<f32>)], // (row_id, vector) pairs
        m: usize,
        ef_construction: usize,
    ) {
        let mut idx = HnswIndex::new(m, ef_construction);
        for (row_id, vec) in rows {
            let node_id = idx.insert(vec.clone());
            self.node_row_map.insert(
                (table.to_string(), column.to_string(), node_id),
                *row_id,
            );
        }
        self.indexes.insert((table.to_string(), column.to_string()), idx);
    }

    /// Insert a single vector into an existing index.
    /// Creates the index on first call if none exists.
    pub fn insert(
        &mut self,
        table: &str,
        column: &str,
        row_id: uuid::Uuid,
        vector: Vec<f32>,
    ) {
        let idx = self.indexes
            .entry((table.to_string(), column.to_string()))
            .or_insert_with(|| HnswIndex::default());
        let node_id = idx.insert(vector);
        self.node_row_map.insert(
            (table.to_string(), column.to_string(), node_id),
            row_id,
        );
    }

    /// Search for top-k nearest neighbours.
    /// Returns `(score, row_uuid)` pairs sorted by descending similarity.
    pub fn search(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Vec<(f32, uuid::Uuid)> {
        let key = (table.to_string(), column.to_string());
        let Some(idx) = self.indexes.get(&key) else { return Vec::new(); };
        idx.search(query, k, ef)
            .into_iter()
            .filter_map(|(score, node_id)| {
                self.node_row_map.get(&(table.to_string(), column.to_string(), node_id))
                    .copied()
                    .map(|row_id| (score, row_id))
            })
            .collect()
    }

    pub fn has_index(&self, table: &str, column: &str) -> bool {
        self.indexes.contains_key(&(table.to_string(), column.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_single_vector() {
        let mut idx = HnswIndex::default();
        idx.insert(vec![1.0, 0.0, 0.0]);
        let res = idx.search(&[1.0, 0.0, 0.0], 1, 50);
        assert_eq!(res.len(), 1);
        assert!((res[0].0 - 1.0).abs() < 1e-5, "identical vector → score ≈ 1.0");
    }

    #[test]
    fn test_hnsw_top_k_order() {
        let mut idx = HnswIndex::default();
        // Insert 5 vectors
        idx.insert(vec![1.0, 0.0]);
        idx.insert(vec![0.9, 0.1]);
        idx.insert(vec![0.0, 1.0]);
        idx.insert(vec![-1.0, 0.0]);
        idx.insert(vec![0.0, -1.0]);

        // Query closest to [1.0, 0.0]
        let res = idx.search(&[1.0, 0.0], 2, 50);
        assert_eq!(res.len(), 2);
        // The first result should be the identical vector
        assert!(res[0].0 >= res[1].0, "results must be in descending similarity order");
        assert!((res[0].0 - 1.0).abs() < 1e-4);
    }

    #[test]
    fn test_hnsw_registry_roundtrip() {
        let mut reg = HnswRegistry::new();
        let id1 = uuid::Uuid::new_v4();
        let id2 = uuid::Uuid::new_v4();
        reg.insert("t", "emb", id1, vec![1.0, 0.0]);
        reg.insert("t", "emb", id2, vec![0.0, 1.0]);

        let res = reg.search("t", "emb", &[1.0, 0.0], 1, 50);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].1, id1);
    }
}
