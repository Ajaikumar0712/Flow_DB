//! AI-native search — local text embeddings via random projection (no network calls).
//!
//! `AI SEARCH <table> "<query>" [LIMIT n]`
//!
//! How it works:
//!   1. Tokenise text into lowercase words.
//!   2. For each word, compute a 64-bit FNV-1a hash, then project the word onto
//!      EMBED_DIM dimensions using independent bit-level sign projections.
//!   3. Accumulate per-dimension sums (bag-of-words), then L2-normalise to a unit vector.
//!   4. Cosine similarity between unit vectors == their dot product.
//!
//! This is equivalent to sign-random-projection hashing (SimHash-family) and captures
//! word-overlap / keyword similarity without any external ML dependencies.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::FlowError;
use crate::storage::table::Database;
use crate::types::Value;

pub const EMBED_DIM: usize = 128;

// ── Embedding ─────────────────────────────────────────────────────────────

/// FNV-1a 64-bit hash.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Mix a dimension index into a hash to generate an independent projection per dimension.
fn fnv1a_mix(h: u64, dim: u64) -> u64 {
    let mixed = h ^ dim.wrapping_mul(0x9e3779b97f4a7c15);
    mixed ^ (mixed >> 30).wrapping_mul(0xbf58476d1ce4e5b9)
}

/// Produce a normalised 128-dimensional embedding for any text string.
/// Returns a unit vector; two texts with identical vocabularies get similarity 1.0.
pub fn embed(text: &str) -> [f32; EMBED_DIM] {
    let mut acc = [0.0f32; EMBED_DIM];
    let mut word_count = 0usize;

    for word in text.split_whitespace() {
        let w = word.to_lowercase();
        // Strip common punctuation
        let w: String = w.chars().filter(|c| c.is_alphanumeric()).collect();
        if w.is_empty() {
            continue;
        }
        word_count += 1;
        let h = fnv1a(w.as_bytes());
        for i in 0..EMBED_DIM {
            let h2 = fnv1a_mix(h, i as u64);
            let sign = if h2 & 1 == 0 { 1.0f32 } else { -1.0f32 };
            acc[i] += sign;
        }
    }

    if word_count == 0 {
        return acc;
    }

    // L2-normalise
    let norm: f32 = acc.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-9 {
        for x in &mut acc {
            *x /= norm;
        }
    }
    acc
}

/// Cosine similarity between two 128-dim unit vectors (= dot product).
pub fn cosine_similarity(a: &[f32; EMBED_DIM], b: &[f32; EMBED_DIM]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
}

// ── AI Search ─────────────────────────────────────────────────────────────

/// Result row: (score, field map).
pub type AiRow = (f32, HashMap<String, Value>);

/// Scan every text-bearing field of every row in `table`, embed them, and return
/// the top-`limit` rows ordered by cosine similarity to the `query` text.
pub fn ai_search(
    db: &Arc<Database>,
    table: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<AiRow>, FlowError> {
    let query_vec = embed(query);

    let tbl = db.get_table(table)?;
    let guard = tbl.read().map_err(|_| FlowError::Io("lock poisoned".into()))?;

    let mut scored: Vec<AiRow> = guard
        .rows
        .values()
        .filter(|r| !r.deleted)
        .filter_map(|row| {
            // Concatenate all text-valued fields into one document
            let doc: String = row
                .fields
                .values()
                .filter_map(|v| match v {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            if doc.is_empty() {
                return None;
            }
            let row_vec = embed(&doc);
            let score = cosine_similarity(&query_vec, &row_vec);
            Some((score, row.fields.clone()))
        })
        .collect();

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    Ok(scored)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embed_is_unit_vector() {
        let v = embed("software engineer in india");
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "expected unit vector, norm={norm}");
    }

    #[test]
    fn test_same_text_similarity_one() {
        let text = "software engineer in india";
        let a = embed(text);
        let b = embed(text);
        let sim = cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-5, "identical texts should have similarity 1.0, got {sim}");
    }

    #[test]
    fn test_empty_text_no_panic() {
        let v = embed("");
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert_eq!(norm, 0.0);
    }

    #[test]
    fn test_similar_texts_higher_than_dissimilar() {
        // Use a repeated shared word to ensure non-zero projections and measurable overlap.
        // "database database database" vs "database storage engine" should be closer
        // than "database database database" vs "cooking recipe pasta", which shares no words.
        let a = embed("database database database query query");
        let b = embed("database database query storage engine");  // 3 of 5 words shared
        let c = embed("cooking recipe pasta baking bread");       // 0 words shared
        let sim_ab = cosine_similarity(&a, &b);
        let sim_ac = cosine_similarity(&a, &c);
        assert!(sim_ab > sim_ac, "sim(a,b)={sim_ab} should be > sim(a,c)={sim_ac}");
    }
}
