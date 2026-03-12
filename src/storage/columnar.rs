//! Columnar storage engine for FlowDB.
//!
//! Provides an optional column-oriented in-memory representation for
//! analytical workloads ("wide table" scans, aggregations).
//!
//! Supported encodings:
//!   - **Raw**        — values stored as-is (integers, floats).
//!   - **Dictionary** — unique values de-duplicated; rows store u32 indices.
//!   - **RunLength**  — (value, count) pairs for repeated sequences.
//!   - **Bitmap**     — bit-packed booleans (64 bits per u64 word).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::Value;

// ── Column encoding variants ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnEncoding {
    /// Values stored verbatim.
    Raw(Vec<Value>),

    /// Dictionary-encoded: unique values + per-row index into the dict.
    Dictionary {
        dict:  Vec<Value>,
        codes: Vec<u32>,
    },

    /// Run-length encoded: (value, run_length) pairs.
    RunLength(Vec<RleRun>),

    /// Bit-packed booleans (64 rows per u64 word).
    Bitmap(Vec<u64>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RleRun {
    pub value: Value,
    pub count: u32,
}

// ── Compressed column ─────────────────────────────────────────────────────

/// A single column in compressed form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedColumn {
    pub name:        String,
    pub encoding:    ColumnEncoding,
    /// `true` at index `i` means row `i` is NULL regardless of the encoding.
    pub null_bitmap: Vec<bool>,
}

impl CompressedColumn {
    /// Decompress this column back to a `Vec<Value>`, restoring NULLs.
    pub fn decode(&self) -> Vec<Value> {
        let raw = match &self.encoding {
            ColumnEncoding::Raw(v) => v.clone(),

            ColumnEncoding::Dictionary { dict, codes } => {
                codes
                    .iter()
                    .map(|&i| dict.get(i as usize).cloned().unwrap_or(Value::Null))
                    .collect()
            }

            ColumnEncoding::RunLength(runs) => {
                let mut out = Vec::new();
                for run in runs {
                    for _ in 0..run.count {
                        out.push(run.value.clone());
                    }
                }
                out
            }

            ColumnEncoding::Bitmap(words) => {
                let mut out = Vec::new();
                for (i, word) in words.iter().enumerate() {
                    for bit in 0..64usize {
                        if i * 64 + bit >= self.null_bitmap.len() {
                            break;
                        }
                        out.push(Value::Bool((word >> bit) & 1 == 1));
                    }
                }
                out
            }
        };

        // Apply null overlay
        raw.into_iter()
            .enumerate()
            .map(|(i, v)| {
                if self.null_bitmap.get(i).copied().unwrap_or(false) {
                    Value::Null
                } else {
                    v
                }
            })
            .collect()
    }

    /// Estimated size in bytes of the encoded representation.
    pub fn encoded_bytes(&self) -> usize {
        match &self.encoding {
            ColumnEncoding::Raw(v)     => v.len() * 16,
            ColumnEncoding::Dictionary { dict, codes } => dict.len() * 16 + codes.len() * 4,
            ColumnEncoding::RunLength(runs)            => runs.len() * 20,
            ColumnEncoding::Bitmap(words)              => words.len() * 8,
        }
    }
}

// ── Column store ─────────────────────────────────────────────────────────

/// A column-oriented store for a snapshot of a table.
/// Enables efficient analytical scans and aggregations.
pub struct ColumnStore {
    pub table_name: String,
    pub row_count:  usize,
    pub columns:    HashMap<String, CompressedColumn>,
}

impl ColumnStore {
    /// Build a `ColumnStore` from a slice of rows.
    ///
    /// Automatically selects the best encoding per column.
    pub fn from_rows(
        table_name: &str,
        rows: &[crate::types::Row],
        col_names: &[String],
    ) -> Self {
        let row_count = rows.len();
        let mut columns = HashMap::new();

        for col_name in col_names {
            let values: Vec<Value> =
                rows.iter().map(|r| r.get_or_null(col_name)).collect();
            let compressed = auto_compress(col_name, values);
            columns.insert(col_name.clone(), compressed);
        }

        Self { table_name: table_name.to_string(), row_count, columns }
    }

    /// Decode a column back to a `Vec<Value>`.
    pub fn decode_column(&self, col: &str) -> Option<Vec<Value>> {
        self.columns.get(col).map(|c| c.decode())
    }

    /// Reconstruct all rows as `HashMap<String,Value>`.
    pub fn to_row_maps(&self) -> Vec<HashMap<String, Value>> {
        if self.row_count == 0 {
            return Vec::new();
        }
        let mut rows: Vec<HashMap<String, Value>> =
            vec![HashMap::new(); self.row_count];

        for (name, col) in &self.columns {
            let values = col.decode();
            for (i, v) in values.into_iter().enumerate() {
                if i < rows.len() {
                    rows[i].insert(name.clone(), v);
                }
            }
        }
        rows
    }

    /// Compute a column aggregate without full decompression where possible.
    pub fn aggregate(&self, col: &str, agg: Agg) -> Value {
        let values = match self.decode_column(col) {
            Some(v) => v,
            None    => return Value::Null,
        };
        match agg {
            Agg::Count => Value::Int(values.iter().filter(|v| !matches!(v, Value::Null)).count() as i64),
            Agg::Sum   => sum_values(&values),
            Agg::Avg   => avg_values(&values),
            Agg::Min   => min_value(&values),
            Agg::Max   => max_value(&values),
        }
    }

    /// Compression statistics.
    pub fn stats(&self) -> StoreStats {
        let compressed_bytes: usize =
            self.columns.values().map(|c| c.encoded_bytes()).sum();
        // Rough raw estimate (16 bytes per value)
        let raw_bytes = self.row_count * self.columns.len() * 16;
        let ratio = if raw_bytes > 0 {
            compressed_bytes as f64 / raw_bytes as f64
        } else {
            1.0
        };
        StoreStats {
            row_count:         self.row_count,
            column_count:      self.columns.len(),
            compressed_bytes,
            raw_bytes,
            compression_ratio: ratio,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Agg {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreStats {
    pub row_count:         usize,
    pub column_count:      usize,
    pub compressed_bytes:  usize,
    pub raw_bytes:         usize,
    pub compression_ratio: f64,
}

// ── Auto-compression ──────────────────────────────────────────────────────

fn auto_compress(col_name: &str, values: Vec<Value>) -> CompressedColumn {
    let null_bitmap: Vec<bool> = values.iter().map(|v| matches!(v, Value::Null)).collect();

    // Boolean → bitmap
    if values.iter().all(|v| matches!(v, Value::Bool(_) | Value::Null)) {
        let words = bitmap_encode(&values);
        return CompressedColumn {
            name:        col_name.to_string(),
            encoding:    ColumnEncoding::Bitmap(words),
            null_bitmap,
        };
    }

    // Text / low-cardinality → dictionary
    if values.iter().any(|v| matches!(v, Value::Text(_))) {
        let enc = dict_encode(values.clone());
        return CompressedColumn {
            name:        col_name.to_string(),
            encoding:    enc,
            null_bitmap,
        };
    }

    // Try RLE — use it if it shrinks to ≤80% of raw size
    if values.len() > 4 {
        let rle = rle_encode(&values);
        if rle.len() <= values.len() * 4 / 5 {
            return CompressedColumn {
                name:        col_name.to_string(),
                encoding:    ColumnEncoding::RunLength(rle),
                null_bitmap,
            };
        }
    }

    // Default: raw
    CompressedColumn {
        name:        col_name.to_string(),
        encoding:    ColumnEncoding::Raw(values),
        null_bitmap,
    }
}

fn dict_encode(values: Vec<Value>) -> ColumnEncoding {
    let mut dict: Vec<Value>    = Vec::new();
    let mut index: HashMap<String, u32> = HashMap::new();
    let mut codes: Vec<u32>     = Vec::with_capacity(values.len());

    for v in values {
        let key = format!("{v:?}");
        let code = *index.entry(key).or_insert_with(|| {
            let idx = dict.len() as u32;
            dict.push(v.clone());
            idx
        });
        codes.push(code);
    }
    ColumnEncoding::Dictionary { dict, codes }
}

fn rle_encode(values: &[Value]) -> Vec<RleRun> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut current = values[0].clone();
    let mut count   = 1u32;

    for v in &values[1..] {
        let same = match (&current, v) {
            (Value::Int(a),   Value::Int(b))   => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Text(a),  Value::Text(b))  => a == b,
            (Value::Bool(a),  Value::Bool(b))  => a == b,
            (Value::Null,     Value::Null)      => true,
            _                                   => false,
        };
        if same {
            count += 1;
        } else {
            runs.push(RleRun { value: current, count });
            current = v.clone();
            count   = 1;
        }
    }
    runs.push(RleRun { value: current, count });
    runs
}

fn bitmap_encode(values: &[Value]) -> Vec<u64> {
    let word_count = (values.len() + 63) / 64;
    let mut words  = vec![0u64; word_count];
    for (i, v) in values.iter().enumerate() {
        if let Value::Bool(true) = v {
            words[i / 64] |= 1u64 << (i % 64);
        }
    }
    words
}

// ── Aggregate helpers ─────────────────────────────────────────────────────

fn sum_values(values: &[Value]) -> Value {
    let mut sum = 0f64;
    for v in values {
        match v {
            Value::Int(n)   => sum += *n as f64,
            Value::Float(f) => sum += f,
            _               => {}
        }
    }
    Value::Float(sum)
}

fn avg_values(values: &[Value]) -> Value {
    let non_null: Vec<&Value> = values.iter().filter(|v| !matches!(v, Value::Null)).collect();
    if non_null.is_empty() {
        return Value::Null;
    }
    match sum_values(&non_null.into_iter().cloned().collect::<Vec<_>>()) {
        Value::Float(s) => Value::Float(s / values.len() as f64),
        other           => other,
    }
}

fn min_value(values: &[Value]) -> Value {
    values
        .iter()
        .filter(|v| !matches!(v, Value::Null))
        .cloned()
        .reduce(|a, b| if a.partial_cmp_val(&b) == Some(std::cmp::Ordering::Less) { a } else { b })
        .unwrap_or(Value::Null)
}

fn max_value(values: &[Value]) -> Value {
    values
        .iter()
        .filter(|v| !matches!(v, Value::Null))
        .cloned()
        .reduce(|a, b| if a.partial_cmp_val(&b) == Some(std::cmp::Ordering::Greater) { a } else { b })
        .unwrap_or(Value::Null)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Row;

    fn make_rows() -> Vec<Row> {
        let mut rows = Vec::new();
        for i in 0..10i64 {
            let mut fields = HashMap::new();
            fields.insert("id".into(), Value::Int(i));
            fields.insert("active".into(), Value::Bool(i % 2 == 0));
            fields.insert("name".into(), Value::Text(format!("user_{i}")));
            rows.push(Row::new(fields));
        }
        rows
    }

    #[test]
    fn test_round_trip() {
        let rows = make_rows();
        let cols: Vec<String> = vec!["id".into(), "active".into(), "name".into()];
        let store = ColumnStore::from_rows("test", &rows, &cols);

        let ids = store.decode_column("id").unwrap();
        assert_eq!(ids.len(), 10);
        assert_eq!(ids[0], Value::Int(0));
        assert_eq!(ids[9], Value::Int(9));
    }

    #[test]
    fn test_agg_sum() {
        let rows = make_rows();
        let cols = vec!["id".into()];
        let store = ColumnStore::from_rows("test", &rows, &cols);
        // sum of 0..9 = 45
        match store.aggregate("id", Agg::Sum) {
            Value::Float(s) => assert!((s - 45.0).abs() < 0.001),
            _ => panic!("expected float"),
        }
    }

    #[test]
    fn test_dict_decode() {
        let input = vec![
            Value::Text("a".into()),
            Value::Text("b".into()),
            Value::Text("a".into()),
        ];
        let enc = dict_encode(input.clone());
        let col = CompressedColumn {
            name:        "x".into(),
            encoding:    enc,
            null_bitmap: vec![false; 3],
        };
        assert_eq!(col.decode(), input);
    }
}
