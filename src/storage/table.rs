use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use tracing::{debug, info};
use uuid::Uuid;

use crate::error::FlowError;
use crate::types::{Row, TableSchema, Value};

// ── Table statistics (for cost-based optimizer) ───────────────────────────

/// One bucket in a column histogram.
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Lower bound (inclusive).
    pub min: crate::types::Value,
    /// Upper bound (inclusive).
    pub max: crate::types::Value,
    /// Number of rows whose value falls in [min, max].
    pub count: usize,
    /// Distinct values in this bucket.
    pub ndv: usize,
}

/// Equi-depth histogram for a single column.
#[derive(Debug, Clone)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub null_count: usize,
}

impl Histogram {
    /// Estimate the fraction of rows satisfying `col OP value`.
    pub fn selectivity(&self, op: &crate::engine::planner::RangeOp, value: &crate::types::Value) -> f64 {
        use crate::engine::planner::RangeOp;
        let total: usize = self.buckets.iter().map(|b| b.count).sum();
        if total == 0 { return 1.0; }

        let matching: usize = self.buckets.iter()
            .filter(|b| {
                match op {
                    RangeOp::Eq => {
                        b.min.partial_cmp_val(value)
                            .map(|o| o != std::cmp::Ordering::Greater)
                            .unwrap_or(false)
                        && b.max.partial_cmp_val(value)
                            .map(|o| o != std::cmp::Ordering::Less)
                            .unwrap_or(false)
                    }
                    RangeOp::Lt => b.min.partial_cmp_val(value)
                        .map(|o| o == std::cmp::Ordering::Less)
                        .unwrap_or(true),
                    RangeOp::Le => b.min.partial_cmp_val(value)
                        .map(|o| o != std::cmp::Ordering::Greater)
                        .unwrap_or(true),
                    RangeOp::Gt => b.max.partial_cmp_val(value)
                        .map(|o| o == std::cmp::Ordering::Greater)
                        .unwrap_or(true),
                    RangeOp::Ge => b.max.partial_cmp_val(value)
                        .map(|o| o != std::cmp::Ordering::Less)
                        .unwrap_or(true),
                }
            })
            .map(|b| b.count)
            .sum();

        (matching as f64 / total as f64).clamp(0.001, 1.0)
    }
}

/// Lightweight statistics snapshot produced by `Table::statistics()`.
/// Consumed by `Planner::choose_scan()` to estimate scan cost.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Number of live (non-deleted) rows.
    pub row_count: usize,
    /// Number of distinct values per column (NDV).
    /// Selectivity of an equality predicate ≈ 1 / ndv.
    pub column_ndv: HashMap<String, usize>,
    /// Column-level histograms for refined selectivity estimates.
    pub histograms: HashMap<String, Histogram>,
}

impl TableStats {
    /// Estimated fraction of rows that survive an equality filter on `column`.
    /// Returns 1.0 (full scan) when the column is not tracked.
    pub fn selectivity_eq(&self, column: &str) -> f64 {
        if self.row_count == 0 { return 1.0; }
        // Try histogram first
        if let Some(hist) = self.histograms.get(column) {
            // Use NDV of each bucket for a refined estimate — return 1/total_ndv
            let total_ndv: usize = hist.buckets.iter().map(|b| b.ndv).sum::<usize>().max(1);
            return (1.0 / total_ndv as f64).clamp(0.001, 1.0);
        }
        let ndv = self.column_ndv.get(column).copied().unwrap_or(1);
        1.0 / ndv as f64
    }

    /// Estimated rows surviving an equality filter.
    pub fn estimated_rows_eq(&self, column: &str) -> usize {
        ((self.row_count as f64 * self.selectivity_eq(column)).ceil() as usize).max(1)
    }

    /// Selectivity for a range predicate using histogram when available.
    pub fn selectivity_range(
        &self,
        column: &str,
        op: &crate::engine::planner::RangeOp,
        value: &crate::types::Value,
    ) -> f64 {
        if self.row_count == 0 { return 1.0; }
        if let Some(hist) = self.histograms.get(column) {
            return hist.selectivity(op, value);
        }
        0.33 // default: assume range selects ~1/3 of rows
    }
}

// ── Index entry — maps a Value to a set of row IDs ────────────────────────

type RowSet = Vec<Uuid>;

/// A simple in-memory B-Tree based index for a single column.
pub struct ColumnIndex {
    /// Sorted map: column value → list of row IDs with that value.
    pub tree: BTreeMap<IndexKey, RowSet>,
}

/// Comparable wrapper around `Value` (only indexable types).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Int(i64),
    Text(String),
    Bool(u8), // false=0, true=1
    Null,
}

impl IndexKey {
    pub fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Int(n)  => Some(IndexKey::Int(*n)),
            Value::Text(s) => Some(IndexKey::Text(s.clone())),
            Value::Bool(b) => Some(IndexKey::Bool(*b as u8)),
            Value::Null    => Some(IndexKey::Null),
            _ => None,
        }
    }
}

impl ColumnIndex {
    pub fn new() -> Self {
        Self { tree: BTreeMap::new() }
    }

    pub fn insert(&mut self, key: IndexKey, row_id: Uuid) {
        self.tree.entry(key).or_default().push(row_id);
    }

    pub fn remove(&mut self, key: &IndexKey, row_id: &Uuid) {
        if let Some(ids) = self.tree.get_mut(key) {
            ids.retain(|id| id != row_id);
            if ids.is_empty() {
                self.tree.remove(key);
            }
        }
    }

    pub fn lookup(&self, key: &IndexKey) -> &[Uuid] {
        self.tree.get(key).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

// ── Table — the core in-memory storage unit ───────────────────────────────

/// An in-memory table holding rows, schema, and column indexes.
pub struct Table {
    pub schema: TableSchema,
    /// Primary storage: row ID → Row.
    pub rows: HashMap<Uuid, Row>,
    /// Column indexes: column name → ColumnIndex.
    pub indexes: HashMap<String, ColumnIndex>,
    /// Stats
    pub total_inserts: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        let mut indexes = HashMap::new();
        // Auto-create an index on every primary key column.
        for col in &schema.columns {
            if col.primary_key {
                info!("auto-creating primary key index on `{}.{}`", schema.name, col.name);
                indexes.insert(col.name.clone(), ColumnIndex::new());
            }
        }
        Self {
            schema,
            rows: HashMap::new(),
            indexes,
            total_inserts: 0,
            total_updates: 0,
            total_deletes: 0,
        }
    }

    // ── Insert ───────────────────────────────────────────────────────────

    pub fn insert(&mut self, fields: HashMap<String, Value>) -> Result<Row, FlowError> {
        // Validate against schema
        self.schema.validate_fields(&fields).map_err(FlowError::type_err)?;

        // Upsert: if a primary-key column exists in `fields` and a matching row
        // already exists, update that row in place instead of inserting a new one.
        let pk_cols: Vec<&str> = self.schema.columns.iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();

        if !pk_cols.is_empty() {
            // Find an existing live row whose PK values all match.
            let existing_id: Option<uuid::Uuid> = self.rows.values()
                .filter(|r| !r.deleted)
                .find(|r| {
                    pk_cols.iter().all(|pk| {
                        fields.get(*pk).map_or(false, |v| r.fields.get(*pk) == Some(v))
                    })
                })
                .map(|r| r.id);

            if let Some(id) = existing_id {
                // Remove old index entries, apply all new fields, re-index.
                for (col_name, idx) in &mut self.indexes {
                    if let Some(row) = self.rows.get(&id) {
                        if let Some(old_val) = row.fields.get(col_name.as_str()) {
                            if let Some(key) = IndexKey::from_value(old_val) {
                                idx.remove(&key, &id);
                            }
                        }
                    }
                }
                let row = self.rows.get_mut(&id).unwrap();
                for (k, v) in fields {
                    row.fields.insert(k, v);
                }
                row.updated_at = chrono::Utc::now();
                for (col_name, idx) in &mut self.indexes {
                    if let Some(val) = row.fields.get(col_name.as_str()) {
                        if let Some(key) = IndexKey::from_value(val) {
                            idx.insert(key, id);
                        }
                    }
                }
                self.total_updates += 1;
                debug!("upserted row {} in `{}`", id, self.schema.name);
                return Ok(self.rows[&id].clone());
            }
        }

        // Normal insert path.
        let row = Row::new(fields.clone());
        let row_id = row.id;

        // Update all existing indexes
        for (col_name, idx) in &mut self.indexes {
            if let Some(val) = fields.get(col_name.as_str()) {
                if let Some(key) = IndexKey::from_value(val) {
                    idx.insert(key, row_id);
                }
            }
        }

        self.rows.insert(row_id, row.clone());
        self.total_inserts += 1;
        debug!("inserted row {} into `{}`", row_id, self.schema.name);
        Ok(row)
    }

    // ── Update ───────────────────────────────────────────────────────────

    /// Apply a partial field update to all rows matching `predicate`.
    /// Returns the number of rows updated.
    pub fn update(
        &mut self,
        updates: HashMap<String, Value>,
        predicate: impl Fn(&Row) -> bool,
    ) -> usize {
        let ids_to_update: Vec<Uuid> = self
            .rows
            .values()
            .filter(|r| !r.deleted && predicate(r))
            .map(|r| r.id)
            .collect();

        let count = ids_to_update.len();
        for id in ids_to_update {
            if let Some(row) = self.rows.get_mut(&id) {
                // Remove old index entries
                for (col_name, idx) in &mut self.indexes {
                    if let Some(old_val) = row.fields.get(col_name.as_str()) {
                        if let Some(key) = IndexKey::from_value(old_val) {
                            idx.remove(&key, &id);
                        }
                    }
                }
                // Apply field updates
                for (k, v) in &updates {
                    row.fields.insert(k.clone(), v.clone());
                }
                row.updated_at = chrono::Utc::now();
                // Re-insert updated index entries
                for (col_name, idx) in &mut self.indexes {
                    if let Some(new_val) = row.fields.get(col_name.as_str()) {
                        if let Some(key) = IndexKey::from_value(new_val) {
                            idx.insert(key, id);
                        }
                    }
                }
            }
        }
        if count > 0 {
            self.total_updates += count as u64;
            debug!("updated {count} rows in `{}`", self.schema.name);
        }
        count
    }

    // ── Delete ───────────────────────────────────────────────────────────

    /// Soft-delete all rows matching `predicate`. Returns count.
    pub fn delete(&mut self, predicate: impl Fn(&Row) -> bool) -> usize {
        let ids: Vec<Uuid> = self
            .rows
            .values()
            .filter(|r| !r.deleted && predicate(r))
            .map(|r| r.id)
            .collect();

        let count = ids.len();
        for id in &ids {
            if let Some(row) = self.rows.get_mut(id) {
                // Remove from all indexes
                for (col_name, idx) in &mut self.indexes {
                    if let Some(val) = row.fields.get(col_name.as_str()) {
                        if let Some(key) = IndexKey::from_value(val) {
                            idx.remove(&key, id);
                        }
                    }
                }
                row.deleted = true;
                row.updated_at = chrono::Utc::now();
            }
        }
        if count > 0 {
            self.total_deletes += count as u64;
            debug!("soft-deleted {count} rows in `{}`", self.schema.name);
        }
        count
    }

    // ── Scan ─────────────────────────────────────────────────────────────

    /// Full table scan returning live (non-deleted) rows matching predicate.
    pub fn scan(&self, predicate: impl Fn(&Row) -> bool) -> Vec<&Row> {
        self.rows
            .values()
            .filter(|r| !r.deleted && predicate(r))
            .collect()
    }

    /// Return all live rows (no filter).
    pub fn scan_all(&self) -> Vec<&Row> {
        self.rows.values().filter(|r| !r.deleted).collect()
    }

    // ── Index management ─────────────────────────────────────────────────

    /// Create a new index on `column`. Builds the index over all existing rows.
    pub fn create_index(&mut self, column: &str) -> Result<(), FlowError> {
        if self.indexes.contains_key(column) {
            return Err(FlowError::IndexAlreadyExists {
                table: self.schema.name.clone(),
                column: column.to_string(),
            });
        }

        let mut idx = ColumnIndex::new();
        for row in self.rows.values().filter(|r| !r.deleted) {
            if let Some(val) = row.fields.get(column) {
                if let Some(key) = IndexKey::from_value(val) {
                    idx.insert(key, row.id);
                }
            }
        }
        let entry_count = idx.tree.values().map(|v| v.len()).sum::<usize>();
        self.indexes.insert(column.to_string(), idx);
        info!(
            "created index on `{}.{}` covering {} entries",
            self.schema.name, column, entry_count
        );
        Ok(())
    }

    // ── Index-based lookups ──────────────────────────────────────────────

    /// Exact equality lookup via a named index. Returns matching row IDs.
    pub fn index_lookup(&self, column: &str, key: &IndexKey) -> Option<Vec<uuid::Uuid>> {
        self.indexes.get(column).map(|idx| idx.lookup(key).to_vec())
    }

    /// Range scan via a named index using `std::ops::Bound` semantics.
    /// Returns row IDs in B-tree order.
    pub fn index_range_scan(
        &self,
        column: &str,
        lower: std::ops::Bound<&IndexKey>,
        upper: std::ops::Bound<&IndexKey>,
    ) -> Option<Vec<uuid::Uuid>> {
        self.indexes.get(column).map(|idx| {
            idx.tree
                .range((lower, upper))
                .flat_map(|(_, ids)| ids.iter().copied())
                .collect()
        })
    }

    /// Fetch live rows by a slice of row IDs (skips soft-deleted rows).
    pub fn get_rows_by_ids(&self, ids: &[uuid::Uuid]) -> Vec<&Row> {
        ids.iter()
            .filter_map(|id| self.rows.get(id))
            .filter(|r| !r.deleted)
            .collect()
    }

    /// Replay a row from a snapshot directly into storage, bypassing schema
    /// validation. Rebuilds all indexes for the row. Used during crash recovery.
    pub fn replay_row(&mut self, row: Row) {
        let row_id = row.id;
        for (col_name, idx) in &mut self.indexes {
            if let Some(val) = row.fields.get(col_name.as_str()) {
                if let Some(key) = IndexKey::from_value(val) {
                    idx.insert(key, row_id);
                }
            }
        }
        self.rows.insert(row_id, row);
    }

    // ── Stats ─────────────────────────────────────────────────────────────

    pub fn row_count(&self) -> usize {
        self.rows.values().filter(|r| !r.deleted).count()
    }

    pub fn index_names(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }

    // ── Cost-based statistics ─────────────────────────────────────────────

    /// Compute lightweight statistics used by the query planner.
    ///
    /// * `row_count`  — number of live rows
    /// * `column_ndv` — number of distinct values per column (NDV)
    /// * `histograms` — equi-depth histograms (up to 16 buckets per column)
    pub fn statistics(&self) -> TableStats {
        const BUCKET_COUNT: usize = 16;

        let live_rows: Vec<&Row> = self.rows.values().filter(|r| !r.deleted).collect();
        let row_count = live_rows.len();

        let mut column_ndv: HashMap<String, usize> = HashMap::new();
        let mut histograms: HashMap<String, Histogram> = HashMap::new();

        for col in &self.schema.columns {
            let mut distinct: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut values: Vec<crate::types::Value> = Vec::with_capacity(row_count);
            let mut null_count = 0usize;

            for row in &live_rows {
                match row.fields.get(&col.name) {
                    None | Some(crate::types::Value::Null) => {
                        null_count += 1;
                        distinct.insert("\0null".into());
                    }
                    Some(v) => {
                        let key = format!("{v:?}");
                        distinct.insert(key);
                        values.push(v.clone());
                    }
                }
            }
            let ndv = distinct.len().max(1);
            column_ndv.insert(col.name.clone(), ndv);

            // Build histogram only for sortable numeric/text columns
            if !values.is_empty()
                && matches!(
                    col.data_type,
                    crate::types::DataType::Int
                        | crate::types::DataType::Float
                        | crate::types::DataType::Text
                )
            {
                values.sort_by(|a, b| a.partial_cmp_val(b).unwrap_or(std::cmp::Ordering::Equal));
                let bucket_size = (values.len() / BUCKET_COUNT).max(1);
                let mut buckets = Vec::new();

                for chunk in values.chunks(bucket_size) {
                    let mut chunk_distinct: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for v in chunk {
                        chunk_distinct.insert(format!("{v:?}"));
                    }
                    buckets.push(HistogramBucket {
                        min:   chunk[0].clone(),
                        max:   chunk[chunk.len() - 1].clone(),
                        count: chunk.len(),
                        ndv:   chunk_distinct.len().max(1),
                    });
                }
                histograms.insert(col.name.clone(), Histogram { buckets, null_count });
            }
        }

        TableStats { row_count, column_ndv, histograms }
    }
}

// ── Database — collection of tables ──────────────────────────────────────

/// Thread-safe in-memory database holding all tables.
pub struct Database {
    /// Table name → Table (behind a per-table RwLock for fine-grained concurrency)
    pub tables: Arc<RwLock<HashMap<String, Arc<RwLock<Table>>>>>,
}

impl Database {
    pub fn new() -> Self {
        info!("FlowDB database initialized");
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── DDL ───────────────────────────────────────────────────────────────

    pub fn create_table(&self, schema: TableSchema) -> Result<(), FlowError> {
        let name = schema.name.clone();
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&name) {
            return Err(FlowError::TableAlreadyExists(name));
        }
        tables.insert(name.clone(), Arc::new(RwLock::new(Table::new(schema))));
        info!("created table `{name}`");
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<(), FlowError> {
        let mut tables = self.tables.write().unwrap();
        if tables.remove(name).is_none() {
            return Err(FlowError::TableNotFound(name.to_string()));
        }
        info!("dropped table `{name}`");
        Ok(())
    }

    /// Get a shared reference to a table (read lock on the table registry).
    pub fn get_table(&self, name: &str) -> Result<Arc<RwLock<Table>>, FlowError> {
        let tables = self.tables.read().unwrap();
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| FlowError::TableNotFound(name.to_string()))
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }
}

impl Default for Database {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType};

    fn make_schema(name: &str) -> TableSchema {
        TableSchema::new(
            name,
            vec![
                ColumnSchema {
                    name: "id".into(),
                    data_type: DataType::Int,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
        )
    }

    #[test]
    fn test_insert_and_scan() {
        let mut table = Table::new(make_schema("users"));
        let mut fields = HashMap::new();
        fields.insert("id".into(), Value::Int(1));
        fields.insert("name".into(), Value::Text("Alice".into()));
        table.insert(fields).unwrap();
        assert_eq!(table.row_count(), 1);
        let rows = table.scan_all();
        assert_eq!(rows[0].get("name").unwrap(), &Value::Text("Alice".into()));
    }

    #[test]
    fn test_soft_delete() {
        let mut table = Table::new(make_schema("users"));
        let mut fields = HashMap::new();
        fields.insert("id".into(), Value::Int(1));
        table.insert(fields).unwrap();
        let count = table.delete(|_| true);
        assert_eq!(count, 1);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_update() {
        let mut table = Table::new(make_schema("users"));
        let mut fields = HashMap::new();
        fields.insert("id".into(), Value::Int(1));
        fields.insert("name".into(), Value::Text("Bob".into()));
        table.insert(fields).unwrap();

        let mut upd = HashMap::new();
        upd.insert("name".into(), Value::Text("Charlie".into()));
        let count = table.update(upd, |_| true);
        assert_eq!(count, 1);

        let rows = table.scan_all();
        assert_eq!(rows[0].get("name").unwrap(), &Value::Text("Charlie".into()));
    }

    #[test]
    fn test_create_index() {
        let mut table = Table::new(make_schema("users"));
        let mut f = HashMap::new();
        f.insert("id".into(), Value::Int(42));
        f.insert("name".into(), Value::Text("Dave".into()));
        table.insert(f).unwrap();

        table.create_index("name").unwrap();
        let idx = table.indexes.get("name").unwrap();
        let key = IndexKey::Text("Dave".into());
        assert!(!idx.lookup(&key).is_empty());
    }

    #[test]
    fn test_database_create_drop() {
        let db = Database::new();
        db.create_table(make_schema("orders")).unwrap();
        assert!(db.get_table("orders").is_ok());
        db.drop_table("orders").unwrap();
        assert!(db.get_table("orders").is_err());
    }
}
