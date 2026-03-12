use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::auth::AuthManager;
use crate::cluster::ClusterRegistry;
use crate::engine::evaluator::{trigram_similarity, Evaluator};
use crate::engine::planner::{Planner, RangeOp, ScanStrategy};
use crate::engine::watch::{WatchOp, WatchRegistry};
use crate::error::FlowError;
use crate::metrics::Metrics;
use crate::sql::ast::{AggFunc, ColumnDef, GroupByClause, JoinClause, JoinKind, OrderBy, Stmt};
use crate::storage::persist;
use crate::storage::table::{Database, IndexKey};
use crate::triggers::{Trigger, TriggerEvent, TriggerStore};
use crate::types::{ColumnSchema, DataType, TableSchema, Value};

// ── Query result ──────────────────────────────────────────────────────────

/// The result of executing any FlowQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    /// Zero or more rows returned
    Rows { columns: Vec<String>, rows: Vec<Vec<Value>>, elapsed_ms: u64 },
    /// Scalar count (affected rows, etc.)
    Count { affected: usize, elapsed_ms: u64 },
    /// Success message
    Ok { message: String, elapsed_ms: u64 },
    /// EXPLAIN plan
    Plan { description: String },
}

impl QueryResult {
    pub fn ok(msg: impl Into<String>, elapsed: Duration) -> Self {
        Self::Ok { message: msg.into(), elapsed_ms: elapsed.as_millis() as u64 }
    }
    pub fn count(n: usize, elapsed: Duration) -> Self {
        Self::Count { affected: n, elapsed_ms: elapsed.as_millis() as u64 }
    }
}

// ── Executor config ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Global default timeout (ms); 0 = no limit
    pub default_timeout_ms: u64,
    /// Max rows returned per query
    pub max_result_rows: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            default_timeout_ms: 30_000, // 30-second default timeout
            max_result_rows: 100_000,
        }
    }
}

// ── Executor ──────────────────────────────────────────────────────────────

/// Executes parsed FlowQL statements against the database.
pub struct Executor {
    pub db: Arc<Database>,
    pub config: ExecutorConfig,
    pub metrics: Arc<Metrics>,
    /// Optional data directory for catalog + snapshot persistence.
    /// `None` in unit-test mode (no I/O).
    pub data_dir: Option<Arc<std::path::PathBuf>>,
    /// Push-subscription registry (None during unit tests).
    pub watch_registry: Option<Arc<WatchRegistry>>,
    /// Cluster peer registry (None in single-node mode).
    pub cluster_registry: Option<Arc<ClusterRegistry>>,
    /// Authentication manager (None = open/unauthenticated mode).
    pub auth_manager: Option<Arc<Mutex<AuthManager>>>,
    /// Runtime config key-value store.
    pub runtime_config: Arc<Mutex<std::collections::HashMap<String, String>>>,
    /// Shard manager for cluster sharding.
    pub shard_manager: Arc<crate::cluster::shard::ShardManager>,
    /// Event-driven trigger store.
    pub trigger_store: Arc<TriggerStore>,
    /// Built-in REST API server store.
    pub api_store: Arc<crate::api::ApiStore>,
}

impl Executor {
    pub fn new(db: Arc<Database>, metrics: Arc<Metrics>) -> Self {
        Self {
            db,
            config: ExecutorConfig::default(),
            metrics,
            data_dir: None,
            watch_registry: None,
            cluster_registry: None,
            auth_manager: None,
            runtime_config: Arc::new(Mutex::new(std::collections::HashMap::new())),
            shard_manager: Arc::new(crate::cluster::shard::ShardManager::new()),
            trigger_store: Arc::new(TriggerStore::new()),
            api_store: Arc::new(crate::api::ApiStore::new()),
        }
    }

    /// Construct an executor that persists catalog + snapshots to `data_dir`.
    pub fn with_data_dir(
        db: Arc<Database>,
        metrics: Arc<Metrics>,
        data_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            db,
            config: ExecutorConfig::default(),
            metrics,
            data_dir: Some(Arc::new(data_dir)),
            watch_registry: None,
            cluster_registry: None,
            auth_manager: None,
            runtime_config: Arc::new(Mutex::new(std::collections::HashMap::new())),
            shard_manager: Arc::new(crate::cluster::shard::ShardManager::new()),
            trigger_store: Arc::new(TriggerStore::new()),
            api_store: Arc::new(crate::api::ApiStore::new()),
        }
    }

    /// Execute a single parsed statement.
    pub fn execute(&self, stmt: Stmt) -> Result<QueryResult, FlowError> {
        self.execute_inner(stmt)
    }

    fn execute_inner(&self, stmt: Stmt) -> Result<QueryResult, FlowError> {
        let t0 = Instant::now();
        match stmt {
            Stmt::Get { table, join, filter, group_by, order_by, limit, timeout_ms } => {
                self.exec_get(&table, join, filter, group_by, order_by, limit, timeout_ms, t0)
            }
            Stmt::Put { table, fields } => self.exec_put(&table, fields, t0),
            Stmt::Set { table, fields, filter } => self.exec_set(&table, fields, filter, t0),
            Stmt::Del { table, filter } => self.exec_del(&table, filter, t0),
            Stmt::Find { table, column, pattern, limit } => {
                self.exec_find(&table, &column.column, &pattern, limit, t0)
            }
            Stmt::MakeTable { name, columns } => self.exec_make_table(name, columns, t0),
            Stmt::DropTable { name } => self.exec_drop_table(&name, t0),
            Stmt::MakeIndex { table, column } => self.exec_make_index(&table, &column, t0),
            Stmt::Begin | Stmt::Commit | Stmt::Rollback => {
                // Transactions are handled at a higher level by the transaction manager.
                // Here we just acknowledge (lite mode: auto-commit)
                Ok(QueryResult::ok("OK", t0.elapsed()))
            }
            Stmt::ShowRunningQueries => {
                let snapshot = self.metrics.running_queries_snapshot();
                Ok(QueryResult::Rows {
                    columns: vec!["id".into(), "query".into(), "elapsed_ms".into()],
                    rows: snapshot,
                    elapsed_ms: t0.elapsed().as_millis() as u64,
                })
            }
            Stmt::ShowTables => {
                let mut names = self.db.table_names();
                names.sort();
                Ok(QueryResult::Rows {
                    columns: vec!["table_name".into()],
                    rows: names
                        .into_iter()
                        .map(|n| vec![crate::types::Value::Text(n)])
                        .collect(),
                    elapsed_ms: t0.elapsed().as_millis() as u64,
                })
            }
            Stmt::KillQuery { id } => {
                self.metrics.kill_query(id)?;
                Ok(QueryResult::ok(format!("query {id} killed"), t0.elapsed()))
            }
            Stmt::Explain(inner) => {
                let desc = self.explain(*inner);
                Ok(QueryResult::Plan { description: desc })
            }
            Stmt::Checkpoint => self.exec_checkpoint(t0),
            // WATCH / UNWATCH are handled at the server layer for async streaming;
            // reaching here means a test or non-streaming caller — treat as no-op.
            Stmt::Watch { .. } | Stmt::Unwatch { .. } => {
                Ok(QueryResult::ok("watch: handled by server layer", t0.elapsed()))
            }
            Stmt::Similar { table, vector, column, limit } => {
                self.exec_similar(&table, &vector, column.as_deref(), limit, t0)
            }
            Stmt::ClusterStatus              => self.exec_cluster_status(t0),
            Stmt::ClusterJoin { addr }       => self.exec_cluster_join(addr, t0),
            Stmt::ClusterPart { addr }       => self.exec_cluster_part(addr, t0),
            Stmt::ClusterShardStatus         => self.exec_shard_status(t0),
            Stmt::ClusterShardAssign { table, shards, nodes } => {
                self.exec_shard_assign(table, shards, nodes, t0)
            }
            Stmt::ClusterShardDrop { table } => self.exec_shard_drop(table, t0),
            Stmt::Auth { username, password } => self.exec_auth(username, password, t0),
            Stmt::CreateUser { username, password, is_admin } => {
                self.exec_create_user(username, password, is_admin, t0)
            }
            Stmt::DropUser { username }      => self.exec_drop_user(username, t0),
            Stmt::Grant { username, op, table } => self.exec_grant(username, op, table, t0),
            Stmt::Revoke { username, op, table } => self.exec_revoke(username, op, table, t0),
            Stmt::ShowUsers                  => self.exec_show_users(t0),
            Stmt::ConfigSet { key, value }   => self.exec_config_set(key, value, t0),
            Stmt::ShowConfig                 => self.exec_show_config(t0),

            // ── AI Search ─────────────────────────────────────────────────────
            Stmt::AiSearch { table, query, limit } =>
                self.exec_ai_search(table, query, limit, t0),

            // ── Time Travel ──────────────────────────────────────────────────
            Stmt::GetAsOf { table, timestamp, filter, order_by, limit } =>
                self.exec_get_as_of(table, timestamp, filter, order_by, limit, t0),
            Stmt::GetVersion { table, version, filter, order_by, limit } =>
                self.exec_get_version(table, version, filter, order_by, limit, t0),

            // ── Triggers ─────────────────────────────────────────────────────────
            Stmt::CreateTrigger { name, event, table, do_query } =>
                self.exec_create_trigger(name, event, table, do_query, t0),
            Stmt::DropTrigger { name }       => self.exec_drop_trigger(name, t0),
            Stmt::ShowTriggers               => self.exec_show_triggers(t0),

            // ── Graph ──────────────────────────────────────────────────────────────
            Stmt::GraphMatch { src_alias, src_table, edge_alias, edge_table, dst_alias, dst_table, filter, limit } =>
                self.exec_graph_match(src_alias, src_table, edge_alias, edge_table, dst_alias, dst_table, filter, limit, t0),

            // ── REST API ───────────────────────────────────────────────────────────
            Stmt::ApiGenerate { table }      => self.exec_api_generate(table, t0),
            Stmt::ApiStop { table }          => self.exec_api_stop(table, t0),
            Stmt::ShowApis                   => self.exec_show_apis(t0),
        }
    }

    // ── GET ──────────────────────────────────────────────────────────────

    fn exec_get(
        &self,
        table_name: &str,
        join: Option<JoinClause>,
        filter: Option<crate::sql::ast::Expr>,
        group_by: Option<GroupByClause>,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
        timeout_ms: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        // ── 1. Ask the planner for the cheapest scan strategy ───────────────
        let strategy = Planner::new(Arc::clone(&self.db))
            .choose_scan(table_name, filter.as_ref());

        let tbl = self.db.get_table(table_name)?;
        let tbl = tbl.read().unwrap();

        let eff_timeout = timeout_ms.unwrap_or(self.config.default_timeout_ms);
        let deadline = if eff_timeout > 0 {
            Some(t0 + Duration::from_millis(eff_timeout))
        } else {
            None
        };

        // ── 2. Acquire candidate rows per the plan ─────────────────────────
        let candidates: Vec<crate::types::Row> = match &strategy {
            ScanStrategy::IndexLookup { column, value } => {
                if let Some(key) = IndexKey::from_value(value) {
                    let ids = tbl.index_lookup(column, &key).unwrap_or_default();
                    tbl.get_rows_by_ids(&ids).into_iter().cloned().collect()
                } else {
                    tbl.scan_all().into_iter().cloned().collect()
                }
            }
            ScanStrategy::IndexRangeScan { column, op, value } => {
                if let Some(key) = IndexKey::from_value(value) {
                    let ids = range_op_to_ids(&tbl, column, op, &key);
                    tbl.get_rows_by_ids(&ids).into_iter().cloned().collect()
                } else {
                    tbl.scan_all().into_iter().cloned().collect()
                }
            }
            ScanStrategy::FullScan { .. } => tbl.scan_all().into_iter().cloned().collect(),
        };
        drop(tbl); // release lock before join

        // ── 3. JOIN ────────────────────────────────────────────────────────
        let (joined_rows, col_set) = if let Some(ref jc) = join {
            let right_tbl = self.db.get_table(&jc.table)?;
            let right_tbl = right_tbl.read().unwrap();
            let right_rows: Vec<crate::types::Row> = right_tbl.scan_all().into_iter().cloned().collect();

            let left_schema: Vec<String> = {
                let lt = self.db.get_table(table_name)?;
                let lt = lt.read().unwrap();
                lt.schema.columns.iter().map(|c| c.name.clone()).collect()
            };
            let right_schema: Vec<String> = right_tbl.schema.columns.iter().map(|c| c.name.clone()).collect();

            // Build merged column list: left cols + right cols (prefixed if name collides)
            let mut merged_cols: Vec<String> = left_schema.clone();
            for rc in &right_schema {
                let col_name = if left_schema.contains(rc) {
                    format!("{}.{}", jc.table, rc)
                } else {
                    rc.clone()
                };
                merged_cols.push(col_name);
            }

            let mut merged_rows: Vec<crate::types::Row> = Vec::new();
            for left_row in &candidates {
                let mut matched = false;
                for right_row in &right_rows {
                    // Build a merged row for ON evaluation
                    let mut merged_fields = left_row.fields.clone();
                    for (k, v) in &right_row.fields {
                        let key = if left_row.fields.contains_key(k) {
                            format!("{}.{}", jc.table, k)
                        } else {
                            k.clone()
                        };
                        merged_fields.insert(key, v.clone());
                    }
                    let merged = crate::types::Row::new(merged_fields);
                    if Evaluator::matches_filter(&jc.on, &merged).unwrap_or(false) {
                        merged_rows.push(merged);
                        matched = true;
                    }
                }
                // LEFT JOIN: keep left row even with no right-side match
                if !matched && jc.kind == JoinKind::Left {
                    let row = left_row.clone();
                    merged_rows.push(row);
                }
            }
            // RIGHT JOIN: also keep right rows with no left-side match
            if jc.kind == JoinKind::Right {
                for right_row in &right_rows {
                    let has_match = merged_rows.iter().any(|mr| {
                        right_row.fields.iter().all(|(k, v)| {
                            let key = if candidates.first().map(|r| r.fields.contains_key(k)).unwrap_or(false) {
                                format!("{}.{}", jc.table, k)
                            } else {
                                k.clone()
                            };
                            mr.fields.get(&key) == Some(v)
                        })
                    });
                    if !has_match {
                        merged_rows.push(right_row.clone());
                    }
                }
            }
            (merged_rows, merged_cols)
        } else {
            let tbl = self.db.get_table(table_name)?;
            let tbl = tbl.read().unwrap();
            let cols: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();
            (candidates, cols)
        };

        // ── 4. Residual WHERE filter + timeout ───────────────────────────
        let mut matched: Vec<crate::types::Row> = joined_rows
            .into_iter()
            .filter(|row| {
                if let Some(dl) = deadline {
                    if Instant::now() > dl { return false; }
                }
                if let Some(ref f) = filter {
                    Evaluator::matches_filter(f, row).unwrap_or(false)
                } else {
                    true
                }
            })
            .collect();

        if let Some(dl) = deadline {
            if Instant::now() > dl && !matched.is_empty() {
                return Err(FlowError::Timeout { ms: eff_timeout });
            }
        }

        // ── 5. GROUP BY / Aggregation ─────────────────────────────────────
        if let Some(gb) = group_by {
            return self.exec_group_by(matched, gb, order_by, limit, t0);
        }

        // ── 6. ORDER BY ───────────────────────────────────────────────
        if let Some(ref ob) = order_by {
            let col = ob.column.column.clone();
            let asc = ob.ascending;
            matched.sort_by(|a, b| {
                let ord = a.get_or_null(&col)
                    .partial_cmp_val(&b.get_or_null(&col))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if asc { ord } else { ord.reverse() }
            });
        }

        // ── 7. LIMIT ───────────────────────────────────────────────────
        let cap = limit.map(|l| l as usize).unwrap_or(self.config.max_result_rows)
            .min(self.config.max_result_rows);
        matched.truncate(cap);

        // ── 8. Project output columns ───────────────────────────────────
        let mut out_cols = col_set;
        for row in &matched {
            for key in row.fields.keys() {
                if !out_cols.contains(key) { out_cols.push(key.clone()); }
            }
        }
        let rows_out: Vec<Vec<Value>> = matched
            .iter()
            .map(|row| out_cols.iter().map(|c| row.get_or_null(c)).collect())
            .collect();

        debug!(
            "GET `{table_name}` via {strategy} → {} rows in {}ms",
            rows_out.len(), t0.elapsed().as_millis()
        );
        Ok(QueryResult::Rows { columns: out_cols, rows: rows_out, elapsed_ms: t0.elapsed().as_millis() as u64 })
    }

    // ── GROUP BY ──────────────────────────────────────────────────────────

    fn exec_group_by(
        &self,
        rows: Vec<crate::types::Row>,
        gb: GroupByClause,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        use std::collections::HashMap;

        // Group rows by the GROUP BY columns
        let mut groups: HashMap<Vec<String>, Vec<crate::types::Row>> = HashMap::new();
        for row in rows {
            let key: Vec<String> = gb.columns.iter()
                .map(|c| format!("{:?}", row.get_or_null(&c.column)))
                .collect();
            groups.entry(key).or_default().push(row);
        }

        // Build output column list
        let mut out_cols: Vec<String> = gb.columns.iter().map(|c| c.column.clone()).collect();
        for agg in &gb.aggregates {
            out_cols.push(agg.alias.clone());
        }

        // Build output rows
        let mut out_rows: Vec<Vec<Value>> = Vec::new();
        for (_, group_rows) in &groups {
            let representative = &group_rows[0];

            // Compute GROUP BY key values
            let key_vals: Vec<Value> = gb.columns.iter()
                .map(|c| representative.get_or_null(&c.column))
                .collect();

            // Compute aggregates
            let mut agg_vals: Vec<Value> = Vec::new();
            for agg in &gb.aggregates {
                let v = compute_aggregate(&agg.func, agg.column.as_ref().map(|c| c.column.as_str()), group_rows);
                agg_vals.push(v);
            }

            // Apply HAVING filter (evaluate against the output row)
            if let Some(ref having) = gb.having {
                let mut temp_fields = std::collections::HashMap::new();
                for (col, val) in out_cols.iter().zip(key_vals.iter().chain(agg_vals.iter())) {
                    temp_fields.insert(col.clone(), val.clone());
                }
                let temp_row = crate::types::Row::new(temp_fields);
                if !Evaluator::matches_filter(having, &temp_row).unwrap_or(false) {
                    continue;
                }
            }

            let mut combined = key_vals;
            combined.extend(agg_vals);
            out_rows.push(combined);
        }

        // ORDER BY on result set
        if let Some(ref ob) = order_by {
            let col_idx = out_cols.iter().position(|c| c == &ob.column.column);
            if let Some(idx) = col_idx {
                let asc = ob.ascending;
                out_rows.sort_by(|a, b| {
                    let ord = a[idx].partial_cmp_val(&b[idx]).unwrap_or(std::cmp::Ordering::Equal);
                    if asc { ord } else { ord.reverse() }
                });
            }
        }

        // LIMIT
        let cap = limit.map(|l| l as usize).unwrap_or(self.config.max_result_rows)
            .min(self.config.max_result_rows);
        out_rows.truncate(cap);

        Ok(QueryResult::Rows { columns: out_cols, rows: out_rows, elapsed_ms: t0.elapsed().as_millis() as u64 })
    }

    // ── PUT ──────────────────────────────────────────────────────────────

    fn exec_put(
        &self,
        table_name: &str,
        fields: Vec<(String, crate::sql::ast::Expr)>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table_name)?;
        let row = {
            let mut tbl = tbl.write().unwrap();
            let mut field_map = std::collections::HashMap::new();
            for (name, expr) in fields {
                let dummy = crate::types::Row::new(Default::default());
                let val = Evaluator::eval(&expr, &dummy)?;
                field_map.insert(name, val);
            }
            tbl.insert(field_map)?
        };
        self.metrics.rows_inserted.fetch_add(1, Ordering::Relaxed);
        if let Some(ref wr) = self.watch_registry {
            wr.notify(table_name, WatchOp::Insert, &row);
        }
        self.fire_triggers(TriggerEvent::Put, table_name);
        Ok(QueryResult::count(1, t0.elapsed()))
    }

    // ── SET ──────────────────────────────────────────────────────────────

    fn exec_set(
        &self,
        table_name: &str,
        fields: Vec<(String, crate::sql::ast::Expr)>,
        filter: Option<crate::sql::ast::Expr>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table_name)?;
        let dummy = crate::types::Row::new(Default::default());
        let mut updates = std::collections::HashMap::new();
        for (name, expr) in fields {
            let val = Evaluator::eval(&expr, &dummy)?;
            updates.insert(name, val);
        }

        let (count, notify_rows) = {
            let mut tbl = tbl.write().unwrap();
            // Phase 1 — collect IDs of rows that will be updated (for post-update notification).
            let notify_ids: Vec<uuid::Uuid> = if self.watch_registry.is_some() {
                tbl.rows.values()
                    .filter(|r| !r.deleted)
                    .filter(|r| match &filter {
                        Some(f) => Evaluator::matches_filter(f, r).unwrap_or(false),
                        None => true,
                    })
                    .map(|r| r.id)
                    .collect()
            } else {
                Vec::new()
            };
            // Phase 2 — apply update.
            let count = tbl.update(updates, |row| {
                if let Some(ref f) = filter {
                    Evaluator::matches_filter(f, row).unwrap_or(false)
                } else {
                    true
                }
            });
            // Phase 3 — read back updated rows for notification.
            let notify_rows: Vec<crate::types::Row> = notify_ids.iter()
                .filter_map(|id| tbl.rows.get(id))
                .filter(|r| !r.deleted)
                .cloned()
                .collect();
            (count, notify_rows)
        };
        self.metrics.rows_updated.fetch_add(count as u64, Ordering::Relaxed);
        if let Some(ref wr) = self.watch_registry {
            for row in &notify_rows { wr.notify(table_name, WatchOp::Update, row); }
        }
        self.fire_triggers(TriggerEvent::Set, table_name);
        Ok(QueryResult::count(count, t0.elapsed()))
    }

    // ── DEL ──────────────────────────────────────────────────────────────

    fn exec_del(
        &self,
        table_name: &str,
        filter: Option<crate::sql::ast::Expr>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table_name)?;
        let (count, notify_rows) = {
            let mut tbl = tbl.write().unwrap();
            // Capture rows before soft-delete so watchers receive the deleted content.
            let notify_rows: Vec<crate::types::Row> = if self.watch_registry.is_some() {
                tbl.rows.values()
                    .filter(|r| !r.deleted)
                    .filter(|r| match &filter {
                        Some(f) => Evaluator::matches_filter(f, r).unwrap_or(false),
                        None => true,
                    })
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            let count = tbl.delete(|row| {
                if let Some(ref f) = filter {
                    Evaluator::matches_filter(f, row).unwrap_or(false)
                } else {
                    true
                }
            });
            (count, notify_rows)
        };
        self.metrics.rows_deleted.fetch_add(count as u64, Ordering::Relaxed);
        if let Some(ref wr) = self.watch_registry {
            for row in &notify_rows { wr.notify(table_name, WatchOp::Delete, row); }
        }
        self.fire_triggers(TriggerEvent::Del, table_name);
        Ok(QueryResult::count(count, t0.elapsed()))
    }

    // ── FIND ─────────────────────────────────────────────────────────────

    fn exec_find(
        &self,
        table_name: &str,
        column: &str,
        pattern: &str,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table_name)?;
        let tbl = tbl.read().unwrap();

        let limit_n = limit.map(|l| l as usize).unwrap_or(self.config.max_result_rows);

        // Score all live rows
        let mut scored: Vec<(f64, crate::types::Row)> = tbl
            .scan_all()
            .into_iter()
            .filter_map(|row| {
                if let Value::Text(text) = row.get_or_null(column) {
                    let score = trigram_similarity(&text, pattern);
                    if score >= 0.2 {
                        Some((score, row.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Sort by score descending
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit_n);

        let col_set: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();
        let rows_out: Vec<Vec<Value>> = scored
            .iter()
            .map(|(_, row)| col_set.iter().map(|c| row.get_or_null(c)).collect())
            .collect();

        Ok(QueryResult::Rows {
            columns: col_set,
            rows: rows_out,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    // ── DDL ──────────────────────────────────────────────────────────────

    fn exec_make_table(
        &self,
        name: String,
        col_defs: Vec<ColumnDef>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let mut columns = Vec::new();
        for def in col_defs {
            let data_type = DataType::from_str(&def.data_type)
                .ok_or_else(|| FlowError::type_err(format!("unknown type `{}`", def.data_type)))?;
            columns.push(ColumnSchema {
                name: def.name,
                data_type,
                nullable: def.nullable,
                primary_key: def.primary_key,
            });
        }
        let schema = TableSchema::new(name.clone(), columns);
        self.db.create_table(schema)?;
        self.save_catalog();
        Ok(QueryResult::ok(format!("table `{name}` created"), t0.elapsed()))
    }

    fn exec_drop_table(&self, name: &str, t0: Instant) -> Result<QueryResult, FlowError> {
        self.db.drop_table(name)?;
        self.save_catalog();
        Ok(QueryResult::ok(format!("table `{name}` dropped"), t0.elapsed()))
    }

    fn exec_make_index(&self, table: &str, column: &str, t0: Instant) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table)?;
        let mut tbl = tbl.write().unwrap();
        tbl.create_index(column)?;
        drop(tbl);
        self.save_catalog();
        Ok(QueryResult::ok(
            format!("index on `{table}.{column}` created"),
            t0.elapsed(),
        ))
    }

    // ── EXPLAIN ──────────────────────────────────────────────────────────

    fn explain(&self, stmt: Stmt) -> String {
        // Delegate to the planner — produces the full plan tree as a string.
        let plan = Planner::new(Arc::clone(&self.db)).plan(&stmt);
        format!("{plan}")
    }

    // ── CHECKPOINT ────────────────────────────────────────────

    fn exec_checkpoint(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.data_dir {
            None => Ok(QueryResult::ok("CHECKPOINT skipped (no data dir configured)", t0.elapsed())),
            Some(dir) => {
                let rows = persist::checkpoint(&self.db, dir)?;
                Ok(QueryResult::ok(
                    format!("CHECKPOINT complete: {rows} rows written"),
                    t0.elapsed(),
                ))
            }
        }
    }

    /// Persist the current catalog to disk (no-op when data_dir is None).
    fn save_catalog(&self) {
        if let Some(dir) = &self.data_dir {
            if let Err(e) = persist::Catalog::build(&self.db).save(dir) {
                tracing::warn!("catalog save failed: {e}");
            }
        }
    }

    // ── SIMILAR (vector cosine search) ───────────────────────────────────

    fn exec_similar(
        &self,
        table_name: &str,
        query_vec: &[f32],
        column: Option<&str>,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(table_name)?;
        let tbl = tbl.read().unwrap();
        let limit_n = limit.map(|l| l as usize).unwrap_or(10);

        let mut scored: Vec<(f32, crate::types::Row)> = tbl
            .scan_all()
            .into_iter()
            .filter_map(|row| {
                let val = if let Some(col) = column {
                    row.fields.get(col)?
                } else {
                    row.fields.values().find(|v| matches!(v, Value::Vector(_)))?
                };
                if let Value::Vector(vec) = val {
                    Some((cosine_sim(query_vec, vec), row.clone()))
                } else {
                    None
                }
            })
            .collect();

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit_n);

        let mut col_set: Vec<String> = tbl.schema.columns.iter().map(|c| c.name.clone()).collect();
        col_set.push("_score".into());

        let rows_out: Vec<Vec<Value>> = scored
            .iter()
            .map(|(score, row)| {
                let mut vals: Vec<Value> = col_set[..col_set.len() - 1]
                    .iter()
                    .map(|c| row.get_or_null(c))
                    .collect();
                vals.push(Value::Float(*score as f64));
                vals
            })
            .collect();

        Ok(QueryResult::Rows {
            columns: col_set,
            rows: rows_out,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    // ── CLUSTER ──────────────────────────────────────────────────────────

    fn exec_cluster_status(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        let peers = match &self.cluster_registry {
            None    => Vec::new(),
            Some(r) => r.status(),
        };
        let rows = peers
            .iter()
            .map(|p| vec![
                Value::Text(p.addr.clone()),
                Value::Bool(p.reachable),
                p.latency_ms.map(|l| Value::Int(l as i64)).unwrap_or(Value::Null),
            ])
            .collect();
        Ok(QueryResult::Rows {
            columns: vec!["addr".into(), "reachable".into(), "latency_ms".into()],
            rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    fn exec_cluster_join(&self, addr: String, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.cluster_registry {
            None    => Err(FlowError::Internal("cluster registry not configured".into())),
            Some(r) => {
                r.join(addr.clone());
                Ok(QueryResult::ok(format!("joined cluster node {addr}"), t0.elapsed()))
            }
        }
    }

    fn exec_cluster_part(&self, addr: String, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.cluster_registry {
            None    => Err(FlowError::Internal("cluster registry not configured".into())),
            Some(r) => {
                if r.part(&addr) {
                    Ok(QueryResult::ok(format!("removed cluster node {addr}"), t0.elapsed()))
                } else {
                    Err(FlowError::Internal(format!("node {addr} not found in cluster")))
                }
            }
        }
    }

    // ── SHARD ─────────────────────────────────────────────────────────────

    fn exec_shard_status(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        let status = self.shard_manager.status();
        let rows = status.into_iter().map(|ts| vec![
            Value::Text(ts.table),
            Value::Int(ts.shard_count as i64),
            Value::Int(ts.replication_factor as i64),
        ]).collect();
        Ok(QueryResult::Rows {
            columns: vec!["table".into(), "shard_count".into(), "replication_factor".into()],
            rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    fn exec_shard_assign(&self, table: String, shards: u32, nodes: Vec<String>, t0: Instant) -> Result<QueryResult, FlowError> {
        let replication = if nodes.len() > 1 { 1u32 } else { 0u32 };
        self.shard_manager.create_shards(&table, shards, replication, &nodes)?;
        Ok(QueryResult::ok(
            format!("table `{table}` sharded into {shards} shards across {} nodes", nodes.len()),
            t0.elapsed(),
        ))
    }

    fn exec_shard_drop(&self, table: String, t0: Instant) -> Result<QueryResult, FlowError> {
        self.shard_manager.remove(&table);
        Ok(QueryResult::ok(format!("sharding for table `{table}` removed"), t0.elapsed()))
    }

    // ── AUTH ──────────────────────────────────────────────────────────────

    fn exec_auth(&self, username: String, password: String, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Ok(QueryResult::ok("open mode: authentication not required", t0.elapsed())),
            Some(am) => {
                let am = am.lock().unwrap();
                am.authenticate(&username, &password)
                    .map(|_| QueryResult::ok(format!("authenticated as `{username}`"), t0.elapsed()))
            }
        }
    }

    fn exec_create_user(&self, username: String, password: String, is_admin: bool, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Err(FlowError::auth("auth not configured; start server with --secured")),
            Some(am) => {
                let am = am.lock().unwrap();
                let acting = crate::auth::User::new("system", "", true);
                am.create_user(&acting, &username, &password, is_admin)?;
                Ok(QueryResult::ok(format!("user `{username}` created"), t0.elapsed()))
            }
        }
    }

    fn exec_drop_user(&self, username: String, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Err(FlowError::auth("auth not configured")),
            Some(am) => {
                let am = am.lock().unwrap();
                let acting = crate::auth::User::new("system", "", true);
                am.drop_user(&acting, &username)?;
                Ok(QueryResult::ok(format!("user `{username}` dropped"), t0.elapsed()))
            }
        }
    }

    fn exec_grant(&self, username: String, op: String, table: Option<String>, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Err(FlowError::auth("auth not configured")),
            Some(am) => {
                let op_parsed = crate::auth::Op::from_str(&op)
                    .ok_or_else(|| FlowError::parse(format!("unknown operation `{op}`")))?;
                let am = am.lock().unwrap();
                let acting = crate::auth::User::new("system", "", true);
                am.grant(&acting, &username, op_parsed, table.clone())?;
                Ok(QueryResult::ok(
                    format!("granted {op} on {} to `{username}`", table.as_deref().unwrap_or("*")),
                    t0.elapsed(),
                ))
            }
        }
    }

    fn exec_revoke(&self, username: String, op: String, table: Option<String>, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Err(FlowError::auth("auth not configured")),
            Some(am) => {
                let op_parsed = crate::auth::Op::from_str(&op)
                    .ok_or_else(|| FlowError::parse(format!("unknown operation `{op}`")))?;
                let am = am.lock().unwrap();
                let acting = crate::auth::User::new("system", "", true);
                am.revoke(&acting, &username, &op_parsed, table.as_deref())?;
                Ok(QueryResult::ok(
                    format!("revoked {op} on {} from `{username}`", table.as_deref().unwrap_or("*")),
                    t0.elapsed(),
                ))
            }
        }
    }

    fn exec_show_users(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        match &self.auth_manager {
            None => Ok(QueryResult::Rows {
                columns: vec!["username".into(), "is_admin".into()],
                rows: vec![],
                elapsed_ms: t0.elapsed().as_millis() as u64,
            }),
            Some(am) => {
                let am = am.lock().unwrap();
                let rows = am.list_users().into_iter().map(|(name, is_admin)| {
                    vec![Value::Text(name), Value::Bool(is_admin)]
                }).collect();
                Ok(QueryResult::Rows {
                    columns: vec!["username".into(), "is_admin".into()],
                    rows,
                    elapsed_ms: t0.elapsed().as_millis() as u64,
                })
            }
        }
    }

    // ── CONFIG ────────────────────────────────────────────────────────────

    fn exec_config_set(&self, key: String, value: String, t0: Instant) -> Result<QueryResult, FlowError> {
        self.runtime_config.lock().unwrap().insert(key.clone(), value.clone());
        Ok(QueryResult::ok(format!("config `{key}` = `{value}`"), t0.elapsed()))
    }

    fn exec_show_config(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        let cfg = self.runtime_config.lock().unwrap();
        let mut entries: Vec<(String, String)> = cfg.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let rows = entries.into_iter().map(|(k, v)| vec![Value::Text(k), Value::Text(v)]).collect();
        Ok(QueryResult::Rows {
            columns: vec!["key".into(), "value".into()],
            rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    // ── AI Search ────────────────────────────────────────────────────────

    fn exec_ai_search(
        &self,
        table: String,
        query: String,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let k = limit.unwrap_or(10) as usize;
        let results = crate::ai::ai_search(&self.db, &table, &query, k)?;

        if results.is_empty() {
            return Ok(QueryResult::Rows {
                columns: vec!["score".into()],
                rows: vec![],
                elapsed_ms: t0.elapsed().as_millis() as u64,
            });
        }

        // Collect all unique column names from all results
        let mut col_set: std::collections::LinkedList<String> = std::collections::LinkedList::new();
        col_set.push_back("score".into());
        for (_, fields) in &results {
            for k in fields.keys() {
                if !col_set.iter().any(|c| c == k) {
                    col_set.push_back(k.clone());
                }
            }
        }
        let columns: Vec<String> = col_set.into_iter().collect();

        let rows: Vec<Vec<Value>> = results
            .into_iter()
            .map(|(score, fields)| {
                columns
                    .iter()
                    .map(|c| {
                        if c == "score" {
                            Value::Float(score as f64)
                        } else {
                            fields.get(c).cloned().unwrap_or(Value::Null)
                        }
                    })
                    .collect()
            })
            .collect();

        Ok(QueryResult::Rows { columns, rows, elapsed_ms: t0.elapsed().as_millis() as u64 })
    }

    // ── Time Travel ──────────────────────────────────────────────────────

    fn exec_get_as_of(
        &self,
        table: String,
        timestamp: String,
        filter: Option<crate::sql::ast::Expr>,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        use chrono::DateTime;

        let cutoff = DateTime::parse_from_rfc3339(&timestamp)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .or_else(|_| {
                // Try YYYY-MM-DD format
                chrono::NaiveDate::parse_from_str(&timestamp, "%Y-%m-%d")
                    .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap())
                    .map(|ndt| ndt.and_utc())
                    .map_err(|e| e)
            })
            .map_err(|_| FlowError::Parse(format!("invalid timestamp: `{}`", timestamp)))?;

        let tbl = self.db.get_table(&table)?;
        let guard = tbl.read().unwrap();

        let mut rows: Vec<&crate::types::Row> = guard
            .rows
            .values()
            .filter(|r| !r.deleted && r.updated_at <= cutoff)
            .collect();

        if let Some(f) = &filter {
            rows.retain(|r| {
                Evaluator::matches_filter(f, r).unwrap_or(false)
            });
        }

        if let Some(ob) = &order_by {
            rows.sort_by(|a, b| {
                let av = a.get_or_null(&ob.column.column);
                let bv = b.get_or_null(&ob.column.column);
                let ord = av.partial_cmp_val(&bv).unwrap_or(std::cmp::Ordering::Equal);
                if ob.ascending { ord } else { ord.reverse() }
            });
        }

        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }

        let columns = guard.schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        let result_rows: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| columns.iter().map(|c| r.get_or_null(c)).collect())
            .collect();

        Ok(QueryResult::Rows {
            columns,
            rows: result_rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    fn exec_get_version(
        &self,
        table: String,
        version: u64,
        filter: Option<crate::sql::ast::Expr>,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let tbl = self.db.get_table(&table)?;
        let guard = tbl.read().unwrap();

        // VERSION n: rows whose xmin <= n and (xmax == 0 or xmax > n)
        let mut rows: Vec<&crate::types::Row> = guard
            .rows
            .values()
            .filter(|r| r.xmin <= version && (r.xmax == 0 || r.xmax > version))
            .collect();

        if let Some(f) = &filter {
            rows.retain(|r| Evaluator::matches_filter(f, r).unwrap_or(false));
        }

        if let Some(ob) = &order_by {
            rows.sort_by(|a, b| {
                let av = a.get_or_null(&ob.column.column);
                let bv = b.get_or_null(&ob.column.column);
                let ord = av.partial_cmp_val(&bv).unwrap_or(std::cmp::Ordering::Equal);
                if ob.ascending { ord } else { ord.reverse() }
            });
        }

        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }

        let columns = guard.schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        let result_rows: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| columns.iter().map(|c| r.get_or_null(c)).collect())
            .collect();

        Ok(QueryResult::Rows {
            columns,
            rows: result_rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    // ── Triggers ─────────────────────────────────────────────────────────

    fn exec_create_trigger(
        &self,
        name: String,
        event: String,
        table: String,
        do_query: String,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let ev = match event.to_uppercase().as_str() {
            "PUT" => TriggerEvent::Put,
            "SET" => TriggerEvent::Set,
            "DEL" => TriggerEvent::Del,
            other => return Err(FlowError::Parse(format!("unknown trigger event: {}", other))),
        };
        self.trigger_store.create(Trigger { name: name.clone(), event: ev, table, do_query })?;
        Ok(QueryResult::ok(format!("trigger `{}` created", name), t0.elapsed()))
    }

    fn exec_drop_trigger(&self, name: String, t0: Instant) -> Result<QueryResult, FlowError> {
        self.trigger_store.drop_trigger(&name)?;
        Ok(QueryResult::ok(format!("trigger `{}` dropped", name), t0.elapsed()))
    }

    fn exec_show_triggers(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        let triggers = self.trigger_store.list();
        let rows: Vec<Vec<Value>> = triggers
            .into_iter()
            .map(|t| vec![
                Value::Text(t.name),
                Value::Text(t.event.as_str().into()),
                Value::Text(t.table),
                Value::Text(t.do_query),
            ])
            .collect();
        Ok(QueryResult::Rows {
            columns: vec!["name".into(), "event".into(), "table".into(), "do_query".into()],
            rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }

    /// Fire any triggers matching an event + table after a mutating operation.
    /// Errors from triggers are logged but do NOT fail the parent operation.
    fn fire_triggers(&self, event: TriggerEvent, table: &str) {
        let triggers = self.trigger_store.get_matching(&event, table);
        for trigger in triggers {
            let sql = trigger.do_query.clone();
            // Parse and execute the DO query
            match crate::sql::parser::Parser::parse_str(&sql) {
                Ok(stmts) => {
                    for stmt in stmts {
                        if let Err(e) = self.execute_inner(stmt) {
                            debug!("trigger `{}` DO query failed: {}", trigger.name, e);
                        }
                    }
                }
                Err(e) => {
                    debug!("trigger `{}` DO query parse error: {}", trigger.name, e);
                }
            }
        }
    }

    // ── Graph ─────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn exec_graph_match(
        &self,
        src_alias: String,
        src_table: String,
        edge_alias: String,
        edge_table: String,
        dst_alias: String,
        dst_table: String,
        filter: Option<crate::sql::ast::Expr>,
        limit: Option<u64>,
        t0: Instant,
    ) -> Result<QueryResult, FlowError> {
        let k = limit.unwrap_or(100) as usize;
        let results = crate::graph::GraphEngine::match_query(
            &self.db,
            &src_alias, &src_table,
            &edge_alias, &edge_table,
            &dst_alias, &dst_table,
            filter.as_ref(),
            k,
        )?;

        if results.is_empty() {
            return Ok(QueryResult::Rows {
                columns: vec![],
                rows: vec![],
                elapsed_ms: t0.elapsed().as_millis() as u64,
            });
        }

        // Collect all column names from first row (order matters)
        let mut columns: Vec<String> = Vec::new();
        for k in results[0].keys() {
            if !columns.contains(k) {
                columns.push(k.clone());
            }
        }
        columns.sort();

        let rows: Vec<Vec<Value>> = results
            .into_iter()
            .map(|fields| {
                columns.iter().map(|c| fields.get(c).cloned().unwrap_or(Value::Null)).collect()
            })
            .collect();

        Ok(QueryResult::Rows { columns, rows, elapsed_ms: t0.elapsed().as_millis() as u64 })
    }

    // ── REST API ──────────────────────────────────────────────────────────

    fn exec_api_generate(&self, table: String, t0: Instant) -> Result<QueryResult, FlowError> {
        let port = self.api_store.start_sync(Arc::clone(&self.db), table.clone())?;
        Ok(QueryResult::ok(
            format!("REST API for `{}` running at http://127.0.0.1:{}/api/{}", table, port, table),
            t0.elapsed(),
        ))
    }

    fn exec_api_stop(&self, table: String, t0: Instant) -> Result<QueryResult, FlowError> {
        self.api_store.stop_sync(&table)?;
        Ok(QueryResult::ok(format!("REST API for `{}` stopped", table), t0.elapsed()))
    }

    fn exec_show_apis(&self, t0: Instant) -> Result<QueryResult, FlowError> {
        let mut apis = self.api_store.list_sync();
        apis.sort_by(|a, b| a.0.cmp(&b.0));
        let rows: Vec<Vec<Value>> = apis
            .into_iter()
            .map(|(table, port)| vec![
                Value::Text(table.clone()),
                Value::Int(port as i64),
                Value::Text(format!("http://127.0.0.1:{}/api/{}", port, table)),
            ])
            .collect();
        Ok(QueryResult::Rows {
            columns: vec!["table".into(), "port".into(), "url".into()],
            rows,
            elapsed_ms: t0.elapsed().as_millis() as u64,
        })
    }
}

// ── Aggregate computation ─────────────────────────────────────────────────

fn compute_aggregate(func: &AggFunc, col: Option<&str>, rows: &[crate::types::Row]) -> Value {
    match func {
        AggFunc::Count => Value::Int(rows.len() as i64),
        AggFunc::Sum => {
            let col = col.unwrap_or("");
            let sum: f64 = rows.iter().map(|r| val_to_f64(&r.get_or_null(col))).sum();
            Value::Float(sum)
        }
        AggFunc::Avg => {
            if rows.is_empty() { return Value::Null; }
            let col = col.unwrap_or("");
            let sum: f64 = rows.iter().map(|r| val_to_f64(&r.get_or_null(col))).sum();
            Value::Float(sum / rows.len() as f64)
        }
        AggFunc::Min => {
            let col = col.unwrap_or("");
            rows.iter()
                .map(|r| r.get_or_null(col))
                .min_by(|a, b| a.partial_cmp_val(b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Value::Null)
        }
        AggFunc::Max => {
            let col = col.unwrap_or("");
            rows.iter()
                .map(|r| r.get_or_null(col))
                .max_by(|a, b| a.partial_cmp_val(b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(Value::Null)
        }
    }
}

fn val_to_f64(v: &Value) -> f64 {
    match v {
        Value::Int(n)   => *n as f64,
        Value::Float(f) => *f,
        _               => 0.0,
    }
}

// ── Cosine similarity ─────────────────────────────────────────────────────

/// Computes the cosine similarity between two equal-length f32 vectors.
/// Returns 0.0 for zero-length or mismatched vectors.
fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32    = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32  = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32  = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 { return 0.0; }
    dot / (mag_a * mag_b)
}

// ── Index range-scan helper ───────────────────────────────────────────────

/// Convert a `RangeOp` + `IndexKey` into B-tree bounds and execute the scan.
fn range_op_to_ids(
    tbl: &crate::storage::table::Table,
    column: &str,
    op: &RangeOp,
    key: &IndexKey,
) -> Vec<uuid::Uuid> {
    use std::ops::Bound::*;
    let ids = match op {
        RangeOp::Eq => tbl.index_range_scan(column, Included(key), Included(key)),
        RangeOp::Lt => tbl.index_range_scan(column, Unbounded,     Excluded(key)),
        RangeOp::Le => tbl.index_range_scan(column, Unbounded,     Included(key)),
        RangeOp::Gt => tbl.index_range_scan(column, Excluded(key), Unbounded),
        RangeOp::Ge => tbl.index_range_scan(column, Included(key), Unbounded),
    };
    ids.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::Parser;

    fn setup() -> Executor {
        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        Executor::new(db, metrics)
    }

    fn exec(e: &Executor, q: &str) -> QueryResult {
        let stmts = Parser::parse_str(q).expect("parse failed");
        e.execute(stmts.into_iter().next().unwrap()).expect("exec failed")
    }

    #[test]
    fn test_make_and_put_get() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, name text, age int)");
        exec(&e, r#"PUT users { id: 1, name: "Alice", age: 30 }"#);
        exec(&e, r#"PUT users { id: 2, name: "Bob", age: 25 }"#);

        match exec(&e, "GET users") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_get_with_filter() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, age int)");
        exec(&e, "PUT users { id: 1, age: 30 }");
        exec(&e, "PUT users { id: 2, age: 16 }");
        match exec(&e, "GET users WHERE age > 18") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_del() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key)");
        exec(&e, "PUT users { id: 1 }");
        exec(&e, "PUT users { id: 2 }");
        exec(&e, "DEL users WHERE id = 1");
        match exec(&e, "GET users") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_set_update() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, name text)");
        exec(&e, r#"PUT users { id: 1, name: "Alice" }"#);
        exec(&e, r#"SET users { name: "Alicia" } WHERE id = 1"#);
        match exec(&e, "GET users WHERE id = 1") {
            QueryResult::Rows { columns, rows, .. } => {
                let name_idx = columns.iter().position(|c| c == "name").unwrap();
                assert_eq!(rows[0][name_idx], Value::Text("Alicia".into()));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_find_fuzzy() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, name text)");
        exec(&e, r#"PUT users { id: 1, name: "Alice" }"#);
        exec(&e, r#"PUT users { id: 2, name: "Bob" }"#);
        match exec(&e, r#"FIND users WHERE name ~ "Alic""#) {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_limit() {
        let e = setup();
        exec(&e, "MAKE TABLE data (id int primary key)");
        for i in 1..=5 {
            exec(&e, &format!("PUT data {{ id: {i} }}"));
        }
        match exec(&e, "GET data LIMIT 3") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!(),
        }
    }

    // ── DDL ──────────────────────────────────────────────────────────────

    #[test]
    fn test_drop_table() {
        let e = setup();
        exec(&e, "MAKE TABLE tmp (id int primary key)");
        exec(&e, "DROP TABLE tmp");
        // table should no longer exist
        let stmts = Parser::parse_str("GET tmp").unwrap();
        let result = e.execute(stmts.into_iter().next().unwrap());
        assert!(result.is_err(), "expected error after DROP TABLE");
    }

    #[test]
    fn test_make_index_and_lookup() {
        let e = setup();
        exec(&e, "MAKE TABLE products (id int primary key, price float)");
        exec(&e, "PUT products { id: 1, price: 9.99 }");
        exec(&e, "PUT products { id: 2, price: 24.99 }");
        exec(&e, "MAKE INDEX ON products (price)");
        match exec(&e, "GET products WHERE price = 9.99") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    // ── Reading ───────────────────────────────────────────────────────────

    #[test]
    fn test_order_by_asc_desc() {
        let e = setup();
        exec(&e, "MAKE TABLE nums (id int primary key, val int)");
        exec(&e, "PUT nums { id: 1, val: 30 }");
        exec(&e, "PUT nums { id: 2, val: 10 }");
        exec(&e, "PUT nums { id: 3, val: 20 }");

        // ASC
        match exec(&e, "GET nums ORDER BY val ASC") {
            QueryResult::Rows { columns, rows, .. } => {
                let vi = columns.iter().position(|c| c == "val").unwrap();
                assert_eq!(rows[0][vi], Value::Int(10));
                assert_eq!(rows[2][vi], Value::Int(30));
            }
            _ => panic!(),
        }
        // DESC
        match exec(&e, "GET nums ORDER BY val DESC") {
            QueryResult::Rows { columns, rows, .. } => {
                let vi = columns.iter().position(|c| c == "val").unwrap();
                assert_eq!(rows[0][vi], Value::Int(30));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_compound_and_filter() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, age int, active bool)");
        exec(&e, "PUT users { id: 1, age: 30, active: true }");
        exec(&e, "PUT users { id: 2, age: 17, active: true }");
        exec(&e, "PUT users { id: 3, age: 25, active: false }");
        match exec(&e, "GET users WHERE age >= 18 AND active = true") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_or_filter() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, name text)");
        exec(&e, r#"PUT users { id: 1, name: "Alice" }"#);
        exec(&e, r#"PUT users { id: 2, name: "Bob" }"#);
        exec(&e, r#"PUT users { id: 3, name: "Carol" }"#);
        match exec(&e, r#"GET users WHERE name = "Alice" OR name = "Bob""#) {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn test_not_filter() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, active bool)");
        exec(&e, "PUT users { id: 1, active: true }");
        exec(&e, "PUT users { id: 2, active: false }");
        match exec(&e, "GET users WHERE NOT active = true") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_arithmetic_in_filter() {
        let e = setup();
        exec(&e, "MAKE TABLE products (id int primary key, price float)");
        exec(&e, "PUT products { id: 1, price: 20.0 }");
        exec(&e, "PUT products { id: 2, price: 30.0 }");
        // price * 1.2 < 30.0  →  price < 25.0  →  only id=1
        match exec(&e, "GET products WHERE price * 1.2 < 30.0") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_del_all_rows() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key)");
        exec(&e, "PUT t { id: 1 }");
        exec(&e, "PUT t { id: 2 }");
        exec(&e, "DEL t");
        match exec(&e, "GET t") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!(),
        }
    }

    #[test]
    fn test_set_all_rows() {
        let e = setup();
        exec(&e, "MAKE TABLE products (id int primary key, price float)");
        exec(&e, "PUT products { id: 1, price: 9.99 }");
        exec(&e, "PUT products { id: 2, price: 24.99 }");
        exec(&e, "SET products { price: 0.0 }");
        match exec(&e, "GET products") {
            QueryResult::Rows { columns, rows, .. } => {
                let pi = columns.iter().position(|c| c == "price").unwrap();
                assert!(rows.iter().all(|r| r[pi] == Value::Float(0.0)));
            }
            _ => panic!(),
        }
    }

    // ── Upsert ────────────────────────────────────────────────────────────

    #[test]
    fn test_put_upsert() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key, val int)");
        exec(&e, "PUT t { id: 1, val: 10 }");
        exec(&e, "PUT t { id: 1, val: 99 }"); // upsert
        match exec(&e, "GET t WHERE id = 1") {
            QueryResult::Rows { columns, rows, .. } => {
                assert_eq!(rows.len(), 1);
                let vi = columns.iter().position(|c| c == "val").unwrap();
                assert_eq!(rows[0][vi], Value::Int(99));
            }
            _ => panic!(),
        }
    }

    // ── Vector similarity search ──────────────────────────────────────────

    #[test]
    fn test_similar_returns_scores_sorted() {
        let e = setup();
        exec(&e, "MAKE TABLE items (id int primary key, label text, embedding vector)");
        exec(&e, r#"PUT items { id: 1, label: "cat",  embedding: [0.9, 0.1, 0.0] }"#);
        exec(&e, r#"PUT items { id: 2, label: "dog",  embedding: [0.8, 0.2, 0.1] }"#);
        exec(&e, r#"PUT items { id: 3, label: "fish", embedding: [0.1, 0.1, 0.9] }"#);

        match exec(&e, "SIMILAR items ON embedding TO [0.85, 0.15, 0.05] LIMIT 2") {
            QueryResult::Rows { columns, rows, .. } => {
                assert_eq!(rows.len(), 2, "should return at most LIMIT rows");
                let si = columns.iter().position(|c| c == "_score").unwrap();
                // scores must be descending
                let s0 = if let Value::Float(f) = rows[0][si] { f } else { panic!("expected float score") };
                let s1 = if let Value::Float(f) = rows[1][si] { f } else { panic!("expected float score") };
                assert!(s0 >= s1, "scores must be sorted descending: {s0} >= {s1}");
                assert!(s0 > 0.99, "top score vs near-identical vector should be very high: {s0}");
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_similar_default_limit() {
        let e = setup();
        exec(&e, "MAKE TABLE vecs (id int primary key, emb vector)");
        for i in 1..=15 {
            exec(&e, &format!("PUT vecs {{ id: {i}, emb: [{}.0, 0.0] }}", i));
        }
        // No LIMIT → default 10
        match exec(&e, "SIMILAR vecs ON emb TO [1.0, 0.0]") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 10),
            _ => panic!(),
        }
    }

    #[test]
    fn test_similar_score_column_present() {
        let e = setup();
        exec(&e, "MAKE TABLE v (id int primary key, emb vector)");
        exec(&e, "PUT v { id: 1, emb: [1.0, 0.0] }");
        match exec(&e, "SIMILAR v ON emb TO [1.0, 0.0] LIMIT 1") {
            QueryResult::Rows { columns, rows, .. } => {
                assert!(columns.contains(&"_score".to_string()));
                let si = columns.iter().position(|c| c == "_score").unwrap();
                if let Value::Float(s) = rows[0][si] {
                    assert!((s - 1.0_f64).abs() < 1e-5, "identical vector should score ~1.0, got {s}");
                } else {
                    panic!("_score is not a Float");
                }
            }
            _ => panic!(),
        }
    }

    // ── WATCH notifications ───────────────────────────────────────────────

    #[test]
    fn test_watch_insert_notification() {
        use crate::engine::watch::WatchRegistry;
        use crate::engine::watch::WatchOp;

        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        let wr = Arc::new(WatchRegistry::new());
        let mut e = Executor::new(db, metrics);
        e.watch_registry = Some(Arc::clone(&wr));

        exec(&e, "MAKE TABLE users (id int primary key, name text, active bool)");

        // Subscribe with a filter
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let id = wr.subscribe("users".to_string(), None, tx);
        assert!(id > 0);

        exec(&e, r#"PUT users { id: 1, name: "Alice", active: true }"#);

        let evt = rx.try_recv().expect("expected a watch event after PUT");
        assert!(matches!(evt.op, WatchOp::Insert));
        assert_eq!(evt.watch_id, id);
    }

    #[test]
    fn test_watch_update_notification() {
        use crate::engine::watch::WatchRegistry;
        use crate::engine::watch::WatchOp;

        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        let wr = Arc::new(WatchRegistry::new());
        let mut e = Executor::new(db, metrics);
        e.watch_registry = Some(Arc::clone(&wr));

        exec(&e, "MAKE TABLE users (id int primary key, age int)");
        exec(&e, "PUT users { id: 1, age: 30 }");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        wr.subscribe("users".to_string(), None, tx);

        exec(&e, "SET users { age: 31 } WHERE id = 1");

        let evt = rx.try_recv().expect("expected watch event after SET");
        assert!(matches!(evt.op, WatchOp::Update));
    }

    #[test]
    fn test_watch_delete_notification() {
        use crate::engine::watch::WatchRegistry;
        use crate::engine::watch::WatchOp;

        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        let wr = Arc::new(WatchRegistry::new());
        let mut e = Executor::new(db, metrics);
        e.watch_registry = Some(Arc::clone(&wr));

        exec(&e, "MAKE TABLE users (id int primary key)");
        exec(&e, "PUT users { id: 1 }");

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        wr.subscribe("users".to_string(), None, tx);

        exec(&e, "DEL users WHERE id = 1");

        let evt = rx.try_recv().expect("expected watch event after DEL");
        assert!(matches!(evt.op, WatchOp::Delete));
    }

    #[test]
    fn test_watch_unsubscribe_no_more_events() {
        use crate::engine::watch::WatchRegistry;

        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        let wr = Arc::new(WatchRegistry::new());
        let mut e = Executor::new(db, metrics);
        e.watch_registry = Some(Arc::clone(&wr));

        exec(&e, "MAKE TABLE t (id int primary key)");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let id = wr.subscribe("t".to_string(), None, tx);
        wr.unsubscribe(id);

        exec(&e, "PUT t { id: 1 }");
        assert!(rx.try_recv().is_err(), "should receive no events after unsubscribe");
    }

    // ── CLUSTER commands ──────────────────────────────────────────────────

    #[test]
    fn test_cluster_join_part_status() {
        use crate::cluster::ClusterRegistry;

        let db = Arc::new(Database::new());
        let metrics = Arc::new(Metrics::new());
        let cr = Arc::new(ClusterRegistry::new());
        let mut e = Executor::new(db, metrics);
        e.cluster_registry = Some(Arc::clone(&cr));

        // JOIN
        match exec(&e, r#"CLUSTER JOIN "192.168.1.20:7878""#) {
            QueryResult::Ok { message, .. } => assert!(message.contains("192.168.1.20:7878")),
            _ => panic!("expected Ok"),
        }

        // STATUS — one row with reachable = false (never probed)
        match exec(&e, "CLUSTER STATUS") {
            QueryResult::Rows { columns, rows, .. } => {
                assert_eq!(rows.len(), 1);
                let ai = columns.iter().position(|c| c == "addr").unwrap();
                let ri = columns.iter().position(|c| c == "reachable").unwrap();
                assert_eq!(rows[0][ai], Value::Text("192.168.1.20:7878".into()));
                assert_eq!(rows[0][ri], Value::Bool(false));
            }
            _ => panic!("expected Rows"),
        }

        // PART
        match exec(&e, r#"CLUSTER PART "192.168.1.20:7878""#) {
            QueryResult::Ok { message, .. } => assert!(message.contains("192.168.1.20:7878")),
            _ => panic!(),
        }

        // STATUS — now empty
        match exec(&e, "CLUSTER STATUS") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!(),
        }
    }

    #[test]
    fn test_cluster_status_no_registry() {
        let e = setup(); // no cluster_registry
        // CLUSTER STATUS should return empty rows (not an error) when registry is None
        match exec(&e, "CLUSTER STATUS") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected empty Rows"),
        }
    }

    #[test]
    fn test_cluster_join_no_registry_is_error() {
        let e = setup(); // no cluster_registry
        let stmts = Parser::parse_str(r#"CLUSTER JOIN "1.2.3.4:7878""#).unwrap();
        let result = e.execute(stmts.into_iter().next().unwrap());
        assert!(result.is_err(), "JOIN without registry should be an error");
    }

    // ── Admin commands ────────────────────────────────────────────────────

    #[test]
    fn test_explain_returns_plan() {
        let e = setup();
        exec(&e, "MAKE TABLE users (id int primary key, age int)");
        match exec(&e, "EXPLAIN GET users WHERE age > 25 ORDER BY age DESC LIMIT 10") {
            QueryResult::Plan { description } => {
                assert!(!description.is_empty());
            }
            _ => panic!("expected Plan"),
        }
    }

    #[test]
    fn test_show_running_queries_and_kill() {
        let e = setup();
        // SHOW RUNNING QUERIES returns Rows (may be empty during unit test)
        match exec(&e, "SHOW RUNNING QUERIES") {
            QueryResult::Rows { columns, .. } => {
                assert!(columns.contains(&"id".to_string()));
                assert!(columns.contains(&"query".to_string()));
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_begin_commit_rollback_ok() {
        let e = setup();
        // These are acknowledged at the executor level (auto-commit lite mode)
        match exec(&e, "BEGIN") {
            QueryResult::Ok { .. } => {}
            _ => panic!(),
        }
        match exec(&e, "COMMIT") {
            QueryResult::Ok { .. } => {}
            _ => panic!(),
        }
        match exec(&e, "ROLLBACK") {
            QueryResult::Ok { .. } => {}
            _ => panic!(),
        }
    }

    // ── Index range scans ─────────────────────────────────────────────────

    #[test]
    fn test_index_range_scan_gt() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key, val int)");
        exec(&e, "PUT t { id: 1, val: 10 }");
        exec(&e, "PUT t { id: 2, val: 20 }");
        exec(&e, "PUT t { id: 3, val: 30 }");
        exec(&e, "MAKE INDEX ON t (val)");
        match exec(&e, "GET t WHERE val > 15") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn test_index_range_scan_le() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key, val int)");
        exec(&e, "PUT t { id: 1, val: 10 }");
        exec(&e, "PUT t { id: 2, val: 20 }");
        exec(&e, "PUT t { id: 3, val: 30 }");
        exec(&e, "MAKE INDEX ON t (val)");
        match exec(&e, "GET t WHERE val <= 20") {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!(),
        }
    }

    // ── Null handling ─────────────────────────────────────────────────────

    #[test]
    fn test_get_or_null_missing_column() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key, name text)");
        exec(&e, "PUT t { id: 1 }"); // no name
        match exec(&e, "GET t WHERE id = 1") {
            QueryResult::Rows { columns, rows, .. } => {
                let ni = columns.iter().position(|c| c == "name").unwrap();
                assert_eq!(rows[0][ni], Value::Null);
            }
            _ => panic!(),
        }
    }

    // ── Fuzzy search (FIND) ───────────────────────────────────────────────

    #[test]
    fn test_find_partial_match() {
        let e = setup();
        exec(&e, "MAKE TABLE products (id int primary key, name text)");
        exec(&e, r#"PUT products { id: 1, name: "Widget" }"#);
        exec(&e, r#"PUT products { id: 2, name: "Gadget" }"#);
        match exec(&e, r#"FIND products WHERE name ~ "widge" LIMIT 5"#) {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn test_find_no_match() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key, name text)");
        exec(&e, r#"PUT t { id: 1, name: "Elephant" }"#);
        match exec(&e, r#"FIND t WHERE name ~ "xyz""#) {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!(),
        }
    }

    // ── Timeout ───────────────────────────────────────────────────────────

    #[test]
    fn test_get_timeout_syntax_accepted() {
        let e = setup();
        exec(&e, "MAKE TABLE t (id int primary key)");
        exec(&e, "PUT t { id: 1 }");
        // Timeout must be a quoted duration string e.g. "5s"
        match exec(&e, r#"GET t WHERE id = 1 TIMEOUT "5s""#) {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }
}

