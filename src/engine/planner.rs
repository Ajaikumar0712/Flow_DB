//! Query Planner — converts a parsed FlowQL AST into an executable query plan.
//!
//! Pipeline:
//!   Stmt (AST)  →  Planner  →  PlanNode  →  Executor
//!
//! The planner's job:
//!   1. Inspect the WHERE filter for index-usable predicates.
//!   2. Choose the cheapest scan strategy (FullScan, IndexLookup, IndexRangeScan).
//!   3. Wrap scans in Sort and Limit nodes as needed.
//!   4. Produce a human-readable description for EXPLAIN.

use std::fmt;
use std::sync::Arc;

use crate::sql::ast::{BinOp, Expr, Literal, OrderBy, Stmt};
use crate::storage::table::Database;
use crate::types::Value;

// ── Range operator ────────────────────────────────────────────────────────

/// Comparison operator used in an index range scan.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl fmt::Display for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Eq => write!(f, "="),
            RangeOp::Lt => write!(f, "<"),
            RangeOp::Le => write!(f, "<="),
            RangeOp::Gt => write!(f, ">"),
            RangeOp::Ge => write!(f, ">="),
        }
    }
}

// ── Scan strategy ─────────────────────────────────────────────────────────

/// The data-access method chosen for a table scan.
#[derive(Debug, Clone)]
pub enum ScanStrategy {
    /// Read every row — O(n).
    FullScan { estimated_rows: usize },
    /// Exact equality look-up via B-tree index — O(log n + k).
    IndexLookup { column: String, value: Value },
    /// Range scan via B-tree index — O(log n + k).
    IndexRangeScan { column: String, op: RangeOp, value: Value },
}

impl fmt::Display for ScanStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanStrategy::FullScan { estimated_rows } => {
                write!(f, "FullScan (est. {} rows)", estimated_rows)
            }
            ScanStrategy::IndexLookup { column, value } => {
                write!(f, "IndexLookup({} = {})", column, value)
            }
            ScanStrategy::IndexRangeScan { column, op, value } => {
                write!(f, "IndexRangeScan({} {} {})", column, op, value)
            }
        }
    }
}

// ── Plan node ─────────────────────────────────────────────────────────────

/// A node in the query plan tree.
///
/// The tree is evaluated bottom-up: leaf nodes produce rows; wrapper nodes
/// transform them.
#[derive(Debug, Clone)]
pub enum PlanNode {
    // ── Data source ──────────────────────────────────────────────────────
    /// Scan a table using the chosen strategy, then apply any residual filter.
    Scan {
        table: String,
        strategy: ScanStrategy,
        /// Residual predicate applied after index narrowing (may be full WHERE
        /// if no index was used, or a subset when partial index match).
        residual_filter: Option<Expr>,
    },

    // ── Transformations ──────────────────────────────────────────────────
    Sort {
        input: Box<PlanNode>,
        column: String,
        ascending: bool,
    },
    Limit {
        input: Box<PlanNode>,
        count: usize,
    },

    // ── Mutations ─────────────────────────────────────────────────────────
    Insert { table: String },
    Update { table: String, scan: ScanStrategy },
    Delete { table: String, scan: ScanStrategy },

    // ── Special ───────────────────────────────────────────────────────────
    FuzzySearch { table: String, column: String, pattern: String, limit: usize },
    CreateTable { name: String },
    DropTable   { name: String },
    CreateIndex { table: String, column: String },
    Transaction { command: &'static str },
    ShowRunningQueries,
    KillQuery { id: u64 },
}

// ── Planner ───────────────────────────────────────────────────────────────

/// Produces a `PlanNode` tree for a parsed statement.
pub struct Planner {
    db: Arc<Database>,
}

impl Planner {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Plan any statement.
    pub fn plan(&self, stmt: &Stmt) -> PlanNode {
        match stmt {
            Stmt::Get { table, filter, order_by, limit, .. } => {
                self.plan_get(table, filter.as_ref(), order_by.as_ref(), *limit)
            }
            Stmt::Put { table, .. } => PlanNode::Insert { table: table.clone() },
            Stmt::Set { table, filter, .. } => {
                let scan = self.choose_scan(table, filter.as_ref());
                PlanNode::Update { table: table.clone(), scan }
            }
            Stmt::Del { table, filter } => {
                let scan = self.choose_scan(table, filter.as_ref());
                PlanNode::Delete { table: table.clone(), scan }
            }
            Stmt::Find { table, column, pattern, limit } => PlanNode::FuzzySearch {
                table:   table.clone(),
                column:  column.column.clone(),
                pattern: pattern.clone(),
                limit:   limit.map(|l| l as usize).unwrap_or(usize::MAX),
            },
            Stmt::MakeTable  { name, .. }    => PlanNode::CreateTable { name: name.clone() },
            Stmt::DropTable  { name }        => PlanNode::DropTable   { name: name.clone() },
            Stmt::MakeIndex  { table, column } => PlanNode::CreateIndex {
                table: table.clone(), column: column.clone(),
            },
            Stmt::Begin    => PlanNode::Transaction { command: "BEGIN" },
            Stmt::Commit   => PlanNode::Transaction { command: "COMMIT" },
            Stmt::Rollback => PlanNode::Transaction { command: "ROLLBACK" },
            Stmt::ShowRunningQueries => PlanNode::ShowRunningQueries,
            Stmt::ShowTables          => PlanNode::Transaction { command: "SHOW TABLES" },
            Stmt::KillQuery { id }    => PlanNode::KillQuery { id: *id },
            Stmt::Explain(_)          => PlanNode::Transaction { command: "EXPLAIN" },
            Stmt::Checkpoint          => PlanNode::Transaction { command: "CHECKPOINT" },
            Stmt::Watch { table: _, .. } => PlanNode::Transaction { command: "WATCH" },
            Stmt::Unwatch { .. }      => PlanNode::Transaction { command: "UNWATCH" },
            Stmt::Similar { table, limit, .. } => PlanNode::FuzzySearch {
                table: table.clone(),
                column: "<vector>".into(),
                pattern: "cosine_similarity".into(),
                limit: limit.map(|l| l as usize).unwrap_or(10),
            },
            Stmt::ClusterStatus       => PlanNode::Transaction { command: "CLUSTER STATUS" },
            Stmt::ClusterJoin { .. }  => PlanNode::Transaction { command: "CLUSTER JOIN" },
            Stmt::ClusterPart { .. }  => PlanNode::Transaction { command: "CLUSTER PART" },
            Stmt::ClusterShardStatus         => PlanNode::Transaction { command: "CLUSTER SHARD STATUS" },
            Stmt::ClusterShardAssign { .. }  => PlanNode::Transaction { command: "CLUSTER SHARD ASSIGN" },
            Stmt::ClusterShardDrop { .. }    => PlanNode::Transaction { command: "CLUSTER SHARD DROP" },
            Stmt::Auth { .. }                => PlanNode::Transaction { command: "AUTH" },
            Stmt::CreateUser { .. }          => PlanNode::Transaction { command: "CREATE USER" },
            Stmt::DropUser { .. }            => PlanNode::Transaction { command: "DROP USER" },
            Stmt::Grant { .. }               => PlanNode::Transaction { command: "GRANT" },
            Stmt::Revoke { .. }              => PlanNode::Transaction { command: "REVOKE" },
            Stmt::ShowUsers                  => PlanNode::Transaction { command: "SHOW USERS" },
            Stmt::ConfigSet { .. }           => PlanNode::Transaction { command: "CONFIG SET" },
            Stmt::ShowConfig                 => PlanNode::Transaction { command: "SHOW CONFIG" },
            // ── AI / Time Travel / Triggers / Graph / API ─────────────────
            Stmt::AiSearch { .. }            => PlanNode::Transaction { command: "AI SEARCH" },
            Stmt::GetAsOf { .. }             => PlanNode::Transaction { command: "GET AS OF" },
            Stmt::GetVersion { .. }          => PlanNode::Transaction { command: "GET VERSION" },
            Stmt::CreateTrigger { .. }       => PlanNode::Transaction { command: "TRIGGER" },
            Stmt::DropTrigger { .. }         => PlanNode::Transaction { command: "DROP TRIGGER" },
            Stmt::ShowTriggers               => PlanNode::Transaction { command: "SHOW TRIGGERS" },
            Stmt::GraphMatch { .. }          => PlanNode::Transaction { command: "GRAPH MATCH" },
            Stmt::ApiGenerate { .. }         => PlanNode::Transaction { command: "API GENERATE" },
            Stmt::ApiStop { .. }             => PlanNode::Transaction { command: "API STOP" },
            Stmt::ShowApis                   => PlanNode::Transaction { command: "SHOW APIS" },
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    fn plan_get(
        &self,
        table: &str,
        filter: Option<&Expr>,
        order_by: Option<&OrderBy>,
        limit: Option<u64>,
    ) -> PlanNode {
        let strategy = self.choose_scan(table, filter);

        // If we're using an index, the filter may still have parts that can't
        // be served by the index — keep them as the residual filter.
        let residual = match &strategy {
            ScanStrategy::FullScan { .. } => filter.cloned(),
            // Index narrows candidates; still run the full predicate as residual
            // to guarantee correctness (handles compound ANDs, etc.).
            ScanStrategy::IndexLookup { .. } | ScanStrategy::IndexRangeScan { .. } => {
                filter.cloned()
            }
        };

        let mut node = PlanNode::Scan {
            table: table.to_string(),
            strategy,
            residual_filter: residual,
        };

        if let Some(ob) = order_by {
            node = PlanNode::Sort {
                input: Box::new(node),
                column: ob.column.column.clone(),
                ascending: ob.ascending,
            };
        }

        if let Some(n) = limit {
            node = PlanNode::Limit {
                input: Box::new(node),
                count: n as usize,
            };
        }

        node
    }

    /// Choose the cheapest scan strategy for a table given an optional WHERE.
    ///
    /// Uses cost-based statistics when available:
    /// * Full scan cost   ≈ row_count  (read every row)
    /// * Index scan cost  ≈ log(row_count) + selectivity * row_count
    ///
    /// Falls back to FullScan if no indexed predicate is found, or if the
    /// index selectivity is poor (the index would read nearly every row anyway).
    pub fn choose_scan(&self, table: &str, filter: Option<&Expr>) -> ScanStrategy {
        let stats = self.table_stats(table);
        let estimated_rows = stats.row_count;

        let Some(filter) = filter else {
            return ScanStrategy::FullScan { estimated_rows };
        };

        let indexed_cols = self.indexed_columns(table);
        if indexed_cols.is_empty() {
            return ScanStrategy::FullScan { estimated_rows };
        }

        if let Some((col, op, val)) = Self::find_index_predicate(filter, &indexed_cols) {
            // Cost-based decision using histogram selectivity when available.
            let sel = match &op {
                RangeOp::Eq => stats.selectivity_eq(&col),
                other => stats.selectivity_range(&col, other, &val),
            };
            let index_rows = (estimated_rows as f64 * sel).ceil() as usize;
            let index_cost = (estimated_rows as f64).log2().max(1.0) as usize + index_rows;

            if estimated_rows > 0 && index_cost >= estimated_rows {
                // Full scan is cheaper.
                return ScanStrategy::FullScan { estimated_rows };
            }

            match op {
                RangeOp::Eq => ScanStrategy::IndexLookup { column: col, value: val },
                other       => ScanStrategy::IndexRangeScan { column: col, op: other, value: val },
            }
        } else {
            ScanStrategy::FullScan { estimated_rows }
        }
    }

    /// Walk `expr` looking for the first conjunct of the form `indexed_col OP literal`.
    /// Returns `(column, op, literal_value)` if found.
    fn find_index_predicate(
        expr: &Expr,
        indexed_cols: &[String],
    ) -> Option<(String, RangeOp, Value)> {
        match expr {
            Expr::BinOp { op, left, right } => {
                // Pattern: column OP literal
                if let (Expr::Column(col_ref), Expr::Literal(lit)) =
                    (left.as_ref(), right.as_ref())
                {
                    let col = &col_ref.column;
                    if indexed_cols.iter().any(|ic| ic == col) {
                        let range_op = match op {
                            BinOp::Eq => Some(RangeOp::Eq),
                            BinOp::Lt => Some(RangeOp::Lt),
                            BinOp::Le => Some(RangeOp::Le),
                            BinOp::Gt => Some(RangeOp::Gt),
                            BinOp::Ge => Some(RangeOp::Ge),
                            _         => None,
                        };
                        if let Some(rop) = range_op {
                            return Some((col.clone(), rop, literal_to_value(lit)));
                        }
                    }
                }
                // Pattern: literal OP column (reversed — normalise)
                if let (Expr::Literal(lit), Expr::Column(col_ref)) =
                    (left.as_ref(), right.as_ref())
                {
                    let col = &col_ref.column;
                    if indexed_cols.iter().any(|ic| ic == col) {
                        // Flip the operator: 5 > age  →  age < 5
                        let range_op = match op {
                            BinOp::Eq => Some(RangeOp::Eq),
                            BinOp::Lt => Some(RangeOp::Gt),
                            BinOp::Le => Some(RangeOp::Ge),
                            BinOp::Gt => Some(RangeOp::Lt),
                            BinOp::Ge => Some(RangeOp::Le),
                            _         => None,
                        };
                        if let Some(rop) = range_op {
                            return Some((col.clone(), rop, literal_to_value(lit)));
                        }
                    }
                }
                // AND: try both branches
                if matches!(op, BinOp::And) {
                    let l = Self::find_index_predicate(left, indexed_cols);
                    if l.is_some() { return l; }
                    return Self::find_index_predicate(right, indexed_cols);
                }
                None
            }
            _ => None,
        }
    }

    fn indexed_columns(&self, table: &str) -> Vec<String> {
        self.db
            .get_table(table)
            .ok()
            .map(|tbl| tbl.read().unwrap().index_names().iter().map(|s| s.to_string()).collect())
            .unwrap_or_default()
    }

    fn estimate_rows(&self, table: &str) -> usize {
        self.db
            .get_table(table)
            .ok()
            .map(|tbl| tbl.read().unwrap().row_count())
            .unwrap_or(0)
    }

    /// Return statistics for a table, or a zero-stats fallback.
    fn table_stats(&self, table: &str) -> crate::storage::table::TableStats {
        self.db
            .get_table(table)
            .ok()
            .map(|tbl| tbl.read().unwrap().statistics())
            .unwrap_or_else(|| crate::storage::table::TableStats {
                row_count: 0,
                column_ndv: std::collections::HashMap::new(),
                histograms: std::collections::HashMap::new(),
            })
    }
}

// ── helpers ───────────────────────────────────────────────────────────────

pub fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Int(n)   => Value::Int(*n),
        Literal::Float(f) => Value::Float(*f),
        Literal::Text(s)  => Value::Text(s.clone()),
        Literal::Bool(b)   => Value::Bool(*b),
        Literal::Null      => Value::Null,
        Literal::Vector(v) => Value::Vector(v.clone()),
    }
}

// ── Display ───────────────────────────────────────────────────────────────

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_node(self, f, 0)
    }
}

fn fmt_node(node: &PlanNode, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
    let pad = "  ".repeat(depth);
    match node {
        PlanNode::Scan { table, strategy, residual_filter } => {
            write!(f, "{pad}→ Scan `{table}` via {strategy}")?;
            if residual_filter.is_some() {
                write!(f, "\n{pad}  └─ residual Filter (WHERE)")?;
            }
            Ok(())
        }
        PlanNode::Sort { input, column, ascending } => {
            let dir = if *ascending { "ASC" } else { "DESC" };
            writeln!(f, "{pad}→ Sort by `{column}` {dir}")?;
            fmt_node(input, f, depth + 1)
        }
        PlanNode::Limit { input, count } => {
            writeln!(f, "{pad}→ Limit {count}")?;
            fmt_node(input, f, depth + 1)
        }
        PlanNode::Insert { table } => write!(f, "{pad}→ Insert into `{table}`"),
        PlanNode::Update { table, scan } => write!(f, "{pad}→ Update `{table}` via {scan}"),
        PlanNode::Delete { table, scan } => write!(f, "{pad}→ Delete from `{table}` via {scan}"),
        PlanNode::FuzzySearch { table, column, pattern, .. } => {
            write!(f, "{pad}→ FuzzySearch `{table}.{column}` ~ \"{pattern}\" (trigram)")
        }
        PlanNode::CreateTable { name } => write!(f, "{pad}→ CreateTable `{name}`"),
        PlanNode::DropTable   { name } => write!(f, "{pad}→ DropTable `{name}`"),
        PlanNode::CreateIndex { table, column } => {
            write!(f, "{pad}→ CreateIndex on `{table}.{column}`")
        }
        PlanNode::Transaction { command }  => write!(f, "{pad}→ {command}"),
        PlanNode::ShowRunningQueries       => write!(f, "{pad}→ ShowRunningQueries"),
        PlanNode::KillQuery { id }         => write!(f, "{pad}→ KillQuery({id})"),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::Parser;
    use crate::storage::table::Database;
    use crate::types::{ColumnSchema, DataType, TableSchema};

    fn make_db_with_index() -> Arc<Database> {
        let db = Arc::new(Database::new());
        let schema = TableSchema::new(
            "users",
            vec![
                ColumnSchema { name: "id".into(),  data_type: DataType::Int,  nullable: false, primary_key: true },
                ColumnSchema { name: "age".into(), data_type: DataType::Int,  nullable: true,  primary_key: false },
                ColumnSchema { name: "name".into(),data_type: DataType::Text, nullable: true,  primary_key: false },
            ],
        );
        db.create_table(schema).unwrap();
        // primary key auto-indexed; add one for age
        let tbl = db.get_table("users").unwrap();
        tbl.write().unwrap().create_index("age").unwrap();
        db
    }

    fn plan(db: Arc<Database>, query: &str) -> PlanNode {
        let stmt = Parser::parse_str(query).unwrap().into_iter().next().unwrap();
        Planner::new(db).plan(&stmt)
    }

    #[test]
    fn full_scan_without_filter() {
        let db = make_db_with_index();
        match plan(Arc::clone(&db), "GET users") {
            PlanNode::Scan { strategy: ScanStrategy::FullScan { .. }, .. } => {}
            other => panic!("expected FullScan, got {other:?}"),
        }
    }

    #[test]
    fn index_lookup_on_equality() {
        let db = make_db_with_index();
        match plan(Arc::clone(&db), "GET users WHERE age = 30") {
            PlanNode::Scan { strategy: ScanStrategy::IndexLookup { column, .. }, .. }
                if column == "age" => {}
            other => panic!("expected IndexLookup on age, got {other:?}"),
        }
    }

    #[test]
    fn index_range_scan_on_gt() {
        let db = make_db_with_index();
        match plan(Arc::clone(&db), "GET users WHERE age > 18") {
            PlanNode::Scan {
                strategy: ScanStrategy::IndexRangeScan { column, op: RangeOp::Gt, .. }, ..
            } if column == "age" => {}
            other => panic!("expected IndexRangeScan(>), got {other:?}"),
        }
    }

    #[test]
    fn limit_wraps_sort_wraps_scan() {
        let db = make_db_with_index();
        match plan(Arc::clone(&db), "GET users ORDER BY age DESC LIMIT 10") {
            PlanNode::Limit { input, count: 10 } => {
                match *input {
                    PlanNode::Sort { ascending: false, .. } => {}
                    other => panic!("expected Sort, got {other:?}"),
                }
            }
            other => panic!("expected Limit, got {other:?}"),
        }
    }

    #[test]
    fn primary_key_equality_uses_index() {
        let db = make_db_with_index();
        match plan(Arc::clone(&db), "GET users WHERE id = 1") {
            PlanNode::Scan { strategy: ScanStrategy::IndexLookup { column, .. }, .. }
                if column == "id" => {}
            other => panic!("expected IndexLookup on id, got {other:?}"),
        }
    }

    #[test]
    fn and_predicate_picks_first_indexed_conjunct() {
        let db = make_db_with_index();
        // age is indexed; name is not — planner should pick age
        match plan(Arc::clone(&db), r#"GET users WHERE age > 25 AND name = "Alice""#) {
            PlanNode::Scan { strategy: ScanStrategy::IndexRangeScan { column, .. }, .. }
                if column == "age" => {}
            other => panic!("expected IndexRangeScan on age, got {other:?}"),
        }
    }
}
