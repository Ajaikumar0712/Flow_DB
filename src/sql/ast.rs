use serde::{Deserialize, Serialize};

/// A column reference, possibly qualified: `table.column` or just `column`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn simple(column: impl Into<String>) -> Self {
        Self { table: None, column: column.into() }
    }
}

// ── Literal values ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
    /// A floating-point vector literal, e.g. `[0.1, 0.9, 0.3]`
    Vector(Vec<f32>),
}

// ── Expression tree ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),
    Column(ColumnRef),

    // Binary operations
    BinOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },

    // Unary NOT
    Not(Box<Expr>),

    // Fuzzy match: `col ~ "pattern"`
    Fuzzy { column: ColumnRef, pattern: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinOp {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
}

// ── Column definition (used in MAKE TABLE) ────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String, // e.g. "int", "text", "float", "bool", "json", "blob"
    pub nullable: bool,
    pub primary_key: bool,
}

// ── ORDER BY clause ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub column: ColumnRef,
    pub ascending: bool,
}

// ── JOIN clause ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub kind:  JoinKind,
    pub table: String,
    /// The ON predicate — references columns from both tables.
    pub on:    Expr,
}

// ── GROUP BY / aggregate clause ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum   => "sum",
            AggFunc::Avg   => "avg",
            AggFunc::Min   => "min",
            AggFunc::Max   => "max",
        }
    }
}

/// A single aggregate function application within a GROUP BY clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub func:   AggFunc,
    /// `None` means COUNT(*) — count all rows.
    pub column: Option<ColumnRef>,
    /// Output column name (auto-generated or AS alias).
    pub alias:  String,
}

/// GROUP BY clause, including optional aggregate functions and HAVING filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupByClause {
    pub columns:    Vec<ColumnRef>,
    pub aggregates: Vec<AggregateExpr>,
    /// Post-aggregation filter (refers to output column names produced above).
    pub having:     Option<Expr>,
}

// ── Top-level statements ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stmt {
    /// GET <table> [JOIN <t2> ON <expr>] [WHERE <expr>] [GROUP BY <cols> [agg, …]] [HAVING <expr>] [ORDER BY <col>] [LIMIT n] [TIMEOUT ms]
    Get {
        table:     String,
        join:      Option<JoinClause>,
        filter:    Option<Expr>,
        group_by:  Option<GroupByClause>,
        order_by:  Option<OrderBy>,
        limit:     Option<u64>,
        timeout_ms: Option<u64>,
    },

    /// PUT <table> { col: val, … }
    Put {
        table: String,
        fields: Vec<(String, Expr)>,
    },

    /// SET <table> { col: val, … } WHERE <expr>
    Set {
        table: String,
        fields: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },

    /// DEL <table> WHERE <expr>
    Del {
        table: String,
        filter: Option<Expr>,
    },

    /// FIND <table> WHERE <col> ~ "<pattern>" [LIMIT <n>]
    Find {
        table: String,
        column: ColumnRef,
        pattern: String,
        limit: Option<u64>,
    },

    /// MAKE TABLE <name> ( col type, … )
    MakeTable {
        name: String,
        columns: Vec<ColumnDef>,
    },

    /// DROP TABLE <name>
    DropTable {
        name: String,
    },

    /// MAKE INDEX ON <table>(<col>)
    MakeIndex {
        table: String,
        column: String,
    },

    /// Transaction control
    Begin,
    Commit,
    Rollback,

    /// SHOW RUNNING QUERIES
    ShowRunningQueries,

    /// SHOW TABLES
    ShowTables,

    /// KILL QUERY <id>
    KillQuery { id: u64 },

    /// EXPLAIN <stmt>
    Explain(Box<Stmt>),

    /// CHECKPOINT — flush all table data to disk snapshots
    Checkpoint,

    /// WATCH <table> [WHERE <expr>] — push-based subscription
    Watch { table: String, filter: Option<Expr> },

    /// UNWATCH <id> — cancel a subscription
    Unwatch { id: u64 },

    /// SIMILAR <table> TO [f, ...] [ON <col>] [LIMIT n] — vector similarity
    Similar {
        table: String,
        vector: Vec<f32>,
        column: Option<String>,
        limit: Option<u64>,
    },

    /// CLUSTER STATUS
    ClusterStatus,
    /// CLUSTER JOIN "<addr>"
    ClusterJoin { addr: String },
    /// CLUSTER PART "<addr>"
    ClusterPart { addr: String },
    /// CLUSTER SHARD STATUS
    ClusterShardStatus,
    /// CLUSTER SHARD ASSIGN <table> SHARDS <n> [NODES "n1","n2",...]
    ClusterShardAssign { table: String, shards: u32, nodes: Vec<String> },
    /// CLUSTER SHARD DROP <table>
    ClusterShardDrop { table: String },

    // ── Auth ──────────────────────────────────────────────────────────────

    /// AUTH <user> '<password>'  — establish a session
    Auth { username: String, password: String },
    /// CREATE USER <name> PASSWORD '<pwd>' [ADMIN]
    CreateUser { username: String, password: String, is_admin: bool },
    /// DROP USER <name>
    DropUser { username: String },
    /// GRANT <op> ON <table|*> TO <user>
    Grant { username: String, op: String, table: Option<String> },
    /// REVOKE <op> ON <table|*> FROM <user>
    Revoke { username: String, op: String, table: Option<String> },
    /// SHOW USERS
    ShowUsers,

    // ── Config ────────────────────────────────────────────────────────────

    /// CONFIG SET <key> <value>
    ConfigSet { key: String, value: String },
    /// SHOW CONFIG
    ShowConfig,

    // ── AI Search ─────────────────────────────────────────────────────────

    /// AI SEARCH <table> "<query>" [LIMIT n]
    AiSearch {
        table: String,
        query: String,
        limit: Option<u64>,
    },

    // ── Time Travel ───────────────────────────────────────────────────────

    /// GET <table> AS OF "<timestamp>" [WHERE …] [ORDER BY …] [LIMIT n]
    GetAsOf {
        table: String,
        timestamp: String,
        filter: Option<Expr>,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
    },

    /// GET <table> VERSION <n> [WHERE …] [ORDER BY …] [LIMIT n]
    GetVersion {
        table: String,
        version: u64,
        filter: Option<Expr>,
        order_by: Option<OrderBy>,
        limit: Option<u64>,
    },

    // ── Triggers ─────────────────────────────────────────────────────────

    /// TRIGGER <name> WHEN PUT|SET|DEL <table> DO <query>
    CreateTrigger {
        name: String,
        event: String,
        table: String,
        do_query: String,
    },

    /// DROP TRIGGER <name>
    DropTrigger { name: String },

    /// SHOW TRIGGERS
    ShowTriggers,

    // ── Graph ─────────────────────────────────────────────────────────────

    /// GRAPH MATCH (a:src_table) -[e:edge_table]-> (b:dst_table) [WHERE …] [LIMIT n]
    GraphMatch {
        src_alias: String,
        src_table: String,
        edge_alias: String,
        edge_table: String,
        dst_alias: String,
        dst_table: String,
        filter: Option<Expr>,
        limit: Option<u64>,
    },

    // ── REST API ──────────────────────────────────────────────────────────

    /// API GENERATE FOR <table>
    ApiGenerate { table: String },

    /// API STOP FOR <table>
    ApiStop { table: String },

    /// SHOW APIS
    ShowApis,
}
