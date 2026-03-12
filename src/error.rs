use thiserror::Error;

/// All errors that FlowDB can produce.
#[derive(Debug, Error, Clone)]
#[allow(dead_code)]
pub enum FlowError {
    // ── Query language errors ─────────────────────────────────────────────
    #[error("parse error: {0}")]
    Parse(String),

    // ── Type / evaluation errors ──────────────────────────────────────────
    #[error("type error: {0}")]
    Type(String),

    // ── Storage / table errors ────────────────────────────────────────────
    #[error("table `{0}` not found")]
    TableNotFound(String),

    #[error("table `{0}` already exists")]
    TableAlreadyExists(String),

    #[error("column `{0}` not found in table `{1}`")]
    ColumnNotFound(String, String),

    #[error("row not found")]
    RowNotFound,

    // ── Index errors ──────────────────────────────────────────────────────
    #[error("index on `{table}.{column}` already exists")]
    IndexAlreadyExists { table: String, column: String },

    // ── Transaction errors ────────────────────────────────────────────────
    #[error("no active transaction — use BEGIN first")]
    NoActiveTx,

    #[error("transaction already active — commit or rollback first")]
    TxAlreadyActive,

    #[error("transaction {0} not found")]
    TxNotFound(u64),

    // ── WAL / IO errors ───────────────────────────────────────────────────
    #[error("WAL error: {0}")]
    Wal(String),

    #[error("IO error: {0}")]
    Io(String),

    // ── Query limits / timeouts ───────────────────────────────────────────
    #[error("query timed out after {ms}ms")]
    Timeout { ms: u64 },

    #[error("result set exceeds max_result_rows limit ({0} rows)")]
    ResultTooLarge(usize),

    #[error("query {0} not found or already finished")]
    QueryNotFound(u64),

    // ── General / internal ────────────────────────────────────────────────
    #[error("internal error: {0}")]
    Internal(String),

    // ── Authentication / authorization ────────────────────────────────────
    #[error("auth error: {0}")]
    Auth(String),

    // ── Resource limits ───────────────────────────────────────────────────
    #[error("resource limit: {0}")]
    ResourceLimit(String),
}

impl FlowError {
    pub fn parse(msg: impl Into<String>) -> Self {
        FlowError::Parse(msg.into())
    }
    pub fn type_err(msg: impl Into<String>) -> Self {
        FlowError::Type(msg.into())
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        FlowError::Internal(msg.into())
    }
    pub fn auth(msg: impl Into<String>) -> Self {
        FlowError::Auth(msg.into())
    }
    pub fn resource(msg: impl Into<String>) -> Self {
        FlowError::ResourceLimit(msg.into())
    }
}

/// Convert std::io::Error → FlowError.
impl From<std::io::Error> for FlowError {
    fn from(e: std::io::Error) -> Self {
        FlowError::Io(e.to_string())
    }
}

/// Convert serde_json error → FlowError.
impl From<serde_json::Error> for FlowError {
    fn from(e: serde_json::Error) -> Self {
        FlowError::Io(format!("json error: {e}"))
    }
}
