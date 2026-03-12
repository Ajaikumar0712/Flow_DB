/// All tokens produced by the FlowQL lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── DML keywords ──────────────────────────────────────────────────────
    Get,    // GET  — read rows
    Put,    // PUT  — insert a row
    Set,    // SET  — update fields
    Del,    // DEL  — delete rows
    Find,   // FIND — fuzzy / ranked search

    // ── DDL keywords ──────────────────────────────────────────────────────
    Make,   // MAKE TABLE …
    Drop,   // DROP TABLE …

    // ── Clause keywords ───────────────────────────────────────────────────
    Where,
    From,
    Into,
    Values,
    Table,
    Index,
    On,
    Limit,
    Order,
    By,
    Asc,
    Desc,
    Timeout,     // query timeout clause

    // ── Transaction keywords ──────────────────────────────────────────────
    Begin,
    Commit,
    Rollback,

    // ── Admin / monitoring keywords ────────────────────────────────────────
    Show,
    Kill,
    Query,       // SHOW RUNNING QUERIES / KILL QUERY <id>
    Running,
    Queries,
    Tables,      // SHOW TABLES
    Explain,     // EXPLAIN <query>
    Checkpoint,  // CHECKPOINT — flush all table data to disk

    // ── Streaming / vector / cluster keywords ──────────────────────────────
    Watch,       // WATCH — push-based query subscription
    Unwatch,     // UNWATCH <id>
    Similar,     // SIMILAR — vector (cosine) similarity search
    To,          // TO (used in SIMILAR ... TO [v, ...])
    Cluster,     // CLUSTER — cluster management
    Status,      // STATUS (used in CLUSTER STATUS)
    Shard,       // SHARD (used in CLUSTER SHARD)

    // ── JOIN keywords ─────────────────────────────────────────────────────
    Join,        // JOIN
    Inner,       // INNER (optional qualifier: INNER JOIN)
    Left,        // LEFT  (LEFT JOIN)
    Right,       // RIGHT (RIGHT JOIN)

    // ── GROUP BY / aggregation keywords ───────────────────────────────────
    Group,       // GROUP (GROUP BY)
    Having,      // HAVING
    Count,       // COUNT aggregate function
    Sum,         // SUM   aggregate function
    Avg,         // AVG   aggregate function
    Min,         // MIN   aggregate function
    Max,         // MAX   aggregate function
    As,          // AS alias

    // ── Auth keywords ─────────────────────────────────────────────────────
    Create,      // CREATE USER
    User,        // USER
    Users,       // USERS (SHOW USERS)
    Password,    // PASSWORD
    Grant,       // GRANT
    Revoke,      // REVOKE
    Auth,        // AUTH (login)
    Admin,       // ADMIN (CREATE ADMIN USER)

    // ── Config keywords ───────────────────────────────────────────────────
    Config,      // CONFIG SET / SHOW

    // ── AI / Search keywords ──────────────────────────────────────────────
    Ai,          // AI (AI SEARCH)
    Search,      // SEARCH
    Version,     // VERSION (GET … VERSION n)
    Of,          // OF (AS OF)

    // ── Trigger keywords ──────────────────────────────────────────────────
    Trigger,     // TRIGGER
    When,        // WHEN
    Do,          // DO

    // ── Graph keywords ────────────────────────────────────────────────────
    Graph,       // GRAPH
    Match,       // MATCH

    // ── REST API keywords ─────────────────────────────────────────────────
    Api,         // API
    Generate,    // GENERATE
    For,         // FOR
    Stop,        // STOP

    // ── Logical operators ─────────────────────────────────────────────────
    And,
    Or,
    Not,

    // ── Comparison operators ──────────────────────────────────────────────
    Eq,   // =
    Ne,   // !=
    Lt,   // <
    Le,   // <=
    Gt,   // >
    Ge,   // >=
    Tilde, // ~  (fuzzy match)

    // ── Arithmetic operators ───────────────────────────────────────────────
    Plus,
    Minus,
    Star,
    Slash,

    // ── Punctuation ───────────────────────────────────────────────────────
    LParen,   // (
    RParen,   // )
    LBrace,   // {
    RBrace,   // }
    LBracket, // [
    RBracket, // ]
    Comma,
    Semicolon,
    Colon,
    Dot,

    // ── Literals ──────────────────────────────────────────────────────────
    IntLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),
    BoolLiteral(bool),
    Null,

    // ── Identifier ────────────────────────────────────────────────────────
    Ident(String),

    // ── End of input ──────────────────────────────────────────────────────
    Eof,
}

impl Token {
    /// Return a human-readable display name (used in error messages).
    pub fn display(&self) -> String {
        match self {
            Token::Ident(s) => format!("`{s}`"),
            Token::StringLiteral(s) => format!("\"{s}\""),
            Token::IntLiteral(n) => n.to_string(),
            Token::FloatLiteral(f) => f.to_string(),
            Token::BoolLiteral(b) => b.to_string(),
            Token::Null => "null".into(),
            Token::Eof => "<end of input>".into(),
            _ => format!("{:?}", self).to_lowercase(),
        }
    }
}
