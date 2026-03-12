use crate::error::FlowError;
use crate::sql::ast::*;
use crate::sql::lexer::{Lexer, SpannedToken};
use crate::sql::token::Token;

/// Recursive-descent parser that converts `Vec<SpannedToken>` → `Vec<Stmt>`.
pub struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
    /// Original source kept for verbatim span extraction (e.g. trigger DO body).
    src: String,
}

impl Parser {
    pub fn new(tokens: Vec<SpannedToken>) -> Self {
        Self { tokens, pos: 0, src: String::new() }
    }

    /// Parse a complete FlowQL source string.
    pub fn parse_str(src: &str) -> Result<Vec<Stmt>, FlowError> {
        let tokens = Lexer::new(src).tokenize()?;
        let mut p = Parser { tokens, pos: 0, src: src.to_string() };
        p.parse_all()
    }

    /// Parse all statements separated by optional semicolons.
    pub fn parse_all(&mut self) -> Result<Vec<Stmt>, FlowError> {
        let mut stmts = Vec::new();
        while !self.check(Token::Eof) {
            // Skip optional semicolons between statements
            while self.eat(Token::Semicolon) {}
            if self.check(Token::Eof) {
                break;
            }
            stmts.push(self.parse_stmt()?);
        }
        Ok(stmts)
    }

    // ── Statement dispatch ───────────────────────────────────────────────

    fn parse_stmt(&mut self) -> Result<Stmt, FlowError> {
        match self.peek() {
            Token::Get      => self.parse_get(),
            Token::Put      => self.parse_put(),
            Token::Set      => self.parse_set(),
            Token::Del      => self.parse_del(),
            Token::Find     => self.parse_find(),
            Token::Make     => self.parse_make(),
            Token::Drop     => self.parse_drop(),
            Token::Begin    => { self.advance(); Ok(Stmt::Begin) }
            Token::Commit   => { self.advance(); Ok(Stmt::Commit) }
            Token::Rollback => { self.advance(); Ok(Stmt::Rollback) }
            Token::Show     => self.parse_show(),
            Token::Kill     => self.parse_kill(),
            Token::Explain  => self.parse_explain(),
            Token::Checkpoint => { self.advance(); Ok(Stmt::Checkpoint) }
            Token::Watch    => self.parse_watch(),
            Token::Unwatch  => self.parse_unwatch(),
            Token::Similar  => self.parse_similar(),
            Token::Cluster  => self.parse_cluster(),
            Token::Auth     => self.parse_auth(),
            Token::Create   => self.parse_create(),
            Token::Grant    => self.parse_grant(),
            Token::Revoke   => self.parse_revoke(),
            Token::Config   => self.parse_config(),
            Token::Ai       => self.parse_ai(),
            Token::Trigger  => self.parse_trigger(),
            Token::Graph    => self.parse_graph(),
            Token::Api      => self.parse_api(),
            other => {
                let suggestion = self.closest_keyword(other.display().trim_matches('`'));
                Err(FlowError::parse(format!(
                    "unexpected token {}; expected a statement keyword (GET, PUT, SET, DEL, …){}",
                    other.display(),
                    suggestion,
                )))
            }
        }
    }

    // ── GET ──────────────────────────────────────────────────────────────

    fn parse_get(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Get)?;
        let table = self.expect_ident("table name after GET")?;

        // GET <table> AS OF "<timestamp>" [WHERE …] [ORDER BY …] [LIMIT n]
        if self.eat(Token::As) {
            self.expect(Token::Of)?;
            let timestamp = self.expect_string("timestamp after AS OF")?;
            let filter = if self.eat(Token::Where) { Some(self.parse_expr()?) } else { None };
            let order_by = if self.eat(Token::Order) {
                self.expect(Token::By)?;
                let col = self.parse_column_ref()?;
                let ascending = if self.eat(Token::Desc) { false } else { self.eat(Token::Asc); true };
                Some(OrderBy { column: col, ascending })
            } else { None };
            let limit = if self.eat(Token::Limit) { Some(self.expect_int("LIMIT value")? as u64) } else { None };
            return Ok(Stmt::GetAsOf { table, timestamp, filter, order_by, limit });
        }

        // GET <table> VERSION <n> [WHERE …] [ORDER BY …] [LIMIT n]
        if self.eat(Token::Version) {
            let version = self.expect_int("version number after VERSION")? as u64;
            let filter = if self.eat(Token::Where) { Some(self.parse_expr()?) } else { None };
            let order_by = if self.eat(Token::Order) {
                self.expect(Token::By)?;
                let col = self.parse_column_ref()?;
                let ascending = if self.eat(Token::Desc) { false } else { self.eat(Token::Asc); true };
                Some(OrderBy { column: col, ascending })
            } else { None };
            let limit = if self.eat(Token::Limit) { Some(self.expect_int("LIMIT value")? as u64) } else { None };
            return Ok(Stmt::GetVersion { table, version, filter, order_by, limit });
        }

        // Optional INNER/LEFT/RIGHT JOIN <table> ON <expr>
        let join = if self.eat(Token::Join)
            || self.eat(Token::Inner) && self.eat(Token::Join)
        {
            let join_table = self.expect_ident("table name after JOIN")?;
            self.expect(Token::On)?;
            let on = self.parse_expr()?;
            Some(JoinClause { kind: JoinKind::Inner, table: join_table, on })
        } else if self.eat(Token::Left) {
            self.eat(Token::Join); // optional JOIN keyword
            let join_table = self.expect_ident("table name after LEFT JOIN")?;
            self.expect(Token::On)?;
            let on = self.parse_expr()?;
            Some(JoinClause { kind: JoinKind::Left, table: join_table, on })
        } else if self.eat(Token::Right) {
            self.eat(Token::Join); // optional JOIN keyword
            let join_table = self.expect_ident("table name after RIGHT JOIN")?;
            self.expect(Token::On)?;
            let on = self.parse_expr()?;
            Some(JoinClause { kind: JoinKind::Right, table: join_table, on })
        } else {
            None
        };

        let filter = if self.eat(Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Optional GROUP BY <col, …> [agg_func(arg) [AS alias], …] [HAVING <expr>]
        let group_by = if self.eat(Token::Group) {
            self.expect(Token::By)?;
            let mut columns = Vec::new();

            // Parse group columns until we hit an aggregate function, HAVING,
            // ORDER, LIMIT, TIMEOUT, semicolons, or EOF.
            loop {
                if self.is_agg_start() || self.is_end_of_group_by() {
                    break;
                }
                columns.push(self.parse_column_ref()?);
                if !self.eat(Token::Comma) {
                    break;
                }
            }

            // Parse aggregate expressions
            let mut aggregates = Vec::new();
            while self.is_agg_start() {
                aggregates.push(self.parse_aggregate_expr()?);
                if !self.eat(Token::Comma) {
                    break;
                }
            }

            let having = if self.eat(Token::Having) {
                Some(self.parse_expr()?)
            } else {
                None
            };

            Some(GroupByClause { columns, aggregates, having })
        } else {
            None
        };

        let order_by = if self.eat(Token::Order) {
            self.expect(Token::By)?;
            let col = self.parse_column_ref()?;
            let ascending = if self.eat(Token::Desc) { false } else { self.eat(Token::Asc); true };
            Some(OrderBy { column: col, ascending })
        } else {
            None
        };

        let limit = if self.eat(Token::Limit) {
            Some(self.expect_int("LIMIT value")? as u64)
        } else {
            None
        };

        let timeout_ms = if self.eat(Token::Timeout) {
            Some(self.parse_duration_ms()?)
        } else {
            None
        };

        Ok(Stmt::Get { table, join, filter, group_by, order_by, limit, timeout_ms })
    }

    /// Return true if the current token starts an aggregate function call
    /// (i.e. an aggregate keyword followed by `(`).
    fn is_agg_start(&self) -> bool {
        let is_agg = matches!(
            self.peek(),
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max
        );
        is_agg && matches!(
            self.tokens.get(self.pos + 1).map(|s| &s.token),
            Some(Token::LParen)
        )
    }

    /// Return true if the current token signals the end of a GROUP BY clause.
    fn is_end_of_group_by(&self) -> bool {
        matches!(
            self.peek(),
            Token::Having
                | Token::Order
                | Token::Limit
                | Token::Timeout
                | Token::Where
                | Token::Semicolon
                | Token::Eof
        )
    }

    /// Parse an aggregate expression: `func ( * | col ) [AS alias]`
    fn parse_aggregate_expr(&mut self) -> Result<AggregateExpr, FlowError> {
        let func = match self.peek() {
            Token::Count => { self.advance(); AggFunc::Count }
            Token::Sum   => { self.advance(); AggFunc::Sum   }
            Token::Avg   => { self.advance(); AggFunc::Avg   }
            Token::Min   => { self.advance(); AggFunc::Min   }
            Token::Max   => { self.advance(); AggFunc::Max   }
            other => return Err(FlowError::parse(format!(
                "expected aggregate function (COUNT, SUM, AVG, MIN, MAX), got {}",
                other.display()
            ))),
        };
        self.expect(Token::LParen)?;
        let column = if self.eat(Token::Star) {
            None // COUNT(*)
        } else {
            Some(self.parse_column_ref()?)
        };
        self.expect(Token::RParen)?;

        // Optional AS alias
        let alias = if self.eat(Token::As) {
            self.expect_ident("alias after AS")?
        } else {
            // Auto-generate alias
            match (&func, &column) {
                (AggFunc::Count, _)           => "count".into(),
                (AggFunc::Sum,  Some(c)) => format!("sum_{}", c.column),
                (AggFunc::Avg,  Some(c)) => format!("avg_{}", c.column),
                (AggFunc::Min,  Some(c)) => format!("min_{}", c.column),
                (AggFunc::Max,  Some(c)) => format!("max_{}", c.column),
                _ => func.name().into(),
            }
        };

        Ok(AggregateExpr { func, column, alias })
    }


    // ── PUT ──────────────────────────────────────────────────────────────

    fn parse_put(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Put)?;
        let table = self.expect_ident("table name after PUT")?;
        let fields = self.parse_field_map()?;
        Ok(Stmt::Put { table, fields })
    }

    // ── SET ──────────────────────────────────────────────────────────────

    fn parse_set(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Set)?;
        let table = self.expect_ident("table name after SET")?;
        let fields = self.parse_field_map()?;
        let filter = if self.eat(Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Stmt::Set { table, fields, filter })
    }

    // ── DEL ──────────────────────────────────────────────────────────────

    fn parse_del(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Del)?;
        let table = self.expect_ident("table name after DEL")?;
        let filter = if self.eat(Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Stmt::Del { table, filter })
    }

    // ── FIND ─────────────────────────────────────────────────────────────

    fn parse_find(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Find)?;
        let table = self.expect_ident("table name after FIND")?;
        self.expect(Token::Where)?;
        let column = self.parse_column_ref()?;
        self.expect(Token::Tilde)?;
        let pattern = self.expect_string("fuzzy pattern after ~")?;

        let limit = if self.eat(Token::Limit) {
            Some(self.expect_int("LIMIT value")? as u64)
        } else {
            None
        };

        Ok(Stmt::Find { table, column, pattern, limit })
    }

    // ── MAKE ─────────────────────────────────────────────────────────────

    fn parse_make(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Make)?;
        if self.eat(Token::Table) {
            let name = self.expect_ident("table name after MAKE TABLE")?;
            self.expect(Token::LParen)?;
            let mut columns = Vec::new();
            loop {
                if self.check(Token::RParen) {
                    break;
                }
                let col_name = self.expect_ident("column name")?;
                let dtype_tok = self.advance();
                let data_type = self.token_to_type_str(&dtype_tok.token)?;
                let primary_key = if let Token::Ident(ref kw) = self.peek().clone() {
                    kw.to_lowercase() == "primary"
                } else {
                    false
                };
                if primary_key {
                    self.advance(); // consume "primary"
                    if let Token::Ident(ref kw) = self.peek().clone() {
                        if kw.to_lowercase() == "key" { self.advance(); }
                    }
                }
                let nullable = !primary_key; // primary key cols are NOT NULL by default
                columns.push(ColumnDef { name: col_name, data_type, nullable, primary_key });
                if !self.eat(Token::Comma) {
                    break;
                }
            }
            self.expect(Token::RParen)?;
            Ok(Stmt::MakeTable { name, columns })
        } else if self.eat(Token::Index) {
            self.expect(Token::On)?;
            let table = self.expect_ident("table name after MAKE INDEX ON")?;
            self.expect(Token::LParen)?;
            let column = self.expect_ident("column name")?;
            self.expect(Token::RParen)?;
            Ok(Stmt::MakeIndex { table, column })
        } else {
            Err(FlowError::parse(
                "expected TABLE or INDEX after MAKE"
            ))
        }
    }

    fn token_to_type_str(&self, tok: &Token) -> Result<String, FlowError> {
        match tok {
            Token::Ident(s) => Ok(s.to_lowercase()),
            _ => Err(FlowError::parse(format!(
                "expected data type name, got {}",
                tok.display()
            ))),
        }
    }

    // ── DROP ─────────────────────────────────────────────────────────────

    fn parse_drop(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Drop)?;
        if self.eat(Token::Trigger) {
            let name = self.expect_ident("trigger name after DROP TRIGGER")?;
            return Ok(Stmt::DropTrigger { name });
        }
        self.expect(Token::Table)?;
        let name = self.expect_ident("table name after DROP TABLE")?;
        Ok(Stmt::DropTable { name })
    }

    // ── SHOW ─────────────────────────────────────────────────────────────

    fn parse_show(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Show)?;
        match self.peek() {
            Token::Tables => { self.advance(); Ok(Stmt::ShowTables) }
            Token::Users  => { self.advance(); Ok(Stmt::ShowUsers) }
            Token::Config   => { self.advance(); Ok(Stmt::ShowConfig) }
            Token::Trigger  => { self.advance(); Ok(Stmt::ShowTriggers) }
            Token::Api      => { self.advance(); Ok(Stmt::ShowApis) }
            _ => {
                // SHOW RUNNING QUERIES
                self.expect(Token::Running)?;
                self.expect(Token::Queries)?;
                Ok(Stmt::ShowRunningQueries)
            }
        }
    }

    // ── KILL ─────────────────────────────────────────────────────────────

    fn parse_kill(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Kill)?;
        self.expect(Token::Query)?;
        let id = self.expect_int("query ID after KILL QUERY")? as u64;
        Ok(Stmt::KillQuery { id })
    }

    // ── EXPLAIN ──────────────────────────────────────────────────────────

    fn parse_explain(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Explain)?;
        let inner = self.parse_stmt()?;
        Ok(Stmt::Explain(Box::new(inner)))
    }

    // ── WATCH ──────────────────────────────────────────────────

    fn parse_watch(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Watch)?;
        let table = self.expect_ident("table name after WATCH")?;
        let filter = if self.eat(Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Stmt::Watch { table, filter })
    }

    // ── UNWATCH ───────────────────────────────────────────────

    fn parse_unwatch(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Unwatch)?;
        let id = self.expect_int("subscription ID after UNWATCH")? as u64;
        Ok(Stmt::Unwatch { id })
    }

    // ── SIMILAR ───────────────────────────────────────────────

    /// `SIMILAR <table> [ON <col>] TO [f, ...] [LIMIT n]`
    fn parse_similar(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Similar)?;
        let table = self.expect_ident("table name after SIMILAR")?;

        let column = if self.eat(Token::On) {
            Some(self.expect_ident("column name after ON")?)
        } else {
            None
        };

        self.expect(Token::To)?;
        let vector = self.parse_vector_literal()?;

        let limit = if self.eat(Token::Limit) {
            Some(self.expect_int("LIMIT value")? as u64)
        } else {
            None
        };

        Ok(Stmt::Similar { table, vector, column, limit })
    }

    fn parse_vector_literal(&mut self) -> Result<Vec<f32>, FlowError> {
        self.expect(Token::LBracket)?;
        let mut vals: Vec<f32> = Vec::new();
        loop {
            if self.check(Token::RBracket) { break; }
            let neg = self.eat(Token::Minus);
            let v = match self.peek() {
                Token::FloatLiteral(f) => { self.advance(); f as f32 }
                Token::IntLiteral(n)   => { self.advance(); n as f32 }
                other => return Err(FlowError::parse(format!(
                    "expected number in vector literal, got {}", other.display()
                ))),
            };
            vals.push(if neg { -v } else { v });
            if !self.eat(Token::Comma) { break; }
        }
        self.expect(Token::RBracket)?;
        Ok(vals)
    }

    // ── CLUSTER ───────────────────────────────────────────────

    fn parse_cluster(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Cluster)?;
        match self.peek() {
            Token::Status => {
                self.advance();
                Ok(Stmt::ClusterStatus)
            }
            Token::Shard => {
                self.advance(); // consume SHARD
                match self.peek() {
                    Token::Status => {
                        self.advance();
                        Ok(Stmt::ClusterShardStatus)
                    }
                    Token::Drop => {
                        self.advance();
                        let table = self.expect_ident("table name after CLUSTER SHARD DROP")?;
                        Ok(Stmt::ClusterShardDrop { table })
                    }
                    Token::Ident(ref kw) if kw.to_lowercase() == "assign" => {
                        self.advance();
                        let table = self.expect_ident("table name after CLUSTER SHARD ASSIGN")?;
                        self.expect_kw("SHARDS")?;
                        let shards = self.expect_int("shard count")? as u32;
                        let nodes = if self.peek_kw("NODES") {
                            self.advance();
                            self.parse_string_list()?
                        } else {
                            Vec::new()
                        };
                        Ok(Stmt::ClusterShardAssign { table, shards, nodes })
                    }
                    other => Err(FlowError::parse(format!(
                        "expected STATUS, ASSIGN or DROP after CLUSTER SHARD, got {}",
                        other.display()
                    ))),
                }
            }
            // JOIN and PART are now proper tokens, handle them directly
            Token::Join => {
                self.advance();
                let addr = self.expect_string("peer address after CLUSTER JOIN")?;
                Ok(Stmt::ClusterJoin { addr })
            }
            Token::Ident(ref kw) => {
                let kw = kw.to_lowercase();
                self.advance();
                match kw.as_str() {
                    "join" => {
                        let addr = self.expect_string("peer address after CLUSTER JOIN")?;
                        Ok(Stmt::ClusterJoin { addr })
                    }
                    "part" => {
                        let addr = self.expect_string("peer address after CLUSTER PART")?;
                        Ok(Stmt::ClusterPart { addr })
                    }
                    other => Err(FlowError::parse(format!(
                        "expected STATUS, JOIN, PART or SHARD after CLUSTER, got `{other}`"
                    ))),
                }
            }
            other => Err(FlowError::parse(format!(
                "expected STATUS, JOIN, PART or SHARD after CLUSTER, got {}",
                other.display()
            ))),
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), FlowError> {
        let s = self.expect_ident(&format!("`{kw}` keyword"))?;
        if s.to_lowercase() != kw.to_lowercase() {
            return Err(FlowError::parse(format!(
                "expected keyword `{kw}`, got `{s}`"
            )));
        }
        Ok(())
    }

    fn peek_kw(&self, kw: &str) -> bool {
        if let Token::Ident(s) = self.peek() {
            s.to_lowercase() == kw.to_lowercase()
        } else {
            false
        }
    }

    fn parse_string_list(&mut self) -> Result<Vec<String>, FlowError> {
        let mut list = Vec::new();
        loop {
            list.push(self.expect_string("string in list")?);
            if !self.eat(Token::Comma) { break; }
        }
        Ok(list)
    }

    // ── AUTH ──────────────────────────────────────────────────────────────

    fn parse_auth(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Auth)?;
        let username = self.expect_ident("username after AUTH")?;
        let password = self.expect_string("password string after AUTH <user>")?;
        Ok(Stmt::Auth { username, password })
    }

    // ── CREATE USER ───────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Create)?;
        let is_admin = self.eat(Token::Admin);
        self.expect(Token::User)?;
        let username = self.expect_ident("username after CREATE USER")?;
        self.expect(Token::Password)?;
        let password = self.expect_string("password string after PASSWORD")?;
        Ok(Stmt::CreateUser { username, password, is_admin })
    }

    // ── GRANT ─────────────────────────────────────────────────────────────

    fn parse_grant(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Grant)?;
        let op = self.expect_ident("operation (select/insert/update/delete/all) after GRANT")?;
        self.expect(Token::On)?;
        let table = if self.eat(Token::Star) {
            None
        } else {
            Some(self.expect_ident("table name or * after ON")?)
        };
        self.expect_kw("TO")?;
        let username = self.expect_ident("username after TO")?;
        Ok(Stmt::Grant { username, op, table })
    }

    // ── REVOKE ────────────────────────────────────────────────────────────

    fn parse_revoke(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Revoke)?;
        let op = self.expect_ident("operation after REVOKE")?;
        self.expect(Token::On)?;
        let table = if self.eat(Token::Star) {
            None
        } else {
            Some(self.expect_ident("table name or * after ON")?)
        };
        self.expect_kw("FROM")?;
        let username = self.expect_ident("username after FROM")?;
        Ok(Stmt::Revoke { username, op, table })
    }

    // ── CONFIG ────────────────────────────────────────────────────────────

    // ── AI SEARCH ────────────────────────────────────────────────────────

    fn parse_ai(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Ai)?;
        self.expect(Token::Search)?;
        let table = self.expect_ident("table name after AI SEARCH")?;
        let query = self.expect_string("search query string")?;
        let limit = if self.eat(Token::Limit) {
            Some(self.expect_int("LIMIT value")? as u64)
        } else {
            None
        };
        Ok(Stmt::AiSearch { table, query, limit })
    }

    // ── TRIGGER ──────────────────────────────────────────────────────────

    fn parse_trigger(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Trigger)?;
        let name = self.expect_ident("trigger name")?;
        self.expect(Token::When)?;
        let event = match self.peek() {
            Token::Put => { self.advance(); "PUT".to_string() }
            Token::Set => { self.advance(); "SET".to_string() }
            Token::Del => { self.advance(); "DEL".to_string() }
            other => return Err(FlowError::parse(format!(
                "expected PUT, SET, or DEL after WHEN, got {}", other.display()
            ))),
        };
        let table = self.expect_ident("table name after event in TRIGGER")?;
        self.expect(Token::Do)?;
        // Extract DO query verbatim from source using token spans
        let start_byte = if self.pos < self.tokens.len() {
            self.tokens[self.pos].start
        } else {
            self.src.len()
        };
        // advance until semicolon or EOF
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            self.advance();
        }
        let end_byte = if self.pos > 0 && self.pos <= self.tokens.len() {
            self.tokens[self.pos - 1].end
        } else {
            self.src.len()
        };
        let do_query = self.src.get(start_byte..end_byte)
            .unwrap_or("").trim().to_string();
        Ok(Stmt::CreateTrigger { name, event, table, do_query })
    }

    // ── GRAPH MATCH ──────────────────────────────────────────────────────

    fn parse_graph(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Graph)?;
        self.expect(Token::Match)?;
        // Parse: (src_alias:src_table) -[edge_alias:edge_table]-> (dst_alias:dst_table)
        self.expect(Token::LParen)?;
        let src_alias = self.expect_ident("source alias")?;
        self.expect(Token::Colon)?;
        let src_table = self.expect_ident("source table")?;
        self.expect(Token::RParen)?;
        self.expect(Token::Minus)?;
        self.expect(Token::LBracket)?;
        let edge_alias = self.expect_ident("edge alias")?;
        self.expect(Token::Colon)?;
        let edge_table = self.expect_ident("edge table")?;
        self.expect(Token::RBracket)?;
        self.expect(Token::Minus)?;
        self.expect(Token::Gt)?;
        self.expect(Token::LParen)?;
        let dst_alias = self.expect_ident("destination alias")?;
        self.expect(Token::Colon)?;
        let dst_table = self.expect_ident("destination table")?;
        self.expect(Token::RParen)?;
        let filter = if self.eat(Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let limit = if self.eat(Token::Limit) {
            Some(self.expect_int("LIMIT value")? as u64)
        } else {
            None
        };
        Ok(Stmt::GraphMatch {
            src_alias, src_table,
            edge_alias, edge_table,
            dst_alias, dst_table,
            filter, limit,
        })
    }

    // ── API ──────────────────────────────────────────────────────────────

    fn parse_api(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Api)?;
        match self.peek() {
            Token::Generate => {
                self.advance();
                self.expect(Token::For)?;
                let table = self.expect_ident("table name after API GENERATE FOR")?;
                Ok(Stmt::ApiGenerate { table })
            }
            Token::Stop => {
                self.advance();
                self.expect(Token::For)?;
                let table = self.expect_ident("table name after API STOP FOR")?;
                Ok(Stmt::ApiStop { table })
            }
            other => Err(FlowError::parse(format!(
                "expected GENERATE or STOP after API, got {}", other.display()
            ))),
        }
    }

    // ── CONFIG ───────────────────────────────────────────────────────────

    fn parse_config(&mut self) -> Result<Stmt, FlowError> {
        self.expect(Token::Config)?;
        if let Token::Ident(ref kw) = self.peek().clone() {
            if kw.to_lowercase() == "set" {
                self.advance();
                let key = self.expect_ident("config key after CONFIG SET")?;
                self.eat(Token::Eq);
                let value = match self.peek().clone() {
                    Token::IntLiteral(n)    => { self.advance(); n.to_string() }
                    Token::StringLiteral(s) => { self.advance(); s }
                    Token::FloatLiteral(f)  => { self.advance(); f.to_string() }
                    Token::Ident(s)         => { self.advance(); s }
                    other => return Err(FlowError::parse(format!(
                        "expected value after CONFIG SET {key}, got {}", other.display()
                    ))),
                };
                return Ok(Stmt::ConfigSet { key, value });
            }
        }
        Err(FlowError::parse("expected SET after CONFIG"))
    }

    // ── Expression parsing (Pratt / precedence climbing) ─────────────────

    fn parse_expr(&mut self) -> Result<Expr, FlowError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, FlowError> {
        let mut left = self.parse_and()?;
        while self.eat(Token::Or) {
            let right = self.parse_and()?;
            left = Expr::BinOp { op: BinOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, FlowError> {
        let mut left = self.parse_not()?;
        while self.eat(Token::And) {
            let right = self.parse_not()?;
            left = Expr::BinOp { op: BinOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, FlowError> {
        if self.eat(Token::Not) {
            let inner = self.parse_comparison()?;
            Ok(Expr::Not(Box::new(inner)))
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, FlowError> {
        let left = self.parse_additive()?;

        let op = match self.peek() {
            Token::Eq    => Some(BinOp::Eq),
            Token::Ne    => Some(BinOp::Ne),
            Token::Lt    => Some(BinOp::Lt),
            Token::Le    => Some(BinOp::Le),
            Token::Gt    => Some(BinOp::Gt),
            Token::Ge    => Some(BinOp::Ge),
            Token::Tilde => {
                self.advance();
                // fuzzy match: left must be a column ref
                if let Expr::Column(col) = left {
                    let pattern = self.expect_string("fuzzy pattern after ~")?;
                    return Ok(Expr::Fuzzy { column: col, pattern });
                } else {
                    return Err(FlowError::parse(
                        "left side of `~` must be a column name"
                    ));
                }
            }
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expr::BinOp { op, left: Box::new(left), right: Box::new(right) })
        } else {
            Ok(left)
        }
    }

    fn parse_additive(&mut self) -> Result<Expr, FlowError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus  => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, FlowError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star  => BinOp::Mul,
                Token::Slash => BinOp::Div,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, FlowError> {
        if self.eat(Token::Minus) {
            let inner = self.parse_primary()?;
            // fold unary minus into literal if possible
            Ok(match inner {
                Expr::Literal(Literal::Int(n)) => Expr::Literal(Literal::Int(-n)),
                Expr::Literal(Literal::Float(f)) => Expr::Literal(Literal::Float(-f)),
                other => Expr::BinOp {
                    op: BinOp::Sub,
                    left: Box::new(Expr::Literal(Literal::Int(0))),
                    right: Box::new(other),
                },
            })
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, FlowError> {
        match self.peek().clone() {
            Token::IntLiteral(n)    => { self.advance(); Ok(Expr::Literal(Literal::Int(n))) }
            Token::FloatLiteral(f)  => { self.advance(); Ok(Expr::Literal(Literal::Float(f))) }
            Token::StringLiteral(s) => { self.advance(); Ok(Expr::Literal(Literal::Text(s))) }
            Token::BoolLiteral(b)   => { self.advance(); Ok(Expr::Literal(Literal::Bool(b))) }
            Token::Null             => { self.advance(); Ok(Expr::Literal(Literal::Null)) }
            Token::LBracket => {
                // Vector literal: [f, f, ...]
                let floats = self.parse_vector_literal()?;
                Ok(Expr::Literal(Literal::Vector(floats)))
            }
            Token::LParen => {
                self.advance();
                let e = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(e)
            }
            Token::Ident(_) => Ok(Expr::Column(self.parse_column_ref()?)),
            other => Err(FlowError::parse(format!(
                "expected expression but got {}",
                other.display()
            ))),
        }
    }

    // ── Field map `{ col: expr, … }` ─────────────────────────────────────

    fn parse_field_map(&mut self) -> Result<Vec<(String, Expr)>, FlowError> {
        self.expect(Token::LBrace)?;
        let mut fields = Vec::new();
        loop {
            if self.check(Token::RBrace) { break; }
            let name = self.expect_ident("field name")?;
            self.expect(Token::Colon)?;
            let val = self.parse_expr()?;
            fields.push((name, val));
            if !self.eat(Token::Comma) { break; }
        }
        self.expect(Token::RBrace)?;
        Ok(fields)
    }

    // ── Column ref `table.col` or just `col` ─────────────────────────────

    fn parse_column_ref(&mut self) -> Result<ColumnRef, FlowError> {
        let name = self.expect_ident("column name")?;
        if self.eat(Token::Dot) {
            let col = self.expect_ident("column name after `.`")?;
            Ok(ColumnRef { table: Some(name), column: col })
        } else {
            Ok(ColumnRef::simple(name))
        }
    }

    // ── Duration parser (e.g. "5s", "200ms", bare integer as ms) ─────────

    fn parse_duration_ms(&mut self) -> Result<u64, FlowError> {
        match self.peek().clone() {
            Token::StringLiteral(s) => {
                self.advance();
                parse_duration_str(&s)
            }
            Token::IntLiteral(n) => {
                self.advance();
                Ok(n as u64) // bare integer → milliseconds
            }
            other => Err(FlowError::parse(format!(
                "expected timeout duration (e.g. \"5s\" or 500), got {}",
                other.display()
            ))),
        }
    }

    // ── Token helpers ─────────────────────────────────────────────────────

    fn peek(&self) -> Token {
        self.tokens
            .get(self.pos)
            .map(|s| s.token.clone())
            .unwrap_or(Token::Eof)
    }

    fn advance(&mut self) -> SpannedToken {
        let tok = self.tokens
            .get(self.pos)
            .cloned()
            .unwrap_or_else(|| SpannedToken { token: Token::Eof, start: 0, end: 0 });
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn check(&self, tok: Token) -> bool {
        self.peek() == tok
    }

    /// Consume and return `true` if the next token matches.
    fn eat(&mut self, tok: Token) -> bool {
        if self.check(tok) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Consume the expected token or return a parse error.
    fn expect(&mut self, tok: Token) -> Result<SpannedToken, FlowError> {
        if self.check(tok.clone()) {
            Ok(self.advance())
        } else {
            Err(FlowError::parse(format!(
                "expected {} but got {}",
                tok.display(),
                self.peek().display()
            )))
        }
    }

    fn expect_ident(&mut self, ctx: &str) -> Result<String, FlowError> {
        // Allow keywords that are commonly used as identifiers (table/column names)
        let kw = match self.peek() {
            Token::Join     => Some("join"),
            Token::Inner    => Some("inner"),
            Token::Left     => Some("left"),
            Token::Right    => Some("right"),
            Token::Group    => Some("group"),
            Token::Having   => Some("having"),
            Token::Count    => Some("count"),
            Token::Sum      => Some("sum"),
            Token::Avg      => Some("avg"),
            Token::Min      => Some("min"),
            Token::Max      => Some("max"),
            Token::As       => Some("as"),
            Token::Create   => Some("create"),
            Token::User     => Some("user"),
            Token::Users    => Some("users"),
            Token::Password => Some("password"),
            Token::Grant    => Some("grant"),
            Token::Revoke   => Some("revoke"),
            Token::Auth     => Some("auth"),
            Token::Admin    => Some("admin"),
            Token::Config   => Some("config"),
            Token::Shard    => Some("shard"),
            Token::Ai       => Some("ai"),
            Token::Search   => Some("search"),
            Token::Version  => Some("version"),
            Token::Of       => Some("of"),
            Token::Trigger  => Some("trigger"),
            Token::When     => Some("when"),
            Token::Do       => Some("do"),
            Token::Graph    => Some("graph"),
            Token::Match    => Some("match"),
            Token::Api      => Some("api"),
            Token::Generate => Some("generate"),
            Token::For      => Some("for"),
            Token::Stop     => Some("stop"),
            _               => None,
        };
        if let Some(s) = kw {
            self.advance();
            return Ok(s.to_string());
        }
        match self.peek() {
            Token::Ident(s) => { self.advance(); Ok(s) }
            other => Err(FlowError::parse(format!(
                "expected identifier ({ctx}), got {}", other.display()
            ))),
        }
    }

    fn expect_int(&mut self, ctx: &str) -> Result<i64, FlowError> {
        match self.peek() {
            Token::IntLiteral(n) => { self.advance(); Ok(n) }
            other => Err(FlowError::parse(format!(
                "expected integer ({ctx}), got {}", other.display()
            ))),
        }
    }

    fn expect_string(&mut self, ctx: &str) -> Result<String, FlowError> {
        match self.peek() {
            Token::StringLiteral(s) => { self.advance(); Ok(s) }
            other => Err(FlowError::parse(format!(
                "expected string ({ctx}), got {}", other.display()
            ))),
        }
    }

    /// Levenshtein-based typo suggestion for keywords.
    fn closest_keyword(&self, input: &str) -> String {
        let keywords = [
            "GET", "PUT", "SET", "DEL", "FIND", "MAKE", "DROP",
            "BEGIN", "COMMIT", "ROLLBACK", "SHOW", "KILL", "EXPLAIN",
            "CHECKPOINT", "WATCH", "UNWATCH", "SIMILAR", "CLUSTER",
        ];
        let best = keywords
            .iter()
            .min_by_key(|kw| levenshtein(input, &kw.to_lowercase()));
        if let Some(kw) = best {
            if levenshtein(input, &kw.to_lowercase()) <= 3 {
                return format!("; did you mean `{kw}`?");
            }
        }
        String::new()
    }
}

/// Parse duration strings like "5s", "200ms", "1m".
fn parse_duration_str(s: &str) -> Result<u64, FlowError> {
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>().map_err(|_| FlowError::parse(format!("invalid duration `{s}`")))
    } else if let Some(sec) = s.strip_suffix('s') {
        sec.parse::<u64>()
            .map(|n| n * 1000)
            .map_err(|_| FlowError::parse(format!("invalid duration `{s}`")))
    } else if let Some(min) = s.strip_suffix('m') {
        min.parse::<u64>()
            .map(|n| n * 60_000)
            .map_err(|_| FlowError::parse(format!("invalid duration `{s}`")))
    } else {
        Err(FlowError::parse(format!(
            "unknown duration unit in `{s}`; use ms, s, or m"
        )))
    }
}

/// Simple Levenshtein distance (used for typo suggestions — small strings only).
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 0..=m { dp[i][0] = i; }
    for j in 0..=n { dp[0][j] = j; }
    for i in 1..=m {
        for j in 1..=n {
            dp[i][j] = if a[i - 1] == b[j - 1] {
                dp[i - 1][j - 1]
            } else {
                1 + dp[i - 1][j].min(dp[i][j - 1]).min(dp[i - 1][j - 1])
            };
        }
    }
    dp[m][n]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> Vec<Stmt> {
        Parser::parse_str(src).expect("parse failed")
    }

    #[test]
    fn test_parse_get() {
        let stmts = parse("GET users WHERE id = 1 LIMIT 10");
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Get { table, limit, .. } => {
                assert_eq!(table, "users");
                assert_eq!(*limit, Some(10));
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_put() {
        let stmts = parse(r#"PUT users { name: "Alice", age: 30 }"#);
        match &stmts[0] {
            Stmt::Put { table, fields } => {
                assert_eq!(table, "users");
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_parse_make_table() {
        let stmts = parse("MAKE TABLE products (id int primary key, name text, price float)");
        match &stmts[0] {
            Stmt::MakeTable { name, columns } => {
                assert_eq!(name, "products");
                assert_eq!(columns.len(), 3);
                assert!(columns[0].primary_key);
            }
            _ => panic!("expected MakeTable"),
        }
    }

    #[test]
    fn test_parse_find() {
        let stmts = parse(r#"FIND users WHERE name ~ "Ali" LIMIT 5"#);
        match &stmts[0] {
            Stmt::Find { table, pattern, limit, .. } => {
                assert_eq!(table, "users");
                assert_eq!(pattern, "Ali");
                assert_eq!(*limit, Some(5));
            }
            _ => panic!("expected Find"),
        }
    }

    #[test]
    fn test_parse_timeout() {
        let stmts = parse(r#"GET orders TIMEOUT "5s""#);
        match &stmts[0] {
            Stmt::Get { timeout_ms, .. } => assert_eq!(*timeout_ms, Some(5000)),
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_transaction() {
        let stmts = parse("BEGIN; COMMIT");
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Stmt::Begin));
        assert!(matches!(stmts[1], Stmt::Commit));
    }

    #[test]
    fn test_parse_and_filter() {
        let stmts = parse("GET users WHERE age > 18 AND active = true");
        match &stmts[0] {
            Stmt::Get { filter: Some(Expr::BinOp { op: BinOp::And, .. }), .. } => {}
            _ => panic!("expected AND expr"),
        }
    }
}
