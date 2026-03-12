use crate::sql::token::Token;
use crate::error::FlowError;

/// A spanned token — the token plus its byte offset in the source string.
/// Used to produce precise error messages.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SpannedToken {
    pub token: Token,
    pub start: usize, // byte offset of first character
    pub end: usize,   // byte offset one past the last character
}

/// Converts a raw FlowQL string into a `Vec<SpannedToken>`.
pub struct Lexer<'src> {
    src: &'src str,
    pos: usize,
}

impl<'src> Lexer<'src> {
    pub fn new(src: &'src str) -> Self {
        Self { src, pos: 0 }
    }

    /// Lex the entire input and return all tokens, including a final `Eof`.
    pub fn tokenize(mut self) -> Result<Vec<SpannedToken>, FlowError> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = tok.token == Token::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn peek2(&self) -> Option<char> {
        let mut chars = self.src[self.pos..].chars();
        chars.next();
        chars.next()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.src[self.pos..].chars().next()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while matches!(self.peek(), Some(c) if c.is_whitespace()) {
                self.advance();
            }
            // Skip `--` line comments
            if self.src[self.pos..].starts_with("--") {
                while !matches!(self.peek(), None | Some('\n')) {
                    self.advance();
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<SpannedToken, FlowError> {
        self.skip_whitespace_and_comments();
        let start = self.pos;

        let c = match self.advance() {
            None => return Ok(self.spanned(Token::Eof, start, self.pos)),
            Some(c) => c,
        };

        let token = match c {
            '(' => Token::LParen,
            ')' => Token::RParen,
            '{' => Token::LBrace,
            '}' => Token::RBrace,
            '[' => Token::LBracket,
            ']' => Token::RBracket,
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            ':' => Token::Colon,
            '.' => Token::Dot,
            '+' => Token::Plus,
            '-' => Token::Minus,
            '*' => Token::Star,
            '/' => Token::Slash,
            '~' => Token::Tilde,

            '=' => Token::Eq,
            '!' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Ne
                } else {
                    return Err(FlowError::parse(
                        format!("unexpected character `!` at offset {start}; did you mean `!=`?"),
                    ));
                }
            }
            '<' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '>' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }

            // String literals — single or double quote
            '"' | '\'' => self.lex_string(c)?,

            // Numbers
            '0'..='9' => self.lex_number(c)?,

            // Identifiers / keywords
            c if c.is_alphabetic() || c == '_' => self.lex_ident(c),

            other => {
                return Err(FlowError::parse(format!(
                    "unexpected character `{other}` at offset {start}"
                )));
            }
        };

        Ok(self.spanned(token, start, self.pos))
    }

    fn spanned(&self, token: Token, start: usize, end: usize) -> SpannedToken {
        SpannedToken { token, start, end }
    }

    fn lex_string(&mut self, quote: char) -> Result<Token, FlowError> {
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(FlowError::parse("unterminated string literal"));
                }
                Some(c) if c == quote => break,
                Some('\\') => {
                    // basic escape sequences
                    match self.advance() {
                        Some('n') => s.push('\n'),
                        Some('t') => s.push('\t'),
                        Some('r') => s.push('\r'),
                        Some('\\') => s.push('\\'),
                        Some(q) if q == quote => s.push(q),
                        Some(other) => {
                            s.push('\\');
                            s.push(other);
                        }
                        None => {
                            return Err(FlowError::parse(
                                "unterminated escape sequence in string",
                            ));
                        }
                    }
                }
                Some(c) => s.push(c),
            }
        }
        Ok(Token::StringLiteral(s))
    }

    fn lex_number(&mut self, first: char) -> Result<Token, FlowError> {
        let mut s = String::from(first);
        let mut is_float = false;

        loop {
            match self.peek() {
                Some(c) if c.is_ascii_digit() => {
                    s.push(c);
                    self.advance();
                }
                Some('.') if !is_float && matches!(self.peek2(), Some(d) if d.is_ascii_digit()) => {
                    is_float = true;
                    s.push('.');
                    self.advance();
                }
                _ => break,
            }
        }

        if is_float {
            s.parse::<f64>()
                .map(Token::FloatLiteral)
                .map_err(|_| FlowError::parse(format!("invalid float literal `{s}`")))
        } else {
            s.parse::<i64>()
                .map(Token::IntLiteral)
                .map_err(|_| FlowError::parse(format!("invalid integer literal `{s}`")))
        }
    }

    fn lex_ident(&mut self, first: char) -> Token {
        let mut s = String::from(first);
        while matches!(self.peek(), Some(c) if c.is_alphanumeric() || c == '_') {
            s.push(self.advance().unwrap());
        }

        // Case-insensitive keyword matching
        match s.to_lowercase().as_str() {
            "get"      => Token::Get,
            "put"      => Token::Put,
            "set"      => Token::Set,
            "del"      => Token::Del,
            "find"     => Token::Find,
            "make"     => Token::Make,
            "drop"     => Token::Drop,
            "where"    => Token::Where,
            "from"     => Token::From,
            "into"     => Token::Into,
            "values"   => Token::Values,
            "table"    => Token::Table,
            "index"    => Token::Index,
            "on"       => Token::On,
            "limit"    => Token::Limit,
            "order"    => Token::Order,
            "by"       => Token::By,
            "asc"      => Token::Asc,
            "desc"     => Token::Desc,
            "timeout"  => Token::Timeout,
            "begin"    => Token::Begin,
            "commit"   => Token::Commit,
            "rollback" => Token::Rollback,
            "show"     => Token::Show,
            "kill"     => Token::Kill,
            "query"    => Token::Query,
            "running"  => Token::Running,
            "queries"  => Token::Queries,
            "tables"   => Token::Tables,
            "explain"    => Token::Explain,
            "checkpoint" => Token::Checkpoint,
            "watch"      => Token::Watch,
            "unwatch"    => Token::Unwatch,
            "similar"    => Token::Similar,
            "to"         => Token::To,
            "cluster"    => Token::Cluster,
            "status"     => Token::Status,
            "shard"      => Token::Shard,
            // JOIN
            "join"       => Token::Join,
            "inner"      => Token::Inner,
            "left"       => Token::Left,
            "right"      => Token::Right,
            // GROUP BY / aggregation
            "group"      => Token::Group,
            "having"     => Token::Having,
            "count"      => Token::Count,
            "sum"        => Token::Sum,
            "avg"        => Token::Avg,
            "min"        => Token::Min,
            "max"        => Token::Max,
            "as"         => Token::As,
            // Auth
            "create"     => Token::Create,
            "user"       => Token::User,
            "users"      => Token::Users,
            "password"   => Token::Password,
            "grant"      => Token::Grant,
            "revoke"     => Token::Revoke,
            "auth"       => Token::Auth,
            "admin"      => Token::Admin,
            // Config
            "config"     => Token::Config,
            // AI / Search
            "ai"         => Token::Ai,
            "search"     => Token::Search,
            "version"    => Token::Version,
            "of"         => Token::Of,
            // Triggers
            "trigger" | "triggers" => Token::Trigger,
            "when"       => Token::When,
            "do"         => Token::Do,
            // Graph
            "graph"      => Token::Graph,
            "match"      => Token::Match,
            // REST API
            "api" | "apis" => Token::Api,
            "generate"   => Token::Generate,
            "for"        => Token::For,
            "stop"       => Token::Stop,
            "and"      => Token::And,
            "or"       => Token::Or,
            "not"      => Token::Not,
            "true"     => Token::BoolLiteral(true),
            "false"    => Token::BoolLiteral(false),
            "null"     => Token::Null,
            _          => Token::Ident(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Vec<Token> {
        Lexer::new(src)
            .tokenize()
            .expect("lex failed")
            .into_iter()
            .map(|s| s.token)
            .collect()
    }

    #[test]
    fn test_basic_get() {
        let tokens = lex("GET users WHERE id = 1");
        assert_eq!(
            tokens,
            vec![
                Token::Get,
                Token::Users,
                Token::Where,
                Token::Ident("id".into()),
                Token::Eq,
                Token::IntLiteral(1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_string_literal() {
        let tokens = lex(r#"PUT users { name: "Alice" }"#);
        assert!(tokens.contains(&Token::StringLiteral("Alice".into())));
    }

    #[test]
    fn test_case_insensitive() {
        let tokens = lex("get USERS where ID = 1");
        assert_eq!(tokens[0], Token::Get);
        assert_eq!(tokens[2], Token::Where);
    }

    #[test]
    fn test_float() {
        let tokens = lex("GET products WHERE price > 9.99");
        assert!(tokens.contains(&Token::FloatLiteral(9.99)));
    }

    #[test]
    fn test_comparison_operators() {
        let tokens = lex("a != b <= c >= d");
        assert!(tokens.contains(&Token::Ne));
        assert!(tokens.contains(&Token::Le));
        assert!(tokens.contains(&Token::Ge));
    }
}
