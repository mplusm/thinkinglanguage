// ThinkingLanguage — Recursive Descent Parser
// Licensed under MIT OR Apache-2.0
//
// Phase 0: Parses the core language subset:
//   - let bindings
//   - fn declarations
//   - if/else, while, for
//   - expressions (arithmetic, comparison, logical, pipe)
//   - function calls
//   - case expressions

use tl_ast::*;
use tl_errors::{ParserError, Span, TlError};
use tl_lexer::{SpannedToken, Token};

pub struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<SpannedToken>) -> Self {
        Self { tokens, pos: 0 }
    }

    /// Parse a complete program
    pub fn parse_program(&mut self) -> Result<Program, TlError> {
        let mut statements = Vec::new();
        while !self.is_at_end() {
            statements.push(self.parse_statement()?);
        }
        Ok(Program { statements })
    }

    // ── Helpers ──────────────────────────────────────────────

    fn peek(&self) -> &Token {
        &self.tokens[self.pos].token
    }

    fn peek_span(&self) -> Span {
        self.tokens[self.pos].span
    }

    fn advance(&mut self) -> &SpannedToken {
        let tok = &self.tokens[self.pos];
        if !self.is_at_end() {
            self.pos += 1;
        }
        tok
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.tokens.len() || matches!(self.peek(), Token::None_)
    }

    fn expect(&mut self, expected: &Token) -> Result<Span, TlError> {
        if self.peek() == expected {
            let span = self.peek_span();
            self.advance();
            Ok(span)
        } else {
            Err(TlError::Parser(ParserError {
                message: format!("Expected `{}`, found `{}`", token_name(expected), self.peek()),
                span: self.peek_span(),
                hint: None,
            }))
        }
    }

    fn expect_ident(&mut self) -> Result<String, TlError> {
        match self.peek().clone() {
            Token::Ident(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!("Expected identifier, found `{}`", self.peek()),
                span: self.peek_span(),
                hint: None,
            })),
        }
    }

    fn check(&self, token: &Token) -> bool {
        self.peek() == token
    }

    fn match_token(&mut self, token: &Token) -> bool {
        if self.check(token) {
            self.advance();
            true
        } else {
            false
        }
    }

    // ── Statement Parsing ────────────────────────────────────

    fn parse_statement(&mut self) -> Result<Stmt, TlError> {
        match self.peek() {
            Token::Let => self.parse_let(),
            Token::Fn => self.parse_fn_decl(),
            Token::If => self.parse_if(),
            Token::While => self.parse_while(),
            Token::For => self.parse_for(),
            Token::Return => self.parse_return(),
            Token::Schema => self.parse_schema(),
            Token::Model => self.parse_train(),
            Token::Pipeline => self.parse_pipeline(),
            Token::Stream => self.parse_stream_decl(),
            Token::Source => self.parse_source_decl(),
            Token::Sink => self.parse_sink_decl(),
            Token::Break => {
                self.advance();
                Ok(Stmt::Break)
            }
            Token::Continue => {
                self.advance();
                Ok(Stmt::Continue)
            }
            _ => {
                let expr = self.parse_expression()?;
                Ok(Stmt::Expr(expr))
            }
        }
    }

    /// Parse `schema Name { field: type, ... }`
    fn parse_schema(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'schema'
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut fields = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let field_name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let type_ann = self.parse_type()?;
            self.match_token(&Token::Comma); // optional trailing comma
            fields.push(SchemaField {
                name: field_name,
                type_ann,
            });
        }
        self.expect(&Token::RBrace)?;
        Ok(Stmt::Schema { name, fields })
    }

    /// Parse `model <name> = train <algorithm> { key: value, ... }`
    fn parse_train(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'model'
        let name = self.expect_ident()?;
        self.expect(&Token::Assign)?;
        self.expect(&Token::Train)?;
        let algorithm = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut config = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_expression()?;
            self.match_token(&Token::Comma); // optional trailing comma
            config.push((key, value));
        }
        self.expect(&Token::RBrace)?;
        Ok(Stmt::Train {
            name,
            algorithm,
            config,
        })
    }

    /// Parse `pipeline NAME { schedule: "...", timeout: "...", retries: N, extract { ... } transform { ... } load { ... } on_failure { ... } on_success { ... } }`
    fn parse_pipeline(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'pipeline'
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut extract = Vec::new();
        let mut transform = Vec::new();
        let mut load = Vec::new();
        let mut schedule = None;
        let mut timeout = None;
        let mut retries = None;
        let mut on_failure = None;
        let mut on_success = None;

        while !self.check(&Token::RBrace) && !self.is_at_end() {
            match self.peek() {
                Token::Extract => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    extract = self.parse_block_body()?;
                    self.expect(&Token::RBrace)?;
                }
                Token::Transform => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    transform = self.parse_block_body()?;
                    self.expect(&Token::RBrace)?;
                }
                Token::Load => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    load = self.parse_block_body()?;
                    self.expect(&Token::RBrace)?;
                }
                Token::Ident(s) if s == "schedule" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        schedule = Some(s);
                    } else {
                        schedule = Some(self.parse_duration_literal()?);
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "timeout" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        timeout = Some(s);
                    } else {
                        timeout = Some(self.parse_duration_literal()?);
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "retries" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::Int(n) = self.peek().clone() {
                        self.advance();
                        retries = Some(n);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected integer for retries".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "on_failure" => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    on_failure = Some(self.parse_block_body()?);
                    self.expect(&Token::RBrace)?;
                }
                Token::Ident(s) if s == "on_success" => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    on_success = Some(self.parse_block_body()?);
                    self.expect(&Token::RBrace)?;
                }
                _ => {
                    return Err(TlError::Parser(ParserError {
                        message: format!(
                            "Unexpected token in pipeline block: `{}`",
                            self.peek()
                        ),
                        span: self.peek_span(),
                        hint: Some("Expected extract, transform, load, schedule, timeout, retries, on_failure, or on_success".into()),
                    }));
                }
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(Stmt::Pipeline {
            name,
            extract,
            transform,
            load,
            schedule,
            timeout,
            retries,
            on_failure,
            on_success,
        })
    }

    /// Parse `stream NAME { source: expr, window: spec, watermark: "duration", transform: { ... }, sink: expr }`
    fn parse_stream_decl(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'stream'
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut source = None;
        let mut transform = Vec::new();
        let mut sink = None;
        let mut window = None;
        let mut watermark = None;

        while !self.check(&Token::RBrace) && !self.is_at_end() {
            match self.peek() {
                Token::Source => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    source = Some(self.parse_expression()?);
                    self.match_token(&Token::Comma);
                }
                Token::Sink => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    sink = Some(self.parse_expression()?);
                    self.match_token(&Token::Comma);
                }
                Token::Transform => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    self.expect(&Token::LBrace)?;
                    transform = self.parse_block_body()?;
                    self.expect(&Token::RBrace)?;
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "window" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    window = Some(self.parse_window_spec()?);
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "watermark" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        watermark = Some(s);
                    } else {
                        watermark = Some(self.parse_duration_literal()?);
                    }
                    self.match_token(&Token::Comma);
                }
                _ => {
                    return Err(TlError::Parser(ParserError {
                        message: format!(
                            "Unexpected token in stream block: `{}`",
                            self.peek()
                        ),
                        span: self.peek_span(),
                        hint: Some("Expected source, sink, transform, window, or watermark".into()),
                    }));
                }
            }
        }
        self.expect(&Token::RBrace)?;

        let source = source.ok_or_else(|| {
            TlError::Parser(ParserError {
                message: "Stream declaration requires a source".to_string(),
                span: self.peek_span(),
                hint: None,
            })
        })?;

        Ok(Stmt::StreamDecl {
            name,
            source,
            transform,
            sink,
            window,
            watermark,
        })
    }

    /// Parse `source NAME = connector TYPE { key: value, ... }`
    fn parse_source_decl(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'source'
        let name = self.expect_ident()?;
        self.expect(&Token::Assign)?;
        self.expect(&Token::Connector)?;
        let connector_type = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut config = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_expression()?;
            self.match_token(&Token::Comma);
            config.push((key, value));
        }
        self.expect(&Token::RBrace)?;
        Ok(Stmt::SourceDecl {
            name,
            connector_type,
            config,
        })
    }

    /// Parse `sink NAME = connector TYPE { key: value, ... }`
    fn parse_sink_decl(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'sink'
        let name = self.expect_ident()?;
        self.expect(&Token::Assign)?;
        self.expect(&Token::Connector)?;
        let connector_type = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut config = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_expression()?;
            self.match_token(&Token::Comma);
            config.push((key, value));
        }
        self.expect(&Token::RBrace)?;
        Ok(Stmt::SinkDecl {
            name,
            connector_type,
            config,
        })
    }

    /// Parse a window specification: `tumbling(DURATION)`, `sliding(DURATION, DURATION)`, `session(DURATION)`
    fn parse_window_spec(&mut self) -> Result<WindowSpec, TlError> {
        let kind = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        match kind.as_str() {
            "tumbling" => {
                let dur = self.parse_duration_literal()?;
                self.expect(&Token::RParen)?;
                Ok(WindowSpec::Tumbling(dur))
            }
            "sliding" => {
                let window = self.parse_duration_literal()?;
                self.expect(&Token::Comma)?;
                let slide = self.parse_duration_literal()?;
                self.expect(&Token::RParen)?;
                Ok(WindowSpec::Sliding(window, slide))
            }
            "session" => {
                let gap = self.parse_duration_literal()?;
                self.expect(&Token::RParen)?;
                Ok(WindowSpec::Session(gap))
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!("Unknown window type: `{kind}`"),
                span: self.peek_span(),
                hint: Some("Expected tumbling, sliding, or session".into()),
            })),
        }
    }

    /// Parse a duration literal token (e.g., `5m`, `30s`, `100ms`) into a string like "5m"
    fn parse_duration_literal(&mut self) -> Result<String, TlError> {
        match self.peek().clone() {
            Token::DurationMs(n) => {
                self.advance();
                Ok(format!("{n}ms"))
            }
            Token::DurationS(n) => {
                self.advance();
                Ok(format!("{n}s"))
            }
            Token::DurationM(n) => {
                self.advance();
                Ok(format!("{n}m"))
            }
            Token::DurationH(n) => {
                self.advance();
                Ok(format!("{n}h"))
            }
            Token::DurationD(n) => {
                self.advance();
                Ok(format!("{n}d"))
            }
            Token::String(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!("Expected duration literal, found `{}`", self.peek()),
                span: self.peek_span(),
                hint: Some("Expected a duration like 5m, 30s, 100ms, 1h, or 1d".into()),
            })),
        }
    }

    fn parse_let(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'let'
        let mutable = self.match_token(&Token::Mut);
        let name = self.expect_ident()?;
        let type_ann = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };
        self.expect(&Token::Assign)?;
        let value = self.parse_expression()?;
        Ok(Stmt::Let {
            name,
            mutable,
            type_ann,
            value,
        })
    }

    fn parse_fn_decl(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'fn'
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let params = self.parse_param_list()?;
        self.expect(&Token::RParen)?;
        let return_type = if self.match_token(&Token::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };
        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        Ok(Stmt::FnDecl {
            name,
            params,
            return_type,
            body,
        })
    }

    fn parse_if(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'if'
        let condition = self.parse_expression()?;
        self.expect(&Token::LBrace)?;
        let then_body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;

        let mut else_ifs = Vec::new();
        let mut else_body = None;

        while self.match_token(&Token::Else) {
            if self.match_token(&Token::If) {
                let cond = self.parse_expression()?;
                self.expect(&Token::LBrace)?;
                let body = self.parse_block_body()?;
                self.expect(&Token::RBrace)?;
                else_ifs.push((cond, body));
            } else {
                self.expect(&Token::LBrace)?;
                else_body = Some(self.parse_block_body()?);
                self.expect(&Token::RBrace)?;
                break;
            }
        }

        Ok(Stmt::If {
            condition,
            then_body,
            else_ifs,
            else_body,
        })
    }

    fn parse_while(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'while'
        let condition = self.parse_expression()?;
        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        Ok(Stmt::While { condition, body })
    }

    fn parse_for(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'for'
        let name = self.expect_ident()?;
        self.expect(&Token::In)?;
        let iter = self.parse_expression()?;
        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        Ok(Stmt::For { name, iter, body })
    }

    fn parse_return(&mut self) -> Result<Stmt, TlError> {
        self.advance(); // consume 'return'
        if self.check(&Token::RBrace) || self.is_at_end() {
            Ok(Stmt::Return(None))
        } else {
            Ok(Stmt::Return(Some(self.parse_expression()?)))
        }
    }

    fn parse_block_body(&mut self) -> Result<Vec<Stmt>, TlError> {
        let mut stmts = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            stmts.push(self.parse_statement()?);
        }
        Ok(stmts)
    }

    // ── Expression Parsing (Pratt / precedence climbing) ─────

    fn parse_expression(&mut self) -> Result<Expr, TlError> {
        let expr = self.parse_pipe()?;
        // Assignment: target = value
        if self.match_token(&Token::Assign) {
            let value = self.parse_expression()?;
            return Ok(Expr::Assign {
                target: Box::new(expr),
                value: Box::new(value),
            });
        }
        Ok(expr)
    }

    /// Pipe: expr |> expr |> expr
    fn parse_pipe(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_null_coalesce()?;
        while self.match_token(&Token::Pipe) {
            let right = self.parse_null_coalesce()?;
            left = Expr::Pipe {
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Null coalesce: expr ?? expr
    fn parse_null_coalesce(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_or()?;
        while self.match_token(&Token::NullCoalesce) {
            let right = self.parse_or()?;
            left = Expr::NullCoalesce {
                expr: Box::new(left),
                default: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Logical OR: expr or expr
    fn parse_or(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_and()?;
        while self.match_token(&Token::Or) {
            let right = self.parse_and()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Logical AND: expr and expr
    fn parse_and(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_comparison()?;
        while self.match_token(&Token::And) {
            let right = self.parse_comparison()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Comparison: expr (== | != | < | > | <= | >=) expr
    fn parse_comparison(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_addition()?;
        loop {
            let op = match self.peek() {
                Token::Eq => BinOp::Eq,
                Token::Neq => BinOp::Neq,
                Token::Lt => BinOp::Lt,
                Token::Gt => BinOp::Gt,
                Token::Lte => BinOp::Lte,
                Token::Gte => BinOp::Gte,
                _ => break,
            };
            self.advance();
            let right = self.parse_addition()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Addition/subtraction: expr (+ | -) expr
    fn parse_addition(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Multiplication/division/modulo: expr (* | / | %) expr
    fn parse_multiplication(&mut self) -> Result<Expr, TlError> {
        let mut left = self.parse_power()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_power()?;
            left = Expr::BinOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Power: expr ** expr (right-associative)
    fn parse_power(&mut self) -> Result<Expr, TlError> {
        let left = self.parse_unary()?;
        if self.match_token(&Token::Power) {
            let right = self.parse_power()?; // right-associative
            Ok(Expr::BinOp {
                left: Box::new(left),
                op: BinOp::Pow,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    /// Unary: not expr | -expr
    fn parse_unary(&mut self) -> Result<Expr, TlError> {
        if self.match_token(&Token::Not) {
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        if self.match_token(&Token::Minus) {
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        self.parse_postfix()
    }

    /// Postfix: expr.field | expr(args) | expr[index]
    fn parse_postfix(&mut self) -> Result<Expr, TlError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.match_token(&Token::Dot) {
                let field = self.expect_ident()?;
                expr = Expr::Member {
                    object: Box::new(expr),
                    field,
                };
            } else if self.check(&Token::LParen) {
                self.advance();
                let args = self.parse_arg_list()?;
                self.expect(&Token::RParen)?;
                expr = Expr::Call {
                    function: Box::new(expr),
                    args,
                };
            } else if self.match_token(&Token::LBracket) {
                let index = self.parse_expression()?;
                self.expect(&Token::RBracket)?;
                expr = Expr::Index {
                    object: Box::new(expr),
                    index: Box::new(index),
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    /// Primary: literals, identifiers, parenthesized expressions, etc.
    fn parse_primary(&mut self) -> Result<Expr, TlError> {
        let token = self.peek().clone();
        match token {
            Token::Int(n) => {
                self.advance();
                Ok(Expr::Int(n))
            }
            Token::Float(n) => {
                self.advance();
                Ok(Expr::Float(n))
            }
            Token::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::String(s))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Bool(false))
            }
            Token::None_ => {
                self.advance();
                Ok(Expr::None)
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();
                Ok(Expr::Ident(name))
            }
            Token::LParen => {
                if self.is_closure_ahead() {
                    self.parse_closure()
                } else {
                    self.advance();
                    let expr = self.parse_expression()?;
                    self.expect(&Token::RParen)?;
                    Ok(expr)
                }
            }
            Token::LBracket => {
                self.advance();
                let mut elements = Vec::new();
                if !self.check(&Token::RBracket) {
                    elements.push(self.parse_expression()?);
                    while self.match_token(&Token::Comma) {
                        if self.check(&Token::RBracket) {
                            break;
                        }
                        elements.push(self.parse_expression()?);
                    }
                }
                self.expect(&Token::RBracket)?;
                Ok(Expr::List(elements))
            }
            Token::Case => {
                self.advance();
                self.expect(&Token::LBrace)?;
                let mut arms = Vec::new();
                while !self.check(&Token::RBrace) && !self.is_at_end() {
                    let pattern = self.parse_expression()?;
                    self.expect(&Token::FatArrow)?;
                    let body = self.parse_expression()?;
                    self.match_token(&Token::Comma); // optional trailing comma
                    arms.push((pattern, body));
                }
                self.expect(&Token::RBrace)?;
                Ok(Expr::Case { arms })
            }
            Token::Match => {
                self.advance(); // consume 'match'
                let subject = self.parse_expression()?;
                self.expect(&Token::LBrace)?;
                let mut arms = Vec::new();
                while !self.check(&Token::RBrace) && !self.is_at_end() {
                    let pattern = self.parse_expression()?;
                    self.expect(&Token::FatArrow)?;
                    let body = self.parse_expression()?;
                    self.match_token(&Token::Comma); // optional trailing comma
                    arms.push((pattern, body));
                }
                self.expect(&Token::RBrace)?;
                Ok(Expr::Match {
                    subject: Box::new(subject),
                    arms,
                })
            }
            Token::With => {
                self.advance(); // consume 'with'
                self.expect(&Token::LBrace)?;
                let mut pairs = Vec::new();
                while !self.check(&Token::RBrace) && !self.is_at_end() {
                    let key_name = self.expect_ident()?;
                    self.expect(&Token::Assign)?;
                    let value = self.parse_expression()?;
                    self.match_token(&Token::Comma); // optional trailing comma
                    pairs.push((Expr::String(key_name), value));
                }
                self.expect(&Token::RBrace)?;
                Ok(Expr::Call {
                    function: Box::new(Expr::Ident("with".to_string())),
                    args: vec![Expr::Map(pairs)],
                })
            }
            Token::Emit => {
                // 'emit' is a keyword but used as a builtin function
                self.advance();
                Ok(Expr::Ident("emit".to_string()))
            }
            Token::Underscore => {
                self.advance();
                Ok(Expr::Ident("_".to_string()))
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!("Unexpected token: `{}`", self.peek()),
                span: self.peek_span(),
                hint: Some("Expected an expression (literal, variable, or function call)".into()),
            })),
        }
    }

    /// Look ahead from current `(` to determine if this is a closure `(params) => expr`.
    /// Scans forward to find the matching `)`, then checks if the next token is `=>`.
    fn is_closure_ahead(&self) -> bool {
        // Current token should be LParen
        let mut i = self.pos + 1; // skip the LParen
        let mut depth = 1;
        while i < self.tokens.len() {
            match &self.tokens[i].token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        // Check if next token after `)` is `=>`
                        return i + 1 < self.tokens.len()
                            && self.tokens[i + 1].token == Token::FatArrow;
                    }
                }
                Token::None_ => return false, // hit EOF
                _ => {}
            }
            i += 1;
        }
        false
    }

    /// Parse a closure: `(params) => expr`
    fn parse_closure(&mut self) -> Result<Expr, TlError> {
        self.expect(&Token::LParen)?;
        let params = self.parse_param_list()?;
        self.expect(&Token::RParen)?;
        self.expect(&Token::FatArrow)?;
        let body = self.parse_expression()?;
        Ok(Expr::Closure {
            params,
            body: Box::new(body),
        })
    }

    // ── Argument & Parameter Lists ───────────────────────────

    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, TlError> {
        let mut args = Vec::new();
        if self.check(&Token::RParen) {
            return Ok(args);
        }
        args.push(self.parse_arg()?);
        while self.match_token(&Token::Comma) {
            if self.check(&Token::RParen) {
                break;
            }
            args.push(self.parse_arg()?);
        }
        Ok(args)
    }

    fn parse_arg(&mut self) -> Result<Expr, TlError> {
        // Check for named argument: `name: value`
        if let Token::Ident(name) = self.peek().clone() {
            let name = name.clone();
            if self.pos + 1 < self.tokens.len() && self.tokens[self.pos + 1].token == Token::Colon {
                self.advance(); // consume name
                self.advance(); // consume colon
                let value = self.parse_expression()?;
                return Ok(Expr::NamedArg {
                    name,
                    value: Box::new(value),
                });
            }
        }
        self.parse_expression()
    }

    fn parse_param_list(&mut self) -> Result<Vec<Param>, TlError> {
        let mut params = Vec::new();
        if self.check(&Token::RParen) {
            return Ok(params);
        }
        params.push(self.parse_param()?);
        while self.match_token(&Token::Comma) {
            if self.check(&Token::RParen) {
                break;
            }
            params.push(self.parse_param()?);
        }
        Ok(params)
    }

    fn parse_param(&mut self) -> Result<Param, TlError> {
        let name = self.expect_ident()?;
        let type_ann = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };
        Ok(Param { name, type_ann })
    }

    // ── Type Parsing ─────────────────────────────────────────

    fn parse_type(&mut self) -> Result<TypeExpr, TlError> {
        let name = self.expect_ident()?;
        if self.match_token(&Token::Lt) {
            let mut args = Vec::new();
            args.push(self.parse_type()?);
            while self.match_token(&Token::Comma) {
                args.push(self.parse_type()?);
            }
            self.expect(&Token::Gt)?;
            Ok(TypeExpr::Generic { name, args })
        } else {
            Ok(TypeExpr::Named(name))
        }
    }
}

/// Helper: human-readable token name for error messages
fn token_name(token: &Token) -> &'static str {
    match token {
        Token::LParen => "(",
        Token::RParen => ")",
        Token::LBrace => "{",
        Token::RBrace => "}",
        Token::LBracket => "[",
        Token::RBracket => "]",
        Token::Comma => ",",
        Token::Colon => ":",
        Token::Semicolon => ";",
        Token::Assign => "=",
        Token::Arrow => "->",
        Token::FatArrow => "=>",
        Token::Pipe => "|>",
        Token::Let => "let",
        Token::Fn => "fn",
        Token::If => "if",
        Token::Else => "else",
        Token::Return => "return",
        Token::In => "in",
        Token::Dot => ".",
        Token::Lt => "<",
        Token::Gt => ">",
        _ => "token",
    }
}

/// Convenience: parse source text directly
pub fn parse(source: &str) -> Result<Program, TlError> {
    let tokens = tl_lexer::tokenize(source)?;
    let mut parser = Parser::new(tokens);
    parser.parse_program()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_let() {
        let program = parse("let x = 42").unwrap();
        assert_eq!(program.statements.len(), 1);
        assert!(matches!(&program.statements[0], Stmt::Let { name, .. } if name == "x"));
    }

    #[test]
    fn test_parse_fn() {
        let program = parse("fn add(a: int64, b: int64) -> int64 { a + b }").unwrap();
        assert_eq!(program.statements.len(), 1);
        assert!(
            matches!(&program.statements[0], Stmt::FnDecl { name, .. } if name == "add")
        );
    }

    #[test]
    fn test_parse_pipe() {
        let program = parse("let result = x |> double()").unwrap();
        if let Stmt::Let { value, .. } = &program.statements[0] {
            assert!(matches!(value, Expr::Pipe { .. }));
        } else {
            panic!("Expected let statement");
        }
    }

    #[test]
    fn test_parse_if_else() {
        let program = parse("if x > 5 { x } else { 0 }").unwrap();
        assert!(matches!(program.statements[0], Stmt::If { .. }));
    }

    #[test]
    fn test_parse_nested_arithmetic() {
        let program = parse("let x = 1 + 2 * 3").unwrap();
        // Should parse as 1 + (2 * 3) due to precedence
        if let Stmt::Let { value, .. } = &program.statements[0] {
            assert!(matches!(value, Expr::BinOp { op: BinOp::Add, .. }));
        }
    }

    #[test]
    fn test_parse_match() {
        let program = parse("match x { 1 => \"one\", 2 => \"two\", _ => \"other\" }").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let Stmt::Expr(Expr::Match { subject, arms }) = &program.statements[0] {
            assert!(matches!(subject.as_ref(), Expr::Ident(n) if n == "x"));
            assert_eq!(arms.len(), 3);
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_closure() {
        let program = parse("let double = (x) => x * 2").unwrap();
        if let Stmt::Let { value, .. } = &program.statements[0] {
            assert!(matches!(value, Expr::Closure { .. }));
        } else {
            panic!("Expected let with closure");
        }
    }

    #[test]
    fn test_parse_function_call() {
        let program = parse("print(42)").unwrap();
        if let Stmt::Expr(Expr::Call { function, args, .. }) = &program.statements[0] {
            assert!(matches!(function.as_ref(), Expr::Ident(n) if n == "print"));
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected function call");
        }
    }

    #[test]
    fn test_parse_schema() {
        let program = parse("schema User { id: int64, name: string, age: float64 }").unwrap();
        if let Stmt::Schema { name, fields } = &program.statements[0] {
            assert_eq!(name, "User");
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].name, "id");
            assert_eq!(fields[1].name, "name");
            assert_eq!(fields[2].name, "age");
        } else {
            panic!("Expected schema statement");
        }
    }

    #[test]
    fn test_parse_pipeline_basic() {
        let program = parse(r#"pipeline etl {
            extract { let data = read_csv("input.csv") }
            transform { let cleaned = data }
            load { write_csv(cleaned, "output.csv") }
        }"#).unwrap();
        if let Stmt::Pipeline { name, extract, transform, load, .. } = &program.statements[0] {
            assert_eq!(name, "etl");
            assert_eq!(extract.len(), 1);
            assert_eq!(transform.len(), 1);
            assert_eq!(load.len(), 1);
        } else {
            panic!("Expected pipeline statement");
        }
    }

    #[test]
    fn test_parse_pipeline_with_options() {
        let program = parse(r#"pipeline daily_etl {
            schedule: "0 0 * * *",
            timeout: "30m",
            retries: 3,
            extract { let data = read_csv("input.csv") }
            transform { let cleaned = data }
            load { write_csv(cleaned, "output.csv") }
            on_failure { println("Pipeline failed!") }
            on_success { println("Pipeline succeeded!") }
        }"#).unwrap();
        if let Stmt::Pipeline { name, schedule, timeout, retries, on_failure, on_success, .. } = &program.statements[0] {
            assert_eq!(name, "daily_etl");
            assert_eq!(schedule.as_deref(), Some("0 0 * * *"));
            assert_eq!(timeout.as_deref(), Some("30m"));
            assert_eq!(*retries, Some(3));
            assert!(on_failure.is_some());
            assert!(on_success.is_some());
        } else {
            panic!("Expected pipeline statement");
        }
    }

    #[test]
    fn test_parse_stream_decl() {
        let program = parse(r#"stream events {
            source: src,
            window: tumbling(5m),
            transform: { let x = 1 },
            sink: out
        }"#).unwrap();
        if let Stmt::StreamDecl { name, source, window, sink, .. } = &program.statements[0] {
            assert_eq!(name, "events");
            assert!(matches!(source, Expr::Ident(s) if s == "src"));
            assert!(matches!(window, Some(WindowSpec::Tumbling(d)) if d == "5m"));
            assert!(matches!(sink, Some(Expr::Ident(s)) if s == "out"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_stream_sliding_window() {
        let program = parse(r#"stream metrics {
            source: input,
            window: sliding(10m, 1m),
            transform: { let x = 1 }
        }"#).unwrap();
        if let Stmt::StreamDecl { window, .. } = &program.statements[0] {
            assert!(matches!(window, Some(WindowSpec::Sliding(w, s)) if w == "10m" && s == "1m"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_stream_session_window() {
        let program = parse(r#"stream sessions {
            source: clicks,
            window: session(30m),
            transform: { let x = 1 }
        }"#).unwrap();
        if let Stmt::StreamDecl { window, .. } = &program.statements[0] {
            assert!(matches!(window, Some(WindowSpec::Session(d)) if d == "30m"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_source_decl() {
        let program = parse(r#"source kafka_in = connector kafka {
            topic: "events",
            group: "my_group"
        }"#).unwrap();
        if let Stmt::SourceDecl { name, connector_type, config } = &program.statements[0] {
            assert_eq!(name, "kafka_in");
            assert_eq!(connector_type, "kafka");
            assert_eq!(config.len(), 2);
            assert_eq!(config[0].0, "topic");
            assert_eq!(config[1].0, "group");
        } else {
            panic!("Expected source declaration");
        }
    }

    #[test]
    fn test_parse_sink_decl() {
        let program = parse(r#"sink output = connector channel {
            buffer: 100
        }"#).unwrap();
        if let Stmt::SinkDecl { name, connector_type, config } = &program.statements[0] {
            assert_eq!(name, "output");
            assert_eq!(connector_type, "channel");
            assert_eq!(config.len(), 1);
            assert_eq!(config[0].0, "buffer");
        } else {
            panic!("Expected sink declaration");
        }
    }

    #[test]
    fn test_parse_pipeline_with_duration_tokens() {
        let program = parse(r#"pipeline fast {
            timeout: 30s,
            extract { let x = 1 }
            transform { let y = x }
            load { println(y) }
        }"#).unwrap();
        if let Stmt::Pipeline { timeout, .. } = &program.statements[0] {
            assert_eq!(timeout.as_deref(), Some("30s"));
        } else {
            panic!("Expected pipeline statement");
        }
    }

    #[test]
    fn test_parse_stream_with_watermark() {
        let program = parse(r#"stream delayed {
            source: input,
            watermark: 10s,
            transform: { let x = 1 }
        }"#).unwrap();
        if let Stmt::StreamDecl { watermark, .. } = &program.statements[0] {
            assert_eq!(watermark.as_deref(), Some("10s"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_with_block() {
        let program = parse("with { doubled = age * 2, name = first }").unwrap();
        if let Stmt::Expr(Expr::Call { function, args }) = &program.statements[0] {
            assert!(matches!(function.as_ref(), Expr::Ident(n) if n == "with"));
            assert_eq!(args.len(), 1);
            if let Expr::Map(pairs) = &args[0] {
                assert_eq!(pairs.len(), 2);
            } else {
                panic!("Expected Map arg");
            }
        } else {
            panic!("Expected with call expression");
        }
    }
}
