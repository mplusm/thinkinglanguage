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

/// Helper to create a Stmt with span from start position to previous token
fn make_stmt(kind: StmtKind, start: usize, end: usize) -> Stmt {
    Stmt {
        kind,
        span: Span::new(start, end),
        doc_comment: None,
    }
}

pub struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
    depth: usize,
}

const MAX_PARSER_DEPTH: usize = 256;

impl Parser {
    pub fn new(tokens: Vec<SpannedToken>) -> Self {
        Self {
            tokens,
            pos: 0,
            depth: 0,
        }
    }

    fn enter_depth(&mut self) -> Result<(), TlError> {
        self.depth += 1;
        if self.depth > MAX_PARSER_DEPTH {
            Err(TlError::Parser(ParserError {
                message: "Maximum parser nesting depth (256) exceeded".to_string(),
                span: Span::new(0, 0),
                hint: None,
            }))
        } else {
            Ok(())
        }
    }

    fn leave_depth(&mut self) {
        self.depth -= 1;
    }

    /// Parse a complete program
    pub fn parse_program(&mut self) -> Result<Program, TlError> {
        // Collect module-level //! doc comments at the start
        let module_doc = self.consume_inner_doc_comments();

        let mut statements = Vec::new();
        while !self.is_at_end() {
            statements.push(self.parse_statement()?);
        }
        Ok(Program {
            statements,
            module_doc,
        })
    }

    /// Consume consecutive `///` doc comment tokens and join them
    fn consume_doc_comments(&mut self) -> Option<String> {
        let mut lines = Vec::new();
        while let Token::DocComment(text) = self.peek().clone() {
            lines.push(text.trim().to_string());
            self.advance();
        }
        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    /// Consume consecutive `//!` inner doc comment tokens and join them
    fn consume_inner_doc_comments(&mut self) -> Option<String> {
        let mut lines = Vec::new();
        while let Token::InnerDocComment(text) = self.peek().clone() {
            lines.push(text.trim().to_string());
            self.advance();
        }
        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    // ── Helpers ──────────────────────────────────────────────

    fn peek(&self) -> &Token {
        if self.pos >= self.tokens.len() {
            return &Token::None_;
        }
        &self.tokens[self.pos].token
    }

    fn peek_span(&self) -> Span {
        if self.pos >= self.tokens.len() {
            return Span::new(0, 0);
        }
        self.tokens[self.pos].span
    }

    fn previous_span(&self) -> Span {
        if self.tokens.is_empty() {
            return Span::new(0, 0);
        }
        if self.pos > 0 {
            self.tokens[self.pos - 1].span
        } else {
            self.tokens[0].span
        }
    }

    fn advance(&mut self) -> &SpannedToken {
        if self.pos >= self.tokens.len() {
            // Return last token (EOF sentinel) if past end
            return self.tokens.last().expect("token list must not be empty");
        }
        let tok = &self.tokens[self.pos];
        if !self.is_at_end() {
            self.pos += 1;
        }
        tok
    }

    fn is_at_end(&self) -> bool {
        if self.tokens.is_empty() {
            return true;
        }
        // The tokenizer always appends a Token::None_ (EOF) sentinel as the last token.
        // Check position rather than token value so the `none` keyword works correctly.
        self.pos >= self.tokens.len() - 1
    }

    fn expect(&mut self, expected: &Token) -> Result<Span, TlError> {
        if self.peek() == expected {
            let span = self.peek_span();
            self.advance();
            Ok(span)
        } else {
            Err(TlError::Parser(ParserError {
                message: format!(
                    "Expected `{}`, found `{}`",
                    token_name(expected),
                    self.peek()
                ),
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
            Token::Self_ => {
                self.advance();
                Ok("self".to_string())
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
        // Consume any doc comments before the statement
        let doc = self.consume_doc_comments();
        // Skip any inner doc comments that appear mid-file
        while matches!(self.peek(), Token::InnerDocComment(_)) {
            self.advance();
        }
        let mut stmt = self.parse_statement_inner()?;
        if let Some(ref doc_text) = doc {
            if stmt.doc_comment.is_none() {
                stmt.doc_comment = doc.clone();
            }
            // Extract @version annotation from doc comment and apply to Schema
            if let StmtKind::Schema {
                ref mut version,
                ref mut parent_version,
                ..
            } = stmt.kind
            {
                for line in doc_text.lines() {
                    let trimmed = line.trim();
                    if let Some(rest) = trimmed.strip_prefix("@version") {
                        if let Ok(v) = rest.trim().parse::<i64>() {
                            *version = Some(v);
                        }
                    } else if let Some(rest) = trimmed.strip_prefix("@evolves")
                        && let Ok(v) = rest.trim().parse::<i64>()
                    {
                        *parent_version = Some(v);
                    }
                }
            }
        }
        Ok(stmt)
    }

    fn parse_statement_inner(&mut self) -> Result<Stmt, TlError> {
        match self.peek() {
            Token::Let => self.parse_let(),
            Token::Type => self.parse_type_alias(false),
            Token::Fn => self.parse_fn_decl(),
            Token::Async => {
                let start = self.peek_span().start;
                self.advance(); // consume 'async'
                if self.check(&Token::Fn) {
                    let mut stmt = self.parse_fn_decl()?;
                    if let StmtKind::FnDecl {
                        ref mut is_async, ..
                    } = stmt.kind
                    {
                        *is_async = true;
                    }
                    stmt.span = Span::new(start, stmt.span.end);
                    Ok(stmt)
                } else {
                    Err(TlError::Parser(tl_errors::ParserError {
                        message: "Expected 'fn' after 'async'".to_string(),
                        span: Span::new(start, self.peek_span().end),
                        hint: None,
                    }))
                }
            }
            Token::If => self.parse_if(),
            Token::While => self.parse_while(),
            Token::For => self.parse_for(),
            Token::Return => self.parse_return(),
            Token::Schema => self.parse_schema(),
            Token::Struct => self.parse_struct_decl(),
            Token::Enum => self.parse_enum_decl(),
            Token::Impl => self.parse_impl(),
            Token::Trait => self.parse_trait_def(),
            Token::Model => self.parse_train(),
            Token::Pipeline => self.parse_pipeline(),
            Token::Agent => self.parse_agent(),
            Token::Stream => self.parse_stream_decl(),
            Token::Source => self.parse_source_decl(),
            Token::Sink => self.parse_sink_decl(),
            Token::Try => self.parse_try_catch(),
            Token::Throw => self.parse_throw(),
            Token::Import => self.parse_import(),
            Token::Use => self.parse_use(false),
            Token::Pub => self.parse_pub(),
            Token::Mod => self.parse_mod(false),
            Token::Test => self.parse_test(),
            Token::Migrate => self.parse_migrate(),
            Token::Parallel => {
                let start = self.peek_span().start;
                self.advance(); // consume 'parallel'
                self.expect(&Token::For)?;
                let name = self.expect_ident()?;
                self.expect(&Token::In)?;
                let iter = self.parse_expression()?;
                self.expect(&Token::LBrace)?;
                let body = self.parse_block_body()?;
                self.expect(&Token::RBrace)?;
                let end = self.previous_span().end;
                Ok(make_stmt(
                    StmtKind::ParallelFor { name, iter, body },
                    start,
                    end,
                ))
            }
            Token::Break => {
                let start = self.peek_span().start;
                self.advance();
                let end = self.previous_span().end;
                Ok(make_stmt(StmtKind::Break, start, end))
            }
            Token::Continue => {
                let start = self.peek_span().start;
                self.advance();
                let end = self.previous_span().end;
                Ok(make_stmt(StmtKind::Continue, start, end))
            }
            _ => {
                let start = self.peek_span().start;
                let expr = self.parse_expression()?;
                let end = self.previous_span().end;
                Ok(make_stmt(StmtKind::Expr(expr), start, end))
            }
        }
    }

    /// Parse `pub` visibility modifier
    fn parse_pub(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'pub'
        match self.peek() {
            Token::Fn => {
                let mut stmt = self.parse_fn_decl()?;
                if let StmtKind::FnDecl {
                    ref mut is_public, ..
                } = stmt.kind
                {
                    *is_public = true;
                }
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Struct => {
                let mut stmt = self.parse_struct_decl()?;
                if let StmtKind::StructDecl {
                    ref mut is_public, ..
                } = stmt.kind
                {
                    *is_public = true;
                }
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Enum => {
                let mut stmt = self.parse_enum_decl()?;
                if let StmtKind::EnumDecl {
                    ref mut is_public, ..
                } = stmt.kind
                {
                    *is_public = true;
                }
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Schema => {
                let mut stmt = self.parse_schema()?;
                if let StmtKind::Schema {
                    ref mut is_public, ..
                } = stmt.kind
                {
                    *is_public = true;
                }
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Let => {
                let mut stmt = self.parse_let_with_pub(true)?;
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Use => {
                let mut stmt = self.parse_use(true)?;
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Mod => {
                let mut stmt = self.parse_mod(true)?;
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Trait => {
                let mut stmt = self.parse_trait_def()?;
                if let StmtKind::TraitDef {
                    ref mut is_public, ..
                } = stmt.kind
                {
                    *is_public = true;
                }
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            Token::Type => {
                let mut stmt = self.parse_type_alias(true)?;
                stmt.span = Span::new(start, stmt.span.end);
                Ok(stmt)
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!(
                    "`pub` can only be applied to fn, struct, enum, schema, let, use, mod, trait, or type, found `{}`",
                    self.peek()
                ),
                span: self.peek_span(),
                hint: None,
            })),
        }
    }

    /// Parse `use` import statement with dot-path syntax
    fn parse_use(&mut self, is_public: bool) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'use'

        // Parse dot-separated path segments
        let mut segments = Vec::new();
        segments.push(self.expect_ident()?);

        while self.match_token(&Token::Dot) {
            match self.peek() {
                Token::LBrace => {
                    // Group import: use data.transforms.{a, b}
                    self.advance(); // consume '{'
                    let mut names = Vec::new();
                    names.push(self.expect_ident()?);
                    while self.match_token(&Token::Comma) {
                        if self.check(&Token::RBrace) {
                            break; // trailing comma
                        }
                        names.push(self.expect_ident()?);
                    }
                    self.expect(&Token::RBrace)?;
                    let end = self.previous_span().end;
                    return Ok(make_stmt(
                        StmtKind::Use {
                            item: UseItem::Group(segments, names),
                            is_public,
                        },
                        start,
                        end,
                    ));
                }
                Token::Star => {
                    // Wildcard import: use data.transforms.*
                    self.advance(); // consume '*'
                    let end = self.previous_span().end;
                    return Ok(make_stmt(
                        StmtKind::Use {
                            item: UseItem::Wildcard(segments),
                            is_public,
                        },
                        start,
                        end,
                    ));
                }
                Token::Ident(_) => {
                    segments.push(self.expect_ident()?);
                }
                _ => {
                    return Err(TlError::Parser(ParserError {
                        message: format!(
                            "Expected identifier, `{{`, or `*` after `.` in use path, found `{}`",
                            self.peek()
                        ),
                        span: self.peek_span(),
                        hint: None,
                    }));
                }
            }
        }

        // Check for alias: use data.postgres as pg
        if self.match_token(&Token::As) {
            let alias = self.expect_ident()?;
            let end = self.previous_span().end;
            return Ok(make_stmt(
                StmtKind::Use {
                    item: UseItem::Aliased(segments, alias),
                    is_public,
                },
                start,
                end,
            ));
        }

        // Single import
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Use {
                item: UseItem::Single(segments),
                is_public,
            },
            start,
            end,
        ))
    }

    /// Parse `mod name` declaration
    fn parse_mod(&mut self, is_public: bool) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'mod'
        let name = self.expect_ident()?;
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::ModDecl { name, is_public }, start, end))
    }

    /// Parse `schema Name { field: type, ... }`
    /// Supports `@version N` in doc comment and field-level `///` doc comments with `= default`
    fn parse_schema(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'schema'
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut fields = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            // Consume field-level doc comments
            let field_doc = self.consume_doc_comments();
            let field_name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let type_ann = self.parse_type()?;
            // Optional default value: `= expr`
            let default_value = if self.match_token(&Token::Assign) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            self.match_token(&Token::Comma); // optional trailing comma
            // Parse security annotations from doc comment
            let annotations = parse_field_annotations(field_doc.as_deref());
            fields.push(SchemaField {
                name: field_name,
                type_ann,
                doc_comment: field_doc,
                default_value,
                annotations,
            });
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Schema {
                name,
                fields,
                is_public: false,
                version: None,
                parent_version: None,
            },
            start,
            end,
        ))
    }

    /// Parse `migrate SchemaName from V1 to V2 { ops... }`
    fn parse_migrate(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'migrate'
        let schema_name = self.expect_ident()?;

        // expect 'from'
        match self.peek() {
            Token::Ident(s) if s == "from" => {
                self.advance();
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected `from` after schema name in migrate statement".to_string(),
                    span: self.peek_span(),
                    hint: Some("migrate SchemaName from V1 to V2 { ... }".to_string()),
                }));
            }
        }
        let from_version = match self.peek() {
            Token::Int(n) => {
                let n = *n;
                self.advance();
                n
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected version number after `from`".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        };

        // expect 'to'
        match self.peek() {
            Token::Ident(s) if s == "to" => {
                self.advance();
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected `to` after from-version in migrate statement".to_string(),
                    span: self.peek_span(),
                    hint: Some("migrate SchemaName from V1 to V2 { ... }".to_string()),
                }));
            }
        }
        let to_version = match self.peek() {
            Token::Int(n) => {
                let n = *n;
                self.advance();
                n
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected version number after `to`".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        };

        self.expect(&Token::LBrace)?;
        let mut operations = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            operations.push(self.parse_migrate_op()?);
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Migrate {
                schema_name,
                from_version,
                to_version,
                operations,
            },
            start,
            end,
        ))
    }

    /// Parse a single migration operation like `add_column(name: type, default: expr)`
    fn parse_migrate_op(&mut self) -> Result<MigrateOp, TlError> {
        let op_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let result = match op_name.as_str() {
            "add_column" => {
                let name = self.expect_ident()?;
                self.expect(&Token::Colon)?;
                let type_ann = self.parse_type()?;
                let default = if self.match_token(&Token::Comma) {
                    // Check for `default:` named arg
                    if matches!(self.peek(), Token::Ident(s) if s == "default") {
                        self.advance(); // consume 'default'
                        self.expect(&Token::Colon)?;
                        Some(self.parse_expression()?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                MigrateOp::AddColumn { name, type_ann, default }
            }
            "drop_column" => {
                let name = self.expect_ident()?;
                MigrateOp::DropColumn { name }
            }
            "rename_column" => {
                let from = self.expect_ident()?;
                self.expect(&Token::Comma)?;
                let to = self.expect_ident()?;
                MigrateOp::RenameColumn { from, to }
            }
            "alter_type" => {
                let column = self.expect_ident()?;
                self.expect(&Token::Comma)?;
                let new_type = self.parse_type()?;
                MigrateOp::AlterType { column, new_type }
            }
            "add_constraint" => {
                let column = self.expect_ident()?;
                self.expect(&Token::Comma)?;
                let constraint = self.expect_ident()?;
                MigrateOp::AddConstraint { column, constraint }
            }
            "drop_constraint" => {
                let column = self.expect_ident()?;
                self.expect(&Token::Comma)?;
                let constraint = self.expect_ident()?;
                MigrateOp::DropConstraint { column, constraint }
            }
            _ => return Err(TlError::Parser(ParserError {
                message: format!("Unknown migration operation: `{op_name}`"),
                span: self.peek_span(),
                hint: Some("Valid operations: add_column, drop_column, rename_column, alter_type, add_constraint, drop_constraint".to_string()),
            })),
        };
        self.expect(&Token::RParen)?;
        Ok(result)
    }

    /// Parse `model <name> = train <algorithm> { key: value, ... }`
    fn parse_train(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Train {
                name,
                algorithm,
                config,
            },
            start,
            end,
        ))
    }

    /// Parse `pipeline NAME { schedule: "...", timeout: "...", retries: N, extract { ... } transform { ... } load { ... } on_failure { ... } on_success { ... } }`
    fn parse_pipeline(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Pipeline {
                name,
                extract,
                transform,
                load,
                schedule,
                timeout,
                retries,
                on_failure,
                on_success,
            },
            start,
            end,
        ))
    }

    /// Convert a keyword token to a string name when used as a map key.
    fn token_as_key_name(token: &Token) -> Option<String> {
        match token {
            Token::Type => Some("type".into()),
            Token::Model => Some("model".into()),
            Token::Source => Some("source".into()),
            Token::Sink => Some("sink".into()),
            Token::True => Some("true".into()),
            Token::False => Some("false".into()),
            Token::None_ => Some("none".into()),
            Token::Match => Some("match".into()),
            Token::If => Some("if".into()),
            _ => None,
        }
    }

    /// Parse a JSON-like map literal: `{ key: value, ... }` → `Expr::Map`
    /// Used in agent tool definitions where `{ description: "...", parameters: {...} }` syntax is needed.
    fn parse_map_literal(&mut self) -> Result<Expr, TlError> {
        self.expect(&Token::LBrace)?;
        let mut pairs = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            // Key can be an identifier, string, or keyword used as key
            let key = match self.peek().clone() {
                Token::Ident(s) => {
                    self.advance();
                    Expr::String(s)
                }
                Token::String(s) => {
                    self.advance();
                    Expr::String(s)
                }
                // Allow keywords as map keys (e.g., "type", "model", etc.)
                ref t if Self::token_as_key_name(t).is_some() => {
                    let name = Self::token_as_key_name(t).unwrap();
                    self.advance();
                    Expr::String(name)
                }
                _ => {
                    return Err(TlError::Parser(ParserError {
                        message: "Expected identifier or string key in map".to_string(),
                        span: self.peek_span(),
                        hint: None,
                    }));
                }
            };
            self.expect(&Token::Colon)?;
            // Value: recurse for nested maps, otherwise parse expression
            let value = if self.check(&Token::LBrace) {
                self.parse_map_literal()?
            } else if self.check(&Token::LBracket) {
                // Allow JSON-like arrays too
                self.parse_primary()?
            } else {
                self.parse_expression()?
            };
            pairs.push((key, value));
            self.match_token(&Token::Comma);
        }
        self.expect(&Token::RBrace)?;
        Ok(Expr::Map(pairs))
    }

    /// Parse `agent NAME { model: "...", system: "...", tools { ... }, max_turns: N }`
    fn parse_agent(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'agent'
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut model = None;
        let mut system_prompt = None;
        let mut tools: Vec<(String, Expr)> = Vec::new();
        let mut max_turns = None;
        let mut temperature = None;
        let mut max_tokens = None;
        let mut base_url = None;
        let mut api_key = None;
        let mut output_format = None;
        let mut on_tool_call = None;
        let mut on_complete = None;
        let mut mcp_servers: Vec<Expr> = Vec::new();

        while !self.check(&Token::RBrace) && !self.is_at_end() {
            match self.peek().clone() {
                Token::Model => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        model = Some(s);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected string for model".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "system" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        system_prompt = Some(s);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected string for system prompt".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "tools" => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    // Parse tool definitions: fn_name: { description: "...", parameters: {...} }
                    while !self.check(&Token::RBrace) && !self.is_at_end() {
                        let tool_name = self.expect_ident()?;
                        self.expect(&Token::Colon)?;
                        let tool_def = self.parse_map_literal()?;
                        tools.push((tool_name, tool_def));
                        self.match_token(&Token::Comma);
                    }
                    self.expect(&Token::RBrace)?;
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "max_turns" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::Int(n) = self.peek().clone() {
                        self.advance();
                        max_turns = Some(n);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected integer for max_turns".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "temperature" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    match self.peek().clone() {
                        Token::Float(f) => {
                            self.advance();
                            temperature = Some(f);
                        }
                        Token::Int(n) => {
                            self.advance();
                            temperature = Some(n as f64);
                        }
                        _ => {
                            return Err(TlError::Parser(ParserError {
                                message: "Expected number for temperature".to_string(),
                                span: self.peek_span(),
                                hint: None,
                            }));
                        }
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "max_tokens" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::Int(n) = self.peek().clone() {
                        self.advance();
                        max_tokens = Some(n);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected integer for max_tokens".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "base_url" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        base_url = Some(s);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected string for base_url".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "api_key" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        self.advance();
                        api_key = Some(s);
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected string for api_key".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "output_format" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    if let Token::String(s) = self.peek().clone() {
                        output_format = Some(s);
                        self.advance();
                    } else {
                        return Err(TlError::Parser(ParserError {
                            message: "Expected string for output_format".to_string(),
                            span: self.peek_span(),
                            hint: None,
                        }));
                    }
                    self.match_token(&Token::Comma);
                }
                Token::Ident(s) if s == "on_tool_call" => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    on_tool_call = Some(self.parse_block_body()?);
                    self.expect(&Token::RBrace)?;
                }
                Token::Ident(s) if s == "on_complete" => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    on_complete = Some(self.parse_block_body()?);
                    self.expect(&Token::RBrace)?;
                }
                Token::Ident(s) if s == "mcp_servers" => {
                    self.advance();
                    self.expect(&Token::Colon)?;
                    self.expect(&Token::LBracket)?;
                    while !self.check(&Token::RBracket) && !self.is_at_end() {
                        mcp_servers.push(self.parse_expression()?);
                        if !self.match_token(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect(&Token::RBracket)?;
                    self.match_token(&Token::Comma);
                }
                _ => {
                    return Err(TlError::Parser(ParserError {
                        message: format!(
                            "Unexpected token in agent block: `{}`",
                            self.peek()
                        ),
                        span: self.peek_span(),
                        hint: Some("Expected model, system, tools, max_turns, temperature, max_tokens, base_url, api_key, on_tool_call, on_complete, or mcp_servers".into()),
                    }));
                }
            }
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;

        let model = model.ok_or_else(|| {
            TlError::Parser(ParserError {
                message: "Agent definition requires a 'model' field".to_string(),
                span: Span::new(start, end),
                hint: None,
            })
        })?;

        Ok(make_stmt(
            StmtKind::Agent {
                name,
                model,
                system_prompt,
                tools,
                max_turns,
                temperature,
                max_tokens,
                base_url,
                api_key,
                output_format,
                on_tool_call,
                on_complete,
                mcp_servers,
            },
            start,
            end,
        ))
    }

    /// Parse `stream NAME { source: expr, window: spec, watermark: "duration", transform: { ... }, sink: expr }`
    fn parse_stream_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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
                        message: format!("Unexpected token in stream block: `{}`", self.peek()),
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

        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::StreamDecl {
                name,
                source,
                transform,
                sink,
                window,
                watermark,
            },
            start,
            end,
        ))
    }

    /// Parse `source NAME = connector TYPE { key: value, ... }`
    fn parse_source_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::SourceDecl {
                name,
                connector_type,
                config,
            },
            start,
            end,
        ))
    }

    /// Parse `sink NAME = connector TYPE { key: value, ... }`
    fn parse_sink_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::SinkDecl {
                name,
                connector_type,
                config,
            },
            start,
            end,
        ))
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
        self.parse_let_with_pub(false)
    }

    fn parse_let_with_pub(&mut self, is_public: bool) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'let'
        let mutable = self.match_token(&Token::Mut);
        // Check for destructuring patterns: let { x, y } = expr or let [a, b] = expr
        // Also Ident::Variant for enum destructure
        match self.peek() {
            Token::LBrace | Token::LBracket => {
                let pattern = self.parse_pattern()?;
                self.expect(&Token::Assign)?;
                let value = self.parse_expression()?;
                let end = self.previous_span().end;
                return Ok(make_stmt(
                    StmtKind::LetDestructure {
                        pattern,
                        mutable,
                        value,
                        is_public,
                    },
                    start,
                    end,
                ));
            }
            Token::Ident(_) => {
                // Lookahead: if Ident::ColonColon, it's enum destructure
                if self.pos + 1 < self.tokens.len()
                    && matches!(self.tokens[self.pos + 1].token, Token::ColonColon)
                {
                    let pattern = self.parse_pattern()?;
                    self.expect(&Token::Assign)?;
                    let value = self.parse_expression()?;
                    let end = self.previous_span().end;
                    return Ok(make_stmt(
                        StmtKind::LetDestructure {
                            pattern,
                            mutable,
                            value,
                            is_public,
                        },
                        start,
                        end,
                    ));
                }
                // Also check for Named struct: Ident { ... }
                if self.pos + 1 < self.tokens.len()
                    && matches!(self.tokens[self.pos + 1].token, Token::LBrace)
                    && self.pos + 2 < self.tokens.len()
                {
                    let third = &self.tokens[self.pos + 2].token;
                    if matches!(third, Token::Ident(_) | Token::RBrace) {
                        let pattern = self.parse_pattern()?;
                        self.expect(&Token::Assign)?;
                        let value = self.parse_expression()?;
                        let end = self.previous_span().end;
                        return Ok(make_stmt(
                            StmtKind::LetDestructure {
                                pattern,
                                mutable,
                                value,
                                is_public,
                            },
                            start,
                            end,
                        ));
                    }
                }
            }
            _ => {}
        }
        let name = self.expect_ident()?;
        let type_ann = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };
        self.expect(&Token::Assign)?;
        let value = self.parse_expression()?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::Let {
                name,
                mutable,
                type_ann,
                value,
                is_public,
            },
            start,
            end,
        ))
    }

    fn parse_fn_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'fn'
        let name = self.expect_ident()?;

        // Optional type parameters with inline bounds: <T: Comparable, U>
        let (type_params, mut bounds) = self.parse_optional_type_params_with_bounds()?;

        self.expect(&Token::LParen)?;
        let params = self.parse_param_list()?;
        self.expect(&Token::RParen)?;
        let return_type = if self.match_token(&Token::Arrow) {
            Some(self.parse_type()?)
        } else {
            None
        };

        // Optional where clause: `where T: Comparable + Hashable`
        if self.check(&Token::Where) {
            self.advance(); // consume 'where'
            let where_bounds = self.parse_where_clause()?;
            bounds.extend(where_bounds);
        }

        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        let is_generator = body_contains_yield(&body);
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::FnDecl {
                name,
                type_params,
                params,
                return_type,
                bounds,
                body,
                is_generator,
                is_public: false,
                is_async: false,
            },
            start,
            end,
        ))
    }

    fn parse_if(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
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

        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            },
            start,
            end,
        ))
    }

    fn parse_while(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'while'
        let condition = self.parse_expression()?;
        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::While { condition, body }, start, end))
    }

    fn parse_for(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'for'
        let name = self.expect_ident()?;
        self.expect(&Token::In)?;
        let iter = self.parse_expression()?;
        self.expect(&Token::LBrace)?;
        let body = self.parse_block_body()?;
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::For { name, iter, body }, start, end))
    }

    fn parse_return(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'return'
        if self.check(&Token::RBrace) || self.is_at_end() {
            let end = self.previous_span().end;
            Ok(make_stmt(StmtKind::Return(None), start, end))
        } else {
            let expr = self.parse_expression()?;
            let end = self.previous_span().end;
            Ok(make_stmt(StmtKind::Return(Some(expr)), start, end))
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
        self.enter_depth()?;
        let result = self.parse_expression_inner();
        self.leave_depth();
        result
    }

    fn parse_expression_inner(&mut self) -> Result<Expr, TlError> {
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

    /// Unary: not expr | -expr | yield expr | await expr
    fn parse_unary(&mut self) -> Result<Expr, TlError> {
        if self.match_token(&Token::Yield) {
            // yield with no value if next token is a statement boundary or statement-starting keyword
            if self.is_at_end()
                || matches!(
                    self.peek(),
                    Token::RBrace
                        | Token::RParen
                        | Token::Comma
                        | Token::Semicolon
                        | Token::Let
                        | Token::Fn
                        | Token::If
                        | Token::While
                        | Token::For
                        | Token::Return
                        | Token::Yield
                        | Token::Struct
                        | Token::Enum
                        | Token::Impl
                        | Token::Trait
                        | Token::Import
                        | Token::Try
                        | Token::Throw
                )
            {
                return Ok(Expr::Yield(None));
            }
            let expr = self.parse_expression()?;
            return Ok(Expr::Yield(Some(Box::new(expr))));
        }
        if self.match_token(&Token::Await) {
            let expr = self.parse_unary()?;
            return Ok(Expr::Await(Box::new(expr)));
        }
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
        if self.match_token(&Token::Ampersand) {
            let expr = self.parse_unary()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOp::Ref,
                expr: Box::new(expr),
            });
        }
        self.parse_postfix()
    }

    /// Postfix: expr.field | expr(args) | expr[index] | Ident { field: val } | Ident::Variant
    fn parse_postfix(&mut self) -> Result<Expr, TlError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.match_token(&Token::Dot) {
                let field = self.expect_ident()?;
                expr = Expr::Member {
                    object: Box::new(expr),
                    field,
                };
            } else if self.check(&Token::ColonColon) {
                // Enum::Variant or Enum::Variant(args)
                if let Expr::Ident(enum_name) = &expr {
                    let enum_name = enum_name.clone();
                    self.advance(); // consume '::'
                    let variant = self.expect_ident()?;
                    let mut args = Vec::new();
                    if self.match_token(&Token::LParen) {
                        if !self.check(&Token::RParen) {
                            args.push(self.parse_expression()?);
                            while self.match_token(&Token::Comma) {
                                if self.check(&Token::RParen) {
                                    break;
                                }
                                args.push(self.parse_expression()?);
                            }
                        }
                        self.expect(&Token::RParen)?;
                    }
                    expr = Expr::EnumVariant {
                        enum_name,
                        variant,
                        args,
                    };
                } else {
                    break;
                }
            } else if self.check(&Token::LBrace) {
                // Struct init: if expr is an Ident and next tokens look like `{ ident :`
                if let Expr::Ident(name) = &expr {
                    // Lookahead: check if this is a struct init { field: val } vs a block
                    if self.is_struct_init_ahead() {
                        let name = name.clone();
                        self.advance(); // consume '{'
                        let mut fields = Vec::new();
                        while !self.check(&Token::RBrace) && !self.is_at_end() {
                            let field_name = self.expect_ident()?;
                            self.expect(&Token::Colon)?;
                            let value = self.parse_expression()?;
                            self.match_token(&Token::Comma);
                            fields.push((field_name, value));
                        }
                        self.expect(&Token::RBrace)?;
                        expr = Expr::StructInit { name, fields };
                    } else {
                        break;
                    }
                } else {
                    break;
                }
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
            } else if self.match_token(&Token::Question) {
                // Postfix ? operator: expr? → Try(expr)
                expr = Expr::Try(Box::new(expr));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // Lookahead: is next `{ ident :` (struct init) vs `{ stmt` (block)?
    // ── Pattern Matching ─────────────────────────────────────

    /// Parse a match arm: `pattern [if guard] => body`
    fn parse_match_arm(&mut self) -> Result<MatchArm, TlError> {
        let pattern = self.parse_pattern()?;
        let guard = if self.match_token(&Token::If) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        self.expect(&Token::FatArrow)?;
        let body = self.parse_expression()?;
        Ok(MatchArm {
            pattern,
            guard,
            body,
        })
    }

    /// Parse a case arm: `condition => body` or `_ => body` (default).
    /// Case arms use boolean expressions as conditions, not patterns.
    /// We represent them as MatchArm with Wildcard pattern + guard.
    fn parse_case_arm(&mut self) -> Result<MatchArm, TlError> {
        // Wildcard / default case
        if self.check(&Token::Underscore) {
            self.advance();
            self.expect(&Token::FatArrow)?;
            let body = self.parse_expression()?;
            return Ok(MatchArm {
                pattern: Pattern::Wildcard,
                guard: None,
                body,
            });
        }
        // Otherwise, parse expression as guard condition
        let condition = self.parse_expression()?;
        self.expect(&Token::FatArrow)?;
        let body = self.parse_expression()?;
        Ok(MatchArm {
            pattern: Pattern::Wildcard,
            guard: Some(condition),
            body,
        })
    }

    /// Parse a pattern (for match arms and let-destructuring).
    fn parse_pattern(&mut self) -> Result<Pattern, TlError> {
        let pat = self.parse_single_pattern()?;
        // Check for OR pattern: pat1 | pat2 | pat3
        // Must be `|` without `>` (to distinguish from `|>` pipe)
        if self.check(&Token::Or) {
            let mut patterns = vec![pat];
            while self.check(&Token::Or) {
                self.advance(); // consume 'or'
                patterns.push(self.parse_single_pattern()?);
            }
            Ok(Pattern::Or(patterns))
        } else {
            Ok(pat)
        }
    }

    /// Parse a single (non-OR) pattern.
    fn parse_single_pattern(&mut self) -> Result<Pattern, TlError> {
        let token = self.peek().clone();
        match token {
            // Wildcard: _
            Token::Underscore => {
                self.advance();
                Ok(Pattern::Wildcard)
            }
            // Literal patterns
            Token::Int(n) => {
                self.advance();
                Ok(Pattern::Literal(Expr::Int(n)))
            }
            Token::Float(n) => {
                self.advance();
                Ok(Pattern::Literal(Expr::Float(n)))
            }
            Token::String(s) => {
                self.advance();
                Ok(Pattern::Literal(Expr::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Pattern::Literal(Expr::Bool(true)))
            }
            Token::False => {
                self.advance();
                Ok(Pattern::Literal(Expr::Bool(false)))
            }
            Token::None_ => {
                self.advance();
                Ok(Pattern::Literal(Expr::None))
            }
            // Negative literal: -1, -3.14
            Token::Minus => {
                self.advance();
                match self.peek().clone() {
                    Token::Int(n) => {
                        self.advance();
                        Ok(Pattern::Literal(Expr::Int(-n)))
                    }
                    Token::Float(n) => {
                        self.advance();
                        Ok(Pattern::Literal(Expr::Float(-n)))
                    }
                    _ => Err(TlError::Parser(ParserError {
                        message: "Expected number after '-' in pattern".to_string(),
                        span: self.peek_span(),
                        hint: None,
                    })),
                }
            }
            // List pattern: [a, b, ...rest]
            Token::LBracket => {
                self.advance(); // consume '['
                let mut elements = Vec::new();
                let mut rest = None;
                while !self.check(&Token::RBracket) && !self.is_at_end() {
                    // Check for rest pattern: ...name
                    if self.check(&Token::DotDotDot) {
                        self.advance(); // consume '...'
                        let name = self.expect_ident()?;
                        rest = Some(name);
                        self.match_token(&Token::Comma); // optional trailing comma
                        break;
                    }
                    elements.push(self.parse_pattern()?);
                    if !self.match_token(&Token::Comma) {
                        break;
                    }
                }
                self.expect(&Token::RBracket)?;
                Ok(Pattern::List { elements, rest })
            }
            // Struct pattern: { x, y } or { x: pat, y }
            Token::LBrace => {
                self.advance(); // consume '{'
                let mut fields = Vec::new();
                while !self.check(&Token::RBrace) && !self.is_at_end() {
                    let name = self.expect_ident()?;
                    let sub_pat = if self.match_token(&Token::Colon) {
                        Some(self.parse_pattern()?)
                    } else {
                        None
                    };
                    fields.push(StructPatternField {
                        name,
                        pattern: sub_pat,
                    });
                    if !self.match_token(&Token::Comma) {
                        break;
                    }
                }
                self.expect(&Token::RBrace)?;
                Ok(Pattern::Struct { name: None, fields })
            }
            // Identifier-based patterns: binding, or Enum::Variant
            Token::Ident(name) => {
                // Check for Ident::Ident (enum variant pattern)
                if self.pos + 1 < self.tokens.len()
                    && matches!(self.tokens[self.pos + 1].token, Token::ColonColon)
                {
                    let type_name = name.clone();
                    self.advance(); // consume type name
                    self.advance(); // consume '::'
                    let variant = self.expect_ident()?;
                    // Optional args: Variant(pat1, pat2)
                    let mut args = Vec::new();
                    if self.match_token(&Token::LParen) {
                        while !self.check(&Token::RParen) && !self.is_at_end() {
                            args.push(self.parse_pattern()?);
                            if !self.match_token(&Token::Comma) {
                                break;
                            }
                        }
                        self.expect(&Token::RParen)?;
                    }
                    return Ok(Pattern::Enum {
                        type_name,
                        variant,
                        args,
                    });
                }
                // Check for Named struct pattern: Name { x, y }
                if self.pos + 1 < self.tokens.len()
                    && matches!(self.tokens[self.pos + 1].token, Token::LBrace)
                {
                    // Lookahead: check if this is ident { ident [,:|} ] — struct pattern
                    // vs ident { expr } — block (but blocks aren't patterns)
                    if self.pos + 2 < self.tokens.len() {
                        let third = &self.tokens[self.pos + 2].token;
                        if matches!(third, Token::Ident(_) | Token::RBrace) {
                            let struct_name = name.clone();
                            self.advance(); // consume name
                            self.advance(); // consume '{'
                            let mut fields = Vec::new();
                            while !self.check(&Token::RBrace) && !self.is_at_end() {
                                let fname = self.expect_ident()?;
                                let sub_pat = if self.match_token(&Token::Colon) {
                                    Some(self.parse_pattern()?)
                                } else {
                                    None
                                };
                                fields.push(StructPatternField {
                                    name: fname,
                                    pattern: sub_pat,
                                });
                                if !self.match_token(&Token::Comma) {
                                    break;
                                }
                            }
                            self.expect(&Token::RBrace)?;
                            return Ok(Pattern::Struct {
                                name: Some(struct_name),
                                fields,
                            });
                        }
                    }
                }
                // Simple binding
                self.advance();
                Ok(Pattern::Binding(name))
            }
            _ => Err(TlError::Parser(ParserError {
                message: format!("Expected pattern, found `{}`", self.peek()),
                span: self.peek_span(),
                hint: None,
            })),
        }
    }

    fn is_struct_init_ahead(&self) -> bool {
        // Check: LBrace Ident Colon
        if self.pos + 2 < self.tokens.len() {
            matches!(self.tokens[self.pos].token, Token::LBrace)
                && matches!(&self.tokens[self.pos + 1].token, Token::Ident(_))
                && matches!(self.tokens[self.pos + 2].token, Token::Colon)
        } else {
            false
        }
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
            Token::DecimalLiteral(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Decimal(s))
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
                // Check for shorthand closure: x => expr
                if self.pos + 1 < self.tokens.len()
                    && self.tokens[self.pos + 1].token == Token::FatArrow
                {
                    self.advance(); // consume ident
                    self.advance(); // consume =>
                    let body = self.parse_expression()?;
                    return Ok(Expr::Closure {
                        params: vec![Param {
                            name,
                            type_ann: None,
                        }],
                        return_type: None,
                        body: ClosureBody::Expr(Box::new(body)),
                    });
                }
                self.advance();
                Ok(Expr::Ident(name))
            }
            Token::Self_ => {
                self.advance();
                Ok(Expr::Ident("self".to_string()))
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
                    let arm = self.parse_case_arm()?;
                    self.match_token(&Token::Comma); // optional trailing comma
                    arms.push(arm);
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
                    let arm = self.parse_match_arm()?;
                    self.match_token(&Token::Comma); // optional trailing comma
                    arms.push(arm);
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

    /// Look ahead from current `(` to determine if this is a closure `(params) => expr`
    /// or `(params) -> Type { ... }`.
    /// Scans forward to find the matching `)`, then checks if the next token is `=>` or `->`.
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
                        // Check if next token after `)` is `=>` or `->`
                        return i + 1 < self.tokens.len()
                            && matches!(self.tokens[i + 1].token, Token::FatArrow | Token::Arrow);
                    }
                }
                _ if i >= self.tokens.len() - 1 => return false, // hit EOF sentinel
                _ => {}
            }
            i += 1;
        }
        false
    }

    /// Parse a closure: `(params) => expr` or `(params) -> Type { stmts; expr }`
    fn parse_closure(&mut self) -> Result<Expr, TlError> {
        use tl_ast::ClosureBody;
        self.expect(&Token::LParen)?;
        let params = self.parse_param_list()?;
        self.expect(&Token::RParen)?;

        if self.match_token(&Token::FatArrow) {
            // Expression-body closure: (x) => x * 2
            let body = self.parse_expression()?;
            Ok(Expr::Closure {
                params,
                return_type: None,
                body: ClosureBody::Expr(Box::new(body)),
            })
        } else if self.match_token(&Token::Arrow) {
            // Block-body closure: (x) -> int64 { let y = x * 2; y + 1 }
            let return_type = self.parse_type()?;
            self.expect(&Token::LBrace)?;
            let stmts = self.parse_block_body()?;
            // Check if the last statement is an expression statement — if so, treat as tail expr
            let (stmts, expr) = self.extract_tail_expr(stmts);
            self.expect(&Token::RBrace)?;
            Ok(Expr::Closure {
                params,
                return_type: Some(return_type),
                body: ClosureBody::Block { stmts, expr },
            })
        } else {
            Err(TlError::Parser(ParserError {
                message: "Expected `=>` or `->` after closure parameters".to_string(),
                span: self.peek_span(),
                hint: Some("Use `=>` for expression closures or `->` for block closures".into()),
            }))
        }
    }

    /// Extract the last expression-statement from a block as a tail expression.
    fn extract_tail_expr(&self, mut stmts: Vec<Stmt>) -> (Vec<Stmt>, Option<Box<Expr>>) {
        if let Some(last) = stmts.last()
            && let StmtKind::Expr(_) = &last.kind
            && let StmtKind::Expr(e) = stmts.pop().unwrap().kind
        {
            return (stmts, Some(Box::new(e)));
        }
        (stmts, None)
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
        let name = if self.check(&Token::Self_) {
            self.advance();
            "self".to_string()
        } else {
            self.expect_ident()?
        };
        let type_ann = if self.match_token(&Token::Colon) {
            Some(self.parse_type()?)
        } else {
            None
        };
        Ok(Param { name, type_ann })
    }

    // ── Type Parsing ─────────────────────────────────────────

    fn parse_type(&mut self) -> Result<TypeExpr, TlError> {
        // Handle `fn(params) -> ret` function types
        let base = if self.check(&Token::Fn) {
            self.advance(); // consume 'fn'
            self.expect(&Token::LParen)?;
            let mut params = Vec::new();
            if !self.check(&Token::RParen) {
                params.push(self.parse_type()?);
                while self.match_token(&Token::Comma) {
                    if self.check(&Token::RParen) {
                        break;
                    }
                    params.push(self.parse_type()?);
                }
            }
            self.expect(&Token::RParen)?;
            let return_type = if self.match_token(&Token::Arrow) {
                self.parse_type()?
            } else {
                TypeExpr::Named("unit".to_string())
            };
            TypeExpr::Function {
                params,
                return_type: Box::new(return_type),
            }
        } else {
            let name = self.expect_ident()?;
            if self.match_token(&Token::Lt) {
                let mut args = Vec::new();
                args.push(self.parse_type()?);
                while self.match_token(&Token::Comma) {
                    args.push(self.parse_type()?);
                }
                self.expect(&Token::Gt)?;
                TypeExpr::Generic { name, args }
            } else {
                TypeExpr::Named(name)
            }
        };
        // Handle postfix `?` for optional types: T? -> Optional(T)
        if self.match_token(&Token::Question) {
            Ok(TypeExpr::Optional(Box::new(base)))
        } else {
            Ok(base)
        }
    }

    // ── Phase 5: New parsing methods ─────────────────────────

    /// Parse `struct Name { field: type, ... }`
    fn parse_struct_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'struct'
        let name = self.expect_ident()?;
        let type_params = self.parse_optional_type_params()?;
        self.expect(&Token::LBrace)?;
        let mut fields = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let field_doc = self.consume_doc_comments();
            let field_name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let type_ann = self.parse_type()?;
            self.match_token(&Token::Comma);
            let annotations = parse_field_annotations(field_doc.as_deref());
            fields.push(SchemaField {
                name: field_name,
                type_ann,
                doc_comment: field_doc,
                default_value: None,
                annotations,
            });
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::StructDecl {
                name,
                type_params,
                fields,
                is_public: false,
            },
            start,
            end,
        ))
    }

    /// Parse `enum Name { Variant, Variant(Type, Type), ... }`
    fn parse_enum_decl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'enum'
        let name = self.expect_ident()?;
        let type_params = self.parse_optional_type_params()?;
        self.expect(&Token::LBrace)?;
        let mut variants = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let variant_name = self.expect_ident()?;
            let mut fields = Vec::new();
            if self.match_token(&Token::LParen) {
                if !self.check(&Token::RParen) {
                    fields.push(self.parse_type()?);
                    while self.match_token(&Token::Comma) {
                        if self.check(&Token::RParen) {
                            break;
                        }
                        fields.push(self.parse_type()?);
                    }
                }
                self.expect(&Token::RParen)?;
            }
            self.match_token(&Token::Comma);
            variants.push(EnumVariant {
                name: variant_name,
                fields,
            });
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::EnumDecl {
                name,
                type_params,
                variants,
                is_public: false,
            },
            start,
            end,
        ))
    }

    /// Parse `impl Type { ... }` or `impl<T> Type { ... }` or `impl Trait for Type { ... }`
    fn parse_impl(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'impl'

        // Optional type params: impl<T>
        let impl_type_params = self.parse_optional_type_params()?;

        let first_name = self.expect_ident()?;

        // Check if this is `impl Trait for Type { ... }`
        if self.check(&Token::For) {
            self.advance(); // consume 'for'
            let type_name = self.expect_ident()?;
            // Optional type params on the type: `for Type<T>`
            let _type_args = self.parse_optional_type_params()?;

            self.expect(&Token::LBrace)?;
            let mut methods = Vec::new();
            while !self.check(&Token::RBrace) && !self.is_at_end() {
                let doc = self.consume_doc_comments();
                if self.check(&Token::Fn) {
                    let mut method = self.parse_fn_decl()?;
                    method.doc_comment = doc;
                    methods.push(method);
                } else {
                    return Err(TlError::Parser(ParserError {
                        message: "Expected `fn` in impl block".to_string(),
                        span: self.peek_span(),
                        hint: None,
                    }));
                }
            }
            self.expect(&Token::RBrace)?;
            let end = self.previous_span().end;
            return Ok(make_stmt(
                StmtKind::TraitImpl {
                    trait_name: first_name,
                    type_name,
                    type_params: impl_type_params,
                    methods,
                },
                start,
                end,
            ));
        }

        // Regular impl block: `impl Type { ... }`
        self.expect(&Token::LBrace)?;
        let mut methods = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            let doc = self.consume_doc_comments();
            if self.check(&Token::Fn) {
                let mut method = self.parse_fn_decl()?;
                method.doc_comment = doc;
                methods.push(method);
            } else {
                return Err(TlError::Parser(ParserError {
                    message: "Expected `fn` in impl block".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::ImplBlock {
                type_name: first_name,
                type_params: impl_type_params,
                methods,
            },
            start,
            end,
        ))
    }

    /// Parse `trait Name { fn method(self) -> type; ... }`
    /// Parse `type Name<T> = TypeExpr`
    fn parse_type_alias(&mut self, is_public: bool) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'type'
        let name = self.expect_ident()?;
        let type_params = self.parse_optional_type_params()?;
        self.expect(&Token::Assign)?;
        let value = self.parse_type()?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::TypeAlias {
                name,
                type_params,
                value,
                is_public,
            },
            start,
            end,
        ))
    }

    fn parse_trait_def(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'trait'
        let name = self.expect_ident()?;

        // Optional type parameters <T, U>
        let type_params = self.parse_optional_type_params()?;

        self.expect(&Token::LBrace)?;
        let mut methods = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            // Skip doc comments before trait methods
            let _doc = self.consume_doc_comments();
            if self.check(&Token::Fn) {
                self.advance(); // consume 'fn'
                let method_name = self.expect_ident()?;
                self.expect(&Token::LParen)?;
                let params = self.parse_param_list()?;
                self.expect(&Token::RParen)?;
                let return_type = if self.match_token(&Token::Arrow) {
                    Some(self.parse_type()?)
                } else {
                    None
                };
                methods.push(TraitMethod {
                    name: method_name,
                    params,
                    return_type,
                });
            } else {
                return Err(TlError::Parser(ParserError {
                    message: "Expected `fn` in trait definition".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::TraitDef {
                name,
                type_params,
                methods,
                is_public: false,
            },
            start,
            end,
        ))
    }

    /// Parse optional type parameters: `<T, U>` — returns empty vec if no `<`
    fn parse_optional_type_params(&mut self) -> Result<Vec<String>, TlError> {
        let (params, _bounds) = self.parse_optional_type_params_with_bounds()?;
        Ok(params)
    }

    /// Parse optional type parameters with inline bounds: `<T: Comparable, U>`
    /// Returns (type_param_names, trait_bounds)
    fn parse_optional_type_params_with_bounds(
        &mut self,
    ) -> Result<(Vec<String>, Vec<TraitBound>), TlError> {
        if !self.check(&Token::Lt) {
            return Ok((vec![], vec![]));
        }
        self.advance(); // consume '<'
        let mut params = Vec::new();
        let mut bounds = Vec::new();
        loop {
            let name = self.expect_ident()?;
            // Check for inline bounds: `T: Comparable + Hashable`
            if self.match_token(&Token::Colon) {
                let mut traits = Vec::new();
                traits.push(self.expect_ident()?);
                while self.match_token(&Token::Plus) {
                    traits.push(self.expect_ident()?);
                }
                bounds.push(TraitBound {
                    type_param: name.clone(),
                    traits,
                });
            }
            params.push(name);
            if !self.match_token(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::Gt)?;
        Ok((params, bounds))
    }

    /// Parse where clause: `T: Comparable + Hashable, U: Default`
    fn parse_where_clause(&mut self) -> Result<Vec<TraitBound>, TlError> {
        let mut bounds = Vec::new();
        loop {
            let type_param = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let mut traits = Vec::new();
            traits.push(self.expect_ident()?);
            while self.match_token(&Token::Plus) {
                traits.push(self.expect_ident()?);
            }
            bounds.push(TraitBound { type_param, traits });
            if !self.match_token(&Token::Comma) {
                break;
            }
            // Stop if next token is `{` (body start)
            if self.check(&Token::LBrace) {
                break;
            }
        }
        Ok(bounds)
    }

    /// Parse `try { ... } catch var { ... }`
    fn parse_try_catch(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'try'
        self.expect(&Token::LBrace)?;
        let mut try_body = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            try_body.push(self.parse_statement()?);
        }
        self.expect(&Token::RBrace)?;
        self.expect(&Token::Catch)?;
        let catch_var = self.expect_ident()?;
        self.expect(&Token::LBrace)?;
        let mut catch_body = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            catch_body.push(self.parse_statement()?);
        }
        self.expect(&Token::RBrace)?;
        // Parse optional finally block
        let finally_body = if self.check(&Token::Finally) {
            self.advance(); // consume 'finally'
            self.expect(&Token::LBrace)?;
            let mut body = Vec::new();
            while !self.check(&Token::RBrace) && !self.is_at_end() {
                body.push(self.parse_statement()?);
            }
            self.expect(&Token::RBrace)?;
            Some(body)
        } else {
            None
        };
        let end = self.previous_span().end;
        Ok(make_stmt(
            StmtKind::TryCatch {
                try_body,
                catch_var,
                catch_body,
                finally_body,
            },
            start,
            end,
        ))
    }

    /// Parse `throw expr`
    fn parse_throw(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'throw'
        let expr = self.parse_expression()?;
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::Throw(expr), start, end))
    }

    /// Parse `import "path.tl"` or `import "path.tl" as name`
    fn parse_import(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'import'
        let path = match self.peek().clone() {
            Token::String(s) => {
                self.advance();
                s
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected string path after `import`".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        };
        let alias = if self.match_token(&Token::As) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::Import { path, alias }, start, end))
    }

    /// Parse `test "name" { body }`
    fn parse_test(&mut self) -> Result<Stmt, TlError> {
        let start = self.peek_span().start;
        self.advance(); // consume 'test'
        let name = match self.peek().clone() {
            Token::String(s) => {
                self.advance();
                s
            }
            _ => {
                return Err(TlError::Parser(ParserError {
                    message: "Expected string after `test`".to_string(),
                    span: self.peek_span(),
                    hint: None,
                }));
            }
        };
        self.expect(&Token::LBrace)?;
        let mut body = Vec::new();
        while !self.check(&Token::RBrace) && !self.is_at_end() {
            body.push(self.parse_statement()?);
        }
        self.expect(&Token::RBrace)?;
        let end = self.previous_span().end;
        Ok(make_stmt(StmtKind::Test { name, body }, start, end))
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

/// Check if a function body contains any yield expressions (makes it a generator).
/// Parse security annotations from a doc comment string.
fn parse_field_annotations(doc: Option<&str>) -> Vec<tl_ast::Annotation> {
    let mut annotations = Vec::new();
    if let Some(doc) = doc {
        if doc.contains("@sensitive") {
            annotations.push(tl_ast::Annotation::Sensitive);
        }
        if doc.contains("@redact") {
            annotations.push(tl_ast::Annotation::Redact);
        }
        if doc.contains("@pii") {
            annotations.push(tl_ast::Annotation::Pii);
        }
    }
    annotations
}

fn body_contains_yield(stmts: &[Stmt]) -> bool {
    for stmt in stmts {
        if stmt_contains_yield(stmt) {
            return true;
        }
    }
    false
}

fn stmt_contains_yield(stmt: &Stmt) -> bool {
    match &stmt.kind {
        StmtKind::Expr(e) | StmtKind::Return(Some(e)) | StmtKind::Throw(e) => {
            expr_contains_yield(e)
        }
        StmtKind::Let { value, .. } => expr_contains_yield(value),
        StmtKind::If {
            condition,
            then_body,
            else_ifs,
            else_body,
        } => {
            expr_contains_yield(condition)
                || body_contains_yield(then_body)
                || else_ifs
                    .iter()
                    .any(|(c, b)| expr_contains_yield(c) || body_contains_yield(b))
                || else_body.as_ref().is_some_and(|b| body_contains_yield(b))
        }
        StmtKind::While { condition, body } => {
            expr_contains_yield(condition) || body_contains_yield(body)
        }
        StmtKind::For { iter, body, .. } => expr_contains_yield(iter) || body_contains_yield(body),
        StmtKind::TryCatch {
            try_body,
            catch_body,
            ..
        } => body_contains_yield(try_body) || body_contains_yield(catch_body),
        // Don't recurse into nested FnDecl — yield in nested fn is for that fn
        StmtKind::FnDecl { .. } => false,
        _ => false,
    }
}

fn expr_contains_yield(expr: &Expr) -> bool {
    match expr {
        Expr::Yield(_) => true,
        Expr::BinOp { left, right, .. } => expr_contains_yield(left) || expr_contains_yield(right),
        Expr::UnaryOp { expr, .. } => expr_contains_yield(expr),
        Expr::Call { function, args } => {
            expr_contains_yield(function) || args.iter().any(expr_contains_yield)
        }
        Expr::Pipe { left, right } => expr_contains_yield(left) || expr_contains_yield(right),
        Expr::Member { object, .. } => expr_contains_yield(object),
        Expr::Index { object, index } => expr_contains_yield(object) || expr_contains_yield(index),
        Expr::List(items) => items.iter().any(expr_contains_yield),
        Expr::Map(pairs) => pairs
            .iter()
            .any(|(k, v)| expr_contains_yield(k) || expr_contains_yield(v)),
        Expr::Block { stmts, expr } => {
            body_contains_yield(stmts) || expr.as_ref().is_some_and(|e| expr_contains_yield(e))
        }
        Expr::Closure { .. } => false, // Don't recurse — yield in closure is not our yield
        Expr::Assign { target, value } => expr_contains_yield(target) || expr_contains_yield(value),
        Expr::NullCoalesce { expr, default } => {
            expr_contains_yield(expr) || expr_contains_yield(default)
        }
        Expr::Range { start, end } => expr_contains_yield(start) || expr_contains_yield(end),
        Expr::Await(e) => expr_contains_yield(e),
        Expr::NamedArg { value, .. } => expr_contains_yield(value),
        Expr::Case { arms } | Expr::Match { arms, .. } => arms.iter().any(|arm| {
            (arm.guard.as_ref().is_some_and(expr_contains_yield)) || expr_contains_yield(&arm.body)
        }),
        Expr::StructInit { fields, .. } => fields.iter().any(|(_, e)| expr_contains_yield(e)),
        Expr::EnumVariant { args, .. } => args.iter().any(expr_contains_yield),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_let() {
        let program = parse("let x = 42").unwrap();
        assert_eq!(program.statements.len(), 1);
        assert!(matches!(&program.statements[0].kind, StmtKind::Let { name, .. } if name == "x"));
    }

    #[test]
    fn test_parse_fn() {
        let program = parse("fn add(a: int64, b: int64) -> int64 { a + b }").unwrap();
        assert_eq!(program.statements.len(), 1);
        assert!(
            matches!(&program.statements[0].kind, StmtKind::FnDecl { name, .. } if name == "add")
        );
    }

    #[test]
    fn test_parse_pipe() {
        let program = parse("let result = x |> double()").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            assert!(matches!(value, Expr::Pipe { .. }));
        } else {
            panic!("Expected let statement");
        }
    }

    #[test]
    fn test_parse_if_else() {
        let program = parse("if x > 5 { x } else { 0 }").unwrap();
        assert!(matches!(program.statements[0].kind, StmtKind::If { .. }));
    }

    #[test]
    fn test_parse_nested_arithmetic() {
        let program = parse("let x = 1 + 2 * 3").unwrap();
        // Should parse as 1 + (2 * 3) due to precedence
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            assert!(matches!(value, Expr::BinOp { op: BinOp::Add, .. }));
        }
    }

    #[test]
    fn test_parse_match() {
        let program = parse("match x { 1 => \"one\", 2 => \"two\", _ => \"other\" }").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let StmtKind::Expr(Expr::Match { subject, arms }) = &program.statements[0].kind {
            assert!(matches!(subject.as_ref(), Expr::Ident(n) if n == "x"));
            assert_eq!(arms.len(), 3);
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_closure() {
        let program = parse("let double = (x) => x * 2").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            assert!(matches!(value, Expr::Closure { .. }));
        } else {
            panic!("Expected let with closure");
        }
    }

    #[test]
    fn test_parse_function_call() {
        let program = parse("print(42)").unwrap();
        if let StmtKind::Expr(Expr::Call { function, args, .. }) = &program.statements[0].kind {
            assert!(matches!(function.as_ref(), Expr::Ident(n) if n == "print"));
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected function call");
        }
    }

    #[test]
    fn test_parse_schema() {
        let program = parse("schema User { id: int64, name: string, age: float64 }").unwrap();
        if let StmtKind::Schema { name, fields, .. } = &program.statements[0].kind {
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
        let program = parse(
            r#"pipeline etl {
            extract { let data = read_csv("input.csv") }
            transform { let cleaned = data }
            load { write_csv(cleaned, "output.csv") }
        }"#,
        )
        .unwrap();
        if let StmtKind::Pipeline {
            name,
            extract,
            transform,
            load,
            ..
        } = &program.statements[0].kind
        {
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
        let program = parse(
            r#"pipeline daily_etl {
            schedule: "0 0 * * *",
            timeout: "30m",
            retries: 3,
            extract { let data = read_csv("input.csv") }
            transform { let cleaned = data }
            load { write_csv(cleaned, "output.csv") }
            on_failure { println("Pipeline failed!") }
            on_success { println("Pipeline succeeded!") }
        }"#,
        )
        .unwrap();
        if let StmtKind::Pipeline {
            name,
            schedule,
            timeout,
            retries,
            on_failure,
            on_success,
            ..
        } = &program.statements[0].kind
        {
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
        let program = parse(
            r#"stream events {
            source: src,
            window: tumbling(5m),
            transform: { let x = 1 },
            sink: out
        }"#,
        )
        .unwrap();
        if let StmtKind::StreamDecl {
            name,
            source,
            window,
            sink,
            ..
        } = &program.statements[0].kind
        {
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
        let program = parse(
            r#"stream metrics {
            source: input,
            window: sliding(10m, 1m),
            transform: { let x = 1 }
        }"#,
        )
        .unwrap();
        if let StmtKind::StreamDecl { window, .. } = &program.statements[0].kind {
            assert!(matches!(window, Some(WindowSpec::Sliding(w, s)) if w == "10m" && s == "1m"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_stream_session_window() {
        let program = parse(
            r#"stream sessions {
            source: clicks,
            window: session(30m),
            transform: { let x = 1 }
        }"#,
        )
        .unwrap();
        if let StmtKind::StreamDecl { window, .. } = &program.statements[0].kind {
            assert!(matches!(window, Some(WindowSpec::Session(d)) if d == "30m"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_source_decl() {
        let program = parse(
            r#"source kafka_in = connector kafka {
            topic: "events",
            group: "my_group"
        }"#,
        )
        .unwrap();
        if let StmtKind::SourceDecl {
            name,
            connector_type,
            config,
        } = &program.statements[0].kind
        {
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
        let program = parse(
            r#"sink output = connector channel {
            buffer: 100
        }"#,
        )
        .unwrap();
        if let StmtKind::SinkDecl {
            name,
            connector_type,
            config,
        } = &program.statements[0].kind
        {
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
        let program = parse(
            r#"pipeline fast {
            timeout: 30s,
            extract { let x = 1 }
            transform { let y = x }
            load { println(y) }
        }"#,
        )
        .unwrap();
        if let StmtKind::Pipeline { timeout, .. } = &program.statements[0].kind {
            assert_eq!(timeout.as_deref(), Some("30s"));
        } else {
            panic!("Expected pipeline statement");
        }
    }

    #[test]
    fn test_parse_stream_with_watermark() {
        let program = parse(
            r#"stream delayed {
            source: input,
            watermark: 10s,
            transform: { let x = 1 }
        }"#,
        )
        .unwrap();
        if let StmtKind::StreamDecl { watermark, .. } = &program.statements[0].kind {
            assert_eq!(watermark.as_deref(), Some("10s"));
        } else {
            panic!("Expected stream declaration");
        }
    }

    #[test]
    fn test_parse_with_block() {
        let program = parse("with { doubled = age * 2, name = first }").unwrap();
        if let StmtKind::Expr(Expr::Call { function, args }) = &program.statements[0].kind {
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

    #[test]
    fn test_parse_struct_decl() {
        let program = parse("struct Point { x: float64, y: float64 }").unwrap();
        if let StmtKind::StructDecl { name, fields, .. } = &program.statements[0].kind {
            assert_eq!(name, "Point");
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "x");
            assert!(matches!(&fields[0].type_ann, TypeExpr::Named(t) if t == "float64"));
            assert_eq!(fields[1].name, "y");
            assert!(matches!(&fields[1].type_ann, TypeExpr::Named(t) if t == "float64"));
        } else {
            panic!("Expected struct declaration");
        }
    }

    #[test]
    fn test_parse_struct_init() {
        let program = parse("let p = Point { x: 1.0, y: 2.0 }").unwrap();
        if let StmtKind::Let { name, value, .. } = &program.statements[0].kind {
            assert_eq!(name, "p");
            if let Expr::StructInit {
                name: struct_name,
                fields,
            } = value
            {
                assert_eq!(struct_name, "Point");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "x");
                assert!(matches!(&fields[0].1, Expr::Float(v) if *v == 1.0));
                assert_eq!(fields[1].0, "y");
                assert!(matches!(&fields[1].1, Expr::Float(v) if *v == 2.0));
            } else {
                panic!("Expected StructInit expression");
            }
        } else {
            panic!("Expected let statement");
        }
    }

    #[test]
    fn test_parse_enum_decl() {
        let program = parse("enum Color { Red, Green, Blue }").unwrap();
        if let StmtKind::EnumDecl { name, variants, .. } = &program.statements[0].kind {
            assert_eq!(name, "Color");
            assert_eq!(variants.len(), 3);
            assert_eq!(variants[0].name, "Red");
            assert!(variants[0].fields.is_empty());
            assert_eq!(variants[1].name, "Green");
            assert!(variants[1].fields.is_empty());
            assert_eq!(variants[2].name, "Blue");
            assert!(variants[2].fields.is_empty());
        } else {
            panic!("Expected enum declaration");
        }
    }

    #[test]
    fn test_parse_enum_variant() {
        // Simple variant: Color::Red
        let program = parse("Color::Red").unwrap();
        if let StmtKind::Expr(Expr::EnumVariant {
            enum_name,
            variant,
            args,
        }) = &program.statements[0].kind
        {
            assert_eq!(enum_name, "Color");
            assert_eq!(variant, "Red");
            assert!(args.is_empty());
        } else {
            panic!("Expected enum variant expression");
        }

        // Variant with args: Color::Custom(1, 2, 3)
        let program = parse("Color::Custom(1, 2, 3)").unwrap();
        if let StmtKind::Expr(Expr::EnumVariant {
            enum_name,
            variant,
            args,
        }) = &program.statements[0].kind
        {
            assert_eq!(enum_name, "Color");
            assert_eq!(variant, "Custom");
            assert_eq!(args.len(), 3);
            assert!(matches!(&args[0], Expr::Int(1)));
            assert!(matches!(&args[1], Expr::Int(2)));
            assert!(matches!(&args[2], Expr::Int(3)));
        } else {
            panic!("Expected enum variant expression with args");
        }
    }

    #[test]
    fn test_parse_impl_block() {
        let program = parse("impl Point { fn area(self) { self.x * self.y } }").unwrap();
        if let StmtKind::ImplBlock {
            type_name, methods, ..
        } = &program.statements[0].kind
        {
            assert_eq!(type_name, "Point");
            assert_eq!(methods.len(), 1);
            if let StmtKind::FnDecl {
                name, params, body, ..
            } = &methods[0].kind
            {
                assert_eq!(name, "area");
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].name, "self");
                assert_eq!(body.len(), 1);
            } else {
                panic!("Expected fn declaration inside impl block");
            }
        } else {
            panic!("Expected impl block");
        }
    }

    #[test]
    fn test_parse_try_catch() {
        let program = parse("try { 1 + 2 } catch e { println(e) }").unwrap();
        if let StmtKind::TryCatch {
            try_body,
            catch_var,
            catch_body,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(try_body.len(), 1);
            assert_eq!(catch_var, "e");
            assert_eq!(catch_body.len(), 1);
            // Verify try body contains the expression 1 + 2
            if let StmtKind::Expr(Expr::BinOp { op, .. }) = &try_body[0].kind {
                assert_eq!(*op, BinOp::Add);
            } else {
                panic!("Expected binary op in try body");
            }
            // Verify catch body contains println(e)
            if let StmtKind::Expr(Expr::Call { function, args }) = &catch_body[0].kind {
                assert!(matches!(function.as_ref(), Expr::Ident(n) if n == "println"));
                assert_eq!(args.len(), 1);
            } else {
                panic!("Expected function call in catch body");
            }
        } else {
            panic!("Expected try/catch statement");
        }
    }

    #[test]
    fn test_parse_throw() {
        let program = parse(r#"throw "error""#).unwrap();
        if let StmtKind::Throw(expr) = &program.statements[0].kind {
            assert!(matches!(expr, Expr::String(s) if s == "error"));
        } else {
            panic!("Expected throw statement");
        }
    }

    #[test]
    fn test_parse_import() {
        // Simple import
        let program = parse(r#"import "utils.tl""#).unwrap();
        if let StmtKind::Import { path, alias } = &program.statements[0].kind {
            assert_eq!(path, "utils.tl");
            assert!(alias.is_none());
        } else {
            panic!("Expected import statement");
        }

        // Import with alias
        let program = parse(r#"import "math.tl" as math"#).unwrap();
        if let StmtKind::Import { path, alias } = &program.statements[0].kind {
            assert_eq!(path, "math.tl");
            assert_eq!(alias.as_deref(), Some("math"));
        } else {
            panic!("Expected import statement with alias");
        }
    }

    #[test]
    fn test_parse_test() {
        let program = parse(r#"test "my test" { assert(true) }"#).unwrap();
        if let StmtKind::Test { name, body } = &program.statements[0].kind {
            assert_eq!(name, "my test");
            assert_eq!(body.len(), 1);
            if let StmtKind::Expr(Expr::Call { function, args }) = &body[0].kind {
                assert!(matches!(function.as_ref(), Expr::Ident(n) if n == "assert"));
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::Bool(true)));
            } else {
                panic!("Expected function call in test body");
            }
        } else {
            panic!("Expected test statement");
        }
    }

    #[test]
    fn test_parse_method_call() {
        let program = parse(r#""hello".split(" ")"#).unwrap();
        if let StmtKind::Expr(Expr::Call { function, args }) = &program.statements[0].kind {
            // The function should be a Member access: "hello".split
            if let Expr::Member { object, field } = function.as_ref() {
                assert!(matches!(object.as_ref(), Expr::String(s) if s == "hello"));
                assert_eq!(field, "split");
            } else {
                panic!("Expected member access as call function");
            }
            assert_eq!(args.len(), 1);
            assert!(matches!(&args[0], Expr::String(s) if s == " "));
        } else {
            panic!("Expected method call expression");
        }
    }

    // Phase 7: Concurrency parser tests

    #[test]
    fn test_parse_await_expr() {
        let program = parse("await x").unwrap();
        if let StmtKind::Expr(Expr::Await(inner)) = &program.statements[0].kind {
            assert!(matches!(inner.as_ref(), Expr::Ident(s) if s == "x"));
        } else {
            panic!("Expected Await expression, got {:?}", program.statements[0]);
        }
    }

    #[test]
    fn test_parse_await_spawn() {
        let program = parse("await spawn(f)").unwrap();
        if let StmtKind::Expr(Expr::Await(inner)) = &program.statements[0].kind {
            assert!(matches!(inner.as_ref(), Expr::Call { .. }));
        } else {
            panic!("Expected Await(Call(...))");
        }
    }

    #[test]
    fn test_parse_yield_expr() {
        let program = parse("yield 42").unwrap();
        if let StmtKind::Expr(Expr::Yield(Some(inner))) = &program.statements[0].kind {
            assert!(matches!(inner.as_ref(), Expr::Int(42)));
        } else {
            panic!("Expected Yield(Some(Int(42)))");
        }
    }

    #[test]
    fn test_parse_bare_yield() {
        let program = parse("fn gen() { yield }").unwrap();
        if let StmtKind::FnDecl {
            body, is_generator, ..
        } = &program.statements[0].kind
        {
            assert!(*is_generator);
            if let StmtKind::Expr(Expr::Yield(None)) = &body[0].kind {
                // ok
            } else {
                panic!("Expected bare Yield(None), got {:?}", body[0]);
            }
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_parse_generator_fn() {
        let program = parse("fn gen() { yield 1\nyield 2 }").unwrap();
        if let StmtKind::FnDecl {
            name,
            is_generator,
            body,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "gen");
            assert!(*is_generator);
            assert_eq!(body.len(), 2);
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_parse_non_generator_fn() {
        let program = parse("fn add(a, b) { return a }").unwrap();
        if let StmtKind::FnDecl { is_generator, .. } = &program.statements[0].kind {
            assert!(!*is_generator);
        } else {
            panic!("Expected FnDecl");
        }
    }

    // ── Phase 11: Module System Parser Tests ──

    #[test]
    fn test_parse_pub_fn() {
        let program = parse("pub fn foo() { 1 }").unwrap();
        if let StmtKind::FnDecl {
            name, is_public, ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "foo");
            assert!(*is_public);
        } else {
            panic!("Expected pub FnDecl");
        }
    }

    #[test]
    fn test_parse_pub_struct() {
        let program = parse("pub struct Foo { x: int }").unwrap();
        if let StmtKind::StructDecl {
            name, is_public, ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Foo");
            assert!(*is_public);
        } else {
            panic!("Expected pub StructDecl");
        }
    }

    #[test]
    fn test_parse_use_single() {
        let program = parse("use data.transforms.clean").unwrap();
        if let StmtKind::Use { item, is_public } = &program.statements[0].kind {
            assert!(!*is_public);
            if let UseItem::Single(path) = item {
                assert_eq!(path, &["data", "transforms", "clean"]);
            } else {
                panic!("Expected UseItem::Single");
            }
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_parse_use_group() {
        let program = parse("use data.transforms.{a, b}").unwrap();
        if let StmtKind::Use { item, .. } = &program.statements[0].kind {
            if let UseItem::Group(prefix, names) = item {
                assert_eq!(prefix, &["data", "transforms"]);
                assert_eq!(names, &["a", "b"]);
            } else {
                panic!("Expected UseItem::Group");
            }
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_parse_use_wildcard() {
        let program = parse("use data.transforms.*").unwrap();
        if let StmtKind::Use { item, .. } = &program.statements[0].kind {
            if let UseItem::Wildcard(path) = item {
                assert_eq!(path, &["data", "transforms"]);
            } else {
                panic!("Expected UseItem::Wildcard");
            }
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_parse_use_aliased() {
        let program = parse("use data.postgres as pg").unwrap();
        if let StmtKind::Use { item, .. } = &program.statements[0].kind {
            if let UseItem::Aliased(path, alias) = item {
                assert_eq!(path, &["data", "postgres"]);
                assert_eq!(alias, "pg");
            } else {
                panic!("Expected UseItem::Aliased");
            }
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_parse_pub_use() {
        let program = parse("pub use data.clean").unwrap();
        if let StmtKind::Use { item, is_public } = &program.statements[0].kind {
            assert!(*is_public);
            assert!(matches!(item, UseItem::Single(p) if p == &["data", "clean"]));
        } else {
            panic!("Expected pub Use statement");
        }
    }

    #[test]
    fn test_parse_pub_mod() {
        let program = parse("pub mod transforms").unwrap();
        if let StmtKind::ModDecl { name, is_public } = &program.statements[0].kind {
            assert_eq!(name, "transforms");
            assert!(*is_public);
        } else {
            panic!("Expected pub ModDecl");
        }
    }

    #[test]
    fn test_parse_mod() {
        let program = parse("mod quality").unwrap();
        if let StmtKind::ModDecl { name, is_public } = &program.statements[0].kind {
            assert_eq!(name, "quality");
            assert!(!*is_public);
        } else {
            panic!("Expected ModDecl");
        }
    }

    #[test]
    fn test_fn_default_not_public() {
        let program = parse("fn foo() { 1 }").unwrap();
        if let StmtKind::FnDecl { is_public, .. } = &program.statements[0].kind {
            assert!(!*is_public);
        } else {
            panic!("Expected FnDecl");
        }
    }

    // ── Phase 12: Generics & Traits ──────────────────────────

    #[test]
    fn test_generic_fn() {
        let program = parse("fn identity<T>(x: T) -> T { x }").unwrap();
        if let StmtKind::FnDecl {
            name,
            type_params,
            params,
            return_type,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "identity");
            assert_eq!(type_params, &vec!["T".to_string()]);
            assert_eq!(params.len(), 1);
            assert!(return_type.is_some());
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_generic_struct() {
        let program = parse("struct Pair<A, B> { first: A, second: B }").unwrap();
        if let StmtKind::StructDecl {
            name,
            type_params,
            fields,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Pair");
            assert_eq!(type_params, &vec!["A".to_string(), "B".to_string()]);
            assert_eq!(fields.len(), 2);
        } else {
            panic!("Expected StructDecl");
        }
    }

    #[test]
    fn test_generic_enum() {
        let program = parse("enum MyOption<T> { Some(T), Nothing }").unwrap();
        if let StmtKind::EnumDecl {
            name,
            type_params,
            variants,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "MyOption");
            assert_eq!(type_params, &vec!["T".to_string()]);
            assert_eq!(variants.len(), 2);
            assert_eq!(variants[0].name, "Some");
            assert_eq!(variants[1].name, "Nothing");
        } else {
            panic!("Expected EnumDecl");
        }
    }

    #[test]
    fn test_inline_trait_bound() {
        let program = parse("fn foo<T: Comparable>(x: T) { x }").unwrap();
        if let StmtKind::FnDecl {
            type_params,
            bounds,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(type_params, &vec!["T".to_string()]);
            assert_eq!(bounds.len(), 1);
            assert_eq!(bounds[0].type_param, "T");
            assert_eq!(bounds[0].traits, vec!["Comparable".to_string()]);
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_where_clause() {
        let program = parse("fn foo<T>(x: T) where T: Comparable + Hashable { x }").unwrap();
        if let StmtKind::FnDecl {
            type_params,
            bounds,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(type_params, &vec!["T".to_string()]);
            assert_eq!(bounds.len(), 1);
            assert_eq!(bounds[0].type_param, "T");
            assert_eq!(
                bounds[0].traits,
                vec!["Comparable".to_string(), "Hashable".to_string()]
            );
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_trait_def() {
        let program = parse("trait Display { fn show(self) -> string }").unwrap();
        if let StmtKind::TraitDef {
            name,
            type_params,
            methods,
            is_public,
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Display");
            assert!(type_params.is_empty());
            assert_eq!(methods.len(), 1);
            assert_eq!(methods[0].name, "show");
            assert!(!*is_public);
        } else {
            panic!("Expected TraitDef");
        }
    }

    #[test]
    fn test_trait_impl() {
        let program =
            parse("impl Display for Point { fn show(self) -> string { \"point\" } }").unwrap();
        if let StmtKind::TraitImpl {
            trait_name,
            type_name,
            methods,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(trait_name, "Display");
            assert_eq!(type_name, "Point");
            assert_eq!(methods.len(), 1);
        } else {
            panic!("Expected TraitImpl");
        }
    }

    #[test]
    fn test_generic_trait_impl() {
        let program =
            parse("impl<T> Display for Box<T> { fn show(self) -> string { \"box\" } }").unwrap();
        if let StmtKind::TraitImpl {
            trait_name,
            type_name,
            type_params,
            methods,
        } = &program.statements[0].kind
        {
            assert_eq!(trait_name, "Display");
            assert_eq!(type_name, "Box");
            assert_eq!(type_params, &vec!["T".to_string()]);
            assert_eq!(methods.len(), 1);
        } else {
            panic!("Expected TraitImpl");
        }
    }

    #[test]
    fn test_pub_trait() {
        let program = parse("pub trait Serializable { fn serialize(self) -> string }").unwrap();
        if let StmtKind::TraitDef {
            name, is_public, ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Serializable");
            assert!(*is_public);
        } else {
            panic!("Expected TraitDef");
        }
    }

    #[test]
    fn test_multiple_type_params() {
        let program = parse("fn zip<A, B>(a: list<A>, b: list<B>) { a }").unwrap();
        if let StmtKind::FnDecl { type_params, .. } = &program.statements[0].kind {
            assert_eq!(type_params, &vec!["A".to_string(), "B".to_string()]);
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_existing_code_no_type_params() {
        // Existing code should still parse with empty type_params
        let program = parse("fn add(a, b) { a + b }").unwrap();
        if let StmtKind::FnDecl {
            type_params,
            bounds,
            ..
        } = &program.statements[0].kind
        {
            assert!(type_params.is_empty());
            assert!(bounds.is_empty());
        } else {
            panic!("Expected FnDecl");
        }
    }

    #[test]
    fn test_trait_with_multiple_methods() {
        let program =
            parse("trait Container { fn len(self) -> int fn is_empty(self) -> bool }").unwrap();
        if let StmtKind::TraitDef { name, methods, .. } = &program.statements[0].kind {
            assert_eq!(name, "Container");
            assert_eq!(methods.len(), 2);
            assert_eq!(methods[0].name, "len");
            assert_eq!(methods[1].name, "is_empty");
        } else {
            panic!("Expected TraitDef");
        }
    }

    #[test]
    fn test_generic_impl_block() {
        let program = parse("impl<T> Box { fn get(self) -> T { self.val } }").unwrap();
        if let StmtKind::ImplBlock {
            type_name,
            type_params,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(type_name, "Box");
            assert_eq!(type_params, &vec!["T".to_string()]);
        } else {
            panic!("Expected ImplBlock");
        }
    }

    // ── Phase 17: Pattern Matching ──

    #[test]
    fn test_parse_match_wildcard() {
        let program = parse("match x { _ => 1 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            assert!(matches!(arms[0].pattern, Pattern::Wildcard));
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_literal() {
        let program = parse("match x { 1 => \"one\", 2 => \"two\", _ => \"other\" }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            assert_eq!(arms.len(), 3);
            assert!(matches!(arms[0].pattern, Pattern::Literal(Expr::Int(1))));
            assert!(matches!(arms[1].pattern, Pattern::Literal(Expr::Int(2))));
            assert!(matches!(arms[2].pattern, Pattern::Wildcard));
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_binding() {
        let program = parse("match x { val => val + 1 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Binding(name) = &arms[0].pattern {
                assert_eq!(name, "val");
            } else {
                panic!("Expected binding pattern");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_enum_variant() {
        let program = parse("match x { Color::Red => 1, Color::Blue => 2 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Enum {
                type_name,
                variant,
                args,
            } = &arms[0].pattern
            {
                assert_eq!(type_name, "Color");
                assert_eq!(variant, "Red");
                assert!(args.is_empty());
            } else {
                panic!("Expected enum pattern");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_enum_with_args() {
        let program =
            parse("match x { Shape::Circle(r) => r, Shape::Rect(w, h) => w * h }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Enum { variant, args, .. } = &arms[0].pattern {
                assert_eq!(variant, "Circle");
                assert_eq!(args.len(), 1);
                assert!(matches!(args[0], Pattern::Binding(_)));
            } else {
                panic!("Expected enum pattern with args");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_guard() {
        let program = parse("match x { n if n > 0 => \"pos\", _ => \"other\" }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            assert!(arms[0].guard.is_some());
            assert!(matches!(arms[0].pattern, Pattern::Binding(_)));
            assert!(arms[1].guard.is_none());
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_match_or_pattern() {
        let program = parse("match x { 1 or 2 or 3 => \"small\", _ => \"big\" }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Or(pats) = &arms[0].pattern {
                assert_eq!(pats.len(), 3);
            } else {
                panic!("Expected OR pattern");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_list_pattern() {
        let program = parse("match x { [a, b] => a + b, _ => 0 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::List { elements, rest } = &arms[0].pattern {
                assert_eq!(elements.len(), 2);
                assert!(rest.is_none());
            } else {
                panic!("Expected list pattern");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_list_rest_pattern() {
        let program = parse("match x { [head, ...tail] => head, _ => 0 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::List { elements, rest } = &arms[0].pattern {
                assert_eq!(elements.len(), 1);
                assert_eq!(rest.as_deref(), Some("tail"));
            } else {
                panic!("Expected list pattern with rest");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_struct_pattern() {
        let program = parse("match p { Point { x, y } => x + y, _ => 0 }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Struct { name, fields } = &arms[0].pattern {
                assert_eq!(name.as_deref(), Some("Point"));
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "x");
                assert_eq!(fields[1].name, "y");
            } else {
                panic!("Expected struct pattern");
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_negative_literal_pattern() {
        let program = parse("match x { -5 => \"neg five\", _ => \"other\" }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            if let Pattern::Literal(Expr::Int(-5)) = &arms[0].pattern {
                // ok
            } else {
                panic!(
                    "Expected negative literal pattern, got {:?}",
                    arms[0].pattern
                );
            }
        } else {
            panic!("Expected match expression");
        }
    }

    #[test]
    fn test_parse_let_destructure_list() {
        let program = parse("let [a, b, c] = [1, 2, 3]").unwrap();
        if let StmtKind::LetDestructure { pattern, .. } = &program.statements[0].kind {
            if let Pattern::List { elements, rest } = pattern {
                assert_eq!(elements.len(), 3);
                assert!(rest.is_none());
            } else {
                panic!("Expected list pattern");
            }
        } else {
            panic!("Expected LetDestructure");
        }
    }

    #[test]
    fn test_parse_let_destructure_struct() {
        let program = parse("let { x, y } = point").unwrap();
        if let StmtKind::LetDestructure { pattern, .. } = &program.statements[0].kind {
            if let Pattern::Struct { name, fields } = pattern {
                assert!(name.is_none());
                assert_eq!(fields.len(), 2);
            } else {
                panic!("Expected struct pattern");
            }
        } else {
            panic!("Expected LetDestructure");
        }
    }

    #[test]
    fn test_parse_case_with_match_arm() {
        let program = parse("case { x > 10 => \"big\", _ => \"small\" }").unwrap();
        if let StmtKind::Expr(Expr::Case { arms }) = &program.statements[0].kind {
            assert_eq!(arms.len(), 2);
            // First arm: Wildcard + guard
            assert!(matches!(arms[0].pattern, Pattern::Wildcard));
            assert!(arms[0].guard.is_some());
            // Second arm: Wildcard, no guard (default)
            assert!(matches!(arms[1].pattern, Pattern::Wildcard));
            assert!(arms[1].guard.is_none());
        } else {
            panic!("Expected case expression");
        }
    }

    #[test]
    fn test_parse_backward_compat_match() {
        // Existing match syntax should still work
        let program = parse("match x { 1 => \"one\", 2 => \"two\", _ => \"other\" }").unwrap();
        if let StmtKind::Expr(Expr::Match { arms, .. }) = &program.statements[0].kind {
            assert_eq!(arms.len(), 3);
        } else {
            panic!("Expected match expression");
        }
    }

    // ── Phase 18: Closures & Lambdas Improvements ────────────────

    #[test]
    fn test_parse_expr_closure_still_works() {
        let program = parse("let f = (x) => x * 2").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            if let Expr::Closure {
                params,
                body,
                return_type,
            } = value
            {
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].name, "x");
                assert!(return_type.is_none());
                assert!(matches!(body, ClosureBody::Expr(_)));
            } else {
                panic!("Expected closure");
            }
        } else {
            panic!("Expected let");
        }
    }

    #[test]
    fn test_parse_block_body_closure() {
        let program = parse("let f = (x: int64) -> int64 { let y = x * 2\n y + 1 }").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            if let Expr::Closure {
                params,
                body,
                return_type,
            } = value
            {
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].name, "x");
                assert!(return_type.is_some());
                if let ClosureBody::Block { stmts, expr } = body {
                    assert_eq!(stmts.len(), 1); // let y = x * 2
                    assert!(expr.is_some()); // y + 1
                } else {
                    panic!("Expected block body");
                }
            } else {
                panic!("Expected closure");
            }
        } else {
            panic!("Expected let");
        }
    }

    #[test]
    fn test_is_closure_ahead_arrow() {
        // (x) -> int64 { ... } should be detected as closure
        let program = parse("let f = (x) -> int64 { x + 1 }").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            assert!(matches!(value, Expr::Closure { .. }));
        } else {
            panic!("Expected let with closure");
        }
    }

    #[test]
    fn test_parse_block_body_closure_no_params() {
        let program = parse("let f = () -> int64 { 42 }").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            if let Expr::Closure { params, body, .. } = value {
                assert_eq!(params.len(), 0);
                if let ClosureBody::Block { stmts, expr } = body {
                    assert!(stmts.is_empty());
                    assert!(expr.is_some());
                } else {
                    panic!("Expected block body");
                }
            } else {
                panic!("Expected closure");
            }
        } else {
            panic!("Expected let");
        }
    }

    #[test]
    fn test_parse_type_alias_simple() {
        let program = parse("type Mapper = fn(int64) -> int64").unwrap();
        if let StmtKind::TypeAlias {
            name,
            type_params,
            value,
            is_public,
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Mapper");
            assert!(type_params.is_empty());
            assert!(!is_public);
            assert!(matches!(value, TypeExpr::Function { .. }));
        } else {
            panic!("Expected TypeAlias");
        }
    }

    #[test]
    fn test_parse_type_alias_generic() {
        let program = parse("type Pair<T> = list<T>").unwrap();
        if let StmtKind::TypeAlias {
            name, type_params, ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Pair");
            assert_eq!(type_params, &["T"]);
        } else {
            panic!("Expected TypeAlias");
        }
    }

    #[test]
    fn test_parse_pub_type_alias() {
        let program = parse("pub type Predicate = fn(string) -> bool").unwrap();
        if let StmtKind::TypeAlias {
            name, is_public, ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "Predicate");
            assert!(is_public);
        } else {
            panic!("Expected TypeAlias");
        }
    }

    #[test]
    fn test_parse_shorthand_closure() {
        let program = parse("let f = x => x * 2").unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            if let Expr::Closure {
                params,
                body,
                return_type,
            } = value
            {
                assert_eq!(params.len(), 1);
                assert_eq!(params[0].name, "x");
                assert!(return_type.is_none());
                assert!(matches!(body, ClosureBody::Expr(_)));
            } else {
                panic!("Expected closure");
            }
        } else {
            panic!("Expected let");
        }
    }

    #[test]
    fn test_parse_shorthand_closure_in_call() {
        // Shorthand closure as argument: map(list, x => x + 1)
        let program = parse("map(nums, x => x + 1)").unwrap();
        if let StmtKind::Expr(Expr::Call { args, .. }) = &program.statements[0].kind {
            assert_eq!(args.len(), 2);
            assert!(matches!(&args[1], Expr::Closure { .. }));
        } else {
            panic!("Expected call with closure arg");
        }
    }

    // ── Phase 19: Doc comment parsing tests ─────────────────────

    #[test]
    fn test_doc_comment_on_fn() {
        let program = parse("/// Adds two numbers\nfn add(a, b) { a + b }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("Adds two numbers")
        );
    }

    #[test]
    fn test_doc_comment_on_struct() {
        let program = parse("/// A 2D point\nstruct Point { x: int, y: int }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("A 2D point")
        );
    }

    #[test]
    fn test_doc_comment_on_enum() {
        let program = parse("/// Color values\nenum Color { Red, Green, Blue }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("Color values")
        );
    }

    #[test]
    fn test_doc_comment_on_trait() {
        let program =
            parse("/// Display trait\ntrait Display { fn show(self) -> string }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("Display trait")
        );
    }

    #[test]
    fn test_doc_comment_on_pub_fn() {
        let program = parse("/// Public function\npub fn greet() { print(\"hi\") }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("Public function")
        );
    }

    #[test]
    fn test_multiline_doc_comment() {
        let source = "/// First line\n/// Second line\n/// Third line\nfn foo() {}";
        let program = parse(source).unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("First line\nSecond line\nThird line")
        );
    }

    #[test]
    fn test_no_doc_comment() {
        let program = parse("fn foo() {}").unwrap();
        assert!(program.statements[0].doc_comment.is_none());
    }

    #[test]
    fn test_inner_doc_comment_module() {
        let source = "//! This module does stuff\n//! More info\nfn foo() {}";
        let program = parse(source).unwrap();
        assert_eq!(
            program.module_doc.as_deref(),
            Some("This module does stuff\nMore info")
        );
    }

    #[test]
    fn test_doc_comment_not_on_expr() {
        // Doc comments before expressions still get attached (parser is lenient)
        let source = "/// Some doc\n42";
        let program = parse(source).unwrap();
        // The doc attaches to the expression statement
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("Some doc")
        );
    }

    #[test]
    fn test_doc_comment_on_let() {
        let program = parse("/// The answer\nlet x = 42").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("The answer")
        );
    }

    #[test]
    fn test_doc_comment_on_schema() {
        let program = parse("/// User schema\nschema User { name: string, age: int }").unwrap();
        assert_eq!(
            program.statements[0].doc_comment.as_deref(),
            Some("User schema")
        );
    }

    // ── Phase 21: Schema Evolution & Migration ──────────────────────

    #[test]
    fn test_parse_versioned_schema() {
        let source = "/// User schema\n/// @version 1\nschema User { name: string }";
        let program = parse(source).unwrap();
        if let StmtKind::Schema { name, version, .. } = &program.statements[0].kind {
            assert_eq!(name, "User");
            assert_eq!(*version, Some(1));
        } else {
            panic!("Expected Schema statement");
        }
    }

    #[test]
    fn test_parse_schema_field_doc_comments() {
        let source = "schema User {\n  /// User's name\n  /// @since 1\n  name: string\n}";
        let program = parse(source).unwrap();
        if let StmtKind::Schema { fields, .. } = &program.statements[0].kind {
            assert_eq!(fields[0].name, "name");
            assert!(fields[0].doc_comment.is_some());
            assert!(fields[0].doc_comment.as_ref().unwrap().contains("@since"));
        } else {
            panic!("Expected Schema statement");
        }
    }

    #[test]
    fn test_parse_schema_field_default_value() {
        let source = "schema User { name: string = \"unknown\", age: int64 }";
        let program = parse(source).unwrap();
        if let StmtKind::Schema { fields, .. } = &program.statements[0].kind {
            assert_eq!(fields.len(), 2);
            assert!(fields[0].default_value.is_some());
            assert!(fields[1].default_value.is_none());
        } else {
            panic!("Expected Schema statement");
        }
    }

    #[test]
    fn test_parse_migrate_add_column() {
        let source = "migrate User from 1 to 2 { add_column(email: string) }";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate {
            schema_name,
            from_version,
            to_version,
            operations,
        } = &program.statements[0].kind
        {
            assert_eq!(schema_name, "User");
            assert_eq!(*from_version, 1);
            assert_eq!(*to_version, 2);
            assert_eq!(operations.len(), 1);
            assert!(matches!(&operations[0], MigrateOp::AddColumn { name, .. } if name == "email"));
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_migrate_drop_column() {
        let source = "migrate User from 2 to 3 { drop_column(legacy) }";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate { operations, .. } = &program.statements[0].kind {
            assert!(matches!(&operations[0], MigrateOp::DropColumn { name } if name == "legacy"));
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_migrate_rename_column() {
        let source = "migrate User from 1 to 2 { rename_column(old_name, new_name) }";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate { operations, .. } = &program.statements[0].kind {
            assert!(
                matches!(&operations[0], MigrateOp::RenameColumn { from, to } if from == "old_name" && to == "new_name")
            );
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_migrate_alter_type() {
        let source = "migrate User from 1 to 2 { alter_type(age, float64) }";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate { operations, .. } = &program.statements[0].kind {
            assert!(
                matches!(&operations[0], MigrateOp::AlterType { column, .. } if column == "age")
            );
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_migrate_multiple_operations() {
        let source = "migrate User from 1 to 2 {\n  add_column(email: string)\n  drop_column(legacy)\n  rename_column(fname, first_name)\n}";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate { operations, .. } = &program.statements[0].kind {
            assert_eq!(operations.len(), 3);
            assert!(matches!(&operations[0], MigrateOp::AddColumn { .. }));
            assert!(matches!(&operations[1], MigrateOp::DropColumn { .. }));
            assert!(matches!(&operations[2], MigrateOp::RenameColumn { .. }));
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_migrate_error_no_versions() {
        let result = parse("migrate User { add_column(x: int64) }");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_migrate_error_invalid_op() {
        let result = parse("migrate User from 1 to 2 { invalid_op(x) }");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_migrate_add_column_with_default() {
        let source = "migrate User from 1 to 2 { add_column(email: string, default: \"\") }";
        let program = parse(source).unwrap();
        if let StmtKind::Migrate { operations, .. } = &program.statements[0].kind {
            if let MigrateOp::AddColumn { name, default, .. } = &operations[0] {
                assert_eq!(name, "email");
                assert!(default.is_some());
            } else {
                panic!("Expected AddColumn");
            }
        } else {
            panic!("Expected Migrate statement");
        }
    }

    #[test]
    fn test_parse_schema_version_in_doc() {
        let source = "/// @version 5\nschema Events { ts: int64 }";
        let program = parse(source).unwrap();
        if let StmtKind::Schema { version, .. } = &program.statements[0].kind {
            assert_eq!(*version, Some(5));
        } else {
            panic!("Expected Schema statement");
        }
    }

    // ── Phase 22-24 Parser Tests ───────────────────────────────────

    #[test]
    fn test_parse_decimal_literal() {
        let source = "let x = 3.14d";
        let program = parse(source).unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            match value {
                // The lexer strips the trailing 'd' and underscores
                Expr::Decimal(s) => assert_eq!(s, "3.14"),
                _ => panic!("Expected Expr::Decimal, got {value:?}"),
            }
        } else {
            panic!("Expected Let statement");
        }
    }

    #[test]
    fn test_parse_decimal_underscore() {
        let source = "let x = 1_000.50d";
        let program = parse(source).unwrap();
        if let StmtKind::Let { value, .. } = &program.statements[0].kind {
            match value {
                // Underscores and trailing 'd' are stripped by lexer
                Expr::Decimal(s) => assert_eq!(s, "1000.50"),
                _ => panic!("Expected Expr::Decimal"),
            }
        } else {
            panic!("Expected Let statement");
        }
    }

    #[test]
    fn test_parse_async_fn() {
        let source = "async fn fetch() { return 42 }";
        let program = parse(source).unwrap();
        if let StmtKind::FnDecl { name, is_async, .. } = &program.statements[0].kind {
            assert_eq!(name, "fetch");
            assert!(*is_async);
        } else {
            panic!("Expected FnDecl statement");
        }
    }

    #[test]
    fn test_parse_async_fn_with_params() {
        let source = "async fn get(url, timeout) { return url }";
        let program = parse(source).unwrap();
        if let StmtKind::FnDecl {
            name,
            is_async,
            params,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "get");
            assert!(*is_async);
            assert_eq!(params.len(), 2);
        } else {
            panic!("Expected FnDecl statement");
        }
    }

    #[test]
    fn test_parse_sensitive_annotation() {
        let source = r#"
/// @sensitive
schema Secret {
    password: string
}
"#;
        let program = parse(source).unwrap();
        if let StmtKind::Schema { fields, .. } = &program.statements[0].kind {
            assert!(!fields.is_empty());
            // The annotation is on the schema-level doc, not on individual fields
        } else {
            panic!("Expected Schema statement");
        }
    }

    // ── Phase 34: Agent Framework ──

    #[test]
    fn test_parse_agent_basic() {
        let program = parse(
            r#"agent bot {
                model: "gpt-4o",
                system: "You are helpful.",
                tools {
                    search: {
                        description: "Search the web",
                        parameters: {}
                    }
                },
                max_turns: 10
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent {
            name,
            model,
            system_prompt,
            tools,
            max_turns,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "bot");
            assert_eq!(model, "gpt-4o");
            assert_eq!(system_prompt.as_deref(), Some("You are helpful."));
            assert_eq!(tools.len(), 1);
            assert_eq!(tools[0].0, "search");
            assert_eq!(max_turns, &Some(10));
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_minimal() {
        let program = parse(
            r#"agent minimal {
                model: "claude-sonnet-4-20250514"
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent {
            name,
            model,
            system_prompt,
            tools,
            max_turns,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "minimal");
            assert_eq!(model, "claude-sonnet-4-20250514");
            assert!(system_prompt.is_none());
            assert!(tools.is_empty());
            assert!(max_turns.is_none());
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_multiple_tools() {
        let program = parse(
            r#"agent assistant {
                model: "gpt-4o",
                tools {
                    search: { description: "Search", parameters: {} },
                    weather: { description: "Get weather", parameters: {} }
                }
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent { tools, .. } = &program.statements[0].kind {
            assert_eq!(tools.len(), 2);
            assert_eq!(tools[0].0, "search");
            assert_eq!(tools[1].0, "weather");
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_with_base_url() {
        let program = parse(
            r#"agent local {
                model: "llama3",
                base_url: "http://localhost:11434/v1",
                max_turns: 3,
                temperature: 0.7
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent {
            name,
            base_url,
            temperature,
            max_turns,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "local");
            assert_eq!(base_url.as_deref(), Some("http://localhost:11434/v1"));
            assert_eq!(temperature, &Some(0.7));
            assert_eq!(max_turns, &Some(3));
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_lifecycle_hooks() {
        let program = parse(
            r#"agent bot {
                model: "gpt-4o",
                tools {
                    search: { description: "Search", parameters: {} }
                },
                on_tool_call {
                    println("Tool called: " + tool_name)
                }
                on_complete {
                    println("Done!")
                }
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent {
            name,
            on_tool_call,
            on_complete,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "bot");
            assert!(on_tool_call.is_some());
            assert_eq!(on_tool_call.as_ref().unwrap().len(), 1);
            assert!(on_complete.is_some());
            assert_eq!(on_complete.as_ref().unwrap().len(), 1);
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_with_mcp_servers() {
        let program = parse(
            r#"agent mcp_bot {
                model: "gpt-4o",
                mcp_servers: [fs_server, db_server],
                max_turns: 5
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent {
            name,
            mcp_servers,
            max_turns,
            ..
        } = &program.statements[0].kind
        {
            assert_eq!(name, "mcp_bot");
            assert_eq!(mcp_servers.len(), 2);
            // Each should be an Ident expression
            assert!(matches!(&mcp_servers[0], Expr::Ident(s) if s == "fs_server"));
            assert!(matches!(&mcp_servers[1], Expr::Ident(s) if s == "db_server"));
            assert_eq!(max_turns, &Some(5));
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_empty_mcp_servers() {
        let program = parse(
            r#"agent bot {
                model: "gpt-4o",
                mcp_servers: []
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent { mcp_servers, .. } = &program.statements[0].kind {
            assert!(mcp_servers.is_empty());
        } else {
            panic!("Expected Agent statement");
        }
    }

    #[test]
    fn test_parse_agent_mcp_servers_single() {
        let program = parse(
            r#"agent bot {
                model: "gpt-4o",
                mcp_servers: [my_server]
            }"#,
        )
        .unwrap();
        if let StmtKind::Agent { mcp_servers, .. } = &program.statements[0].kind {
            assert_eq!(mcp_servers.len(), 1);
            assert!(matches!(&mcp_servers[0], Expr::Ident(s) if s == "my_server"));
        } else {
            panic!("Expected Agent statement");
        }
    }
}
