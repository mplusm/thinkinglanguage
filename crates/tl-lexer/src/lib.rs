// ThinkingLanguage — Lexer (Tokenizer)
// Licensed under MIT OR Apache-2.0
//
// Uses the `logos` crate for fast, zero-copy tokenization.
// Converts .tl source text into a stream of tokens.

use logos::Logos;
use tl_errors::Span;

/// All tokens in the ThinkingLanguage grammar.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t]+")]        // Skip whitespace (not newlines — they're significant)
#[logos(skip r"//[^\n]*")]      // Skip line comments (doc comments /// and //! matched by token rules with higher priority)
pub enum Token {
    // ── Doc Comments ────────────────────────────────────────
    /// `/// doc text` — documentation comment for the following item
    #[regex(r"///[^\n]*", |lex| lex.slice()[3..].to_string(), priority = 5)]
    DocComment(String),

    /// `//! module doc text` — inner documentation comment for the enclosing module
    #[regex(r"//![^\n]*", |lex| lex.slice()[3..].to_string(), priority = 5)]
    InnerDocComment(String),

    // ── Literals ─────────────────────────────────────────────

    /// Integer literal: 42, 1_000_000
    #[regex(r"[0-9][0-9_]*", |lex| lex.slice().replace('_', "").parse::<i64>().ok())]
    Int(i64),

    /// Float literal: 3.14, 1_000.5, 1.0e10
    #[regex(r"[0-9][0-9_]*\.[0-9][0-9_]*([eE][+-]?[0-9]+)?", |lex| lex.slice().replace('_', "").parse::<f64>().ok())]
    Float(f64),

    /// String literal: "hello {name}"
    #[regex(r#""([^"\\]|\\.)*""#, |lex| {
        let s = lex.slice();
        Some(s[1..s.len()-1].to_string())
    })]
    String(String),

    /// Duration literal: 90d, 5h, 30m, 15s, 100ms
    #[regex(r"[0-9]+ms", |lex| { let s = lex.slice(); Some(s[..s.len()-2].to_string()) }, priority = 5)]
    DurationMs(String),
    #[regex(r"[0-9]+s", |lex| { let s = lex.slice(); Some(s[..s.len()-1].to_string()) }, priority = 4)]
    DurationS(String),
    #[regex(r"[0-9]+m", |lex| { let s = lex.slice(); Some(s[..s.len()-1].to_string()) }, priority = 3)]
    DurationM(String),
    #[regex(r"[0-9]+h", |lex| { let s = lex.slice(); Some(s[..s.len()-1].to_string()) }, priority = 4)]
    DurationH(String),
    #[regex(r"[0-9]+d", |lex| { let s = lex.slice(); Some(s[..s.len()-1].to_string()) }, priority = 4)]
    DurationD(String),

    /// Boolean literals
    #[token("true")]
    True,
    #[token("false")]
    False,

    // ── Keywords ─────────────────────────────────────────────

    // Data constructs
    #[token("table")]
    Table,
    #[token("stream")]
    Stream,
    #[token("schema")]
    Schema,
    #[token("struct")]
    Struct,
    #[token("source")]
    Source,
    #[token("sink")]
    Sink,
    #[token("extract")]
    Extract,
    #[token("transform")]
    Transform,
    #[token("load")]
    Load,
    #[token("pipeline")]
    Pipeline,
    #[token("with")]
    With,
    #[token("connector")]
    Connector,

    // AI
    #[token("model")]
    Model,
    #[token("train")]
    Train,

    // Control flow
    #[token("if")]
    If,
    #[token("else")]
    Else,
    #[token("match")]
    Match,
    #[token("case")]
    Case,
    #[token("for")]
    For,
    #[token("while")]
    While,
    #[token("in")]
    In,
    #[token("return")]
    Return,
    #[token("break")]
    Break,
    #[token("continue")]
    Continue,
    #[token("yield")]
    Yield,

    // Concurrency
    #[token("parallel")]
    Parallel,
    #[token("async")]
    Async,
    #[token("await")]
    Await,
    #[token("emit")]
    Emit,

    // Functions & modules
    #[token("fn")]
    Fn,
    #[token("use")]
    Use,
    #[token("pub")]
    Pub,
    #[token("mod")]
    Mod,
    #[token("trait")]
    Trait,
    #[token("where")]
    Where,
    #[token("impl")]
    Impl,
    #[token("let")]
    Let,
    #[token("mut")]
    Mut,
    #[token("const")]
    Const,
    #[token("type")]
    Type,
    #[token("as")]
    As,
    #[token("enum")]
    Enum,

    // Error handling
    #[token("try")]
    Try,
    #[token("catch")]
    Catch,
    #[token("throw")]
    Throw,

    // Module
    #[token("import")]
    Import,

    // Testing
    #[token("test")]
    Test,

    // Schema evolution
    #[token("migrate")]
    Migrate,

    // Primitives
    #[token("none")]
    None_,
    #[token("self")]
    Self_,

    // ── Operators ────────────────────────────────────────────

    // Pipe & flow
    #[token("|>")]
    Pipe,
    #[token("->")]
    Arrow,
    #[token("=>")]
    FatArrow,

    // Arithmetic
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token("**")]
    Power,

    // Comparison
    #[token("==")]
    Eq,
    #[token("!=")]
    Neq,
    #[token("<=")]
    Lte,
    #[token(">=")]
    Gte,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,

    // Logical
    #[token("and")]
    And,
    #[token("or")]
    Or,
    #[token("not")]
    Not,

    // Assignment
    #[token("=")]
    Assign,
    #[token("+=")]
    PlusAssign,
    #[token("-=")]
    MinusAssign,
    #[token("*=")]
    StarAssign,
    #[token("/=")]
    SlashAssign,

    // Null handling
    #[token("??")]
    NullCoalesce,
    #[token("?")]
    Question,

    // Range
    #[token("...")]
    DotDotDot,
    #[token("..")]
    DotDot,

    // Column reference
    #[token("@")]
    At,

    // Reference
    #[token("&")]
    Ampersand,

    // ── Punctuation ──────────────────────────────────────────

    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(",")]
    Comma,
    #[token("::", priority = 3)]
    ColonColon,
    #[token(":")]
    Colon,
    #[token(";")]
    Semicolon,
    #[token(".")]
    Dot,
    #[token("_")]
    Underscore,
    #[token("\n")]
    Newline,

    // ── Identifier ───────────────────────────────────────────

    /// Identifier: variable names, function names, type names
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice().to_string(), priority = 1)]
    Ident(String),
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Int(n) => write!(f, "{n}"),
            Token::Float(n) => write!(f, "{n}"),
            Token::String(s) => write!(f, "\"{s}\""),
            Token::True => write!(f, "true"),
            Token::False => write!(f, "false"),
            Token::Ident(s) => write!(f, "{s}"),
            Token::Pipe => write!(f, "|>"),
            Token::Arrow => write!(f, "->"),
            Token::FatArrow => write!(f, "=>"),
            Token::Assign => write!(f, "="),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Newline => write!(f, "\\n"),
            Token::DocComment(s) => write!(f, "///{s}"),
            Token::InnerDocComment(s) => write!(f, "//!{s}"),
            tok => write!(f, "{tok:?}"),
        }
    }
}

/// A token with its span in the source text
#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

/// Tokenize source text into a vector of spanned tokens.
/// Filters out newlines (for now — may be significant later).
pub fn tokenize(source: &str) -> Result<Vec<SpannedToken>, tl_errors::TlError> {
    let mut tokens = Vec::new();
    let mut lexer = Token::lexer(source);

    while let Some(result) = lexer.next() {
        let span = Span::from(lexer.span());
        match result {
            Ok(token) => {
                // Skip newlines for now (optional semicolons, newline-terminated)
                if token != Token::Newline {
                    tokens.push(SpannedToken { token, span });
                }
            }
            Err(()) => {
                return Err(tl_errors::TlError::Lexer(tl_errors::LexerError {
                    message: format!(
                        "Unexpected character: '{}'",
                        &source[span.start..span.end]
                    ),
                    span,
                }));
            }
        }
    }

    tokens.push(SpannedToken {
        token: Token::EOF,
        span: Span::new(source.len(), source.len()),
    });

    Ok(tokens)
}

// EOF token — not generated by logos, added manually
impl Token {
    pub const EOF: Token = Token::None_; // Temporary sentinel
}

// ── Trivia-preserving tokenization (for formatter) ─────────────────────────

/// Trivia items (non-code tokens)
#[derive(Debug, Clone, PartialEq)]
pub enum Trivia {
    Comment(String),
    Newline,
    Whitespace(String),
}

/// A token or trivia with its source span
#[derive(Debug, Clone)]
pub enum TriviaOrToken {
    Token(SpannedToken),
    Trivia(Trivia, Span),
}

/// Tokenize source preserving comments, newlines, and whitespace as trivia.
/// Used by the formatter for comment preservation.
pub fn tokenize_with_trivia(source: &str) -> Vec<TriviaOrToken> {
    let mut result = Vec::new();
    // First get all real tokens (including newlines) from logos
    let mut lexer = Token::lexer(source);
    let mut logo_tokens: Vec<(Token, Span)> = Vec::new();
    while let Some(res) = lexer.next() {
        let span = Span::from(lexer.span());
        if let Ok(tok) = res {
            logo_tokens.push((tok, span));
        }
    }

    // Also scan for comments since logos skips them
    let mut comments: Vec<(String, Span)> = Vec::new();
    let bytes = source.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            // skip string literals
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' { i += 1; }
                i += 1;
            }
            if i < bytes.len() { i += 1; }
        } else if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            // Skip doc comments (/// and //!) — they're tokens, not trivia
            if i + 2 < bytes.len() && (bytes[i + 2] == b'/' || bytes[i + 2] == b'!') {
                // This is a doc comment token — skip past it (logos handles it)
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            } else {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                let text = source[start..i].to_string();
                comments.push((text, Span::new(start, i)));
            }
        } else {
            i += 1;
        }
    }

    // Merge tokens and comments, sorted by start position
    #[derive(Debug)]
    enum Item {
        Tok(Token, Span),
        Comment(String, Span),
    }

    let mut items: Vec<Item> = Vec::new();
    for (tok, span) in logo_tokens {
        items.push(Item::Tok(tok, span));
    }
    for (text, span) in comments {
        items.push(Item::Comment(text, span));
    }
    items.sort_by_key(|it| match it {
        Item::Tok(_, s) => s.start,
        Item::Comment(_, s) => s.start,
    });

    let mut last_end = 0;
    for item in items {
        let (item_start, item_end) = match &item {
            Item::Tok(_, s) => (s.start, s.end),
            Item::Comment(_, s) => (s.start, s.end),
        };

        // Emit whitespace gap between last_end and item_start
        if item_start > last_end {
            let gap = &source[last_end..item_start];
            // Split gap into whitespace and newline trivia
            let mut ws_start = last_end;
            for (bi, b) in gap.bytes().enumerate() {
                if b == b'\n' {
                    if last_end + bi > ws_start {
                        result.push(TriviaOrToken::Trivia(
                            Trivia::Whitespace(source[ws_start..last_end + bi].to_string()),
                            Span::new(ws_start, last_end + bi),
                        ));
                    }
                    result.push(TriviaOrToken::Trivia(
                        Trivia::Newline,
                        Span::new(last_end + bi, last_end + bi + 1),
                    ));
                    ws_start = last_end + bi + 1;
                }
            }
            if ws_start < item_start {
                let ws = &source[ws_start..item_start];
                if !ws.is_empty() {
                    result.push(TriviaOrToken::Trivia(
                        Trivia::Whitespace(ws.to_string()),
                        Span::new(ws_start, item_start),
                    ));
                }
            }
        }

        match item {
            Item::Tok(tok, span) => {
                if tok == Token::Newline {
                    result.push(TriviaOrToken::Trivia(Trivia::Newline, span));
                } else {
                    result.push(TriviaOrToken::Token(SpannedToken { token: tok, span }));
                }
            }
            Item::Comment(text, span) => {
                result.push(TriviaOrToken::Trivia(Trivia::Comment(text), span));
            }
        }

        last_end = item_end;
    }

    // Trailing whitespace/newlines
    if last_end < source.len() {
        let remaining = &source[last_end..];
        let mut ws_start = last_end;
        for (bi, b) in remaining.bytes().enumerate() {
            if b == b'\n' {
                if last_end + bi > ws_start {
                    result.push(TriviaOrToken::Trivia(
                        Trivia::Whitespace(source[ws_start..last_end + bi].to_string()),
                        Span::new(ws_start, last_end + bi),
                    ));
                }
                result.push(TriviaOrToken::Trivia(
                    Trivia::Newline,
                    Span::new(last_end + bi, last_end + bi + 1),
                ));
                ws_start = last_end + bi + 1;
            }
        }
        if ws_start < source.len() {
            let ws = &source[ws_start..source.len()];
            if !ws.is_empty() {
                result.push(TriviaOrToken::Trivia(
                    Trivia::Whitespace(ws.to_string()),
                    Span::new(ws_start, source.len()),
                ));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokens() {
        let tokens = tokenize("let x = 42").unwrap();
        assert!(matches!(&tokens[0].token, Token::Let));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "x"));
        assert!(matches!(&tokens[2].token, Token::Assign));
        assert!(matches!(&tokens[3].token, Token::Int(42)));
    }

    #[test]
    fn test_pipe_operator() {
        let tokens = tokenize("users |> filter(age > 25)").unwrap();
        assert!(matches!(&tokens[0].token, Token::Ident(s) if s == "users"));
        assert!(matches!(&tokens[1].token, Token::Pipe));
        assert!(matches!(&tokens[2].token, Token::Ident(s) if s == "filter"));
    }

    #[test]
    fn test_string_literal() {
        let tokens = tokenize(r#"let name = "hello""#).unwrap();
        assert!(matches!(&tokens[2].token, Token::Assign));
        assert!(matches!(&tokens[3].token, Token::String(s) if s == "hello"));
    }

    #[test]
    fn test_keywords() {
        let tokens = tokenize("fn schema struct with pipeline").unwrap();
        assert!(matches!(tokens[0].token, Token::Fn));
        assert!(matches!(tokens[1].token, Token::Schema));
        assert!(matches!(tokens[2].token, Token::Struct));
        assert!(matches!(tokens[3].token, Token::With));
        assert!(matches!(tokens[4].token, Token::Pipeline));
    }

    #[test]
    fn test_float() {
        let tokens = tokenize("3.14").unwrap();
        assert!(matches!(tokens[0].token, Token::Float(f) if (f - 3.14).abs() < f64::EPSILON));
    }

    #[test]
    fn test_comments_skipped() {
        let tokens = tokenize("let x = 5 // this is a comment").unwrap();
        // Should only have: let, x, =, 5, EOF
        assert_eq!(tokens.len(), 5);
    }

    #[test]
    fn test_colon_colon() {
        let tokens = tokenize("Foo::Bar").unwrap();
        assert!(matches!(&tokens[0].token, Token::Ident(s) if s == "Foo"));
        assert!(matches!(&tokens[1].token, Token::ColonColon));
        assert!(matches!(&tokens[2].token, Token::Ident(s) if s == "Bar"));
    }

    #[test]
    fn test_try_catch_throw_tokens() {
        let tokens = tokenize("try catch throw").unwrap();
        assert!(matches!(tokens[0].token, Token::Try));
        assert!(matches!(tokens[1].token, Token::Catch));
        assert!(matches!(tokens[2].token, Token::Throw));
    }

    #[test]
    fn test_import_token() {
        let tokens = tokenize("import foo").unwrap();
        assert!(matches!(tokens[0].token, Token::Import));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "foo"));
    }

    #[test]
    fn test_test_token() {
        let tokens = tokenize("test my_test").unwrap();
        assert!(matches!(tokens[0].token, Token::Test));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "my_test"));
    }

    // Phase 7: Concurrency token tests

    #[test]
    fn test_await_token() {
        let tokens = tokenize("await task").unwrap();
        assert!(matches!(tokens[0].token, Token::Await));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "task"));
    }

    #[test]
    fn test_concurrency_keywords_combo() {
        let tokens = tokenize("async await parallel").unwrap();
        assert!(matches!(tokens[0].token, Token::Async));
        assert!(matches!(tokens[1].token, Token::Await));
        assert!(matches!(tokens[2].token, Token::Parallel));
    }

    #[test]
    fn test_await_in_expression() {
        let tokens = tokenize("let x = await spawn(f)").unwrap();
        assert!(matches!(tokens[0].token, Token::Let));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "x"));
        assert!(matches!(tokens[2].token, Token::Assign));
        assert!(matches!(tokens[3].token, Token::Await));
        assert!(matches!(&tokens[4].token, Token::Ident(s) if s == "spawn"));
    }

    // Phase 8: Generator token tests

    #[test]
    fn test_yield_token() {
        let tokens = tokenize("yield 42").unwrap();
        assert!(matches!(tokens[0].token, Token::Yield));
        assert!(matches!(tokens[1].token, Token::Int(42)));
    }

    #[test]
    fn test_yield_in_function() {
        let tokens = tokenize("fn gen() { yield x }").unwrap();
        assert!(matches!(tokens[0].token, Token::Fn));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "gen"));
        assert!(matches!(tokens[4].token, Token::LBrace));
        assert!(matches!(tokens[5].token, Token::Yield));
        assert!(matches!(&tokens[6].token, Token::Ident(s) if s == "x"));
    }

    // Phase 14: Trivia tokenizer tests

    #[test]
    fn test_trivia_comment_preserved() {
        let items = tokenize_with_trivia("let x = 5 // comment");
        let has_comment = items.iter().any(|it| matches!(it, TriviaOrToken::Trivia(Trivia::Comment(c), _) if c == "// comment"));
        assert!(has_comment, "Comment should be preserved in trivia output");
    }

    #[test]
    fn test_trivia_newlines_preserved() {
        let items = tokenize_with_trivia("let x = 5\nlet y = 10");
        let newline_count = items.iter().filter(|it| matches!(it, TriviaOrToken::Trivia(Trivia::Newline, _))).count();
        assert!(newline_count >= 1, "Newlines should be preserved");
    }

    // Phase 19: Doc comment tests

    #[test]
    fn test_doc_comment_token() {
        let tokens = tokenize("/// Adds two numbers\nfn add() {}").unwrap();
        assert!(matches!(&tokens[0].token, Token::DocComment(s) if s == " Adds two numbers"),
            "/// should produce DocComment token, got {:?}", tokens[0].token);
        assert!(matches!(&tokens[1].token, Token::Fn));
    }

    #[test]
    fn test_inner_doc_comment_token() {
        let tokens = tokenize("//! Module docs\nfn foo() {}").unwrap();
        assert!(matches!(&tokens[0].token, Token::InnerDocComment(s) if s == " Module docs"),
            "//! should produce InnerDocComment token, got {:?}", tokens[0].token);
    }

    #[test]
    fn test_regular_comment_still_skipped() {
        let tokens = tokenize("// regular comment\nlet x = 5").unwrap();
        // Should only have: let, x, =, 5, EOF  (comment skipped)
        assert_eq!(tokens.len(), 5, "Regular comments should still be skipped");
        assert!(matches!(&tokens[0].token, Token::Let));
    }

    #[test]
    fn test_trivia_token_spans_match() {
        let source = "let x = 42";
        let trivia_items = tokenize_with_trivia(source);
        let regular_tokens = tokenize(source).unwrap();

        // Collect tokens from trivia output
        let trivia_tokens: Vec<&SpannedToken> = trivia_items
            .iter()
            .filter_map(|it| match it {
                TriviaOrToken::Token(st) => Some(st),
                _ => None,
            })
            .collect();

        // Should have same number of tokens (minus EOF in regular)
        assert_eq!(trivia_tokens.len(), regular_tokens.len() - 1, "Token count should match (excluding EOF)");

        // Spans should match
        for (tt, rt) in trivia_tokens.iter().zip(regular_tokens.iter()) {
            assert_eq!(tt.span.start, rt.span.start, "Token start spans should match");
            assert_eq!(tt.span.end, rt.span.end, "Token end spans should match");
        }
    }

    // Phase 21: Schema Evolution

    #[test]
    fn test_migrate_token() {
        let tokens = tokenize("migrate User from 1 to 2").unwrap();
        assert!(matches!(tokens[0].token, Token::Migrate));
        assert!(matches!(&tokens[1].token, Token::Ident(s) if s == "User"));
    }
}
