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
#[logos(skip r"//[^\n]*")]      // Skip line comments
pub enum Token {
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
}
