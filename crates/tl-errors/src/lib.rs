// ThinkingLanguage — Error types and diagnostics
// Licensed under MIT OR Apache-2.0

use ariadne::{Color, Label, Report, ReportKind, Source};
use std::fmt;
use std::ops::Range;

/// A source location span (byte offsets into source text)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }
}

impl From<Range<usize>> for Span {
    fn from(range: Range<usize>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

// logos::Span is just Range<usize>, covered by the From<Range<usize>> impl above

/// All error types in the ThinkingLanguage compiler
#[derive(Debug, Clone)]
pub enum TlError {
    Lexer(LexerError),
    Parser(ParserError),
    Runtime(RuntimeError),
}

#[derive(Debug, Clone)]
pub struct LexerError {
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct ParserError {
    pub message: String,
    pub span: Span,
    pub hint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeError {
    pub message: String,
    pub span: Option<Span>,
}

impl fmt::Display for TlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlError::Lexer(e) => write!(f, "Lexer error: {}", e.message),
            TlError::Parser(e) => write!(f, "Parse error: {}", e.message),
            TlError::Runtime(e) => write!(f, "Runtime error: {}", e.message),
        }
    }
}

impl std::error::Error for TlError {}

/// Pretty-print a parser error with source context using ariadne
pub fn report_parser_error(source: &str, filename: &str, error: &ParserError) {
    let mut builder = Report::build(ReportKind::Error, filename, error.span.start)
        .with_message(&error.message)
        .with_label(
            Label::new((filename, error.span.range()))
                .with_message(&error.message)
                .with_color(Color::Red),
        );

    if let Some(hint) = &error.hint {
        builder = builder.with_help(hint);
    }

    builder
        .finish()
        .eprint((filename, Source::from(source)))
        .unwrap();
}

/// Pretty-print a runtime error
pub fn report_runtime_error(source: &str, filename: &str, error: &RuntimeError) {
    if let Some(span) = &error.span {
        Report::build(ReportKind::Error, filename, span.start)
            .with_message(&error.message)
            .with_label(
                Label::new((filename, span.range()))
                    .with_message(&error.message)
                    .with_color(Color::Red),
            )
            .finish()
            .eprint((filename, Source::from(source)))
            .unwrap();
    } else {
        eprintln!("Runtime error: {}", error.message);
    }
}
