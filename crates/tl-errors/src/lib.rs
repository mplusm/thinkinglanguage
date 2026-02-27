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
    Type(TypeError),
}

#[derive(Debug, Clone)]
pub struct TypeError {
    pub message: String,
    pub span: Span,
    pub expected: Option<String>,
    pub found: Option<String>,
    pub hint: Option<String>,
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
    pub stack_trace: Vec<StackFrame>,
}

/// A frame in a stack trace.
#[derive(Debug, Clone)]
pub struct StackFrame {
    pub function: String,
    pub line: u32,
}

impl fmt::Display for TlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlError::Lexer(e) => write!(f, "Lexer error: {}", e.message),
            TlError::Parser(e) => write!(f, "Parse error: {}", e.message),
            TlError::Runtime(e) => write!(f, "Runtime error: {}", e.message),
            TlError::Type(e) => {
                write!(f, "Type error: {}", e.message)?;
                if let (Some(expected), Some(found)) = (&e.expected, &e.found) {
                    write!(f, " (expected `{expected}`, found `{found}`)")?;
                }
                Ok(())
            }
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

/// Pretty-print a type error with source context using ariadne
pub fn report_type_error(source: &str, filename: &str, error: &TypeError) {
    let mut builder = Report::build(ReportKind::Error, filename, error.span.start)
        .with_message(&error.message);

    let mut label_msg = error.message.clone();
    if let (Some(expected), Some(found)) = (&error.expected, &error.found) {
        label_msg = format!("expected `{expected}`, found `{found}`");
    }

    builder = builder.with_label(
        Label::new((filename, error.span.range()))
            .with_message(&label_msg)
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

/// Pretty-print a type warning with source context using ariadne
pub fn report_type_warning(source: &str, filename: &str, error: &TypeError) {
    let mut builder = Report::build(ReportKind::Warning, filename, error.span.start)
        .with_message(&error.message);

    let mut label_msg = error.message.clone();
    if let (Some(expected), Some(found)) = (&error.expected, &error.found) {
        label_msg = format!("expected `{expected}`, found `{found}`");
    }

    builder = builder.with_label(
        Label::new((filename, error.span.range()))
            .with_message(&label_msg)
            .with_color(Color::Yellow),
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
    // If we have a span, use ariadne for pretty source display
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
    } else if !error.stack_trace.is_empty() && error.stack_trace[0].line > 0 {
        // No span but we have a line number from the stack trace — build a span from it
        let line = error.stack_trace[0].line as usize;
        let lines: Vec<&str> = source.lines().collect();
        if line > 0 && line <= lines.len() {
            let mut offset = 0;
            for l in &lines[..line - 1] {
                offset += l.len() + 1; // +1 for newline
            }
            let line_len = lines[line - 1].len().max(1);
            let span = Span::new(offset, offset + line_len);
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
            eprintln!("Runtime error (line {}): {}", line, error.message);
        }
    } else {
        eprintln!("Runtime error: {}", error.message);
    }
    // Print stack trace if available (skip if only one frame at top-level)
    if error.stack_trace.len() > 1 || (error.stack_trace.len() == 1 && error.stack_trace[0].function != "<main>") {
        eprintln!("Stack trace:");
        for frame in &error.stack_trace {
            if frame.line > 0 {
                eprintln!("  at {} (line {})", frame.function, frame.line);
            } else {
                eprintln!("  at {}", frame.function);
            }
        }
    }
}
