// ThinkingLanguage — LSP Diagnostics
// Converts TL errors/warnings into LSP Diagnostic objects.

use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use tl_errors::Span;

/// Convert a byte offset to an LSP Position (line, character).
pub fn offset_to_position(source: &str, offset: usize) -> Position {
    let offset = offset.min(source.len());
    let mut line = 0u32;
    let mut col = 0u32;
    for (i, ch) in source.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += ch.len_utf16() as u32;
        }
    }
    Position::new(line, col)
}

/// Convert a TL Span to an LSP Range.
pub fn span_to_range(source: &str, span: Span) -> Range {
    Range::new(
        offset_to_position(source, span.start),
        offset_to_position(source, span.end),
    )
}

/// Build LSP diagnostics from parse errors, type errors, and type warnings.
pub fn build_diagnostics(
    source: &str,
    parse_errors: &[tl_errors::ParserError],
    type_errors: &[tl_types::checker::TypeError],
    type_warnings: &[tl_types::checker::TypeError],
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for e in parse_errors {
        diagnostics.push(Diagnostic {
            range: span_to_range(source, e.span),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("tl".to_string()),
            message: e.message.clone(),
            ..Default::default()
        });
    }

    for e in type_errors {
        let mut msg = e.message.clone();
        if let (Some(expected), Some(found)) = (&e.expected, &e.found) {
            msg = format!("{msg} (expected `{expected}`, found `{found}`)");
        }
        diagnostics.push(Diagnostic {
            range: span_to_range(source, e.span),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("tl".to_string()),
            message: msg,
            ..Default::default()
        });
    }

    for w in type_warnings {
        diagnostics.push(Diagnostic {
            range: span_to_range(source, w.span),
            severity: Some(DiagnosticSeverity::WARNING),
            source: Some("tl".to_string()),
            message: w.message.clone(),
            ..Default::default()
        });
    }

    diagnostics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_to_range_single_line() {
        let source = "let x = 42";
        let span = Span::new(4, 5); // 'x'
        let range = span_to_range(source, span);
        assert_eq!(range.start.line, 0);
        assert_eq!(range.start.character, 4);
        assert_eq!(range.end.line, 0);
        assert_eq!(range.end.character, 5);
    }

    #[test]
    fn test_span_to_range_multi_line() {
        let source = "let x = 5\nlet y = 10";
        // "let x = 5\n" = 10 bytes, so offset 10 = start of line 2
        let span = Span::new(10, 13); // 'let' on line 2
        let range = span_to_range(source, span);
        assert_eq!(range.start.line, 1);
        assert_eq!(range.start.character, 0);
        assert_eq!(range.end.line, 1);
        assert_eq!(range.end.character, 3);
    }

    #[test]
    fn test_build_diagnostics_parse_error() {
        let source = "let = 42";
        let errors = vec![tl_errors::ParserError {
            message: "Expected identifier".to_string(),
            span: Span::new(4, 5),
            hint: None,
        }];
        let diags = build_diagnostics(source, &errors, &[], &[]);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert!(diags[0].message.contains("Expected identifier"));
    }

    #[test]
    fn test_build_diagnostics_type_error() {
        let source = "let x: int = true";
        let errors = vec![tl_types::checker::TypeError {
            message: "Type mismatch".to_string(),
            span: Span::new(13, 17),
            expected: Some("int".to_string()),
            found: Some("bool".to_string()),
            hint: None,
        }];
        let diags = build_diagnostics(source, &[], &errors, &[]);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
    }

    #[test]
    fn test_build_diagnostics_type_warning() {
        let source = "let x = 42";
        let warnings = vec![tl_types::checker::TypeError {
            message: "Unused variable `x`".to_string(),
            span: Span::new(4, 5),
            expected: None,
            found: None,
            hint: None,
        }];
        let diags = build_diagnostics(source, &[], &[], &warnings);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::WARNING));
    }
}
