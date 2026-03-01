// ThinkingLanguage — LSP Integration Tests
// End-to-end verification of LSP features.

use lsp_types::{DiagnosticSeverity, Position, SymbolKind};
use tl_lsp::completion;
use tl_lsp::diagnostics::build_diagnostics;
use tl_lsp::format::Formatter;
use tl_lsp::goto_def;
use tl_lsp::hover;
use tl_lsp::symbols;

// -- Diagnostics integration --

#[test]
fn test_parse_error_produces_correct_range() {
    let source = "let = 42";
    let errors = vec![tl_errors::ParserError {
        message: "Expected identifier after 'let'".to_string(),
        span: tl_errors::Span::new(4, 5),
        hint: None,
    }];
    let diags = build_diagnostics(source, &errors, &[], &[]);
    assert_eq!(diags.len(), 1);
    assert_eq!(diags[0].range.start.line, 0);
    assert_eq!(diags[0].range.start.character, 4);
    assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
}

#[test]
fn test_unused_variable_warning_severity() {
    let source = "let x = 42";
    let program = tl_parser::parse(source).unwrap();
    let config = tl_types::checker::CheckerConfig::default();
    let result = tl_types::checker::check_program(&program, &config);

    let diags = build_diagnostics(source, &[], &result.errors, &result.warnings);

    let warning_diags: Vec<_> = diags
        .iter()
        .filter(|d| d.severity == Some(DiagnosticSeverity::WARNING))
        .collect();
    assert!(
        !warning_diags.is_empty(),
        "Unused variable should produce WARNING diagnostics"
    );
}

// -- Completion integration --

#[test]
fn test_keyword_completions_present() {
    let items = completion::provide_completions("", None, None, Position::new(0, 0));
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    assert!(labels.contains(&"fn"));
    assert!(labels.contains(&"let"));
    assert!(labels.contains(&"struct"));
    assert!(labels.contains(&"pipeline"));
    assert!(labels.contains(&"trait"));
}

// -- Hover integration --

#[test]
fn test_function_signature_hover() {
    let source = "fn greet(name: string) -> string { name }";
    let ast = tl_parser::parse(source).unwrap();
    let result = hover::provide_hover(source, Some(&ast), None, Position::new(0, 4));
    assert!(result.is_some());
    let hover = result.unwrap();
    if let lsp_types::HoverContents::Markup(content) = &hover.contents {
        assert!(
            content.value.contains("fn greet"),
            "Hover should show function name"
        );
        assert!(
            content.value.contains("string"),
            "Hover should show param/return type"
        );
    }
}

// -- Go-to-definition integration --

#[test]
fn test_goto_def_resolves_variable() {
    let source = "let foo = 42\nprint(foo)";
    let ast = tl_parser::parse(source).unwrap();
    let uri: lsp_types::Uri = "file:///test.tl".parse().unwrap();
    // Position on 'foo' in print(foo) — line 1, char 6
    let result = goto_def::provide_goto_definition(source, Some(&ast), Position::new(1, 6), &uri);
    assert!(
        result.is_some(),
        "Should resolve variable to its definition"
    );
    if let Some(lsp_types::GotoDefinitionResponse::Scalar(loc)) = result {
        assert_eq!(loc.range.start.line, 0, "Definition should be on line 0");
    }
}

// -- Formatter integration --

#[test]
fn test_format_multi_function_file() {
    let source = "fn add(a,b) { a+b }\nfn sub(a,b) { a-b }";
    let result = Formatter::format(source).unwrap();
    assert!(
        result.contains("fn add(a, b)"),
        "Should format params: {result}"
    );
    assert!(
        result.contains("a + b"),
        "Should add operator spacing: {result}"
    );
    assert!(
        result.contains("\n\nfn sub"),
        "Should add blank line between functions: {result}"
    );
}

#[test]
fn test_format_preserves_comments() {
    let source = "// This is a function\nfn foo() { 42 }";
    let result = Formatter::format(source).unwrap();
    assert!(
        result.contains("// This is a function"),
        "Comment should be preserved: {result}"
    );
}

// -- Symbols integration --

#[test]
fn test_document_symbols_complete() {
    let source =
        "struct Point { x: int, y: int }\nfn distance(a, b) { 0 }\nenum Color { Red, Blue }";
    let ast = tl_parser::parse(source).unwrap();
    #[allow(deprecated)]
    let symbols = symbols::provide_document_symbols(source, Some(&ast));
    assert_eq!(symbols.len(), 3);
    #[allow(deprecated)]
    {
        assert_eq!(symbols[0].kind, SymbolKind::STRUCT);
        assert_eq!(symbols[1].kind, SymbolKind::FUNCTION);
        assert_eq!(symbols[2].kind, SymbolKind::ENUM);
    }
}
