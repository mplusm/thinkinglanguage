// ThinkingLanguage — LSP Document Symbols Provider
// Shows outline of functions, structs, enums, etc.

#[allow(deprecated)]
use lsp_types::{Location, SymbolInformation, SymbolKind};
use tl_ast::{Program, StmtKind};

use crate::diagnostics::span_to_range;

#[allow(deprecated)]
pub fn provide_document_symbols(
    source: &str,
    ast: Option<&Program>,
) -> Vec<SymbolInformation> {
    let mut symbols = Vec::new();
    let program = match ast {
        Some(p) => p,
        None => return symbols,
    };

    // Use a dummy URI — the caller provides the real one
    let uri: lsp_types::Uri = "file:///document".parse().unwrap();

    for stmt in &program.statements {
        match &stmt.kind {
            StmtKind::FnDecl { name, params, .. } => {
                let detail = format!("({} params)", params.len());
                symbols.push(make_symbol(name, SymbolKind::FUNCTION, Some(detail), source, stmt.span, &uri));
            }
            StmtKind::StructDecl { name, fields, .. } => {
                let detail = format!("({} fields)", fields.len());
                symbols.push(make_symbol(name, SymbolKind::STRUCT, Some(detail), source, stmt.span, &uri));
            }
            StmtKind::EnumDecl { name, variants, .. } => {
                let detail = format!("({} variants)", variants.len());
                symbols.push(make_symbol(name, SymbolKind::ENUM, Some(detail), source, stmt.span, &uri));
            }
            StmtKind::TraitDef { name, methods, .. } => {
                let detail = format!("({} methods)", methods.len());
                symbols.push(make_symbol(name, SymbolKind::INTERFACE, Some(detail), source, stmt.span, &uri));
            }
            StmtKind::ImplBlock { type_name, methods, .. } => {
                let detail = format!("impl ({} methods)", methods.len());
                symbols.push(make_symbol(type_name, SymbolKind::CLASS, Some(detail), source, stmt.span, &uri));
            }
            StmtKind::Let { name, .. } => {
                symbols.push(make_symbol(name, SymbolKind::VARIABLE, None, source, stmt.span, &uri));
            }
            StmtKind::Test { name, .. } => {
                let detail = Some("test".to_string());
                symbols.push(make_symbol(name, SymbolKind::FUNCTION, detail, source, stmt.span, &uri));
            }
            StmtKind::Pipeline { name, .. } => {
                let detail = Some("pipeline".to_string());
                symbols.push(make_symbol(name, SymbolKind::FUNCTION, detail, source, stmt.span, &uri));
            }
            StmtKind::Schema { name, fields, .. } => {
                let detail = format!("({} fields)", fields.len());
                symbols.push(make_symbol(name, SymbolKind::STRUCT, Some(detail), source, stmt.span, &uri));
            }
            _ => {}
        }
    }

    symbols
}

#[allow(deprecated)]
fn make_symbol(
    name: &str,
    kind: SymbolKind,
    _detail: Option<String>,
    source: &str,
    span: tl_errors::Span,
    uri: &lsp_types::Uri,
) -> SymbolInformation {
    let range = span_to_range(source, span);
    SymbolInformation {
        name: name.to_string(),
        kind,
        tags: None,
        deprecated: None,
        location: Location {
            uri: uri.clone(),
            range,
        },
        container_name: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_symbols() {
        let source = "fn add(a, b) { a + b }\nfn sub(a, b) { a - b }";
        let ast = tl_parser::parse(source).unwrap();
        let symbols = provide_document_symbols(source, Some(&ast));
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].name, "add");
        assert_eq!(symbols[0].kind, SymbolKind::FUNCTION);
        assert_eq!(symbols[1].name, "sub");
    }

    #[test]
    fn test_struct_with_fields() {
        let source = "struct Point { x: int, y: int }";
        let ast = tl_parser::parse(source).unwrap();
        let symbols = provide_document_symbols(source, Some(&ast));
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].name, "Point");
        assert_eq!(symbols[0].kind, SymbolKind::STRUCT);
    }

    #[test]
    fn test_empty_program_no_symbols() {
        let source = "";
        let ast = tl_parser::parse(source).unwrap();
        let symbols = provide_document_symbols(source, Some(&ast));
        assert!(symbols.is_empty());
    }
}
