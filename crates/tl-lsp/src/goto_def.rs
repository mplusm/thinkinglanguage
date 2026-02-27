// ThinkingLanguage — LSP Go-to-Definition Provider
// Navigate to where a symbol is defined.

use lsp_types::{GotoDefinitionResponse, Location, Position, Uri};
use tl_ast::Program;

use crate::ast_util;
use crate::diagnostics::span_to_range;

pub fn provide_goto_definition(
    source: &str,
    ast: Option<&Program>,
    position: Position,
    uri: &Uri,
) -> Option<GotoDefinitionResponse> {
    let program = ast?;
    let offset = ast_util::position_to_offset(source, position.line, position.character);
    let (name, _, _) = ast_util::find_ident_at_offset(source, offset)?;

    // Check if this is a builtin — no source location
    if crate::completion::BUILTINS.contains(&name.as_str()) {
        return None;
    }

    let defs = ast_util::collect_definitions(program);
    for (def_name, _kind, span) in &defs {
        if def_name == &name {
            return Some(GotoDefinitionResponse::Scalar(Location {
                uri: uri.clone(),
                range: span_to_range(source, *span),
            }));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_uri() -> Uri {
        "file:///test.tl".parse().unwrap()
    }

    #[test]
    fn test_goto_def_variable() {
        let source = "let x = 42\nprint(x)";
        let ast = tl_parser::parse(source).unwrap();
        let uri = test_uri();
        // Position on 'x' in print(x) — line 1, char 6
        let result = provide_goto_definition(source, Some(&ast), Position::new(1, 6), &uri);
        assert!(result.is_some(), "Should find definition of variable x");
        if let Some(GotoDefinitionResponse::Scalar(loc)) = result {
            assert_eq!(loc.range.start.line, 0, "Definition should be on line 0");
        }
    }

    #[test]
    fn test_goto_def_function() {
        let source = "fn add(a, b) { a + b }\nadd(1, 2)";
        let ast = tl_parser::parse(source).unwrap();
        let uri = test_uri();
        let result = provide_goto_definition(source, Some(&ast), Position::new(1, 1), &uri);
        assert!(result.is_some(), "Should find definition of function add");
    }

    #[test]
    fn test_goto_def_struct() {
        let source = "struct Point { x: int, y: int }\nlet p = Point { x: 1, y: 2 }";
        let ast = tl_parser::parse(source).unwrap();
        let uri = test_uri();
        let result = provide_goto_definition(source, Some(&ast), Position::new(1, 9), &uri);
        assert!(result.is_some(), "Should find definition of struct Point");
    }

    #[test]
    fn test_goto_def_builtin_returns_none() {
        let source = "print(42)";
        let ast = tl_parser::parse(source).unwrap();
        let uri = test_uri();
        let result = provide_goto_definition(source, Some(&ast), Position::new(0, 2), &uri);
        assert!(result.is_none(), "Builtins have no source location");
    }
}
