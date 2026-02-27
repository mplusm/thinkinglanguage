// ThinkingLanguage — LSP Hover Provider
// Shows type information and signatures on hover.

use lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind, Position};
use tl_ast::{Program, StmtKind};
use tl_types::checker::CheckResult;

use crate::ast_util;

/// Builtin function signatures for hover display
fn builtin_signature(name: &str) -> Option<&'static str> {
    match name {
        "print" => Some("fn print(value: any)"),
        "println" => Some("fn println(value: any)"),
        "len" => Some("fn len(value: string | list | map | set) -> int"),
        "str" => Some("fn str(value: any) -> string"),
        "int" => Some("fn int(value: any) -> int"),
        "float" => Some("fn float(value: any) -> float"),
        "abs" => Some("fn abs(value: int | float) -> int | float"),
        "min" => Some("fn min(a: any, b: any) -> any"),
        "max" => Some("fn max(a: any, b: any) -> any"),
        "range" => Some("fn range(start: int, end: int, step?: int) -> list<int>"),
        "push" => Some("fn push(list: list, value: any) -> list"),
        "type_of" => Some("fn type_of(value: any) -> string"),
        "map" => Some("fn map(list: list, f: fn(any) -> any) -> list"),
        "filter" => Some("fn filter(list: list, f: fn(any) -> bool) -> list"),
        "reduce" => Some("fn reduce(list: list, f: fn(any, any) -> any, init: any) -> any"),
        "sum" => Some("fn sum(list: list<int | float>) -> int | float"),
        "sqrt" => Some("fn sqrt(x: float) -> float"),
        "pow" => Some("fn pow(base: float, exp: float) -> float"),
        "assert" => Some("fn assert(condition: bool, message?: string)"),
        "assert_eq" => Some("fn assert_eq(a: any, b: any)"),
        "json_parse" => Some("fn json_parse(s: string) -> any"),
        "json_stringify" => Some("fn json_stringify(value: any) -> string"),
        "spawn" => Some("fn spawn(f: fn() -> any) -> task<any>"),
        "sleep" => Some("fn sleep(ms: int)"),
        "channel" => Some("fn channel(capacity?: int) -> channel<any>"),
        "send" => Some("fn send(ch: channel, value: any)"),
        "recv" => Some("fn recv(ch: channel) -> any"),
        "Ok" => Some("fn Ok(value: any) -> Result<any, any>"),
        "Err" => Some("fn Err(value: any) -> Result<any, any>"),
        "unwrap" => Some("fn unwrap(result: Result<any, any>) -> any"),
        _ => None,
    }
}

pub fn provide_hover(
    source: &str,
    ast: Option<&Program>,
    _check_result: Option<&CheckResult>,
    position: Position,
) -> Option<Hover> {
    let offset = ast_util::position_to_offset(source, position.line, position.character);
    let (name, _, _) = ast_util::find_ident_at_offset(source, offset)?;

    // Check builtins first
    if let Some(sig) = builtin_signature(&name) {
        return Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: format!("```tl\n{sig}\n```\n\nBuiltin function"),
            }),
            range: None,
        });
    }

    let program = ast?;

    // Search definitions
    for stmt in &program.statements {
        match &stmt.kind {
            StmtKind::FnDecl { name: fn_name, params, return_type, .. } if fn_name == &name => {
                let params_str: Vec<String> = params.iter().map(|p| {
                    if let Some(ann) = &p.type_ann {
                        format!("{}: {}", p.name, format_type_expr(ann))
                    } else {
                        p.name.clone()
                    }
                }).collect();
                let ret_str = return_type.as_ref()
                    .map(|t| format!(" -> {}", format_type_expr(t)))
                    .unwrap_or_default();
                let sig = format!("fn {}({}){}", fn_name, params_str.join(", "), ret_str);
                return Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!("```tl\n{sig}\n```"),
                    }),
                    range: None,
                });
            }
            StmtKind::StructDecl { name: struct_name, fields, .. } if struct_name == &name => {
                let fields_str: Vec<String> = fields.iter()
                    .map(|f| format!("  {}: {}", f.name, format_type_expr(&f.type_ann)))
                    .collect();
                let sig = format!("struct {} {{\n{}\n}}", struct_name, fields_str.join(",\n"));
                return Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!("```tl\n{sig}\n```"),
                    }),
                    range: None,
                });
            }
            StmtKind::EnumDecl { name: enum_name, variants, .. } if enum_name == &name => {
                let variants_str: Vec<String> = variants.iter()
                    .map(|v| {
                        if v.fields.is_empty() {
                            format!("  {}", v.name)
                        } else {
                            let fields: Vec<String> = v.fields.iter().map(|f| format_type_expr(f)).collect();
                            format!("  {}({})", v.name, fields.join(", "))
                        }
                    })
                    .collect();
                let sig = format!("enum {} {{\n{}\n}}", enum_name, variants_str.join(",\n"));
                return Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!("```tl\n{sig}\n```"),
                    }),
                    range: None,
                });
            }
            StmtKind::Let { name: var_name, type_ann, .. } if var_name == &name => {
                let type_str = type_ann.as_ref()
                    .map(|t| format_type_expr(t))
                    .unwrap_or_else(|| "any".to_string());
                return Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!("```tl\nlet {var_name}: {type_str}\n```"),
                    }),
                    range: None,
                });
            }
            StmtKind::TraitDef { name: trait_name, methods, .. } if trait_name == &name => {
                let methods_str: Vec<String> = methods.iter()
                    .map(|m| {
                        let params: Vec<String> = m.params.iter().map(|p| {
                            if let Some(ann) = &p.type_ann {
                                format!("{}: {}", p.name, format_type_expr(ann))
                            } else {
                                p.name.clone()
                            }
                        }).collect();
                        let ret = m.return_type.as_ref()
                            .map(|t| format!(" -> {}", format_type_expr(t)))
                            .unwrap_or_default();
                        format!("  fn {}({}){}", m.name, params.join(", "), ret)
                    })
                    .collect();
                let sig = format!("trait {} {{\n{}\n}}", trait_name, methods_str.join("\n"));
                return Some(Hover {
                    contents: HoverContents::Markup(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!("```tl\n{sig}\n```"),
                    }),
                    range: None,
                });
            }
            _ => {}
        }
    }

    None
}

fn format_type_expr(te: &tl_ast::TypeExpr) -> String {
    match te {
        tl_ast::TypeExpr::Named(name) => name.clone(),
        tl_ast::TypeExpr::Generic { name, args } => {
            let args_str: Vec<String> = args.iter().map(format_type_expr).collect();
            format!("{}<{}>", name, args_str.join(", "))
        }
        tl_ast::TypeExpr::Optional(inner) => format!("{}?", format_type_expr(inner)),
        tl_ast::TypeExpr::Function { params, return_type } => {
            let params_str: Vec<String> = params.iter().map(format_type_expr).collect();
            format!("fn({}) -> {}", params_str.join(", "), format_type_expr(return_type))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hover_on_variable() {
        let source = "let x: int = 42";
        let ast = tl_parser::parse(source).unwrap();
        let result = provide_hover(source, Some(&ast), None, Position::new(0, 4));
        assert!(result.is_some());
        let hover = result.unwrap();
        if let HoverContents::Markup(content) = &hover.contents {
            assert!(content.value.contains("int"), "Should show type 'int'");
        }
    }

    #[test]
    fn test_hover_on_function() {
        let source = "fn add(a: int, b: int) -> int { a + b }";
        let ast = tl_parser::parse(source).unwrap();
        let result = provide_hover(source, Some(&ast), None, Position::new(0, 4));
        assert!(result.is_some());
        let hover = result.unwrap();
        if let HoverContents::Markup(content) = &hover.contents {
            assert!(content.value.contains("fn add"), "Should show function signature");
            assert!(content.value.contains("int"), "Should show return type");
        }
    }

    #[test]
    fn test_hover_on_struct() {
        let source = "struct Point { x: int, y: int }";
        let ast = tl_parser::parse(source).unwrap();
        let result = provide_hover(source, Some(&ast), None, Position::new(0, 8));
        assert!(result.is_some());
        let hover = result.unwrap();
        if let HoverContents::Markup(content) = &hover.contents {
            assert!(content.value.contains("struct Point"), "Should show struct definition");
        }
    }

    #[test]
    fn test_hover_on_builtin() {
        let source = "print(42)";
        let ast = tl_parser::parse(source).unwrap();
        let result = provide_hover(source, Some(&ast), None, Position::new(0, 2));
        assert!(result.is_some());
        let hover = result.unwrap();
        if let HoverContents::Markup(content) = &hover.contents {
            assert!(content.value.contains("fn print"), "Should show builtin signature");
            assert!(content.value.contains("Builtin"), "Should indicate builtin");
        }
    }

    #[test]
    fn test_hover_on_unknown() {
        let source = "let x = 42";
        let ast = tl_parser::parse(source).unwrap();
        // Position on '42' (a number, not an ident)
        let result = provide_hover(source, Some(&ast), None, Position::new(0, 9));
        // Should return None for number literals
        assert!(result.is_none());
    }
}
