// ThinkingLanguage — LSP Completion Provider
// Context-aware code completions.

use lsp_types::{CompletionItem, CompletionItemKind, Position};
use tl_ast::Program;
use tl_types::checker::CheckResult;

use crate::ast_util::{self, DefKind};

/// Keywords in TL
const KEYWORDS: &[&str] = &[
    "let", "fn", "if", "else", "while", "for", "in", "return", "true", "false", "none", "struct",
    "enum", "impl", "try", "catch", "throw", "import", "test", "break", "continue", "and", "or",
    "not", "mut", "await", "yield", "match", "schema", "pipeline", "stream", "source", "sink",
    "use", "pub", "mod", "trait", "where", "agent", "finally", "async",
];

/// Builtin functions in TL
pub const BUILTINS: &[&str] = &[
    "print",
    "println",
    "len",
    "str",
    "int",
    "float",
    "abs",
    "min",
    "max",
    "range",
    "push",
    "type_of",
    "map",
    "filter",
    "reduce",
    "sum",
    "any",
    "all",
    "read_csv",
    "read_parquet",
    "write_csv",
    "write_parquet",
    "collect",
    "show",
    "describe",
    "head",
    "sqrt",
    "pow",
    "floor",
    "ceil",
    "round",
    "sin",
    "cos",
    "tan",
    "log",
    "log2",
    "log10",
    "join",
    "assert",
    "assert_eq",
    "json_parse",
    "json_stringify",
    "map_from",
    "read_file",
    "write_file",
    "append_file",
    "file_exists",
    "list_dir",
    "env_get",
    "env_set",
    "regex_match",
    "regex_find",
    "regex_replace",
    "now",
    "date_format",
    "date_parse",
    "zip",
    "enumerate",
    "bool",
    "spawn",
    "sleep",
    "channel",
    "send",
    "recv",
    "try_recv",
    "await_all",
    "pmap",
    "timeout",
    "next",
    "is_generator",
    "iter",
    "take",
    "skip",
    "gen_collect",
    "gen_map",
    "gen_filter",
    "chain",
    "gen_zip",
    "gen_enumerate",
    "Ok",
    "Err",
    "is_ok",
    "is_err",
    "unwrap",
    "set_from",
    "set_add",
    "set_remove",
    "set_contains",
    "set_union",
    "set_intersection",
    "set_difference",
    "random",
    "random_int",
    "sample",
    "exp",
    "is_nan",
    "is_infinite",
    "sign",
    "clamp",
    "today",
    "date_add",
    "date_diff",
    "date_trunc",
    "date_extract",
    "assert_table_eq",
    "run_agent",
    "stream_agent",
    "http_get",
    "http_post",
    "http_request",
    "embed",
];

/// String methods for dot-completion
const STRING_METHODS: &[&str] = &[
    "len",
    "split",
    "trim",
    "trim_start",
    "trim_end",
    "contains",
    "replace",
    "to_upper",
    "to_lower",
    "starts_with",
    "ends_with",
    "chars",
    "repeat",
    "index_of",
    "substring",
    "pad_left",
    "pad_right",
    "count",
    "is_empty",
    "is_numeric",
    "is_alpha",
    "strip_prefix",
    "strip_suffix",
];

/// List methods for dot-completion
const LIST_METHODS: &[&str] = &[
    "len", "push", "map", "filter", "sort", "reverse", "contains", "index_of", "slice", "flat_map",
    "reduce", "sum", "min", "max", "any", "all", "find", "sort_by", "group_by", "unique",
    "flatten", "chunk", "insert", "remove_at", "is_empty", "each", "zip", "join",
];

/// Map methods for dot-completion
const MAP_METHODS: &[&str] = &[
    "keys", "values", "contains_key", "remove", "len", "get", "merge", "entries",
    "map_values", "filter", "set", "is_empty",
];

/// Set methods for dot-completion
const SET_METHODS: &[&str] = &[
    "len",
    "add",
    "remove",
    "contains",
    "union",
    "intersection",
    "difference",
];

/// Generator methods for dot-completion
#[allow(dead_code)]
const GENERATOR_METHODS: &[&str] = &["next", "collect", "map", "filter", "take", "skip"];

pub fn provide_completions(
    source: &str,
    ast: Option<&Program>,
    _check_result: Option<&CheckResult>,
    position: Position,
) -> Vec<CompletionItem> {
    let offset = ast_util::position_to_offset(source, position.line, position.character);

    // Check if this is a dot-completion
    if offset > 0 && source.as_bytes().get(offset - 1) == Some(&b'.') {
        return provide_dot_completions(source, ast, offset);
    }

    let mut items = Vec::new();

    // Keywords
    for kw in KEYWORDS {
        items.push(CompletionItem {
            label: kw.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        });
    }

    // Builtins
    for bi in BUILTINS {
        items.push(CompletionItem {
            label: bi.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            ..Default::default()
        });
    }

    // Scope-aware names from AST
    if let Some(program) = ast {
        let defs = ast_util::collect_definitions(program);
        for (name, kind, span) in defs {
            if span.start > offset {
                continue; // only completions from before cursor
            }
            let lsp_kind = match kind {
                DefKind::Variable => CompletionItemKind::VARIABLE,
                DefKind::Function => CompletionItemKind::FUNCTION,
                DefKind::Struct => CompletionItemKind::STRUCT,
                DefKind::Enum => CompletionItemKind::ENUM,
                DefKind::Trait => CompletionItemKind::INTERFACE,
                DefKind::Schema => CompletionItemKind::STRUCT,
                DefKind::Pipeline => CompletionItemKind::FUNCTION,
                DefKind::Agent => CompletionItemKind::FUNCTION,
                DefKind::Test => CompletionItemKind::FUNCTION,
            };
            // Avoid duplicates with builtins
            if !items.iter().any(|it| it.label == name) {
                items.push(CompletionItem {
                    label: name,
                    kind: Some(lsp_kind),
                    ..Default::default()
                });
            }
        }

        // Parameters from enclosing function
        let params = ast_util::collect_params_at_offset(program, offset);
        for p in params {
            if !items.iter().any(|it| it.label == p) {
                items.push(CompletionItem {
                    label: p,
                    kind: Some(CompletionItemKind::VARIABLE),
                    ..Default::default()
                });
            }
        }
    }

    items
}

fn provide_dot_completions(
    source: &str,
    ast: Option<&Program>,
    dot_offset: usize,
) -> Vec<CompletionItem> {
    // Find what's before the dot
    let before_dot = &source[..dot_offset - 1];
    let trimmed = before_dot.trim_end();

    // Try to infer the type of the expression before the dot
    // Simple heuristic: look at the identifier and check known types
    if trimmed.len().checked_sub(0).is_some() {
        let ident = extract_trailing_ident(trimmed);

        if let Some(ref name) = ident {
            // Check if this is a string literal ending
            if trimmed.ends_with('"') {
                return methods_to_completions(STRING_METHODS);
            }

            // Check definitions to infer type
            if let Some(program) = ast {
                // First check: is the variable an instance of a struct?
                let mut struct_name_for_var: Option<String> = None;
                for stmt in &program.statements {
                    if let tl_ast::StmtKind::Let {
                        name: var_name,
                        type_ann,
                        value,
                        ..
                    } = &stmt.kind
                        && var_name == name
                    {
                        // Check type annotation
                        if let Some(ann) = type_ann {
                            return methods_for_type_expr(ann);
                        }
                        // Infer from value: StructInit
                        if let tl_ast::Expr::StructInit {
                            name: init_name, ..
                        } = value
                        {
                            struct_name_for_var = Some(init_name.clone());
                        } else {
                            return methods_for_expr(value);
                        }
                    }
                }

                // If we found a struct instance, look up struct fields
                if let Some(sname) = &struct_name_for_var {
                    for stmt in &program.statements {
                        if let tl_ast::StmtKind::StructDecl {
                            name: decl_name,
                            fields,
                            ..
                        } = &stmt.kind
                            && decl_name == sname
                        {
                            return fields
                                .iter()
                                .map(|f| CompletionItem {
                                    label: f.name.clone(),
                                    kind: Some(CompletionItemKind::FIELD),
                                    ..Default::default()
                                })
                                .collect();
                        }
                    }
                }

                // Check if this is a struct name directly
                for stmt in &program.statements {
                    if let tl_ast::StmtKind::StructDecl {
                        name: struct_name,
                        fields,
                        ..
                    } = &stmt.kind
                        && name == struct_name
                    {
                        return fields
                            .iter()
                            .map(|f| CompletionItem {
                                label: f.name.clone(),
                                kind: Some(CompletionItemKind::FIELD),
                                ..Default::default()
                            })
                            .collect();
                    }
                }
            }
        }
    }

    // Default: offer common methods from all types
    let mut items = Vec::new();
    items.extend(methods_to_completions(STRING_METHODS));
    items.extend(methods_to_completions(LIST_METHODS));
    items.extend(methods_to_completions(MAP_METHODS));
    items.dedup_by(|a, b| a.label == b.label);
    items
}

fn extract_trailing_ident(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1].is_ascii_alphanumeric() || bytes[end - 1] == b'_') {
        end -= 1;
    }
    if end < bytes.len() {
        Some(s[end..].to_string())
    } else {
        None
    }
}

fn methods_to_completions(methods: &[&str]) -> Vec<CompletionItem> {
    methods
        .iter()
        .map(|m| CompletionItem {
            label: m.to_string(),
            kind: Some(CompletionItemKind::METHOD),
            ..Default::default()
        })
        .collect()
}

fn methods_for_type_expr(type_expr: &tl_ast::TypeExpr) -> Vec<CompletionItem> {
    match type_expr {
        tl_ast::TypeExpr::Named(name) => match name.as_str() {
            "string" | "str" => methods_to_completions(STRING_METHODS),
            "list" => methods_to_completions(LIST_METHODS),
            "map" => methods_to_completions(MAP_METHODS),
            "set" => methods_to_completions(SET_METHODS),
            _ => vec![],
        },
        tl_ast::TypeExpr::Generic { name, .. } => match name.as_str() {
            "list" => methods_to_completions(LIST_METHODS),
            "map" => methods_to_completions(MAP_METHODS),
            "set" => methods_to_completions(SET_METHODS),
            _ => vec![],
        },
        _ => vec![],
    }
}

fn methods_for_expr(expr: &tl_ast::Expr) -> Vec<CompletionItem> {
    match expr {
        tl_ast::Expr::String(_) => methods_to_completions(STRING_METHODS),
        tl_ast::Expr::List(_) => methods_to_completions(LIST_METHODS),
        tl_ast::Expr::Map(_) => methods_to_completions(MAP_METHODS),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords_in_completions() {
        let items = provide_completions("", None, None, Position::new(0, 0));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"let"));
        assert!(labels.contains(&"fn"));
        assert!(labels.contains(&"struct"));
    }

    #[test]
    fn test_function_names_from_ast() {
        let source = "fn add(a, b) { a + b }\n";
        let ast = tl_parser::parse(source).unwrap();
        let items = provide_completions(source, Some(&ast), None, Position::new(1, 0));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"add"),
            "Function 'add' should appear in completions"
        );
    }

    #[test]
    fn test_struct_fields_after_dot() {
        // Parse without the trailing "p." since that causes parse error
        // The completion is triggered mid-typing, so we parse the valid part
        let valid_source = "struct Point { x: int, y: int }\nlet p = Point { x: 1, y: 2 }";
        let ast = tl_parser::parse(valid_source).ok();
        // Source with dot at end for position calculation
        let source = "struct Point { x: int, y: int }\nlet p = Point { x: 1, y: 2 }\np.";
        let items = provide_completions(source, ast.as_ref(), None, Position::new(2, 2));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"x"),
            "Struct field 'x' should appear after dot: {:?}",
            labels
        );
        assert!(
            labels.contains(&"y"),
            "Struct field 'y' should appear after dot"
        );
    }

    #[test]
    fn test_string_methods_after_dot() {
        let source = "let s: string = \"hello\"\ns.";
        let ast = tl_parser::parse(source).ok();
        let items = provide_completions(source, ast.as_ref(), None, Position::new(1, 2));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"len"), "String method 'len' should appear");
        assert!(
            labels.contains(&"split"),
            "String method 'split' should appear"
        );
    }

    #[test]
    fn test_completions_on_parse_error() {
        // File has a parse error, but we should still get keyword completions
        let source = "let x = \nfn ";
        let ast = tl_parser::parse(source).ok(); // will fail to parse
        assert!(ast.is_none());
        let items = provide_completions(source, ast.as_ref(), None, Position::new(1, 3));
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"fn"),
            "Keywords should still be available on parse error"
        );
    }
}
