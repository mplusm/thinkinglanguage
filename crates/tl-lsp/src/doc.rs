// ThinkingLanguage — Documentation Extraction & Generation
// Licensed under MIT OR Apache-2.0
//
// Extracts structured documentation from AST nodes with doc comments.
// Generates HTML, Markdown, and JSON output formats.

use serde::{Deserialize, Serialize};
use tl_ast::*;

// ── Data Structures ─────────────────────────────────────────────────────

/// Parsed documentation comment with tags extracted
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedDoc {
    /// First paragraph (summary line)
    pub summary: String,
    /// Full description text (without tags)
    pub description: String,
    /// `@param name description` entries
    pub params: Vec<(String, String)>,
    /// `@returns description`
    pub returns: Option<String>,
    /// `@example` code blocks
    pub examples: Vec<String>,
    /// `@deprecated reason`
    pub deprecated: Option<String>,
    /// `@version N` — schema version number
    pub version: Option<i64>,
    /// `@since field description` — per-field lifecycle metadata
    pub since: Vec<(String, String)>,
}

/// Kind of documented item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DocItemKind {
    Function,
    Struct,
    Enum,
    Trait,
    Schema,
    TypeAlias,
    Constant,
}

/// A documented function/method parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocParam {
    pub name: String,
    pub type_ann: Option<String>,
    pub description: Option<String>,
}

/// A documented struct/schema field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocField {
    pub name: String,
    pub type_ann: String,
}

/// A documented enum variant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocVariant {
    pub name: String,
    pub fields: Vec<String>,
}

/// A documented method (from impl blocks)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocMethod {
    pub name: String,
    pub params: Vec<DocParam>,
    pub return_type: Option<String>,
    pub doc: Option<String>,
}

/// A single documented item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocItem {
    pub name: String,
    pub kind: DocItemKind,
    pub doc: Option<ParsedDoc>,
    pub signature: String,
    pub is_public: bool,
    pub type_params: Vec<String>,
    pub params: Vec<DocParam>,
    pub return_type: Option<String>,
    pub fields: Vec<DocField>,
    pub variants: Vec<DocVariant>,
    pub methods: Vec<DocMethod>,
}

/// Documentation for an entire module/file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleDoc {
    pub module_doc: Option<ParsedDoc>,
    pub items: Vec<DocItem>,
    pub source_path: Option<String>,
}

// ── Doc Comment Parsing ─────────────────────────────────────────────────

/// Parse a raw doc comment string into structured ParsedDoc
pub fn parse_doc_comment(raw: &str) -> ParsedDoc {
    let mut description_lines = Vec::new();
    let mut params = Vec::new();
    let mut returns = None;
    let mut examples = Vec::new();
    let mut deprecated = None;
    let mut version = None;
    let mut since = Vec::new();
    let mut in_example = false;
    let mut current_example = Vec::new();

    for line in raw.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("@example") {
            // Finish any previous example
            if in_example && !current_example.is_empty() {
                examples.push(current_example.join("\n"));
                current_example.clear();
            }
            in_example = true;
            // If there's content after @example on the same line, start the example
            let after = trimmed.strip_prefix("@example").unwrap().trim();
            if !after.is_empty() {
                current_example.push(after.to_string());
            }
            continue;
        }

        if in_example {
            // Check if we hit another tag
            if trimmed.starts_with('@') {
                // End example block
                if !current_example.is_empty() {
                    examples.push(current_example.join("\n"));
                    current_example.clear();
                }
                in_example = false;
                // Fall through to process this tag
            } else {
                current_example.push(line.to_string());
                continue;
            }
        }

        if let Some(rest) = trimmed.strip_prefix("@param") {
            let rest = rest.trim();
            if let Some((name, desc)) = rest.split_once(char::is_whitespace) {
                params.push((name.trim().to_string(), desc.trim().to_string()));
            } else if !rest.is_empty() {
                params.push((rest.to_string(), String::new()));
            }
        } else if let Some(rest) = trimmed.strip_prefix("@returns") {
            returns = Some(rest.trim().to_string());
        } else if let Some(rest) = trimmed.strip_prefix("@return") {
            if returns.is_none() {
                returns = Some(rest.trim().to_string());
            }
        } else if let Some(rest) = trimmed.strip_prefix("@deprecated") {
            deprecated = Some(rest.trim().to_string());
        } else if let Some(rest) = trimmed.strip_prefix("@version") {
            if let std::result::Result::Ok(v) = rest.trim().parse::<i64>() {
                version = Some(v);
            }
        } else if let Some(rest) = trimmed.strip_prefix("@since") {
            let rest = rest.trim();
            if let Some((name, desc)) = rest.split_once(char::is_whitespace) {
                since.push((name.trim().to_string(), desc.trim().to_string()));
            } else if !rest.is_empty() {
                since.push((rest.to_string(), String::new()));
            }
        } else {
            description_lines.push(trimmed.to_string());
        }
    }

    // Finish any trailing example
    if in_example && !current_example.is_empty() {
        examples.push(current_example.join("\n"));
    }

    let description = description_lines.join("\n").trim().to_string();

    // Summary is the first paragraph (up to first blank line)
    let summary = description
        .split("\n\n")
        .next()
        .unwrap_or("")
        .replace('\n', " ")
        .trim()
        .to_string();

    ParsedDoc {
        summary,
        description,
        params,
        returns,
        examples,
        deprecated,
        version,
        since,
    }
}

// ── Type Expression Formatting ──────────────────────────────────────────

pub fn format_type_expr(te: &TypeExpr) -> String {
    match te {
        TypeExpr::Named(name) => name.clone(),
        TypeExpr::Generic { name, args } => {
            let args_str: Vec<String> = args.iter().map(format_type_expr).collect();
            format!("{}<{}>", name, args_str.join(", "))
        }
        TypeExpr::Optional(inner) => format!("{}?", format_type_expr(inner)),
        TypeExpr::Function {
            params,
            return_type,
        } => {
            let params_str: Vec<String> = params.iter().map(format_type_expr).collect();
            format!(
                "fn({}) -> {}",
                params_str.join(", "),
                format_type_expr(return_type)
            )
        }
    }
}

// ── AST Doc Extraction ──────────────────────────────────────────────────

/// Extract documentation from a parsed program
pub fn extract_docs(program: &Program, path: Option<&str>) -> ModuleDoc {
    let module_doc = program.module_doc.as_ref().map(|s| parse_doc_comment(s));

    let mut items = Vec::new();
    let mut impl_methods: Vec<(String, Vec<DocMethod>)> = Vec::new();

    // First pass: collect all items and impl blocks
    for stmt in &program.statements {
        match &stmt.kind {
            StmtKind::FnDecl {
                name,
                type_params,
                params,
                return_type,
                is_public,
                ..
            } => {
                let doc_params: Vec<DocParam> = params
                    .iter()
                    .map(|p| DocParam {
                        name: p.name.clone(),
                        type_ann: p.type_ann.as_ref().map(format_type_expr),
                        description: None,
                    })
                    .collect();

                let ret_str = return_type.as_ref().map(format_type_expr);

                let params_sig: Vec<String> = params
                    .iter()
                    .map(|p| {
                        if let Some(ann) = &p.type_ann {
                            format!("{}: {}", p.name, format_type_expr(ann))
                        } else {
                            p.name.clone()
                        }
                    })
                    .collect();
                let ret_sig = ret_str
                    .as_ref()
                    .map(|r| format!(" -> {}", r))
                    .unwrap_or_default();
                let tp_sig = if type_params.is_empty() {
                    String::new()
                } else {
                    format!("<{}>", type_params.join(", "))
                };
                let signature = format!(
                    "fn {}{}({}){}",
                    name,
                    tp_sig,
                    params_sig.join(", "),
                    ret_sig
                );

                let doc = stmt.doc_comment.as_ref().map(|s| {
                    let parsed = parse_doc_comment(s);
                    // Merge @param descriptions into doc_params
                    for (pname, pdesc) in &parsed.params {
                        // Will be applied below
                        let _ = (pname, pdesc);
                    }
                    parsed
                });

                // Apply @param descriptions
                let mut doc_params = doc_params;
                if let Some(ref d) = doc {
                    for dp in &mut doc_params {
                        if let Some((_, desc)) = d.params.iter().find(|(n, _)| n == &dp.name) {
                            dp.description = Some(desc.clone());
                        }
                    }
                }

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::Function,
                    doc,
                    signature,
                    is_public: *is_public,
                    type_params: type_params.clone(),
                    params: doc_params,
                    return_type: ret_str,
                    fields: Vec::new(),
                    variants: Vec::new(),
                    methods: Vec::new(),
                });
            }
            StmtKind::StructDecl {
                name,
                type_params,
                fields,
                is_public,
            } => {
                let doc_fields: Vec<DocField> = fields
                    .iter()
                    .map(|f| DocField {
                        name: f.name.clone(),
                        type_ann: format_type_expr(&f.type_ann),
                    })
                    .collect();

                let fields_sig: Vec<String> = fields
                    .iter()
                    .map(|f| format!("  {}: {}", f.name, format_type_expr(&f.type_ann)))
                    .collect();
                let tp_sig = if type_params.is_empty() {
                    String::new()
                } else {
                    format!("<{}>", type_params.join(", "))
                };
                let signature = format!(
                    "struct {}{} {{\n{}\n}}",
                    name,
                    tp_sig,
                    fields_sig.join(",\n")
                );

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::Struct,
                    doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                    signature,
                    is_public: *is_public,
                    type_params: type_params.clone(),
                    params: Vec::new(),
                    return_type: None,
                    fields: doc_fields,
                    variants: Vec::new(),
                    methods: Vec::new(),
                });
            }
            StmtKind::EnumDecl {
                name,
                type_params,
                variants,
                is_public,
            } => {
                let doc_variants: Vec<DocVariant> = variants
                    .iter()
                    .map(|v| DocVariant {
                        name: v.name.clone(),
                        fields: v.fields.iter().map(format_type_expr).collect(),
                    })
                    .collect();

                let variants_sig: Vec<String> = variants
                    .iter()
                    .map(|v| {
                        if v.fields.is_empty() {
                            format!("  {}", v.name)
                        } else {
                            let fields: Vec<String> =
                                v.fields.iter().map(format_type_expr).collect();
                            format!("  {}({})", v.name, fields.join(", "))
                        }
                    })
                    .collect();
                let tp_sig = if type_params.is_empty() {
                    String::new()
                } else {
                    format!("<{}>", type_params.join(", "))
                };
                let signature = format!(
                    "enum {}{} {{\n{}\n}}",
                    name,
                    tp_sig,
                    variants_sig.join(",\n")
                );

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::Enum,
                    doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                    signature,
                    is_public: *is_public,
                    type_params: type_params.clone(),
                    params: Vec::new(),
                    return_type: None,
                    fields: Vec::new(),
                    variants: doc_variants,
                    methods: Vec::new(),
                });
            }
            StmtKind::TraitDef {
                name,
                type_params,
                methods,
                is_public,
            } => {
                let methods_sig: Vec<String> = methods
                    .iter()
                    .map(|m| {
                        let params: Vec<String> = m
                            .params
                            .iter()
                            .map(|p| {
                                if let Some(ann) = &p.type_ann {
                                    format!("{}: {}", p.name, format_type_expr(ann))
                                } else {
                                    p.name.clone()
                                }
                            })
                            .collect();
                        let ret = m
                            .return_type
                            .as_ref()
                            .map(|t| format!(" -> {}", format_type_expr(t)))
                            .unwrap_or_default();
                        format!("  fn {}({}){}", m.name, params.join(", "), ret)
                    })
                    .collect();

                let tp_sig = if type_params.is_empty() {
                    String::new()
                } else {
                    format!("<{}>", type_params.join(", "))
                };
                let signature = format!(
                    "trait {}{} {{\n{}\n}}",
                    name,
                    tp_sig,
                    methods_sig.join("\n")
                );

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::Trait,
                    doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                    signature,
                    is_public: *is_public,
                    type_params: type_params.clone(),
                    params: Vec::new(),
                    return_type: None,
                    fields: Vec::new(),
                    variants: Vec::new(),
                    methods: Vec::new(),
                });
            }
            StmtKind::Schema {
                name,
                fields,
                is_public,
                ..
            } => {
                let doc_fields: Vec<DocField> = fields
                    .iter()
                    .map(|f| DocField {
                        name: f.name.clone(),
                        type_ann: format_type_expr(&f.type_ann),
                    })
                    .collect();

                let fields_sig: Vec<String> = fields
                    .iter()
                    .map(|f| format!("  {}: {}", f.name, format_type_expr(&f.type_ann)))
                    .collect();
                let signature = format!("schema {} {{\n{}\n}}", name, fields_sig.join(",\n"));

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::Schema,
                    doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                    signature,
                    is_public: *is_public,
                    type_params: Vec::new(),
                    params: Vec::new(),
                    return_type: None,
                    fields: doc_fields,
                    variants: Vec::new(),
                    methods: Vec::new(),
                });
            }
            StmtKind::TypeAlias {
                name,
                type_params,
                value,
                is_public,
            } => {
                let tp_sig = if type_params.is_empty() {
                    String::new()
                } else {
                    format!("<{}>", type_params.join(", "))
                };
                let signature = format!("type {}{} = {}", name, tp_sig, format_type_expr(value));

                items.push(DocItem {
                    name: name.clone(),
                    kind: DocItemKind::TypeAlias,
                    doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                    signature,
                    is_public: *is_public,
                    type_params: type_params.clone(),
                    params: Vec::new(),
                    return_type: None,
                    fields: Vec::new(),
                    variants: Vec::new(),
                    methods: Vec::new(),
                });
            }
            StmtKind::Let {
                name,
                type_ann,
                is_public,
                mutable,
                ..
            } if !mutable => {
                let type_str = type_ann
                    .as_ref()
                    .map(format_type_expr)
                    .unwrap_or_else(|| "any".to_string());
                let signature = format!("let {}: {}", name, type_str);

                if stmt.doc_comment.is_some() {
                    items.push(DocItem {
                        name: name.clone(),
                        kind: DocItemKind::Constant,
                        doc: stmt.doc_comment.as_ref().map(|s| parse_doc_comment(s)),
                        signature,
                        is_public: *is_public,
                        type_params: Vec::new(),
                        params: Vec::new(),
                        return_type: None,
                        fields: Vec::new(),
                        variants: Vec::new(),
                        methods: Vec::new(),
                    });
                }
            }
            StmtKind::ImplBlock {
                type_name, methods, ..
            } => {
                let mut doc_methods = Vec::new();
                for method in methods {
                    if let StmtKind::FnDecl {
                        name,
                        params,
                        return_type,
                        ..
                    } = &method.kind
                    {
                        let method_params: Vec<DocParam> = params
                            .iter()
                            .map(|p| DocParam {
                                name: p.name.clone(),
                                type_ann: p.type_ann.as_ref().map(format_type_expr),
                                description: None,
                            })
                            .collect();
                        doc_methods.push(DocMethod {
                            name: name.clone(),
                            params: method_params,
                            return_type: return_type.as_ref().map(format_type_expr),
                            doc: method.doc_comment.clone(),
                        });
                    }
                }
                impl_methods.push((type_name.clone(), doc_methods));
            }
            _ => {}
        }
    }

    // Second pass: attach impl methods to their parent structs/enums
    for (type_name, methods) in impl_methods {
        if let Some(item) = items.iter_mut().find(|i| i.name == type_name) {
            item.methods.extend(methods);
        }
    }

    ModuleDoc {
        module_doc,
        items,
        source_path: path.map(|s| s.to_string()),
    }
}

/// Extract docs filtering to public items only
pub fn extract_public_docs(program: &Program, path: Option<&str>) -> ModuleDoc {
    let mut docs = extract_docs(program, path);
    docs.items.retain(|item| item.is_public);
    docs
}

// ── HTML Generation ─────────────────────────────────────────────────────

/// Generate standalone HTML documentation from a ModuleDoc
pub fn generate_html(module: &ModuleDoc) -> String {
    let title = module
        .source_path
        .as_deref()
        .unwrap_or("Module Documentation");
    let mut html = String::new();

    html.push_str(&format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 0; color: #1a1a1a; }}
.container {{ display: flex; max-width: 1200px; margin: 0 auto; }}
.sidebar {{ width: 250px; padding: 20px; border-right: 1px solid #e0e0e0; position: sticky; top: 0; height: 100vh; overflow-y: auto; }}
.sidebar h2 {{ font-size: 14px; text-transform: uppercase; color: #666; margin-top: 20px; }}
.sidebar a {{ display: block; padding: 4px 0; color: #0366d6; text-decoration: none; font-size: 14px; }}
.sidebar a:hover {{ text-decoration: underline; }}
.content {{ flex: 1; padding: 20px 40px; max-width: 800px; }}
h1 {{ border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }}
.item {{ margin-bottom: 40px; padding-top: 10px; border-top: 1px solid #f0f0f0; }}
.item h3 {{ margin-bottom: 5px; }}
.item h3 a {{ color: inherit; text-decoration: none; }}
pre {{ background: #f6f8fa; border: 1px solid #e0e0e0; border-radius: 6px; padding: 16px; overflow-x: auto; font-size: 14px; }}
code {{ font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; }}
.deprecated {{ background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; padding: 8px 12px; margin: 8px 0; }}
.params-table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
.params-table th, .params-table td {{ border: 1px solid #e0e0e0; padding: 8px 12px; text-align: left; }}
.params-table th {{ background: #f6f8fa; }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 600; margin-left: 8px; }}
.badge-pub {{ background: #28a745; color: white; }}
.badge-deprecated {{ background: #ffc107; color: #1a1a1a; }}
.example {{ background: #f0f7ff; border: 1px solid #c8e1ff; border-radius: 6px; padding: 12px; margin: 8px 0; }}
@media (max-width: 768px) {{ .container {{ flex-direction: column; }} .sidebar {{ width: 100%; height: auto; position: static; border-right: none; border-bottom: 1px solid #e0e0e0; }} }}
</style>
</head>
<body>
<div class="container">
<nav class="sidebar">
<h1>TL Docs</h1>
"#));

    // Sidebar navigation
    let kinds = [
        ("Functions", DocItemKind::Function),
        ("Structs", DocItemKind::Struct),
        ("Enums", DocItemKind::Enum),
        ("Traits", DocItemKind::Trait),
        ("Schemas", DocItemKind::Schema),
        ("Type Aliases", DocItemKind::TypeAlias),
        ("Constants", DocItemKind::Constant),
    ];

    for (label, kind) in &kinds {
        let matching: Vec<&DocItem> = module
            .items
            .iter()
            .filter(|i| std::mem::discriminant(&i.kind) == std::mem::discriminant(kind))
            .collect();
        if !matching.is_empty() {
            html.push_str(&format!("<h2>{label}</h2>\n"));
            for item in &matching {
                html.push_str(&format!("<a href=\"#{}\">{}</a>\n", item.name, item.name));
            }
        }
    }

    html.push_str("</nav>\n<main class=\"content\">\n");

    // Module-level documentation
    if let Some(doc) = &module.module_doc {
        html.push_str(&format!("<h1>{title}</h1>\n"));
        html.push_str(&format!("<p>{}</p>\n", html_escape(&doc.description)));
    } else {
        html.push_str(&format!("<h1>{title}</h1>\n"));
    }

    // Items
    for item in &module.items {
        html.push_str(&format!("<div class=\"item\" id=\"{}\">\n", item.name));
        html.push_str(&format!(
            "<h3><a href=\"#{}\"><code>{}</code></a>",
            item.name, item.name
        ));
        if item.is_public {
            html.push_str("<span class=\"badge badge-pub\">pub</span>");
        }
        if let Some(ref doc) = item.doc
            && doc.deprecated.is_some()
        {
            html.push_str("<span class=\"badge badge-deprecated\">deprecated</span>");
        }
        html.push_str("</h3>\n");

        // Signature
        html.push_str(&format!(
            "<pre><code>{}</code></pre>\n",
            html_escape(&item.signature)
        ));

        // Documentation
        if let Some(ref doc) = item.doc {
            if !doc.description.is_empty() {
                html.push_str(&format!("<p>{}</p>\n", html_escape(&doc.description)));
            }
            if let Some(ref dep) = doc.deprecated {
                html.push_str(&format!(
                    "<div class=\"deprecated\"><strong>Deprecated:</strong> {}</div>\n",
                    html_escape(dep)
                ));
            }
        }

        // Parameters table
        if !item.params.is_empty() {
            html.push_str("<h4>Parameters</h4>\n<table class=\"params-table\">\n<tr><th>Name</th><th>Type</th><th>Description</th></tr>\n");
            for p in &item.params {
                let type_str = p.type_ann.as_deref().unwrap_or("any");
                let desc_str = p.description.as_deref().unwrap_or("");
                html.push_str(&format!(
                    "<tr><td><code>{}</code></td><td><code>{}</code></td><td>{}</td></tr>\n",
                    html_escape(&p.name),
                    html_escape(type_str),
                    html_escape(desc_str)
                ));
            }
            html.push_str("</table>\n");
        }

        // Return type
        if let Some(ref ret) = item.return_type {
            html.push_str(&format!(
                "<h4>Returns</h4>\n<p><code>{}</code>",
                html_escape(ret)
            ));
            if let Some(ref doc) = item.doc
                && let Some(ref returns_desc) = doc.returns
            {
                html.push_str(&format!(" — {}", html_escape(returns_desc)));
            }
            html.push_str("</p>\n");
        }

        // Fields
        if !item.fields.is_empty() {
            html.push_str("<h4>Fields</h4>\n<table class=\"params-table\">\n<tr><th>Name</th><th>Type</th></tr>\n");
            for f in &item.fields {
                html.push_str(&format!(
                    "<tr><td><code>{}</code></td><td><code>{}</code></td></tr>\n",
                    html_escape(&f.name),
                    html_escape(&f.type_ann)
                ));
            }
            html.push_str("</table>\n");
        }

        // Variants
        if !item.variants.is_empty() {
            html.push_str("<h4>Variants</h4>\n<ul>\n");
            for v in &item.variants {
                if v.fields.is_empty() {
                    html.push_str(&format!("<li><code>{}</code></li>\n", html_escape(&v.name)));
                } else {
                    html.push_str(&format!(
                        "<li><code>{}({})</code></li>\n",
                        html_escape(&v.name),
                        v.fields
                            .iter()
                            .map(|f| html_escape(f))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            }
            html.push_str("</ul>\n");
        }

        // Methods
        if !item.methods.is_empty() {
            html.push_str("<h4>Methods</h4>\n");
            for m in &item.methods {
                let params_str: Vec<String> = m
                    .params
                    .iter()
                    .map(|p| {
                        if let Some(ref t) = p.type_ann {
                            format!("{}: {}", p.name, t)
                        } else {
                            p.name.clone()
                        }
                    })
                    .collect();
                let ret_str = m
                    .return_type
                    .as_ref()
                    .map(|r| format!(" -> {}", r))
                    .unwrap_or_default();
                html.push_str(&format!(
                    "<pre><code>fn {}({}){}</code></pre>\n",
                    html_escape(&m.name),
                    html_escape(&params_str.join(", ")),
                    html_escape(&ret_str)
                ));
                if let Some(ref doc) = m.doc {
                    html.push_str(&format!("<p>{}</p>\n", html_escape(doc)));
                }
            }
        }

        // Examples
        if let Some(ref doc) = item.doc
            && !doc.examples.is_empty()
        {
            html.push_str("<h4>Examples</h4>\n");
            for ex in &doc.examples {
                html.push_str(&format!(
                    "<div class=\"example\"><pre><code>{}</code></pre></div>\n",
                    html_escape(ex)
                ));
            }
        }

        html.push_str("</div>\n");
    }

    html.push_str("</main>\n</div>\n</body>\n</html>");
    html
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ── Markdown Generation ─────────────────────────────────────────────────

/// Generate Markdown documentation from a ModuleDoc
pub fn generate_markdown(module: &ModuleDoc) -> String {
    let mut md = String::new();
    let title = module
        .source_path
        .as_deref()
        .unwrap_or("Module Documentation");

    md.push_str(&format!("# {title}\n\n"));

    // Module doc
    if let Some(ref doc) = module.module_doc {
        md.push_str(&doc.description);
        md.push_str("\n\n");
    }

    // Table of contents
    if !module.items.is_empty() {
        md.push_str("## Table of Contents\n\n");
        for item in &module.items {
            let kind_str = match item.kind {
                DocItemKind::Function => "fn",
                DocItemKind::Struct => "struct",
                DocItemKind::Enum => "enum",
                DocItemKind::Trait => "trait",
                DocItemKind::Schema => "schema",
                DocItemKind::TypeAlias => "type",
                DocItemKind::Constant => "const",
            };
            let anchor = item.name.to_lowercase();
            md.push_str(&format!("- [{} `{}`](#{})\n", kind_str, item.name, anchor));
        }
        md.push('\n');
    }

    // Items
    for item in &module.items {
        md.push_str(&format!("### {}\n\n", item.name));
        md.push_str(&format!("```tl\n{}\n```\n\n", item.signature));

        if let Some(ref doc) = item.doc {
            if let Some(ref dep) = doc.deprecated {
                md.push_str(&format!("> **Deprecated:** {}\n\n", dep));
            }
            if !doc.description.is_empty() {
                md.push_str(&doc.description);
                md.push_str("\n\n");
            }
        }

        // Parameters
        if !item.params.is_empty() {
            md.push_str("**Parameters:**\n\n");
            md.push_str("| Name | Type | Description |\n");
            md.push_str("|------|------|-------------|\n");
            for p in &item.params {
                let type_str = p.type_ann.as_deref().unwrap_or("any");
                let desc_str = p.description.as_deref().unwrap_or("");
                md.push_str(&format!(
                    "| `{}` | `{}` | {} |\n",
                    p.name, type_str, desc_str
                ));
            }
            md.push('\n');
        }

        // Return type
        if let Some(ref ret) = item.return_type {
            md.push_str(&format!("**Returns:** `{}`", ret));
            if let Some(ref doc) = item.doc
                && let Some(ref returns_desc) = doc.returns
            {
                md.push_str(&format!(" — {}", returns_desc));
            }
            md.push_str("\n\n");
        }

        // Fields
        if !item.fields.is_empty() {
            md.push_str("**Fields:**\n\n");
            md.push_str("| Name | Type |\n");
            md.push_str("|------|------|\n");
            for f in &item.fields {
                md.push_str(&format!("| `{}` | `{}` |\n", f.name, f.type_ann));
            }
            md.push('\n');
        }

        // Variants
        if !item.variants.is_empty() {
            md.push_str("**Variants:**\n\n");
            for v in &item.variants {
                if v.fields.is_empty() {
                    md.push_str(&format!("- `{}`\n", v.name));
                } else {
                    md.push_str(&format!("- `{}({})`\n", v.name, v.fields.join(", ")));
                }
            }
            md.push('\n');
        }

        // Methods
        if !item.methods.is_empty() {
            md.push_str("**Methods:**\n\n");
            for m in &item.methods {
                let params_str: Vec<String> = m
                    .params
                    .iter()
                    .map(|p| {
                        if let Some(ref t) = p.type_ann {
                            format!("{}: {}", p.name, t)
                        } else {
                            p.name.clone()
                        }
                    })
                    .collect();
                let ret_str = m
                    .return_type
                    .as_ref()
                    .map(|r| format!(" -> {}", r))
                    .unwrap_or_default();
                md.push_str(&format!(
                    "- `fn {}({}){}`",
                    m.name,
                    params_str.join(", "),
                    ret_str
                ));
                if let Some(ref doc) = m.doc {
                    md.push_str(&format!(" — {}", doc));
                }
                md.push('\n');
            }
            md.push('\n');
        }

        // Examples
        if let Some(ref doc) = item.doc
            && !doc.examples.is_empty()
        {
            md.push_str("**Examples:**\n\n");
            for ex in &doc.examples {
                md.push_str("```tl\n");
                md.push_str(ex);
                md.push_str("\n```\n\n");
            }
        }
    }

    md
}

// ── JSON Generation ─────────────────────────────────────────────────────

/// Generate JSON documentation from a ModuleDoc
pub fn generate_json(module: &ModuleDoc) -> String {
    serde_json::to_string_pretty(module).unwrap_or_else(|_| "{}".to_string())
}

// ── Cross-reference Linking ─────────────────────────────────────────────

/// Replace known item names in text with HTML anchor links
pub fn linkify_types(text: &str, known_names: &[String]) -> String {
    let mut result = text.to_string();
    for name in known_names {
        // Only replace whole words (not inside other words)
        let pattern = format!("`{}`", name);
        let replacement = format!("`<a href=\"#{}\">{}</a>`", name, name);
        result = result.replace(&pattern, &replacement);
    }
    result
}

// ── Project-Level Documentation ─────────────────────────────────────────

/// Documentation for an entire project (multiple modules)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDoc {
    pub modules: Vec<ModuleDoc>,
}

/// Generate project-level HTML documentation (index + per-module sections)
pub fn generate_project_html(project: &ProjectDoc) -> String {
    let mut html = String::new();
    html.push_str(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Project Documentation</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 0; color: #1a1a1a; }
.container { display: flex; max-width: 1200px; margin: 0 auto; }
.sidebar { width: 250px; padding: 20px; border-right: 1px solid #e0e0e0; position: sticky; top: 0; height: 100vh; overflow-y: auto; }
.sidebar h2 { font-size: 14px; text-transform: uppercase; color: #666; margin-top: 20px; }
.sidebar a { display: block; padding: 4px 0; color: #0366d6; text-decoration: none; font-size: 14px; }
.sidebar a:hover { text-decoration: underline; }
.content { flex: 1; padding: 20px 40px; max-width: 800px; }
h1 { border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }
h2 { border-bottom: 1px solid #e0e0e0; padding-bottom: 5px; margin-top: 30px; }
.item { margin-bottom: 30px; padding-top: 10px; border-top: 1px solid #f0f0f0; }
pre { background: #f6f8fa; border: 1px solid #e0e0e0; border-radius: 6px; padding: 16px; overflow-x: auto; font-size: 14px; }
code { font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; }
.params-table { border-collapse: collapse; width: 100%; margin: 10px 0; }
.params-table th, .params-table td { border: 1px solid #e0e0e0; padding: 8px 12px; text-align: left; }
.params-table th { background: #f6f8fa; }
.deprecated { background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; padding: 8px 12px; margin: 8px 0; }
@media (max-width: 768px) { .container { flex-direction: column; } .sidebar { width: 100%; height: auto; position: static; } }
</style>
</head>
<body>
<div class="container">
<nav class="sidebar">
<h1>Project Docs</h1>
"#);

    // Sidebar: list modules
    html.push_str("<h2>Modules</h2>\n");
    for module in &project.modules {
        let name = module.source_path.as_deref().unwrap_or("unknown");
        let anchor = name.replace(['/', '\\', '.'], "_");
        html.push_str(&format!(
            "<a href=\"#mod_{}\">{}</a>\n",
            anchor,
            html_escape(name)
        ));
    }

    html.push_str("</nav>\n<main class=\"content\">\n<h1>Project Documentation</h1>\n");

    // Collect all known names for cross-referencing
    let known_names: Vec<String> = project
        .modules
        .iter()
        .flat_map(|m| m.items.iter().map(|i| i.name.clone()))
        .collect();

    for module in &project.modules {
        let name = module.source_path.as_deref().unwrap_or("unknown");
        let anchor = name.replace(['/', '\\', '.'], "_");
        html.push_str(&format!(
            "<h2 id=\"mod_{}\">{}</h2>\n",
            anchor,
            html_escape(name)
        ));

        if let Some(ref doc) = module.module_doc {
            let desc = linkify_types(&html_escape(&doc.description), &known_names);
            html.push_str(&format!("<p>{}</p>\n", desc));
        }

        for item in &module.items {
            html.push_str(&format!("<div class=\"item\" id=\"{}\">\n", item.name));
            html.push_str(&format!(
                "<h3><code>{}</code></h3>\n",
                html_escape(&item.name)
            ));
            html.push_str(&format!(
                "<pre><code>{}</code></pre>\n",
                html_escape(&item.signature)
            ));
            if let Some(ref doc) = item.doc
                && !doc.description.is_empty()
            {
                let desc = linkify_types(&html_escape(&doc.description), &known_names);
                html.push_str(&format!("<p>{}</p>\n", desc));
            }
            html.push_str("</div>\n");
        }
    }

    html.push_str("</main>\n</div>\n</body>\n</html>");
    html
}

/// Generate project-level Markdown documentation
pub fn generate_project_markdown(project: &ProjectDoc) -> String {
    let mut md = String::new();
    md.push_str("# Project Documentation\n\n");

    // TOC
    md.push_str("## Modules\n\n");
    for module in &project.modules {
        let name = module.source_path.as_deref().unwrap_or("unknown");
        let anchor = name.replace(['/', '\\', '.'], "-").to_lowercase();
        md.push_str(&format!("- [{}](#{})\n", name, anchor));
    }
    md.push('\n');

    for module in &project.modules {
        let name = module.source_path.as_deref().unwrap_or("unknown");
        md.push_str(&format!("## {}\n\n", name));
        if let Some(ref doc) = module.module_doc {
            md.push_str(&doc.description);
            md.push_str("\n\n");
        }
        for item in &module.items {
            md.push_str(&format!("### {}\n\n", item.name));
            md.push_str(&format!("```tl\n{}\n```\n\n", item.signature));
            if let Some(ref doc) = item.doc
                && !doc.description.is_empty()
            {
                md.push_str(&doc.description);
                md.push_str("\n\n");
            }
        }
    }

    md
}

/// Generate project-level JSON documentation
pub fn generate_project_json(project: &ProjectDoc) -> String {
    serde_json::to_string_pretty(project).unwrap_or_else(|_| "{}".to_string())
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_source(source: &str) -> Program {
        tl_parser::parse(source).unwrap()
    }

    // -- parse_doc_comment tests --

    #[test]
    fn test_parse_doc_simple() {
        let doc = parse_doc_comment("Adds two numbers together");
        assert_eq!(doc.summary, "Adds two numbers together");
        assert_eq!(doc.description, "Adds two numbers together");
    }

    #[test]
    fn test_parse_doc_with_params() {
        let doc = parse_doc_comment(
            "Adds numbers\n@param a The first number\n@param b The second number",
        );
        assert_eq!(doc.summary, "Adds numbers");
        assert_eq!(doc.params.len(), 2);
        assert_eq!(
            doc.params[0],
            ("a".to_string(), "The first number".to_string())
        );
        assert_eq!(
            doc.params[1],
            ("b".to_string(), "The second number".to_string())
        );
    }

    #[test]
    fn test_parse_doc_with_returns() {
        let doc = parse_doc_comment("Adds numbers\n@returns The sum");
        assert_eq!(doc.returns.as_deref(), Some("The sum"));
    }

    #[test]
    fn test_parse_doc_with_example() {
        let doc = parse_doc_comment("Adds numbers\n@example\nlet x = add(1, 2)");
        assert_eq!(doc.examples.len(), 1);
        assert_eq!(doc.examples[0], "let x = add(1, 2)");
    }

    #[test]
    fn test_parse_doc_with_deprecated() {
        let doc = parse_doc_comment("Old function\n@deprecated Use new_fn instead");
        assert_eq!(doc.deprecated.as_deref(), Some("Use new_fn instead"));
    }

    #[test]
    fn test_parse_doc_summary_vs_description() {
        let doc = parse_doc_comment("First paragraph.\n\nSecond paragraph with details.");
        assert_eq!(doc.summary, "First paragraph.");
        assert!(doc.description.contains("Second paragraph"));
    }

    // -- extract_docs tests --

    #[test]
    fn test_extract_fn_doc() {
        let program = parse_source(
            "/// Adds two numbers\n/// @param a First number\n/// @param b Second number\n/// @returns The sum\nfn add(a: int, b: int) -> int { a + b }",
        );
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        let item = &docs.items[0];
        assert_eq!(item.name, "add");
        assert!(matches!(item.kind, DocItemKind::Function));
        assert!(item.signature.contains("fn add"));
        let doc = item.doc.as_ref().unwrap();
        assert_eq!(doc.summary, "Adds two numbers");
        assert_eq!(doc.params.len(), 2);
        assert_eq!(doc.returns.as_deref(), Some("The sum"));
        // Check param descriptions were merged
        assert_eq!(item.params[0].description.as_deref(), Some("First number"));
    }

    #[test]
    fn test_extract_struct_doc() {
        let program = parse_source("/// A 2D point\nstruct Point { x: int, y: int }");
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        let item = &docs.items[0];
        assert_eq!(item.name, "Point");
        assert!(matches!(item.kind, DocItemKind::Struct));
        assert_eq!(item.fields.len(), 2);
        assert_eq!(item.fields[0].name, "x");
    }

    #[test]
    fn test_extract_enum_doc() {
        let program = parse_source("/// Color values\nenum Color { Red, Green, Blue }");
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        let item = &docs.items[0];
        assert!(matches!(item.kind, DocItemKind::Enum));
        assert_eq!(item.variants.len(), 3);
        assert_eq!(item.variants[0].name, "Red");
    }

    #[test]
    fn test_extract_trait_doc() {
        let program = parse_source("/// Display trait\ntrait Display { fn show(self) -> string }");
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        assert!(matches!(docs.items[0].kind, DocItemKind::Trait));
    }

    #[test]
    fn test_extract_schema_doc() {
        let program = parse_source("/// User schema\nschema User { name: string, age: int }");
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        assert!(matches!(docs.items[0].kind, DocItemKind::Schema));
        assert_eq!(docs.items[0].fields.len(), 2);
    }

    #[test]
    fn test_extract_type_alias_doc() {
        let program = parse_source("/// A mapper function\ntype Mapper = fn(int) -> int");
        let docs = extract_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        assert!(matches!(docs.items[0].kind, DocItemKind::TypeAlias));
        assert!(docs.items[0].signature.contains("type Mapper"));
    }

    #[test]
    fn test_extract_module_doc() {
        let program =
            parse_source("//! This module provides math utilities\nfn add(a, b) { a + b }");
        let docs = extract_docs(&program, None);
        assert!(docs.module_doc.is_some());
        assert_eq!(
            docs.module_doc.as_ref().unwrap().summary,
            "This module provides math utilities"
        );
    }

    #[test]
    fn test_extract_impl_methods() {
        let source = "struct Point { x: int, y: int }\nimpl Point {\n/// Returns the origin\nfn origin() -> Point { Point { x: 0, y: 0 } }\n}";
        let program = parse_source(source);
        let docs = extract_docs(&program, None);
        let point = docs.items.iter().find(|i| i.name == "Point").unwrap();
        assert_eq!(point.methods.len(), 1);
        assert_eq!(point.methods[0].name, "origin");
        assert!(point.methods[0].doc.is_some());
    }

    #[test]
    fn test_extract_public_only() {
        let source = "/// Public fn\npub fn greet() {}\n/// Private fn\nfn helper() {}";
        let program = parse_source(source);
        let docs = extract_public_docs(&program, None);
        assert_eq!(docs.items.len(), 1);
        assert_eq!(docs.items[0].name, "greet");
    }

    // -- HTML generation tests --

    fn sample_module() -> ModuleDoc {
        let source = "//! Math utilities\n/// Adds two numbers\n/// @param a First\n/// @param b Second\n/// @returns The sum\n/// @example\n/// add(1, 2)\nfn add(a: int, b: int) -> int { a + b }\n/// A point\nstruct Point { x: int, y: int }\n/// Colors\nenum Color { Red, Green, Blue }\n/// Old fn\n/// @deprecated Use new_fn\nfn old_fn() {}";
        let program = parse_source(source);
        extract_docs(&program, Some("math.tl"))
    }

    #[test]
    fn test_html_contains_title() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("math.tl"),
            "HTML should contain source path as title"
        );
    }

    #[test]
    fn test_html_contains_sidebar() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("class=\"sidebar\""),
            "HTML should have sidebar"
        );
        assert!(html.contains("href=\"#add\""), "Sidebar should link to add");
        assert!(
            html.contains("href=\"#Point\""),
            "Sidebar should link to Point"
        );
    }

    #[test]
    fn test_html_contains_signatures() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("fn add"),
            "HTML should contain function signature"
        );
        assert!(
            html.contains("struct Point"),
            "HTML should contain struct signature"
        );
    }

    #[test]
    fn test_html_contains_fields() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("<code>x</code>"),
            "HTML should contain field names"
        );
    }

    #[test]
    fn test_html_contains_variants() {
        let html = generate_html(&sample_module());
        assert!(html.contains("Red"), "HTML should contain enum variants");
        assert!(html.contains("Green"));
    }

    #[test]
    fn test_html_contains_example() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("add(1, 2)"),
            "HTML should contain example code"
        );
    }

    #[test]
    fn test_html_contains_deprecated() {
        let html = generate_html(&sample_module());
        assert!(
            html.contains("Deprecated"),
            "HTML should show deprecated notice"
        );
        assert!(
            html.contains("Use new_fn"),
            "HTML should show deprecation reason"
        );
    }

    // -- Markdown generation tests --

    #[test]
    fn test_markdown_headers() {
        let md = generate_markdown(&sample_module());
        assert!(md.contains("# math.tl"), "MD should have title header");
        assert!(md.contains("### add"), "MD should have item headers");
    }

    #[test]
    fn test_markdown_code_fences() {
        let md = generate_markdown(&sample_module());
        assert!(
            md.contains("```tl\nfn add"),
            "MD should have fenced code blocks"
        );
    }

    #[test]
    fn test_markdown_param_table() {
        let md = generate_markdown(&sample_module());
        assert!(
            md.contains("| `a` | `int` |"),
            "MD should have parameter table"
        );
    }

    // -- JSON generation tests --

    #[test]
    fn test_json_valid_structure() {
        let json = generate_json(&sample_module());
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON should be valid");
        assert!(
            parsed.get("items").is_some(),
            "JSON should have items array"
        );
        assert!(
            parsed.get("module_doc").is_some(),
            "JSON should have module_doc"
        );
    }

    #[test]
    fn test_json_deserializable() {
        let json = generate_json(&sample_module());
        let _: ModuleDoc =
            serde_json::from_str(&json).expect("JSON should deserialize to ModuleDoc");
    }

    #[test]
    fn test_public_only_filter() {
        let source = "/// Public\npub fn greet() {}\n/// Private\nfn helper() {}";
        let program = parse_source(source);
        let docs = extract_public_docs(&program, None);
        let html = generate_html(&docs);
        assert!(
            html.contains("greet"),
            "Public-only HTML should contain public items"
        );
        assert!(
            !html.contains("helper"),
            "Public-only HTML should not contain private items"
        );
    }

    // -- Step 5: Integration tests --

    #[test]
    fn test_cross_reference_links() {
        let names = vec!["Point".to_string(), "add".to_string()];
        let result = linkify_types("Uses `Point` and `add` for math", &names);
        assert!(
            result.contains("<a href=\"#Point\">Point</a>"),
            "Should linkify Point: {result}"
        );
        assert!(
            result.contains("<a href=\"#add\">add</a>"),
            "Should linkify add: {result}"
        );
    }

    #[test]
    fn test_project_docs_multi_module() {
        let src1 = "/// Math add\nfn add(a: int, b: int) -> int { a + b }";
        let src2 = "/// String helper\nfn greet(name: string) { print(name) }";
        let p1 = parse_source(src1);
        let p2 = parse_source(src2);
        let m1 = extract_docs(&p1, Some("math.tl"));
        let m2 = extract_docs(&p2, Some("string.tl"));
        let project = ProjectDoc {
            modules: vec![m1, m2],
        };
        let html = generate_project_html(&project);
        assert!(
            html.contains("math.tl"),
            "Project HTML should contain module names"
        );
        assert!(
            html.contains("string.tl"),
            "Project HTML should contain module names"
        );
        assert!(
            html.contains("add"),
            "Project HTML should contain item names"
        );
        assert!(
            html.contains("greet"),
            "Project HTML should contain item names"
        );
    }

    #[test]
    fn test_project_markdown_multi_module() {
        let src1 = "fn foo() {}";
        let src2 = "fn bar() {}";
        let p1 = parse_source(src1);
        let p2 = parse_source(src2);
        let m1 = extract_docs(&p1, Some("a.tl"));
        let m2 = extract_docs(&p2, Some("b.tl"));
        let project = ProjectDoc {
            modules: vec![m1, m2],
        };
        let md = generate_project_markdown(&project);
        assert!(
            md.contains("## a.tl"),
            "Project MD should have module headers"
        );
        assert!(
            md.contains("## b.tl"),
            "Project MD should have module headers"
        );
    }

    #[test]
    fn test_project_json_valid() {
        let src = "fn foo() {}";
        let p = parse_source(src);
        let m = extract_docs(&p, Some("test.tl"));
        let project = ProjectDoc { modules: vec![m] };
        let json = generate_project_json(&project);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON should be valid");
        assert!(
            parsed.get("modules").is_some(),
            "Project JSON should have modules"
        );
    }

    #[test]
    fn test_empty_file_minimal_output() {
        let program = parse_source("");
        let docs = extract_docs(&program, Some("empty.tl"));
        let html = generate_html(&docs);
        assert!(
            html.contains("empty.tl"),
            "Empty file HTML should still have title"
        );
        let md = generate_markdown(&docs);
        assert!(
            md.contains("empty.tl"),
            "Empty file MD should still have title"
        );
        let json = generate_json(&docs);
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("Empty JSON should be valid");
        assert!(parsed.get("items").unwrap().as_array().unwrap().is_empty());
    }

    #[test]
    fn test_multiple_example_blocks() {
        let doc = parse_doc_comment("Some function\n@example\nlet x = 1\n@example\nlet y = 2");
        assert_eq!(doc.examples.len(), 2, "Should have 2 examples");
        assert_eq!(doc.examples[0], "let x = 1");
        assert_eq!(doc.examples[1], "let y = 2");
    }

    #[test]
    fn test_html_multiple_examples() {
        let source = "/// Multi-example fn\n/// @example\n/// add(1, 2)\n/// @example\n/// add(3, 4)\nfn add(a, b) { a + b }";
        let program = parse_source(source);
        let docs = extract_docs(&program, None);
        let html = generate_html(&docs);
        assert!(html.contains("add(1, 2)"), "HTML should show first example");
        assert!(
            html.contains("add(3, 4)"),
            "HTML should show second example"
        );
    }
}
