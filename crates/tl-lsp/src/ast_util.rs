// ThinkingLanguage — LSP AST Utilities
// Shared traversal helpers for hover, goto-def, and symbols.

use tl_ast::{Program, Stmt, StmtKind};
use tl_errors::Span;

/// Kind of a top-level definition
#[derive(Debug, Clone, PartialEq)]
pub enum DefKind {
    Variable,
    Function,
    Struct,
    Enum,
    Trait,
    Schema,
    Pipeline,
    Test,
}

/// Collect all named definitions from a program with their spans.
pub fn collect_definitions(program: &Program) -> Vec<(String, DefKind, Span)> {
    let mut defs = Vec::new();
    collect_defs_from_body(&program.statements, &mut defs);
    defs
}

fn collect_defs_from_body(stmts: &[Stmt], defs: &mut Vec<(String, DefKind, Span)>) {
    for stmt in stmts {
        match &stmt.kind {
            StmtKind::Let { name, .. } => {
                defs.push((name.clone(), DefKind::Variable, stmt.span));
            }
            StmtKind::FnDecl { name, body, .. } => {
                defs.push((name.clone(), DefKind::Function, stmt.span));
                collect_defs_from_body(body, defs);
            }
            StmtKind::StructDecl { name, .. } => {
                defs.push((name.clone(), DefKind::Struct, stmt.span));
            }
            StmtKind::EnumDecl { name, .. } => {
                defs.push((name.clone(), DefKind::Enum, stmt.span));
            }
            StmtKind::TraitDef { name, .. } => {
                defs.push((name.clone(), DefKind::Trait, stmt.span));
            }
            StmtKind::Schema { name, .. } => {
                defs.push((name.clone(), DefKind::Schema, stmt.span));
            }
            StmtKind::Pipeline { name, .. } => {
                defs.push((name.clone(), DefKind::Pipeline, stmt.span));
            }
            StmtKind::Test { name, .. } => {
                defs.push((name.clone(), DefKind::Test, stmt.span));
            }
            StmtKind::ImplBlock { methods, .. } => {
                collect_defs_from_body(methods, defs);
            }
            StmtKind::If {
                then_body,
                else_ifs,
                else_body,
                ..
            } => {
                collect_defs_from_body(then_body, defs);
                for (_, body) in else_ifs {
                    collect_defs_from_body(body, defs);
                }
                if let Some(body) = else_body {
                    collect_defs_from_body(body, defs);
                }
            }
            StmtKind::While { body, .. } | StmtKind::For { body, .. } => {
                collect_defs_from_body(body, defs);
            }
            StmtKind::TryCatch {
                try_body,
                catch_body,
                ..
            } => {
                collect_defs_from_body(try_body, defs);
                collect_defs_from_body(catch_body, defs);
            }
            _ => {}
        }
    }
}

/// Convert an LSP position (line, character) to a byte offset in source.
pub fn position_to_offset(source: &str, line: u32, character: u32) -> usize {
    let mut current_line = 0u32;
    let mut current_col = 0u32;
    for (i, ch) in source.char_indices() {
        if current_line == line && current_col == character {
            return i;
        }
        if ch == '\n' {
            if current_line == line {
                return i; // end of line
            }
            current_line += 1;
            current_col = 0;
        } else {
            current_col += ch.len_utf16() as u32;
        }
    }
    source.len()
}

/// Find the identifier at a given byte offset in source text.
/// Returns (name, start_offset, end_offset) if found.
pub fn find_ident_at_offset(source: &str, offset: usize) -> Option<(String, usize, usize)> {
    let bytes = source.as_bytes();
    if offset >= bytes.len() {
        return None;
    }

    // Check if we're on an ident character
    let is_ident_char = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    if !is_ident_char(bytes[offset]) {
        // Try offset-1 (cursor may be just past the ident)
        if offset > 0 && is_ident_char(bytes[offset - 1]) {
            return find_ident_at_offset(source, offset - 1);
        }
        return None;
    }

    let mut start = offset;
    while start > 0 && is_ident_char(bytes[start - 1]) {
        start -= 1;
    }

    let mut end = offset;
    while end < bytes.len() && is_ident_char(bytes[end]) {
        end += 1;
    }

    let name = source[start..end].to_string();

    // Don't return pure numbers
    if name.chars().next().is_none_or(|c| c.is_ascii_digit()) {
        return None;
    }

    Some((name, start, end))
}

/// Collect names of parameters from function declarations in scope up to an offset.
pub fn collect_params_at_offset(program: &Program, offset: usize) -> Vec<String> {
    let mut params = Vec::new();
    for stmt in &program.statements {
        if stmt.span.start > offset {
            break;
        }
        if let StmtKind::FnDecl {
            params: fn_params,
            body,
            ..
        } = &stmt.kind
        {
            // Check if offset is within this function's body
            if stmt.span.start <= offset && stmt.span.end >= offset {
                for p in fn_params {
                    params.push(p.name.clone());
                }
                // Check nested functions
                for s in body {
                    if let StmtKind::FnDecl {
                        params: inner_params,
                        ..
                    } = &s.kind
                        && s.span.start <= offset
                        && s.span.end >= offset
                    {
                        for p in inner_params {
                            params.push(p.name.clone());
                        }
                    }
                }
            }
        }
    }
    params
}
