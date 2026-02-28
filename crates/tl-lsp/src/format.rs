// ThinkingLanguage — LSP Code Formatter
// AST-guided formatter with comment preservation.

use lsp_types::{Position, Range, TextEdit};
use tl_ast::*;
use tl_lexer::{tokenize_with_trivia, Trivia, TriviaOrToken};

pub struct Formatter {
    indent: usize,
    output: String,
    comments: Vec<(usize, String)>, // (byte_offset, comment_text)
    next_comment_idx: usize,
}

impl Formatter {
    pub fn format(source: &str) -> Result<String, String> {
        let program = tl_parser::parse(source)
            .map_err(|e| format!("Parse error: {e}"))?;

        // Collect comments with their positions
        let trivia_items = tokenize_with_trivia(source);
        let comments: Vec<(usize, String)> = trivia_items
            .iter()
            .filter_map(|item| {
                if let TriviaOrToken::Trivia(Trivia::Comment(text), span) = item {
                    Some((span.start, text.clone()))
                } else {
                    None
                }
            })
            .collect();

        let mut formatter = Formatter {
            indent: 0,
            output: String::new(),
            comments,
            next_comment_idx: 0,
        };

        formatter.format_program(&program);

        // Emit any remaining comments
        while formatter.next_comment_idx < formatter.comments.len() {
            let (_, text) = &formatter.comments[formatter.next_comment_idx];
            if !formatter.output.ends_with('\n') {
                formatter.output.push('\n');
            }
            formatter.output.push_str(text);
            formatter.output.push('\n');
            formatter.next_comment_idx += 1;
        }

        // Ensure trailing newline
        if !formatter.output.is_empty() && !formatter.output.ends_with('\n') {
            formatter.output.push('\n');
        }

        // Remove trailing blank lines (keep exactly one trailing newline)
        let trimmed = formatter.output.trim_end().to_string();
        if trimmed.is_empty() {
            Ok(String::new())
        } else {
            Ok(trimmed + "\n")
        }
    }

    fn format_program(&mut self, program: &Program) {
        // Emit module-level doc comments
        if let Some(ref doc) = program.module_doc {
            for line in doc.lines() {
                self.output.push_str("//! ");
                self.output.push_str(line);
                self.output.push('\n');
            }
            self.output.push('\n');
        }

        let stmts = &program.statements;
        for (i, stmt) in stmts.iter().enumerate() {
            self.emit_comments_before(stmt.span.start);
            self.format_stmt(stmt);
            // Blank line between top-level declarations
            if i + 1 < stmts.len() {
                let next = &stmts[i + 1];
                if is_top_level_decl(&stmt.kind) || is_top_level_decl(&next.kind) {
                    self.output.push('\n');
                }
            }
        }
    }

    fn emit_comments_before(&mut self, offset: usize) {
        while self.next_comment_idx < self.comments.len() {
            let comment_offset = self.comments[self.next_comment_idx].0;
            if comment_offset < offset {
                let text = self.comments[self.next_comment_idx].1.clone();
                if !self.output.is_empty() && !self.output.ends_with('\n') {
                    self.output.push(' ');
                } else {
                    self.push_indent();
                }
                self.output.push_str(&text);
                self.output.push('\n');
                self.next_comment_idx += 1;
            } else {
                break;
            }
        }
    }

    fn format_stmt(&mut self, stmt: &Stmt) {
        // Emit doc comments before the statement
        if let Some(ref doc) = stmt.doc_comment {
            for line in doc.lines() {
                self.push_indent();
                self.output.push_str("/// ");
                self.output.push_str(line);
                self.output.push('\n');
            }
        }
        match &stmt.kind {
            StmtKind::Let { name, mutable, type_ann, value, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("let ");
                if *mutable { self.output.push_str("mut "); }
                self.output.push_str(name);
                if let Some(ann) = type_ann {
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_type_expr(ann));
                }
                self.output.push_str(" = ");
                self.output.push_str(&self.format_expr(value));
                self.output.push('\n');
            }
            StmtKind::FnDecl { name, type_params, params, return_type, bounds, body, is_generator: _, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("fn ");
                self.output.push_str(name);
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push('(');
                let params_str: Vec<String> = params.iter().map(|p| {
                    if let Some(ann) = &p.type_ann {
                        format!("{}: {}", p.name, self.format_type_expr(ann))
                    } else {
                        p.name.clone()
                    }
                }).collect();
                self.output.push_str(&params_str.join(", "));
                self.output.push(')');
                if let Some(ret) = return_type {
                    self.output.push_str(" -> ");
                    self.output.push_str(&self.format_type_expr(ret));
                }
                if !bounds.is_empty() {
                    self.output.push_str(" where ");
                    let bounds_str: Vec<String> = bounds.iter().map(|b| {
                        format!("{}: {}", b.type_param, b.traits.join(" + "))
                    }).collect();
                    self.output.push_str(&bounds_str.join(", "));
                }
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::Expr(expr) => {
                self.push_indent();
                self.output.push_str(&self.format_expr(expr));
                self.output.push('\n');
            }
            StmtKind::Return(expr) => {
                self.push_indent();
                self.output.push_str("return");
                if let Some(e) = expr {
                    self.output.push(' ');
                    self.output.push_str(&self.format_expr(e));
                }
                self.output.push('\n');
            }
            StmtKind::If { condition, then_body, else_ifs, else_body } => {
                self.push_indent();
                self.output.push_str("if ");
                self.output.push_str(&self.format_expr(condition));
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(then_body);
                self.indent -= 1;
                for (cond, body) in else_ifs {
                    self.push_indent();
                    self.output.push_str("} else if ");
                    self.output.push_str(&self.format_expr(cond));
                    self.output.push_str(" {\n");
                    self.indent += 1;
                    self.format_body(body);
                    self.indent -= 1;
                }
                if let Some(body) = else_body {
                    self.push_indent();
                    self.output.push_str("} else {\n");
                    self.indent += 1;
                    self.format_body(body);
                    self.indent -= 1;
                }
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::While { condition, body } => {
                self.push_indent();
                self.output.push_str("while ");
                self.output.push_str(&self.format_expr(condition));
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::For { name, iter, body } => {
                self.push_indent();
                self.output.push_str("for ");
                self.output.push_str(name);
                self.output.push_str(" in ");
                self.output.push_str(&self.format_expr(iter));
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::StructDecl { name, type_params, fields, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("struct ");
                self.output.push_str(name);
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push_str(" {\n");
                self.indent += 1;
                for (i, f) in fields.iter().enumerate() {
                    self.push_indent();
                    self.output.push_str(&f.name);
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_type_expr(&f.type_ann));
                    if i + 1 < fields.len() { self.output.push(','); }
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::EnumDecl { name, type_params, variants, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("enum ");
                self.output.push_str(name);
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push_str(" {\n");
                self.indent += 1;
                for (i, v) in variants.iter().enumerate() {
                    self.push_indent();
                    self.output.push_str(&v.name);
                    if !v.fields.is_empty() {
                        let fields: Vec<String> = v.fields.iter().map(|f| self.format_type_expr(f)).collect();
                        self.output.push('(');
                        self.output.push_str(&fields.join(", "));
                        self.output.push(')');
                    }
                    if i + 1 < variants.len() { self.output.push(','); }
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::ImplBlock { type_name, type_params, methods } => {
                self.push_indent();
                self.output.push_str("impl");
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push(' ');
                self.output.push_str(type_name);
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(methods);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::TraitDef { name, type_params, methods, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("trait ");
                self.output.push_str(name);
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push_str(" {\n");
                self.indent += 1;
                for m in methods {
                    self.push_indent();
                    self.output.push_str("fn ");
                    self.output.push_str(&m.name);
                    self.output.push('(');
                    let params: Vec<String> = m.params.iter().map(|p| {
                        if let Some(ann) = &p.type_ann {
                            format!("{}: {}", p.name, self.format_type_expr(ann))
                        } else {
                            p.name.clone()
                        }
                    }).collect();
                    self.output.push_str(&params.join(", "));
                    self.output.push(')');
                    if let Some(ret) = &m.return_type {
                        self.output.push_str(" -> ");
                        self.output.push_str(&self.format_type_expr(ret));
                    }
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::TraitImpl { trait_name, type_name, type_params, methods } => {
                self.push_indent();
                self.output.push_str("impl");
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push(' ');
                self.output.push_str(trait_name);
                self.output.push_str(" for ");
                self.output.push_str(type_name);
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(methods);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::TryCatch { try_body, catch_var, catch_body } => {
                self.push_indent();
                self.output.push_str("try {\n");
                self.indent += 1;
                self.format_body(try_body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("} catch ");
                self.output.push_str(catch_var);
                self.output.push_str(" {\n");
                self.indent += 1;
                self.format_body(catch_body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::Throw(expr) => {
                self.push_indent();
                self.output.push_str("throw ");
                self.output.push_str(&self.format_expr(expr));
                self.output.push('\n');
            }
            StmtKind::Import { path, alias } => {
                self.push_indent();
                self.output.push_str("import \"");
                self.output.push_str(path);
                self.output.push('"');
                if let Some(a) = alias {
                    self.output.push_str(" as ");
                    self.output.push_str(a);
                }
                self.output.push('\n');
            }
            StmtKind::Use { item, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("use ");
                self.output.push_str(&format_use_item(item));
                self.output.push('\n');
            }
            StmtKind::ModDecl { name, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("mod ");
                self.output.push_str(name);
                self.output.push('\n');
            }
            StmtKind::Test { name, body } => {
                self.push_indent();
                self.output.push_str("test \"");
                self.output.push_str(name);
                self.output.push_str("\" {\n");
                self.indent += 1;
                self.format_body(body);
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::LetDestructure { pattern, mutable, value, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("let ");
                if *mutable { self.output.push_str("mut "); }
                self.output.push_str(&self.format_pattern(pattern));
                self.output.push_str(" = ");
                let val_str = self.format_expr(value);
                self.output.push_str(&val_str);
                self.output.push('\n');
            }
            StmtKind::Break => {
                self.push_indent();
                self.output.push_str("break\n");
            }
            StmtKind::Continue => {
                self.push_indent();
                self.output.push_str("continue\n");
            }
            StmtKind::Schema { name, fields, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("schema ");
                self.output.push_str(name);
                self.output.push_str(" {\n");
                self.indent += 1;
                for f in fields {
                    self.push_indent();
                    self.output.push_str(&f.name);
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_type_expr(&f.type_ann));
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::Pipeline { name, extract, transform, load, .. } => {
                self.push_indent();
                self.output.push_str("pipeline ");
                self.output.push_str(name);
                self.output.push_str(" {\n");
                self.indent += 1;
                self.push_indent(); self.output.push_str("extract {\n");
                self.indent += 1;
                self.format_body(extract);
                self.indent -= 1;
                self.push_indent(); self.output.push_str("}\n");
                self.push_indent(); self.output.push_str("transform {\n");
                self.indent += 1;
                self.format_body(transform);
                self.indent -= 1;
                self.push_indent(); self.output.push_str("}\n");
                self.push_indent(); self.output.push_str("load {\n");
                self.indent += 1;
                self.format_body(load);
                self.indent -= 1;
                self.push_indent(); self.output.push_str("}\n");
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::Train { name, algorithm, config } => {
                self.push_indent();
                self.output.push_str("model ");
                self.output.push_str(name);
                self.output.push_str(" = train ");
                self.output.push_str(algorithm);
                self.output.push_str(" {\n");
                self.indent += 1;
                for (k, v) in config {
                    self.push_indent();
                    self.output.push_str(k);
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_expr(v));
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::StreamDecl { name, source: src, transform, sink, window: _, watermark: _ } => {
                self.push_indent();
                self.output.push_str("stream ");
                self.output.push_str(name);
                self.output.push_str(" {\n");
                self.indent += 1;
                self.push_indent();
                self.output.push_str("source: ");
                self.output.push_str(&self.format_expr(src));
                self.output.push('\n');
                if !transform.is_empty() {
                    self.push_indent();
                    self.output.push_str("transform {\n");
                    self.indent += 1;
                    self.format_body(transform);
                    self.indent -= 1;
                    self.push_indent();
                    self.output.push_str("}\n");
                }
                if let Some(snk) = sink {
                    self.push_indent();
                    self.output.push_str("sink: ");
                    self.output.push_str(&self.format_expr(snk));
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::SourceDecl { name, connector_type, config } => {
                self.push_indent();
                self.output.push_str("source ");
                self.output.push_str(name);
                self.output.push_str(" = connector ");
                self.output.push_str(connector_type);
                self.output.push_str(" {\n");
                self.indent += 1;
                for (k, v) in config {
                    self.push_indent();
                    self.output.push_str(k);
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_expr(v));
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::SinkDecl { name, connector_type, config } => {
                self.push_indent();
                self.output.push_str("sink ");
                self.output.push_str(name);
                self.output.push_str(" = connector ");
                self.output.push_str(connector_type);
                self.output.push_str(" {\n");
                self.indent += 1;
                for (k, v) in config {
                    self.push_indent();
                    self.output.push_str(k);
                    self.output.push_str(": ");
                    self.output.push_str(&self.format_expr(v));
                    self.output.push('\n');
                }
                self.indent -= 1;
                self.push_indent();
                self.output.push_str("}\n");
            }
            StmtKind::TypeAlias { name, type_params, value, is_public } => {
                self.push_indent();
                if *is_public { self.output.push_str("pub "); }
                self.output.push_str("type ");
                self.output.push_str(name);
                if !type_params.is_empty() {
                    self.output.push('<');
                    self.output.push_str(&type_params.join(", "));
                    self.output.push('>');
                }
                self.output.push_str(" = ");
                self.output.push_str(&self.format_type_expr(value));
                self.output.push('\n');
            }
        }
    }

    fn format_body(&mut self, stmts: &[Stmt]) {
        for stmt in stmts {
            self.emit_comments_before(stmt.span.start);
            self.format_stmt(stmt);
        }
    }

    fn format_expr(&self, expr: &Expr) -> String {
        match expr {
            Expr::Int(n) => n.to_string(),
            Expr::Float(f) => {
                let s = f.to_string();
                if s.contains('.') { s } else { format!("{s}.0") }
            }
            Expr::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
            Expr::Bool(b) => b.to_string(),
            Expr::None => "none".to_string(),
            Expr::Ident(name) => name.clone(),
            Expr::BinOp { left, op, right } => {
                format!("{} {} {}", self.format_expr(left), op, self.format_expr(right))
            }
            Expr::UnaryOp { op, expr } => {
                match op {
                    UnaryOp::Neg => format!("-{}", self.format_expr(expr)),
                    UnaryOp::Not => format!("not {}", self.format_expr(expr)),
                }
            }
            Expr::Call { function, args } => {
                let args_str: Vec<String> = args.iter().map(|a| self.format_expr(a)).collect();
                format!("{}({})", self.format_expr(function), args_str.join(", "))
            }
            Expr::NamedArg { name, value } => {
                format!("{}: {}", name, self.format_expr(value))
            }
            Expr::Pipe { left, right } => {
                format!("{} |> {}", self.format_expr(left), self.format_expr(right))
            }
            Expr::Member { object, field } => {
                format!("{}.{}", self.format_expr(object), field)
            }
            Expr::Index { object, index } => {
                format!("{}[{}]", self.format_expr(object), self.format_expr(index))
            }
            Expr::List(items) => {
                let items_str: Vec<String> = items.iter().map(|i| self.format_expr(i)).collect();
                format!("[{}]", items_str.join(", "))
            }
            Expr::Map(pairs) => {
                if pairs.is_empty() {
                    return "{}".to_string();
                }
                let pairs_str: Vec<String> = pairs.iter()
                    .map(|(k, v)| format!("{}: {}", self.format_expr(k), self.format_expr(v)))
                    .collect();
                format!("{{ {} }}", pairs_str.join(", "))
            }
            Expr::Closure { params, return_type, body } => {
                let params_str: Vec<String> = params.iter().map(|p| {
                    if let Some(ann) = &p.type_ann {
                        format!("{}: {}", p.name, self.format_type_expr(ann))
                    } else {
                        p.name.clone()
                    }
                }).collect();
                match body {
                    tl_ast::ClosureBody::Expr(e) => {
                        format!("({}) => {}", params_str.join(", "), self.format_expr(e))
                    }
                    tl_ast::ClosureBody::Block { stmts, expr } => {
                        let rt = return_type.as_ref()
                            .map(|t| format!(" {}", self.format_type_expr(t)))
                            .unwrap_or_default();
                        let mut parts = Vec::new();
                        for s in stmts {
                            if let StmtKind::Expr(e) = &s.kind {
                                parts.push(self.format_expr(e));
                            } else if let StmtKind::Let { name, mutable, type_ann, value, .. } = &s.kind {
                                let mut s = String::new();
                                s.push_str("let ");
                                if *mutable { s.push_str("mut "); }
                                s.push_str(name);
                                if let Some(ann) = type_ann {
                                    s.push_str(": ");
                                    s.push_str(&self.format_type_expr(ann));
                                }
                                s.push_str(" = ");
                                s.push_str(&self.format_expr(value));
                                parts.push(s);
                            } else if let StmtKind::Return(Some(e)) = &s.kind {
                                parts.push(format!("return {}", self.format_expr(e)));
                            } else {
                                parts.push("...".to_string());
                            }
                        }
                        if let Some(e) = expr {
                            parts.push(self.format_expr(e));
                        }
                        format!("({}) ->{} {{ {} }}", params_str.join(", "), rt, parts.join("; "))
                    }
                }
            }
            Expr::Range { start, end } => {
                format!("{}..{}", self.format_expr(start), self.format_expr(end))
            }
            Expr::NullCoalesce { expr, default } => {
                format!("{} ?? {}", self.format_expr(expr), self.format_expr(default))
            }
            Expr::Assign { target, value } => {
                format!("{} = {}", self.format_expr(target), self.format_expr(value))
            }
            Expr::StructInit { name, fields } => {
                let fields_str: Vec<String> = fields.iter()
                    .map(|(n, v)| format!("{}: {}", n, self.format_expr(v)))
                    .collect();
                format!("{} {{ {} }}", name, fields_str.join(", "))
            }
            Expr::EnumVariant { enum_name, variant, args } => {
                if args.is_empty() {
                    format!("{}::{}", enum_name, variant)
                } else {
                    let args_str: Vec<String> = args.iter().map(|a| self.format_expr(a)).collect();
                    format!("{}::{}({})", enum_name, variant, args_str.join(", "))
                }
            }
            Expr::Await(inner) => format!("await {}", self.format_expr(inner)),
            Expr::Yield(inner) => {
                if let Some(e) = inner {
                    format!("yield {}", self.format_expr(e))
                } else {
                    "yield".to_string()
                }
            }
            Expr::Try(inner) => format!("{}?", self.format_expr(inner)),
            Expr::Block { stmts, expr } => {
                // For block expressions, just format inline if simple
                if stmts.is_empty() {
                    if let Some(e) = expr {
                        return self.format_expr(e);
                    }
                    return "{}".to_string();
                }
                // Complex block — format as multi-line
                let mut out = String::from("{\n");
                for s in stmts {
                    out.push_str(&"    ".repeat(self.indent + 1));
                    // Simple inline format for block statements
                    out.push_str(&format!("{:?}", s.kind));
                    out.push('\n');
                }
                if let Some(e) = expr {
                    out.push_str(&"    ".repeat(self.indent + 1));
                    out.push_str(&self.format_expr(e));
                    out.push('\n');
                }
                out.push_str(&"    ".repeat(self.indent));
                out.push('}');
                out
            }
            Expr::Case { arms } => {
                let mut out = String::from("case {\n");
                for arm in arms {
                    out.push_str(&"    ".repeat(self.indent + 1));
                    out.push_str(&self.format_pattern(&arm.pattern));
                    if let Some(guard) = &arm.guard {
                        out.push_str(" if ");
                        out.push_str(&self.format_expr(guard));
                    }
                    out.push_str(" => ");
                    out.push_str(&self.format_expr(&arm.body));
                    out.push('\n');
                }
                out.push_str(&"    ".repeat(self.indent));
                out.push('}');
                out
            }
            Expr::Match { subject, arms } => {
                let mut out = format!("match {} {{\n", self.format_expr(subject));
                for arm in arms {
                    out.push_str(&"    ".repeat(self.indent + 1));
                    out.push_str(&self.format_pattern(&arm.pattern));
                    if let Some(guard) = &arm.guard {
                        out.push_str(" if ");
                        out.push_str(&self.format_expr(guard));
                    }
                    out.push_str(" => ");
                    out.push_str(&self.format_expr(&arm.body));
                    out.push('\n');
                }
                out.push_str(&"    ".repeat(self.indent));
                out.push('}');
                out
            }
        }
    }

    fn format_pattern(&self, pattern: &Pattern) -> String {
        match pattern {
            Pattern::Wildcard => "_".to_string(),
            Pattern::Binding(name) => name.clone(),
            Pattern::Literal(expr) => self.format_expr(expr),
            Pattern::Enum { type_name, variant, args } => {
                if args.is_empty() {
                    format!("{type_name}::{variant}")
                } else {
                    let args_str: Vec<String> = args.iter().map(|a| self.format_pattern(a)).collect();
                    format!("{type_name}::{variant}({})", args_str.join(", "))
                }
            }
            Pattern::Struct { name, fields } => {
                let prefix = name.as_deref().unwrap_or("");
                let fields_str: Vec<String> = fields.iter().map(|f| {
                    if let Some(pat) = &f.pattern {
                        format!("{}: {}", f.name, self.format_pattern(pat))
                    } else {
                        f.name.clone()
                    }
                }).collect();
                if prefix.is_empty() {
                    format!("{{ {} }}", fields_str.join(", "))
                } else {
                    format!("{prefix} {{ {} }}", fields_str.join(", "))
                }
            }
            Pattern::List { elements, rest } => {
                let mut parts: Vec<String> = elements.iter().map(|e| self.format_pattern(e)).collect();
                if let Some(rest_name) = rest {
                    parts.push(format!("...{rest_name}"));
                }
                format!("[{}]", parts.join(", "))
            }
            Pattern::Or(patterns) => {
                let parts: Vec<String> = patterns.iter().map(|p| self.format_pattern(p)).collect();
                parts.join(" or ")
            }
        }
    }

    fn format_type_expr(&self, te: &TypeExpr) -> String {
        match te {
            TypeExpr::Named(name) => name.clone(),
            TypeExpr::Generic { name, args } => {
                let args_str: Vec<String> = args.iter().map(|a| self.format_type_expr(a)).collect();
                format!("{}<{}>", name, args_str.join(", "))
            }
            TypeExpr::Optional(inner) => format!("{}?", self.format_type_expr(inner)),
            TypeExpr::Function { params, return_type } => {
                let params_str: Vec<String> = params.iter().map(|p| self.format_type_expr(p)).collect();
                format!("fn({}) -> {}", params_str.join(", "), self.format_type_expr(return_type))
            }
        }
    }

    fn push_indent(&mut self) {
        for _ in 0..self.indent {
            self.output.push_str("    ");
        }
    }
}

fn is_top_level_decl(kind: &StmtKind) -> bool {
    matches!(
        kind,
        StmtKind::FnDecl { .. }
            | StmtKind::StructDecl { .. }
            | StmtKind::EnumDecl { .. }
            | StmtKind::TraitDef { .. }
            | StmtKind::ImplBlock { .. }
            | StmtKind::TraitImpl { .. }
            | StmtKind::Test { .. }
            | StmtKind::Pipeline { .. }
            | StmtKind::Schema { .. }
    )
}

fn format_use_item(item: &tl_ast::UseItem) -> String {
    match item {
        tl_ast::UseItem::Single(path) => path.join("."),
        tl_ast::UseItem::Group(prefix, names) => {
            format!("{}.{{{}}}", prefix.join("."), names.join(", "))
        }
        tl_ast::UseItem::Wildcard(path) => format!("{}.*", path.join(".")),
        tl_ast::UseItem::Aliased(path, alias) => {
            format!("{} as {}", path.join("."), alias)
        }
    }
}

/// Provide formatting edits for LSP
pub fn provide_formatting(source: &str) -> Option<Vec<TextEdit>> {
    let formatted = Formatter::format(source).ok()?;
    if formatted == source {
        return Some(vec![]); // no changes needed
    }
    // Replace entire document
    let lines: Vec<&str> = source.lines().collect();
    let last_line = lines.len().saturating_sub(1) as u32;
    let last_col = lines.last().map(|l| l.len()).unwrap_or(0) as u32;
    Some(vec![TextEdit {
        range: Range::new(Position::new(0, 0), Position::new(last_line, last_col)),
        new_text: formatted,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_indentation() {
        let source = "fn foo() {\nlet x = 42\nprint(x)\n}";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("    let x = 42"), "Body should be indented: {result}");
        assert!(result.contains("    print(x)"), "Body should be indented: {result}");
    }

    #[test]
    fn test_format_operator_spacing() {
        let source = "let x = 1+2";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("1 + 2"), "Operators should have spaces: {result}");
    }

    #[test]
    fn test_format_comment_preserved() {
        let source = "// hello world\nlet x = 42";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("// hello world"), "Comment should be preserved: {result}");
    }

    #[test]
    fn test_format_trailing_whitespace() {
        let source = "let x = 42   \n";
        let result = Formatter::format(source).unwrap();
        // Each line should not end with trailing spaces
        for line in result.lines() {
            assert_eq!(line, line.trim_end(), "No trailing whitespace: '{line}'");
        }
    }

    #[test]
    fn test_format_blank_lines_between_declarations() {
        let source = "fn foo() { 1 }\nfn bar() { 2 }";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("}\n\nfn bar"), "Blank line between functions: {result}");
    }

    #[test]
    fn test_format_nested_blocks() {
        let source = "fn foo() {\nif true {\nlet x = 1\n}\n}";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("    if true"), "If should be indented once: {result}");
        assert!(result.contains("        let x = 1"), "Nested let should be indented twice: {result}");
    }

    #[test]
    fn test_format_idempotent() {
        let source = "fn add(a: int, b: int) -> int {\n    return a + b\n}\n";
        let first = Formatter::format(source).unwrap();
        let second = Formatter::format(&first).unwrap();
        assert_eq!(first, second, "Formatting should be idempotent");
    }

    #[test]
    fn test_format_parse_error_returns_err() {
        let source = "fn {{{}}}}";
        let result = Formatter::format(source);
        assert!(result.is_err(), "Parse error should return Err");
    }

    // Phase 19: Doc comment formatting tests

    #[test]
    fn test_format_doc_comment_preserved() {
        let source = "/// Adds two numbers\nfn add(a, b) { a + b }";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("/// Adds two numbers"), "Doc comment should be preserved: {result}");
        assert!(result.contains("fn add"), "Function should follow doc: {result}");
    }

    #[test]
    fn test_format_doc_comment_roundtrip() {
        let source = "/// First line\n/// Second line\nfn foo() {\n    42\n}\n";
        let first = Formatter::format(source).unwrap();
        let second = Formatter::format(&first).unwrap();
        assert_eq!(first, second, "Doc comment formatting should be idempotent");
    }

    #[test]
    fn test_format_module_doc_preserved() {
        let source = "//! Module description\nfn foo() {}";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("//! Module description"), "Module doc should be preserved: {result}");
    }

    #[test]
    fn test_format_doc_comment_indented_in_impl() {
        let source = "struct Foo {}\nimpl Foo {\n/// Method doc\nfn bar(self) { 42 }\n}";
        let result = Formatter::format(source).unwrap();
        assert!(result.contains("    /// Method doc"), "Doc comment in impl should be indented: {result}");
    }
}
