// ThinkingLanguage — Type Checker
// Licensed under MIT OR Apache-2.0
//
// Walks the AST, builds type environment, infers types from expressions,
// and checks annotations. Gradual: unannotated code = `any`, always passes.

use std::collections::HashSet;
use tl_ast::{Expr, Program, Stmt, StmtKind};
use tl_errors::Span;

use crate::convert::{convert_type_expr, convert_type_expr_with_params};
use crate::infer::infer_expr;
use crate::{FnSig, TraitInfo, Type, TypeEnv, is_compatible};

/// A type error with source location.
#[derive(Debug, Clone)]
pub struct TypeError {
    pub message: String,
    pub span: Span,
    pub expected: Option<String>,
    pub found: Option<String>,
    pub hint: Option<String>,
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        if let (Some(expected), Some(found)) = (&self.expected, &self.found) {
            write!(f, " (expected `{expected}`, found `{found}`)")?;
        }
        Ok(())
    }
}

/// Configuration for the type checker.
pub struct CheckerConfig {
    /// If true, require type annotations on function parameters.
    pub strict: bool,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        CheckerConfig { strict: false }
    }
}

/// Result of type checking a program.
pub struct CheckResult {
    pub errors: Vec<TypeError>,
    pub warnings: Vec<TypeError>,
}

impl CheckResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Type check a program. Returns errors and warnings.
pub fn check_program(program: &Program, config: &CheckerConfig) -> CheckResult {
    let mut checker = TypeChecker {
        env: TypeEnv::new(),
        errors: Vec::new(),
        warnings: Vec::new(),
        config,
        current_fn_return: None,
        defined_vars: Vec::new(),
        used_vars: HashSet::new(),
        imported_names: Vec::new(),
        used_imports: HashSet::new(),
    };

    // First pass: register all top-level functions and types
    for stmt in &program.statements {
        checker.register_top_level(stmt);
    }

    // Second pass: check all statements
    checker.check_body(&program.statements);

    // Check for unused variables at top-level scope
    checker.check_unused_vars();

    // Check for unused imports
    checker.check_unused_imports();

    CheckResult {
        errors: checker.errors,
        warnings: checker.warnings,
    }
}

struct TypeChecker<'a> {
    env: TypeEnv,
    errors: Vec<TypeError>,
    warnings: Vec<TypeError>,
    config: &'a CheckerConfig,
    /// The return type of the current function being checked (None if top-level).
    current_fn_return: Option<Type>,
    /// Variables defined in the current scope: (name, span, scope_depth)
    defined_vars: Vec<(String, Span, u32)>,
    /// Variables that have been used/referenced
    used_vars: HashSet<String>,
    /// Names imported via `use` statements: (name, span)
    imported_names: Vec<(String, Span)>,
    /// Import names that have been referenced
    used_imports: HashSet<String>,
}

/// Check if a name follows snake_case convention.
pub fn is_snake_case(s: &str) -> bool {
    if s.is_empty() || s.starts_with('_') {
        return true; // _-prefixed names are always ok
    }
    s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Check if a name follows PascalCase convention.
pub fn is_pascal_case(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }
    let first = s.chars().next().unwrap();
    first.is_ascii_uppercase() && !s.contains('_')
}

/// Check if a statement is a control flow terminator (return/break/continue/throw).
fn is_terminator(kind: &StmtKind) -> bool {
    matches!(
        kind,
        StmtKind::Return(_) | StmtKind::Break | StmtKind::Continue | StmtKind::Throw(_)
    )
}

impl<'a> TypeChecker<'a> {
    fn current_scope_depth(&self) -> u32 {
        self.env.scope_depth()
    }

    fn register_top_level(&mut self, stmt: &Stmt) {
        match &stmt.kind {
            StmtKind::FnDecl {
                name,
                type_params,
                params,
                return_type,
                ..
            } => {
                let param_types: Vec<(String, Type)> = params
                    .iter()
                    .map(|p| {
                        let ty = p
                            .type_ann
                            .as_ref()
                            .map(|t| convert_type_expr_with_params(t, type_params))
                            .unwrap_or(Type::Any);
                        (p.name.clone(), ty)
                    })
                    .collect();
                let ret = return_type
                    .as_ref()
                    .map(|t| convert_type_expr_with_params(t, type_params))
                    .unwrap_or(Type::Any);
                self.env.define_fn(
                    name.clone(),
                    FnSig {
                        params: param_types,
                        ret,
                    },
                );
                // Also define the function name as a variable of function type
                let fn_type = Type::Function {
                    params: params
                        .iter()
                        .map(|p| {
                            p.type_ann
                                .as_ref()
                                .map(|t| convert_type_expr_with_params(t, type_params))
                                .unwrap_or(Type::Any)
                        })
                        .collect(),
                    ret: Box::new(
                        return_type
                            .as_ref()
                            .map(|t| convert_type_expr_with_params(t, type_params))
                            .unwrap_or(Type::Any),
                    ),
                };
                self.env.define(name.clone(), fn_type);
                // Mark function names as "used" — they are declarations, not unused vars
                self.used_vars.insert(name.clone());
            }
            StmtKind::StructDecl { name, fields, .. } => {
                let field_types: Vec<(String, Type)> = fields
                    .iter()
                    .map(|f| (f.name.clone(), convert_type_expr(&f.type_ann)))
                    .collect();
                self.env.define_struct(name.clone(), field_types);
                self.env.define(name.clone(), Type::Struct(name.clone()));
                self.used_vars.insert(name.clone());
            }
            StmtKind::EnumDecl { name, variants, .. } => {
                let variant_types: Vec<(String, Vec<Type>)> = variants
                    .iter()
                    .map(|v| {
                        (
                            v.name.clone(),
                            v.fields.iter().map(|f| convert_type_expr(f)).collect(),
                        )
                    })
                    .collect();
                self.env.define_enum(name.clone(), variant_types);
                self.env.define(name.clone(), Type::Enum(name.clone()));
                self.used_vars.insert(name.clone());
            }
            StmtKind::TraitDef { name, methods, .. } => {
                let method_sigs: Vec<(String, Vec<Type>, Type)> = methods
                    .iter()
                    .map(|m| {
                        let param_types: Vec<Type> = m.params.iter()
                            .map(|p| p.type_ann.as_ref().map(|t| convert_type_expr(t)).unwrap_or(Type::Any))
                            .collect();
                        let ret = m.return_type.as_ref().map(|t| convert_type_expr(t)).unwrap_or(Type::Any);
                        (m.name.clone(), param_types, ret)
                    })
                    .collect();
                self.env.define_trait(
                    name.clone(),
                    TraitInfo {
                        name: name.clone(),
                        methods: method_sigs,
                        supertrait: None,
                    },
                );
                self.used_vars.insert(name.clone());
            }
            _ => {}
        }
    }

    /// Check a body of statements, tracking unreachable code.
    fn check_body(&mut self, stmts: &[Stmt]) {
        let mut terminated = false;
        for stmt in stmts {
            if terminated {
                self.warnings.push(TypeError {
                    message: "Unreachable code".to_string(),
                    span: stmt.span,
                    expected: None,
                    found: None,
                    hint: Some("This code will never be executed".to_string()),
                });
                // Only warn once per block
                return;
            }
            self.check_stmt(stmt);
            if is_terminator(&stmt.kind) {
                terminated = true;
            }
        }
    }

    /// Check a body and return whether ALL paths terminate.
    #[allow(dead_code)]
    fn check_body_terminates(&mut self, stmts: &[Stmt]) -> bool {
        let mut terminated = false;
        for stmt in stmts {
            if terminated {
                self.warnings.push(TypeError {
                    message: "Unreachable code".to_string(),
                    span: stmt.span,
                    expected: None,
                    found: None,
                    hint: Some("This code will never be executed".to_string()),
                });
                return true;
            }
            self.check_stmt(stmt);
            if is_terminator(&stmt.kind) {
                terminated = true;
            }
        }
        terminated
    }

    /// Mark a variable name as used (for unused variable tracking).
    fn mark_used_in_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Ident(name) => {
                self.used_vars.insert(name.clone());
                if self.imported_names.iter().any(|(n, _)| n == name) {
                    self.used_imports.insert(name.clone());
                }
            }
            Expr::BinOp { left, right, .. } => {
                self.mark_used_in_expr(left);
                self.mark_used_in_expr(right);
            }
            Expr::UnaryOp { expr, .. } => self.mark_used_in_expr(expr),
            Expr::Call { function, args } => {
                self.mark_used_in_expr(function);
                for a in args {
                    self.mark_used_in_expr(a);
                }
            }
            Expr::Member { object, .. } => self.mark_used_in_expr(object),
            Expr::Index { object, index } => {
                self.mark_used_in_expr(object);
                self.mark_used_in_expr(index);
            }
            Expr::List(elems) => {
                for e in elems {
                    self.mark_used_in_expr(e);
                }
            }
            Expr::Map(entries) => {
                for (k, v) in entries {
                    self.mark_used_in_expr(k);
                    self.mark_used_in_expr(v);
                }
            }
            Expr::Pipe { left, right } => {
                self.mark_used_in_expr(left);
                self.mark_used_in_expr(right);
            }
            Expr::Closure { body, .. } => self.mark_used_in_expr(body),
            Expr::NullCoalesce { expr, default } => {
                self.mark_used_in_expr(expr);
                self.mark_used_in_expr(default);
            }
            Expr::Assign { target, value } => {
                self.mark_used_in_expr(target);
                self.mark_used_in_expr(value);
            }
            Expr::StructInit { name, fields } => {
                self.used_vars.insert(name.clone());
                for (_, v) in fields {
                    self.mark_used_in_expr(v);
                }
            }
            Expr::EnumVariant { enum_name, args, .. } => {
                self.used_vars.insert(enum_name.clone());
                for a in args {
                    self.mark_used_in_expr(a);
                }
            }
            Expr::Range { start, end } => {
                self.mark_used_in_expr(start);
                self.mark_used_in_expr(end);
            }
            Expr::Block { stmts, expr } => {
                for s in stmts {
                    self.mark_used_in_stmt(s);
                }
                if let Some(e) = expr {
                    self.mark_used_in_expr(e);
                }
            }
            Expr::Match { subject, arms } => {
                self.mark_used_in_expr(subject);
                for (p, b) in arms {
                    self.mark_used_in_expr(p);
                    self.mark_used_in_expr(b);
                }
            }
            Expr::Case { arms } => {
                for (p, b) in arms {
                    self.mark_used_in_expr(p);
                    self.mark_used_in_expr(b);
                }
            }
            Expr::Await(inner) | Expr::Try(inner) => self.mark_used_in_expr(inner),
            Expr::Yield(Some(inner)) => self.mark_used_in_expr(inner),
            Expr::NamedArg { value, .. } => self.mark_used_in_expr(value),
            _ => {}
        }
    }

    fn mark_used_in_stmt(&mut self, stmt: &Stmt) {
        match &stmt.kind {
            StmtKind::Expr(e) | StmtKind::Throw(e) | StmtKind::Return(Some(e)) => {
                self.mark_used_in_expr(e);
            }
            StmtKind::Let { value, .. } => self.mark_used_in_expr(value),
            _ => {}
        }
    }

    fn check_stmt(&mut self, stmt: &Stmt) {
        match &stmt.kind {
            StmtKind::Let {
                name,
                type_ann,
                value,
                ..
            } => {
                // Mark all variables used in the value expression
                self.mark_used_in_expr(value);

                // Check struct init if the value is a struct init expression
                if let Expr::StructInit { name: sname, fields } = value {
                    self.check_struct_init(sname, fields, stmt.span);
                }

                let inferred = infer_expr(value, &self.env);
                if let Some(ann) = type_ann {
                    let expected = convert_type_expr(ann);
                    if !is_compatible(&expected, &inferred) {
                        self.errors.push(TypeError {
                            message: format!("Type mismatch in let binding `{name}`"),
                            span: stmt.span,
                            expected: Some(expected.to_string()),
                            found: Some(inferred.to_string()),
                            hint: None,
                        });
                    }
                    self.env.define(name.clone(), expected);
                } else {
                    self.env.define(name.clone(), inferred);
                }

                // Lint: naming convention — variables should be snake_case
                if !is_snake_case(name) {
                    self.warnings.push(TypeError {
                        message: format!("Variable `{name}` should be snake_case"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Use lowercase with underscores for variable names".to_string()),
                    });
                }

                // Lint: shadowing warning — check if variable already exists in outer scope
                if !name.starts_with('_') {
                    if self.defined_vars.iter().any(|(n, _, _)| n == name) {
                        self.warnings.push(TypeError {
                            message: format!("Variable `{name}` shadows a previous definition"),
                            span: stmt.span,
                            expected: None,
                            found: None,
                            hint: Some("Consider using a different name".to_string()),
                        });
                    }
                }

                // Track defined variable for unused-var checking
                let depth = self.current_scope_depth();
                self.defined_vars.push((name.clone(), stmt.span, depth));
            }

            StmtKind::FnDecl {
                name,
                type_params,
                params,
                return_type,
                bounds,
                body,
                ..
            } => {
                // Lint: function naming convention — should be snake_case
                if !is_snake_case(name) {
                    self.warnings.push(TypeError {
                        message: format!("Function `{name}` should be snake_case"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Use lowercase with underscores for function names".to_string()),
                    });
                }

                // Lint: empty function body
                if body.is_empty() {
                    self.warnings.push(TypeError {
                        message: format!("Empty function body in `{name}`"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Consider adding an implementation or removing the function".to_string()),
                    });
                }

                // Save outer unused-var state
                let outer_defined = std::mem::take(&mut self.defined_vars);
                let outer_used = std::mem::take(&mut self.used_vars);

                self.env.push_scope();

                // Define type parameters in scope
                for tp in type_params {
                    self.env.define(tp.clone(), Type::TypeParam(tp.clone()));
                }

                // Validate trait bounds reference existing traits
                for bound in bounds {
                    for trait_name in &bound.traits {
                        if self.env.lookup_trait(trait_name).is_none() {
                            self.errors.push(TypeError {
                                message: format!("Unknown trait `{trait_name}` in bound for `{}`", bound.type_param),
                                span: stmt.span,
                                expected: None,
                                found: None,
                                hint: Some("Available built-in traits: Numeric, Comparable, Hashable, Displayable, Serializable, Default".to_string()),
                            });
                        }
                    }
                    // Verify the bound references a declared type param
                    if !type_params.contains(&bound.type_param) {
                        self.errors.push(TypeError {
                            message: format!("Trait bound on undeclared type parameter `{}`", bound.type_param),
                            span: stmt.span,
                            expected: None,
                            found: None,
                            hint: Some(format!("Declare it in the type parameter list: `fn {}<{}, ...>(...)`", name, bound.type_param)),
                        });
                    }
                }

                // Bind parameters
                let fn_depth = self.current_scope_depth();
                for p in params {
                    let ty = p
                        .type_ann
                        .as_ref()
                        .map(|t| convert_type_expr_with_params(t, type_params))
                        .unwrap_or(Type::Any);
                    self.env.define(p.name.clone(), ty);
                    // Track param as defined (for unused checking)
                    self.defined_vars.push((p.name.clone(), stmt.span, fn_depth));

                    // In strict mode, require type annotations on params
                    if self.config.strict && p.type_ann.is_none() {
                        self.errors.push(TypeError {
                            message: format!(
                                "Parameter `{}` of function `{name}` requires a type annotation in strict mode",
                                p.name
                            ),
                            span: stmt.span,
                            expected: None,
                            found: None,
                            hint: Some(format!("Add a type annotation: `{}: <type>`", p.name)),
                        });
                    }
                }

                // Set return type for checking returns
                let prev_return = self.current_fn_return.take();
                self.current_fn_return = return_type
                    .as_ref()
                    .map(|t| convert_type_expr_with_params(t, type_params));

                // Check body with unreachable code detection
                self.check_body(body);

                // Check unused vars in this function
                self.check_unused_vars();

                self.current_fn_return = prev_return;
                self.env.pop_scope();

                // Restore outer unused-var state
                self.defined_vars = outer_defined;
                self.used_vars = outer_used;

                // Mark the function name itself as used in the outer scope
                self.used_vars.insert(name.clone());
            }

            StmtKind::Return(Some(expr)) => {
                self.mark_used_in_expr(expr);
                if let Some(expected_ret) = &self.current_fn_return {
                    let inferred = infer_expr(expr, &self.env);
                    if !is_compatible(expected_ret, &inferred) {
                        self.errors.push(TypeError {
                            message: "Return type mismatch".to_string(),
                            span: stmt.span,
                            expected: Some(expected_ret.to_string()),
                            found: Some(inferred.to_string()),
                            hint: None,
                        });
                    }
                }
            }

            StmtKind::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            } => {
                self.mark_used_in_expr(condition);
                let cond_ty = infer_expr(condition, &self.env);
                if !is_compatible(&Type::Bool, &cond_ty)
                    && !matches!(cond_ty, Type::Any | Type::Error)
                {
                    self.warnings.push(TypeError {
                        message: "Condition should be a bool".to_string(),
                        span: stmt.span,
                        expected: Some("bool".to_string()),
                        found: Some(cond_ty.to_string()),
                        hint: None,
                    });
                }
                self.env.push_scope();
                self.check_body(then_body);
                self.env.pop_scope();

                for (cond, body) in else_ifs {
                    self.mark_used_in_expr(cond);
                    let _ = infer_expr(cond, &self.env);
                    self.env.push_scope();
                    self.check_body(body);
                    self.env.pop_scope();
                }

                if let Some(body) = else_body {
                    self.env.push_scope();
                    self.check_body(body);
                    self.env.pop_scope();
                }
            }

            StmtKind::While { condition, body } => {
                self.mark_used_in_expr(condition);
                let _ = infer_expr(condition, &self.env);
                self.env.push_scope();
                self.check_body(body);
                self.env.pop_scope();
            }

            StmtKind::For { name, iter, body } => {
                self.mark_used_in_expr(iter);
                let iter_ty = infer_expr(iter, &self.env);
                let elem_ty = match &iter_ty {
                    Type::List(inner) => *inner.clone(),
                    Type::Set(inner) => *inner.clone(),
                    Type::Generator(inner) => *inner.clone(),
                    Type::Map(_) => Type::Any,
                    Type::String => Type::String,
                    Type::Any => Type::Any,
                    _ => {
                        self.warnings.push(TypeError {
                            message: format!("For-loop iterating over non-iterable type `{iter_ty}`"),
                            span: stmt.span,
                            expected: Some("list, set, generator, map, or string".to_string()),
                            found: Some(iter_ty.to_string()),
                            hint: None,
                        });
                        Type::Any
                    }
                };
                self.env.push_scope();
                self.env.define(name.clone(), elem_ty);
                // Mark loop variable as used — it's defined by the for-loop, not the user
                self.used_vars.insert(name.clone());
                self.check_body(body);
                self.env.pop_scope();
            }

            StmtKind::Expr(expr) => {
                self.mark_used_in_expr(expr);

                // Check struct init field validation
                if let Expr::StructInit { name, fields } = expr {
                    self.check_struct_init(name, fields, stmt.span);
                }

                // Check assignment type compatibility
                if let Expr::Assign { target, value } = expr {
                    self.check_assignment(target, value, stmt.span);
                }

                let _ = infer_expr(expr, &self.env);
            }

            StmtKind::TryCatch {
                try_body,
                catch_var,
                catch_body,
            } => {
                self.env.push_scope();
                self.check_body(try_body);
                self.env.pop_scope();

                self.env.push_scope();
                self.env.define(catch_var.clone(), Type::Any);
                self.used_vars.insert(catch_var.clone()); // catch vars are implicitly used
                self.check_body(catch_body);
                self.env.pop_scope();
            }

            StmtKind::Throw(expr) => {
                self.mark_used_in_expr(expr);
                let _ = infer_expr(expr, &self.env);
            }

            StmtKind::ImplBlock { methods, .. } => {
                for method in methods {
                    self.check_stmt(method);
                }
            }

            StmtKind::Test { body, .. } => {
                self.env.push_scope();
                self.check_body(body);
                self.env.pop_scope();
            }

            StmtKind::Use { item, .. } => {
                // Track imported names for unused import checking
                match item {
                    tl_ast::UseItem::Single(path) => {
                        if let Some(last) = path.last() {
                            self.imported_names.push((last.clone(), stmt.span));
                        }
                    }
                    tl_ast::UseItem::Group(_, names) => {
                        for name in names {
                            self.imported_names.push((name.clone(), stmt.span));
                        }
                    }
                    tl_ast::UseItem::Aliased(_, alias) => {
                        self.imported_names.push((alias.clone(), stmt.span));
                    }
                    tl_ast::UseItem::Wildcard(_) => {} // can't check wildcard imports
                }
            }

            StmtKind::StructDecl { name, .. } => {
                // Lint: struct naming convention — should be PascalCase
                if !is_pascal_case(name) {
                    self.warnings.push(TypeError {
                        message: format!("Struct `{name}` should be PascalCase"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Use PascalCase for struct names".to_string()),
                    });
                }
            }

            StmtKind::EnumDecl { name, .. } => {
                // Lint: enum naming convention — should be PascalCase
                if !is_pascal_case(name) {
                    self.warnings.push(TypeError {
                        message: format!("Enum `{name}` should be PascalCase"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Use PascalCase for enum names".to_string()),
                    });
                }
            }

            // Pass-through for statements we don't type check yet
            StmtKind::Return(None)
            | StmtKind::Break
            | StmtKind::Continue
            | StmtKind::Import { .. }
            | StmtKind::Schema { .. }
            | StmtKind::Train { .. }
            | StmtKind::Pipeline { .. }
            | StmtKind::StreamDecl { .. }
            | StmtKind::SourceDecl { .. }
            | StmtKind::SinkDecl { .. }
            | StmtKind::ModDecl { .. } => {}

            StmtKind::TraitDef { name, type_params: _, methods, .. } => {
                // Lint: trait naming convention — should be PascalCase
                if !is_pascal_case(name) {
                    self.warnings.push(TypeError {
                        message: format!("Trait `{name}` should be PascalCase"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: Some("Use PascalCase for trait names".to_string()),
                    });
                }

                // Register the trait
                let method_sigs: Vec<(String, Vec<Type>, Type)> = methods
                    .iter()
                    .map(|m| {
                        let param_types: Vec<Type> = m.params.iter()
                            .map(|p| p.type_ann.as_ref().map(|t| convert_type_expr(t)).unwrap_or(Type::Any))
                            .collect();
                        let ret = m.return_type.as_ref().map(|t| convert_type_expr(t)).unwrap_or(Type::Any);
                        (m.name.clone(), param_types, ret)
                    })
                    .collect();
                self.env.define_trait(
                    name.clone(),
                    TraitInfo {
                        name: name.clone(),
                        methods: method_sigs,
                        supertrait: None,
                    },
                );
            }

            StmtKind::TraitImpl { trait_name, type_name, methods, .. } => {
                // Validate the trait exists
                if let Some(trait_info) = self.env.lookup_trait(trait_name).cloned() {
                    // Check all required methods are provided
                    let provided: Vec<String> = methods.iter().filter_map(|m| {
                        if let StmtKind::FnDecl { name, .. } = &m.kind {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }).collect();

                    for (required_method, _, _) in &trait_info.methods {
                        if !provided.contains(required_method) {
                            self.errors.push(TypeError {
                                message: format!(
                                    "Missing method `{required_method}` in impl `{trait_name}` for `{type_name}`"
                                ),
                                span: stmt.span,
                                expected: None,
                                found: None,
                                hint: Some(format!("Trait `{trait_name}` requires method `{required_method}`")),
                            });
                        }
                    }

                    // Register the trait impl
                    self.env.register_trait_impl(
                        trait_name.clone(),
                        type_name.clone(),
                        provided,
                    );
                } else {
                    self.errors.push(TypeError {
                        message: format!("Unknown trait `{trait_name}`"),
                        span: stmt.span,
                        expected: None,
                        found: None,
                        hint: None,
                    });
                }

                // Check method bodies
                for method in methods {
                    self.check_stmt(method);
                }
            }
        }
    }

    /// Validate struct initialization fields.
    fn check_struct_init(&mut self, name: &str, fields: &[(String, Expr)], span: Span) {
        if let Some(declared_fields) = self.env.lookup_struct(name).cloned() {
            // Check for unknown fields
            for (field_name, _) in fields {
                if !declared_fields.iter().any(|(f, _)| f == field_name) {
                    self.errors.push(TypeError {
                        message: format!("Unknown field `{field_name}` in struct `{name}`"),
                        span,
                        expected: None,
                        found: None,
                        hint: Some(format!(
                            "Available fields: {}",
                            declared_fields
                                .iter()
                                .map(|(f, _)| f.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )),
                    });
                }
            }

            // Check field types match
            for (field_name, field_value) in fields {
                if let Some((_, expected_ty)) = declared_fields.iter().find(|(f, _)| f == field_name)
                {
                    let inferred = infer_expr(field_value, &self.env);
                    if !is_compatible(expected_ty, &inferred) {
                        self.errors.push(TypeError {
                            message: format!(
                                "Type mismatch for field `{field_name}` in struct `{name}`"
                            ),
                            span,
                            expected: Some(expected_ty.to_string()),
                            found: Some(inferred.to_string()),
                            hint: None,
                        });
                    }
                }
            }
        }
    }

    /// Check assignment type compatibility.
    fn check_assignment(&mut self, target: &Expr, value: &Expr, span: Span) {
        let target_ty = infer_expr(target, &self.env);
        let value_ty = infer_expr(value, &self.env);
        // Only check if target has a known (non-Any) type
        if !matches!(target_ty, Type::Any | Type::Error) && !is_compatible(&target_ty, &value_ty) {
            self.warnings.push(TypeError {
                message: "Assignment type mismatch".to_string(),
                span,
                expected: Some(target_ty.to_string()),
                found: Some(value_ty.to_string()),
                hint: None,
            });
        }
    }

    /// Emit warnings for unused variables.
    fn check_unused_vars(&mut self) {
        for (name, span, _depth) in &self.defined_vars {
            // Skip variables starting with _ (convention for intentionally unused)
            if name.starts_with('_') {
                continue;
            }
            if !self.used_vars.contains(name) {
                self.warnings.push(TypeError {
                    message: format!("Unused variable `{name}`"),
                    span: *span,
                    expected: None,
                    found: None,
                    hint: Some(format!("Prefix with `_` to suppress: `_{name}`")),
                });
            }
        }
    }

    /// Emit warnings for unused imports.
    fn check_unused_imports(&mut self) {
        for (name, span) in &self.imported_names {
            if !self.used_imports.contains(name) {
                self.warnings.push(TypeError {
                    message: format!("Unused import `{name}`"),
                    span: *span,
                    expected: None,
                    found: None,
                    hint: Some("Remove unused import".to_string()),
                });
            }
        }
    }
}

/// Check match arms for exhaustiveness on typed enums/result/option.
pub fn check_match_exhaustiveness(
    subject_type: &Type,
    arm_patterns: &[&str],
    env: &TypeEnv,
) -> Vec<String> {
    let mut missing = Vec::new();

    match subject_type {
        Type::Result(_, _) => {
            if !arm_patterns.iter().any(|p| *p == "Ok") {
                missing.push("Ok".to_string());
            }
            if !arm_patterns.iter().any(|p| *p == "Err") {
                missing.push("Err".to_string());
            }
        }
        Type::Option(_) => {
            if !arm_patterns.iter().any(|p| *p == "none" || *p == "_") {
                missing.push("none".to_string());
            }
        }
        Type::Enum(name) => {
            if let Some(variants) = env.lookup_enum(name) {
                for (variant_name, _) in variants {
                    if !arm_patterns
                        .iter()
                        .any(|p| p == variant_name || *p == "_")
                    {
                        missing.push(variant_name.clone());
                    }
                }
            }
        }
        _ => {}
    }

    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_check(source: &str) -> CheckResult {
        let program = tl_parser::parse(source).unwrap();
        check_program(&program, &CheckerConfig::default())
    }

    fn parse_and_check_strict(source: &str) -> CheckResult {
        let program = tl_parser::parse(source).unwrap();
        check_program(&program, &CheckerConfig { strict: true })
    }

    #[test]
    fn test_correct_let_int() {
        let result = parse_and_check("let x: int = 42\nprint(x)");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_correct_let_string() {
        let result = parse_and_check("let s: string = \"hello\"\nprint(s)");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_mismatch_let() {
        let result = parse_and_check("let x: int = \"hello\"\nprint(x)");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("mismatch"));
    }

    #[test]
    fn test_gradual_untyped() {
        // Untyped code should always pass
        let result = parse_and_check("let x = 42\nlet y = \"hello\"\nprint(x)\nprint(y)");
        assert!(!result.has_errors());
    }

    #[test]
    fn test_function_return_type() {
        let result = parse_and_check("fn f() -> int { return \"hello\" }");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Return type"));
    }

    #[test]
    fn test_function_correct_return() {
        let result = parse_and_check("fn f() -> int { return 42 }");
        assert!(!result.has_errors());
    }

    #[test]
    fn test_gradual_function_no_annotations() {
        let result = parse_and_check("fn f(a, b) { return a + b }");
        assert!(!result.has_errors());
    }

    #[test]
    fn test_strict_mode_requires_param_types() {
        let result = parse_and_check_strict("fn f(a, b) { return a + b }");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("requires a type annotation"));
    }

    #[test]
    fn test_strict_mode_with_annotations() {
        let result = parse_and_check_strict("fn f(a: int, b: int) -> int { return a + b }");
        assert!(!result.has_errors());
    }

    #[test]
    fn test_option_none_compatible() {
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Option(Box::new(Type::Int)));
        assert!(is_compatible(
            &Type::Option(Box::new(Type::Int)),
            &Type::None
        ));
    }

    #[test]
    fn test_int_float_promotion() {
        let result = parse_and_check("let x: float = 42\nprint(x)");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_match_exhaustiveness_result() {
        let env = TypeEnv::new();
        let ty = Type::Result(Box::new(Type::Int), Box::new(Type::String));
        let missing = check_match_exhaustiveness(&ty, &["Ok"], &env);
        assert_eq!(missing, vec!["Err"]);

        let missing = check_match_exhaustiveness(&ty, &["Ok", "Err"], &env);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_match_exhaustiveness_enum() {
        let mut env = TypeEnv::new();
        env.define_enum(
            "Color".into(),
            vec![
                ("Red".into(), vec![]),
                ("Green".into(), vec![]),
                ("Blue".into(), vec![]),
            ],
        );
        let ty = Type::Enum("Color".into());
        let missing = check_match_exhaustiveness(&ty, &["Red", "Green"], &env);
        assert_eq!(missing, vec!["Blue"]);
    }

    // ── Phase 12: Generics & Traits ──────────────────────────

    #[test]
    fn test_generic_fn_type_params() {
        let result = parse_and_check("fn identity<T>(x: T) -> T { return x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_generic_fn_no_errors_untyped() {
        // Gradual: untyped generic code always passes
        let result = parse_and_check("fn identity<T>(x) { return x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_trait_def_registered() {
        let result = parse_and_check("trait Display { fn show(self) -> string }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_trait_impl_validates_methods() {
        let result = parse_and_check(
            "trait Display { fn show(self) -> string }\nimpl Display for Point { fn show(self) -> string { \"point\" } }"
        );
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_trait_impl_missing_method() {
        let result = parse_and_check(
            "trait Display { fn show(self) -> string }\nimpl Display for Point { fn other(self) { 1 } }"
        );
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Missing method"));
    }

    #[test]
    fn test_unknown_trait_in_impl() {
        let result = parse_and_check(
            "impl FooTrait for Point { fn bar(self) { 1 } }"
        );
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Unknown trait"));
    }

    #[test]
    fn test_unknown_trait_in_bound() {
        let result = parse_and_check("fn foo<T: UnknownTrait>(x: T) { x }");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Unknown trait"));
    }

    #[test]
    fn test_builtin_trait_bound_accepted() {
        let result = parse_and_check("fn foo<T: Comparable>(x: T) { x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_multiple_bounds() {
        let result = parse_and_check("fn foo<T: Comparable + Hashable>(x: T) { x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_where_clause_validation() {
        let result = parse_and_check("fn foo<T>(x: T) where T: Comparable { x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_undeclared_type_param_in_bound() {
        let result = parse_and_check("fn foo<T>(x: T) where U: Comparable { x }");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("undeclared type parameter"));
    }

    #[test]
    fn test_builtin_traits_registered() {
        let env = TypeEnv::new();
        assert!(env.lookup_trait("Numeric").is_some());
        assert!(env.lookup_trait("Comparable").is_some());
        assert!(env.lookup_trait("Hashable").is_some());
        assert!(env.lookup_trait("Displayable").is_some());
        assert!(env.lookup_trait("Serializable").is_some());
        assert!(env.lookup_trait("Default").is_some());
    }

    #[test]
    fn test_type_satisfies_numeric() {
        let env = TypeEnv::new();
        assert!(env.type_satisfies_trait(&Type::Int, "Numeric"));
        assert!(env.type_satisfies_trait(&Type::Float, "Numeric"));
        assert!(!env.type_satisfies_trait(&Type::String, "Numeric"));
    }

    #[test]
    fn test_type_satisfies_comparable() {
        let env = TypeEnv::new();
        assert!(env.type_satisfies_trait(&Type::Int, "Comparable"));
        assert!(env.type_satisfies_trait(&Type::String, "Comparable"));
        assert!(!env.type_satisfies_trait(&Type::Bool, "Comparable"));
    }

    #[test]
    fn test_strict_mode_with_generics() {
        // In strict mode, params still need annotations — type params count as annotations
        let result = parse_and_check_strict("fn identity<T>(x: T) -> T { return x }");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    // ── Phase 13: Enhanced Checking ──────────────────────────

    #[test]
    fn test_struct_init_correct_fields() {
        let result = parse_and_check(
            "struct Point { x: int, y: int }\nlet p = Point { x: 1, y: 2 }\nprint(p)",
        );
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_struct_init_unknown_field() {
        let result = parse_and_check(
            "struct Point { x: int, y: int }\nlet p = Point { x: 1, z: 2 }\nprint(p)",
        );
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Unknown field `z`"));
    }

    #[test]
    fn test_struct_init_wrong_field_type() {
        let result = parse_and_check(
            "struct Point { x: int, y: int }\nlet p = Point { x: 1, y: \"hello\" }\nprint(p)",
        );
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("Type mismatch for field `y`"));
    }

    #[test]
    fn test_assignment_type_mismatch() {
        let result = parse_and_check("let mut x: int = 42\nx = \"hello\"");
        // Assignment type mismatch is a warning in gradual mode
        let assign_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Assignment type mismatch"))
            .collect();
        assert!(
            !assign_warnings.is_empty(),
            "Expected assignment type mismatch warning. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_unused_variable_warning() {
        let result = parse_and_check("let x = 42");
        let unused_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unused variable `x`"))
            .collect();
        assert!(
            !unused_warnings.is_empty(),
            "Expected unused variable warning. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_underscore_prefix_no_warning() {
        let result = parse_and_check("let _x = 42");
        let unused_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unused variable"))
            .collect();
        assert!(
            unused_warnings.is_empty(),
            "Should not warn for _-prefixed variables. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_used_variable_no_warning() {
        let result = parse_and_check("let x = 42\nprint(x)");
        let unused_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unused variable `x`"))
            .collect();
        assert!(
            unused_warnings.is_empty(),
            "Should not warn for used variables. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_unreachable_code_after_return() {
        let result = parse_and_check("fn f() {\n  return 1\n  print(\"unreachable\")\n}");
        let unreachable: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unreachable code"))
            .collect();
        assert!(
            !unreachable.is_empty(),
            "Expected unreachable code warning. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_unreachable_code_after_break() {
        let result = parse_and_check("fn f() {\n  while true {\n    break\n    print(\"x\")\n  }\n}");
        let unreachable: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unreachable code"))
            .collect();
        assert!(
            !unreachable.is_empty(),
            "Expected unreachable code warning after break. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_for_loop_non_iterable_warning() {
        let result = parse_and_check("let x: int = 42\nfor i in x { print(i) }");
        let warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("non-iterable"))
            .collect();
        assert!(
            !warnings.is_empty(),
            "Expected non-iterable warning. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_multiple_warnings_accumulated() {
        let result = parse_and_check("let x = 42\nlet y = 43");
        let unused_warnings: Vec<_> = result
            .warnings
            .iter()
            .filter(|w| w.message.contains("Unused variable"))
            .collect();
        assert_eq!(
            unused_warnings.len(),
            2,
            "Expected 2 unused variable warnings. warnings: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_all_existing_patterns_pass() {
        // Verify various existing patterns still work without errors
        let result = parse_and_check("fn f(a, b) { return a + b }");
        assert!(!result.has_errors());

        let result = parse_and_check("let xs = [1, 2, 3]\nprint(xs)");
        assert!(!result.has_errors());
    }

    // ── Phase 14: Lint Rules ──────────────────────────

    #[test]
    fn test_snake_case_function_no_warning() {
        let result = parse_and_check("fn my_func() { 1 }");
        let naming: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("snake_case")).collect();
        assert!(naming.is_empty(), "snake_case function should not produce naming warning");
    }

    #[test]
    fn test_camel_case_function_warning() {
        let result = parse_and_check("fn myFunc() { 1 }");
        let naming: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("snake_case")).collect();
        assert!(!naming.is_empty(), "camelCase function should produce naming warning");
    }

    #[test]
    fn test_pascal_case_struct_no_warning() {
        let result = parse_and_check("struct MyStruct { x: int }");
        let naming: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("PascalCase")).collect();
        assert!(naming.is_empty(), "PascalCase struct should not produce naming warning");
    }

    #[test]
    fn test_lowercase_struct_warning() {
        let result = parse_and_check("struct my_struct { x: int }");
        let naming: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("PascalCase")).collect();
        assert!(!naming.is_empty(), "lowercase struct should produce naming warning");
    }

    #[test]
    fn test_variable_shadowing_warning() {
        let result = parse_and_check("let x = 1\nlet x = 2\nprint(x)");
        let shadow: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("shadows")).collect();
        assert!(!shadow.is_empty(), "Shadowed variable should produce warning: {:?}", result.warnings);
    }

    #[test]
    fn test_underscore_shadow_no_warning() {
        let result = parse_and_check("let _x = 1\nlet _x = 2");
        let shadow: Vec<_> = result.warnings.iter().filter(|w| w.message.contains("shadows")).collect();
        assert!(shadow.is_empty(), "_-prefixed shadow should not warn: {:?}", result.warnings);
    }
}
