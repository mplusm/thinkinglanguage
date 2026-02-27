// ThinkingLanguage — Type Checker
// Licensed under MIT OR Apache-2.0
//
// Walks the AST, builds type environment, infers types from expressions,
// and checks annotations. Gradual: unannotated code = `any`, always passes.

use tl_ast::{Program, Stmt, StmtKind};
use tl_errors::Span;

use crate::convert::convert_type_expr;
use crate::infer::infer_expr;
use crate::{FnSig, Type, TypeEnv, is_compatible};

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
    };

    // First pass: register all top-level functions and types
    for stmt in &program.statements {
        checker.register_top_level(stmt);
    }

    // Second pass: check all statements
    for stmt in &program.statements {
        checker.check_stmt(stmt);
    }

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
}

impl<'a> TypeChecker<'a> {
    fn register_top_level(&mut self, stmt: &Stmt) {
        match &stmt.kind {
            StmtKind::FnDecl {
                name,
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
                            .map(|t| convert_type_expr(t))
                            .unwrap_or(Type::Any);
                        (p.name.clone(), ty)
                    })
                    .collect();
                let ret = return_type
                    .as_ref()
                    .map(|t| convert_type_expr(t))
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
                                .map(|t| convert_type_expr(t))
                                .unwrap_or(Type::Any)
                        })
                        .collect(),
                    ret: Box::new(
                        return_type
                            .as_ref()
                            .map(|t| convert_type_expr(t))
                            .unwrap_or(Type::Any),
                    ),
                };
                self.env.define(name.clone(), fn_type);
            }
            StmtKind::StructDecl { name, fields, .. } => {
                let field_types: Vec<(String, Type)> = fields
                    .iter()
                    .map(|f| (f.name.clone(), convert_type_expr(&f.type_ann)))
                    .collect();
                self.env.define_struct(name.clone(), field_types);
                self.env.define(name.clone(), Type::Struct(name.clone()));
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
            }
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
            }

            StmtKind::FnDecl {
                name,
                params,
                return_type,
                body,
                ..
            } => {
                self.env.push_scope();

                // Bind parameters
                for p in params {
                    let ty = p
                        .type_ann
                        .as_ref()
                        .map(|t| convert_type_expr(t))
                        .unwrap_or(Type::Any);
                    self.env.define(p.name.clone(), ty);

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
                    .map(|t| convert_type_expr(t));

                // Check body
                for s in body {
                    self.check_stmt(s);
                }

                self.current_fn_return = prev_return;
                self.env.pop_scope();
            }

            StmtKind::Return(Some(expr)) => {
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
                for s in then_body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();

                for (cond, body) in else_ifs {
                    let _ = infer_expr(cond, &self.env);
                    self.env.push_scope();
                    for s in body {
                        self.check_stmt(s);
                    }
                    self.env.pop_scope();
                }

                if let Some(body) = else_body {
                    self.env.push_scope();
                    for s in body {
                        self.check_stmt(s);
                    }
                    self.env.pop_scope();
                }
            }

            StmtKind::While { condition, body } => {
                let _ = infer_expr(condition, &self.env);
                self.env.push_scope();
                for s in body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();
            }

            StmtKind::For { name, iter, body } => {
                let iter_ty = infer_expr(iter, &self.env);
                let elem_ty = match iter_ty {
                    Type::List(inner) => *inner,
                    Type::Set(inner) => *inner,
                    Type::Generator(inner) => *inner,
                    _ => Type::Any,
                };
                self.env.push_scope();
                self.env.define(name.clone(), elem_ty);
                for s in body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();
            }

            StmtKind::Expr(expr) => {
                let _ = infer_expr(expr, &self.env);
            }

            StmtKind::TryCatch {
                try_body,
                catch_var,
                catch_body,
            } => {
                self.env.push_scope();
                for s in try_body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();

                self.env.push_scope();
                self.env.define(catch_var.clone(), Type::Any);
                for s in catch_body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();
            }

            StmtKind::Throw(expr) => {
                let _ = infer_expr(expr, &self.env);
            }

            StmtKind::ImplBlock { methods, .. } => {
                for method in methods {
                    self.check_stmt(method);
                }
            }

            StmtKind::Test { body, .. } => {
                self.env.push_scope();
                for s in body {
                    self.check_stmt(s);
                }
                self.env.pop_scope();
            }

            // Pass-through for statements we don't type check yet
            StmtKind::Return(None)
            | StmtKind::Break
            | StmtKind::Continue
            | StmtKind::Import { .. }
            | StmtKind::Schema { .. }
            | StmtKind::StructDecl { .. }
            | StmtKind::EnumDecl { .. }
            | StmtKind::Train { .. }
            | StmtKind::Pipeline { .. }
            | StmtKind::StreamDecl { .. }
            | StmtKind::SourceDecl { .. }
            | StmtKind::SinkDecl { .. }
            | StmtKind::Use { .. }
            | StmtKind::ModDecl { .. } => {}
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
        let result = parse_and_check("let x: int = 42");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_correct_let_string() {
        let result = parse_and_check("let s: string = \"hello\"");
        assert!(!result.has_errors(), "errors: {:?}", result.errors);
    }

    #[test]
    fn test_mismatch_let() {
        let result = parse_and_check("let x: int = \"hello\"");
        assert!(result.has_errors());
        assert!(result.errors[0].message.contains("mismatch"));
    }

    #[test]
    fn test_gradual_untyped() {
        // Untyped code should always pass
        let result = parse_and_check("let x = 42\nlet y = \"hello\"");
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
        // This tests that `none` literal is compatible with `int?`
        // We need the parser to support T? syntax for this to work via annotation
        // For now, test through the checker logic directly
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Option(Box::new(Type::Int)));
        assert!(is_compatible(
            &Type::Option(Box::new(Type::Int)),
            &Type::None
        ));
    }

    #[test]
    fn test_int_float_promotion() {
        let result = parse_and_check("let x: float = 42");
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
}
