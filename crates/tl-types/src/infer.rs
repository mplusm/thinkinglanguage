// ThinkingLanguage — Expression Type Inference
// Licensed under MIT OR Apache-2.0
//
// Infers types from expressions using local, forward-only rules.

use tl_ast::{BinOp, Expr, UnaryOp};

use crate::convert::convert_type_expr;
use crate::{Type, TypeEnv};

/// Infer the type of an expression given the current type environment.
pub fn infer_expr(expr: &Expr, env: &TypeEnv) -> Type {
    match expr {
        // Literals
        Expr::Int(_) => Type::Int,
        Expr::Float(_) => Type::Float,
        Expr::String(_) => Type::String,
        Expr::Bool(_) => Type::Bool,
        Expr::None => Type::None,

        // Variable reference
        Expr::Ident(name) => env.lookup(name).cloned().unwrap_or(Type::Any),

        // Binary operations
        Expr::BinOp { left, op, right } => infer_binop(left, op, right, env),

        // Unary operations
        Expr::UnaryOp { op, expr } => infer_unaryop(op, expr, env),

        // Function call — including method calls (Call { function: Member { .. }, .. })
        Expr::Call { function, args } => {
            // Method call: obj.method(args)
            if let Expr::Member { object, field } = function.as_ref() {
                let obj_ty = infer_expr(object, env);
                return infer_method_call(&obj_ty, field, args, env);
            }

            if let Expr::Ident(name) = function.as_ref() {
                // Check known builtins
                match name.as_str() {
                    "Ok" => Type::Result(Box::new(Type::Any), Box::new(Type::Any)),
                    "Err" => Type::Result(Box::new(Type::Any), Box::new(Type::Any)),
                    "is_ok" | "is_err" => Type::Bool,
                    "unwrap" => Type::Any,
                    "set_from" => Type::Set(Box::new(Type::Any)),
                    "len" | "int" => Type::Int,
                    "float" => Type::Float,
                    "str" | "type_of" => Type::String,
                    "bool" => Type::Bool,
                    // Phase 13: more builtin return types
                    "range" => Type::List(Box::new(Type::Int)),
                    "print" | "println" | "push" | "append" | "write_file" | "append_file" => {
                        Type::Unit
                    }
                    "split" => Type::List(Box::new(Type::String)),
                    "json_parse" => Type::Any,
                    "json_stringify" | "read_file" => Type::String,
                    "channel" => Type::Channel(Box::new(Type::Any)),
                    "spawn" => Type::Task(Box::new(Type::Any)),
                    "map" => {
                        // map(list, fn) -> list
                        if !args.is_empty() {
                            let arg_ty = infer_expr(&args[0], env);
                            match arg_ty {
                                Type::List(_) => arg_ty,
                                _ => Type::List(Box::new(Type::Any)),
                            }
                        } else {
                            Type::List(Box::new(Type::Any))
                        }
                    }
                    "filter" => {
                        if !args.is_empty() {
                            let arg_ty = infer_expr(&args[0], env);
                            match arg_ty {
                                Type::List(_) => arg_ty,
                                _ => Type::List(Box::new(Type::Any)),
                            }
                        } else {
                            Type::List(Box::new(Type::Any))
                        }
                    }
                    "now" => Type::String,
                    "date_format" | "date_parse" => Type::String,
                    "regex_match" => Type::Bool,
                    "regex_find" => Type::List(Box::new(Type::String)),
                    "sleep" => Type::Unit,
                    "env_get" => Type::String,
                    "env_set" => Type::Unit,
                    "send" | "recv" | "try_recv" => Type::Any,
                    "await_all" => Type::List(Box::new(Type::Any)),
                    "collect" | "gen_collect" => Type::List(Box::new(Type::Any)),
                    "iter" => Type::Generator(Box::new(Type::Any)),
                    "next" => Type::Any,
                    "is_generator" | "file_exists" => Type::Bool,
                    "assert" | "assert_eq" => Type::Unit,
                    // Phase 15: Data Quality & Connectors
                    "fill_null" | "drop_null" | "dedup" | "clamp" | "data_profile" | "read_mysql" => Type::Table { name: None, columns: None },
                    // Phase 22: Advanced Types
                    "decimal" => Type::Decimal,
                    "tensor" | "tensor_zeros" | "tensor_ones" | "tensor_reshape" | "tensor_transpose"
                    | "tensor_dot" => Type::Tensor,
                    "tensor_shape" => Type::List(Box::new(Type::Int)),
                    "tensor_sum" | "tensor_mean" => Type::Float,
                    "row_count" | "levenshtein" => Type::Int,
                    "null_rate" => Type::Float,
                    "is_unique" | "is_email" | "is_url" | "is_phone" | "is_between" => Type::Bool,
                    "soundex" | "redis_connect" => Type::String,
                    "graphql_query" => Type::Any,
                    "redis_get" => Type::Any,
                    "redis_set" | "redis_del" | "register_s3" => Type::Unit,
                    // Phase 21: Schema Evolution
                    "schema_register" => Type::Unit,
                    "schema_get" | "schema_latest" => Type::Any,
                    "schema_history" | "schema_versions" => Type::List(Box::new(Type::Int)),
                    "schema_check" | "schema_diff" | "schema_fields" => Type::List(Box::new(Type::String)),
                    "schema_apply_migration" => Type::Unit,
                    // Phase 20: Python FFI
                    "py_import" => Type::PyObject,
                    "py_eval" | "py_call" | "py_getattr" | "py_to_tl" => Type::Any,
                    "py_setattr" => Type::Unit,
                    // Phase 23: Security & Access Control
                    "secret_get" => Type::String,
                    "secret_set" | "secret_delete" => Type::Unit,
                    "secret_list" => Type::List(Box::new(Type::String)),
                    "check_permission" => Type::Bool,
                    "mask_email" | "mask_phone" | "mask_cc" | "redact" | "hash" => Type::String,
                    // Phase 24: Async/Await
                    "async_read_file" | "async_http_get" | "async_http_post" => Type::Task(Box::new(Type::String)),
                    "async_write_file" | "async_sleep" => Type::Task(Box::new(Type::Unit)),
                    "select" | "race_all" => Type::Any,
                    "async_map" => Type::List(Box::new(Type::Any)),
                    "async_filter" => Type::List(Box::new(Type::Any)),
                    _ => {
                        if let Some(sig) = env.lookup_fn(name) {
                            sig.ret.clone()
                        } else {
                            Type::Any
                        }
                    }
                }
            } else {
                // Calling a closure or other expression
                let fn_ty = infer_expr(function, env);
                match fn_ty {
                    Type::Function { ret, .. } => *ret,
                    _ => Type::Any,
                }
            }
        }

        // List literal
        Expr::List(elements) => {
            if elements.is_empty() {
                Type::List(Box::new(Type::Any))
            } else {
                let elem_ty = infer_expr(&elements[0], env);
                Type::List(Box::new(elem_ty))
            }
        }

        // Map literal — infer value type from first entry
        Expr::Map(entries) => {
            if entries.is_empty() {
                Type::Map(Box::new(Type::Any))
            } else {
                let val_ty = infer_expr(&entries[0].1, env);
                Type::Map(Box::new(val_ty))
            }
        }

        // Member access — look up struct field types and known type methods
        Expr::Member { object, field } => {
            let obj_ty = infer_expr(object, env);
            infer_member_access(&obj_ty, field, env)
        }

        // Index access
        Expr::Index { object, .. } => {
            let obj_ty = infer_expr(object, env);
            match obj_ty {
                Type::List(inner) => *inner,
                Type::Map(inner) => *inner,
                _ => Type::Any,
            }
        }

        // Closure — infer param types from annotations and return type from body
        Expr::Closure { params, body, return_type, .. } => {
            let param_types: Vec<Type> = params
                .iter()
                .map(|p| {
                    p.type_ann
                        .as_ref()
                        .map(|t| convert_type_expr(t))
                        .unwrap_or(Type::Any)
                })
                .collect();
            let ret = match body {
                tl_ast::ClosureBody::Expr(e) => infer_expr(e, env),
                tl_ast::ClosureBody::Block { expr: Some(e), .. } => infer_expr(e, env),
                tl_ast::ClosureBody::Block { expr: None, .. } => {
                    // If no tail expr, use return_type annotation or None
                    return_type.as_ref().map(|t| convert_type_expr(t)).unwrap_or(Type::None)
                }
            };
            Type::Function { params: param_types, ret: Box::new(ret) }
        }

        // Null coalesce: option<T> ?? T -> T
        Expr::NullCoalesce { expr, default } => {
            let expr_ty = infer_expr(expr, env);
            match expr_ty {
                Type::Option(inner) => *inner,
                _ => infer_expr(default, env),
            }
        }

        // Try operator: result<T,E>? -> T
        Expr::Try(inner) => {
            let inner_ty = infer_expr(inner, env);
            match inner_ty {
                Type::Result(ok, _) => *ok,
                Type::Option(inner_t) => *inner_t,
                _ => Type::Any,
            }
        }

        // Await
        Expr::Await(inner) => {
            let inner_ty = infer_expr(inner, env);
            match inner_ty {
                Type::Task(inner) => *inner,
                _ => Type::Any,
            }
        }

        // Yield
        Expr::Yield(_) => Type::Any,

        // Range
        Expr::Range { .. } => Type::List(Box::new(Type::Int)),

        // Pipe
        Expr::Pipe { right, .. } => infer_expr(right, env),

        // Block
        Expr::Block { expr, .. } => {
            if let Some(e) = expr {
                infer_expr(e, env)
            } else {
                Type::Unit
            }
        }

        // Assignment
        Expr::Assign { value, .. } => infer_expr(value, env),

        // Struct init
        Expr::StructInit { name, .. } => Type::Struct(name.clone()),

        // Enum variant
        Expr::EnumVariant { enum_name, .. } => Type::Enum(enum_name.clone()),

        // Match/Case
        Expr::Match { arms, .. } | Expr::Case { arms } => {
            if let Some(arm) = arms.first() {
                infer_expr(&arm.body, env)
            } else {
                Type::Any
            }
        }

        _ => Type::Any,
    }
}

/// Infer the type of a member access (obj.field).
fn infer_member_access(obj_ty: &Type, field: &str, env: &TypeEnv) -> Type {
    match obj_ty {
        Type::Struct(name) => {
            if let Some(fields) = env.lookup_struct(name) {
                fields
                    .iter()
                    .find(|(f, _)| f == field)
                    .map(|(_, ty)| ty.clone())
                    .unwrap_or(Type::Any)
            } else {
                Type::Any
            }
        }
        Type::String => match field {
            "len" | "length" => Type::Int,
            "chars" => Type::List(Box::new(Type::String)),
            "split" => Type::Function {
                params: vec![Type::String],
                ret: Box::new(Type::List(Box::new(Type::String))),
            },
            "trim" | "upper" | "lower" | "reverse" | "repeat" | "substring" | "pad_left"
            | "pad_right" => Type::String,
            "contains" | "starts_with" | "ends_with" => Type::Bool,
            "index_of" => Type::Int,
            _ => Type::Any,
        },
        Type::List(inner) => match field {
            "len" | "length" => Type::Int,
            "contains" => Type::Bool,
            "index_of" => Type::Int,
            "sort" | "reverse" | "slice" | "flat_map" => Type::List(inner.clone()),
            "first" | "last" => *inner.clone(),
            _ => Type::Any,
        },
        Type::Map(_) => match field {
            "len" => Type::Int,
            "keys" => Type::List(Box::new(Type::String)),
            "values" => Type::List(Box::new(Type::Any)),
            "contains_key" => Type::Bool,
            _ => Type::Any,
        },
        Type::Set(inner) => match field {
            "len" => Type::Int,
            "contains" => Type::Bool,
            "union" | "intersection" | "difference" => Type::Set(inner.clone()),
            _ => Type::Any,
        },
        _ => Type::Any,
    }
}

/// Infer the return type of a method call (obj.method(args)).
fn infer_method_call(obj_ty: &Type, method: &str, _args: &[Expr], env: &TypeEnv) -> Type {
    match obj_ty {
        Type::String => match method {
            "len" | "length" | "index_of" => Type::Int,
            "split" => Type::List(Box::new(Type::String)),
            "chars" => Type::List(Box::new(Type::String)),
            "trim" | "upper" | "lower" | "reverse" | "repeat" | "replace" | "substring"
            | "pad_left" | "pad_right" => Type::String,
            "contains" | "starts_with" | "ends_with" => Type::Bool,
            _ => Type::Any,
        },
        Type::List(inner) => match method {
            "len" | "length" | "index_of" => Type::Int,
            "contains" => Type::Bool,
            "push" | "append" => Type::Unit,
            "map" | "filter" | "sort" | "reverse" | "slice" | "flat_map" => {
                Type::List(inner.clone())
            }
            "sum" => *inner.clone(),
            "collect" => Type::List(inner.clone()),
            "join" => Type::String,
            "first" | "last" => *inner.clone(),
            _ => Type::Any,
        },
        Type::Map(_val_ty) => match method {
            "len" => Type::Int,
            "keys" => Type::List(Box::new(Type::String)),
            "values" => Type::List(Box::new(Type::Any)),
            "contains_key" => Type::Bool,
            "remove" => Type::Unit,
            _ => Type::Any,
        },
        Type::Set(inner) => match method {
            "len" => Type::Int,
            "contains" => Type::Bool,
            "add" | "remove" => Type::Unit,
            "union" | "intersection" | "difference" => Type::Set(inner.clone()),
            _ => Type::Any,
        },
        Type::Generator(inner) => match method {
            "next" => *inner.clone(),
            "collect" => Type::List(inner.clone()),
            _ => Type::Any,
        },
        Type::Struct(name) => {
            // Look up method in impl blocks via the function registry
            let mangled = format!("{name}::{method}");
            if let Some(sig) = env.lookup_fn(&mangled) {
                sig.ret.clone()
            } else {
                Type::Any
            }
        }
        _ => Type::Any,
    }
}

fn infer_binop(left: &Expr, op: &BinOp, right: &Expr, env: &TypeEnv) -> Type {
    let left_ty = infer_expr(left, env);
    let right_ty = infer_expr(right, env);

    match op {
        // Arithmetic
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Pow => {
            match (&left_ty, &right_ty) {
                (Type::Int, Type::Int) => Type::Int,
                (Type::Float, Type::Float) | (Type::Int, Type::Float) | (Type::Float, Type::Int) => {
                    Type::Float
                }
                // Decimal arithmetic
                (Type::Decimal, Type::Decimal) | (Type::Decimal, Type::Int) | (Type::Int, Type::Decimal) => {
                    Type::Decimal
                }
                // Decimal + Float => Float
                (Type::Decimal, Type::Float) | (Type::Float, Type::Decimal) => Type::Float,
                (Type::String, Type::String) if matches!(op, BinOp::Add) => Type::String,
                _ => {
                    if matches!(left_ty, Type::Any) || matches!(right_ty, Type::Any) {
                        Type::Any
                    } else {
                        Type::Error
                    }
                }
            }
        }
        // Comparison
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => Type::Bool,
        // Logical
        BinOp::And | BinOp::Or => Type::Bool,
    }
}

fn infer_unaryop(op: &UnaryOp, expr: &Expr, env: &TypeEnv) -> Type {
    let inner_ty = infer_expr(expr, env);
    match op {
        UnaryOp::Neg => match inner_ty {
            Type::Int => Type::Int,
            Type::Float => Type::Float,
            _ => Type::Any,
        },
        UnaryOp::Not => Type::Bool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FnSig;

    #[test]
    fn test_infer_literals() {
        let env = TypeEnv::new();
        assert_eq!(infer_expr(&Expr::Int(42), &env), Type::Int);
        assert_eq!(infer_expr(&Expr::Float(3.14), &env), Type::Float);
        assert_eq!(
            infer_expr(&Expr::String("hello".into()), &env),
            Type::String
        );
        assert_eq!(infer_expr(&Expr::Bool(true), &env), Type::Bool);
        assert_eq!(infer_expr(&Expr::None, &env), Type::None);
    }

    #[test]
    fn test_infer_binop_arithmetic() {
        let env = TypeEnv::new();
        let expr = Expr::BinOp {
            left: Box::new(Expr::Int(1)),
            op: BinOp::Add,
            right: Box::new(Expr::Int(2)),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Int);

        let expr = Expr::BinOp {
            left: Box::new(Expr::Int(1)),
            op: BinOp::Add,
            right: Box::new(Expr::Float(2.0)),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Float);
    }

    #[test]
    fn test_infer_binop_comparison() {
        let env = TypeEnv::new();
        let expr = Expr::BinOp {
            left: Box::new(Expr::Int(1)),
            op: BinOp::Lt,
            right: Box::new(Expr::Int(2)),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Bool);
    }

    #[test]
    fn test_infer_variable() {
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Int);
        assert_eq!(infer_expr(&Expr::Ident("x".into()), &env), Type::Int);
        // Unknown variable -> Any
        assert_eq!(infer_expr(&Expr::Ident("y".into()), &env), Type::Any);
    }

    #[test]
    fn test_infer_list() {
        let env = TypeEnv::new();
        let expr = Expr::List(vec![Expr::Int(1), Expr::Int(2)]);
        assert_eq!(infer_expr(&expr, &env), Type::List(Box::new(Type::Int)));

        let empty = Expr::List(vec![]);
        assert_eq!(
            infer_expr(&empty, &env),
            Type::List(Box::new(Type::Any))
        );
    }

    #[test]
    fn test_infer_null_coalesce() {
        let mut env = TypeEnv::new();
        env.define("x".into(), Type::Option(Box::new(Type::Int)));
        let expr = Expr::NullCoalesce {
            expr: Box::new(Expr::Ident("x".into())),
            default: Box::new(Expr::Int(0)),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Int);
    }

    // ── Phase 13: Enhanced Inference Tests ──────────────────────

    #[test]
    fn test_infer_struct_field_access() {
        let mut env = TypeEnv::new();
        env.define_struct(
            "Point".into(),
            vec![("x".into(), Type::Int), ("y".into(), Type::Float)],
        );
        env.define("p".into(), Type::Struct("Point".into()));

        let expr = Expr::Member {
            object: Box::new(Expr::Ident("p".into())),
            field: "x".into(),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Int);

        let expr = Expr::Member {
            object: Box::new(Expr::Ident("p".into())),
            field: "y".into(),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Float);
    }

    #[test]
    fn test_infer_nested_member_access() {
        let mut env = TypeEnv::new();
        env.define_struct("Inner".into(), vec![("val".into(), Type::Int)]);
        env.define_struct(
            "Outer".into(),
            vec![("inner".into(), Type::Struct("Inner".into()))],
        );
        env.define("o".into(), Type::Struct("Outer".into()));

        // o.inner should be Struct("Inner")
        let inner_access = Expr::Member {
            object: Box::new(Expr::Ident("o".into())),
            field: "inner".into(),
        };
        assert_eq!(
            infer_expr(&inner_access, &env),
            Type::Struct("Inner".into())
        );

        // o.inner.val should be Int
        let nested = Expr::Member {
            object: Box::new(inner_access),
            field: "val".into(),
        };
        assert_eq!(infer_expr(&nested, &env), Type::Int);
    }

    #[test]
    fn test_infer_list_method_call() {
        let mut env = TypeEnv::new();
        env.define("xs".into(), Type::List(Box::new(Type::Int)));

        // xs.len() -> int
        let expr = Expr::Call {
            function: Box::new(Expr::Member {
                object: Box::new(Expr::Ident("xs".into())),
                field: "len".into(),
            }),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::Int);

        // xs.contains(1) -> bool
        let expr = Expr::Call {
            function: Box::new(Expr::Member {
                object: Box::new(Expr::Ident("xs".into())),
                field: "contains".into(),
            }),
            args: vec![Expr::Int(1)],
        };
        assert_eq!(infer_expr(&expr, &env), Type::Bool);
    }

    #[test]
    fn test_infer_string_method_call() {
        let mut env = TypeEnv::new();
        env.define("s".into(), Type::String);

        // s.split(",") -> list<string>
        let expr = Expr::Call {
            function: Box::new(Expr::Member {
                object: Box::new(Expr::Ident("s".into())),
                field: "split".into(),
            }),
            args: vec![Expr::String(",".into())],
        };
        assert_eq!(
            infer_expr(&expr, &env),
            Type::List(Box::new(Type::String))
        );

        // s.len() -> int
        let expr = Expr::Call {
            function: Box::new(Expr::Member {
                object: Box::new(Expr::Ident("s".into())),
                field: "len".into(),
            }),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::Int);
    }

    #[test]
    fn test_infer_closure_with_annotations() {
        let env = TypeEnv::new();
        let expr = Expr::Closure {
            params: vec![tl_ast::Param {
                name: "x".into(),
                type_ann: Some(tl_ast::TypeExpr::Named("int".into())),
            }],
            return_type: None,
            body: tl_ast::ClosureBody::Expr(Box::new(Expr::BinOp {
                left: Box::new(Expr::Ident("x".into())),
                op: BinOp::Mul,
                right: Box::new(Expr::Int(2)),
            })),
        };
        let ty = infer_expr(&expr, &env);
        match ty {
            Type::Function { params, .. } => {
                assert_eq!(params, vec![Type::Int]);
            }
            other => panic!("Expected function type, got {other}"),
        }
    }

    #[test]
    fn test_infer_closure_without_annotations() {
        let env = TypeEnv::new();
        let expr = Expr::Closure {
            params: vec![tl_ast::Param {
                name: "x".into(),
                type_ann: None,
            }],
            return_type: None,
            body: tl_ast::ClosureBody::Expr(Box::new(Expr::Int(42))),
        };
        let ty = infer_expr(&expr, &env);
        match ty {
            Type::Function { params, ret } => {
                assert_eq!(params, vec![Type::Any]);
                assert_eq!(*ret, Type::Int);
            }
            other => panic!("Expected function type, got {other}"),
        }
    }

    #[test]
    fn test_infer_map_literal() {
        let env = TypeEnv::new();

        // Non-empty map: infer value type from first entry
        let expr = Expr::Map(vec![
            (Expr::String("a".into()), Expr::Int(1)),
            (Expr::String("b".into()), Expr::Int(2)),
        ]);
        assert_eq!(infer_expr(&expr, &env), Type::Map(Box::new(Type::Int)));

        // Empty map
        let empty = Expr::Map(vec![]);
        assert_eq!(infer_expr(&empty, &env), Type::Map(Box::new(Type::Any)));
    }

    #[test]
    fn test_infer_builtin_return_types() {
        let env = TypeEnv::new();

        // range -> list<int>
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("range".into())),
            args: vec![Expr::Int(0), Expr::Int(10)],
        };
        assert_eq!(
            infer_expr(&expr, &env),
            Type::List(Box::new(Type::Int))
        );

        // split -> list<string>
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("split".into())),
            args: vec![Expr::String("a,b".into()), Expr::String(",".into())],
        };
        assert_eq!(
            infer_expr(&expr, &env),
            Type::List(Box::new(Type::String))
        );

        // channel -> channel<any>
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("channel".into())),
            args: vec![],
        };
        assert_eq!(
            infer_expr(&expr, &env),
            Type::Channel(Box::new(Type::Any))
        );

        // spawn -> task<any>
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("spawn".into())),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::Task(Box::new(Type::Any)));
    }

    #[test]
    fn test_infer_unknown_member_returns_any() {
        let mut env = TypeEnv::new();
        env.define("p".into(), Type::Struct("Point".into()));

        // Unknown struct (not registered) — returns Any
        let expr = Expr::Member {
            object: Box::new(Expr::Ident("p".into())),
            field: "z".into(),
        };
        assert_eq!(infer_expr(&expr, &env), Type::Any);
    }

    #[test]
    fn test_infer_user_defined_fn_return_type() {
        let mut env = TypeEnv::new();
        env.define_fn(
            "my_fn".into(),
            FnSig {
                params: vec![("x".into(), Type::Int)],
                ret: Type::String,
            },
        );

        let expr = Expr::Call {
            function: Box::new(Expr::Ident("my_fn".into())),
            args: vec![Expr::Int(42)],
        };
        assert_eq!(infer_expr(&expr, &env), Type::String);
    }

    // Phase 21: Schema Evolution type inference

    #[test]
    fn test_infer_schema_register_returns_unit() {
        let env = TypeEnv::new();
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("schema_register".into())),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::Unit);
    }

    #[test]
    fn test_infer_schema_history_returns_list_int() {
        let env = TypeEnv::new();
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("schema_history".into())),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::List(Box::new(Type::Int)));
    }

    #[test]
    fn test_infer_schema_check_returns_list_string() {
        let env = TypeEnv::new();
        let expr = Expr::Call {
            function: Box::new(Expr::Ident("schema_check".into())),
            args: vec![],
        };
        assert_eq!(infer_expr(&expr, &env), Type::List(Box::new(Type::String)));
    }
}
