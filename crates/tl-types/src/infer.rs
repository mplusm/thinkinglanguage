// ThinkingLanguage — Expression Type Inference
// Licensed under MIT OR Apache-2.0
//
// Infers types from expressions using local, forward-only rules.

use tl_ast::{BinOp, Expr, UnaryOp};

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

        // Function call
        Expr::Call { function, .. } => {
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
                    _ => {
                        if let Some(sig) = env.lookup_fn(name) {
                            sig.ret.clone()
                        } else {
                            Type::Any
                        }
                    }
                }
            } else {
                Type::Any
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

        // Map literal
        Expr::Map(_) => Type::Map(Box::new(Type::Any)),

        // Member access
        Expr::Member { object, .. } => {
            let obj_ty = infer_expr(object, env);
            match &obj_ty {
                Type::Struct(name) => {
                    // Could look up field type from env
                    if env.lookup_struct(name).is_some() {
                        Type::Any // field type lookup deferred to checker
                    } else {
                        Type::Any
                    }
                }
                _ => Type::Any,
            }
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

        // Closure
        Expr::Closure { .. } => Type::Function {
            params: vec![Type::Any],
            ret: Box::new(Type::Any),
        },

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
            if let Some((_, body)) = arms.first() {
                infer_expr(body, env)
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
}
