// ThinkingLanguage — TypeExpr (AST) to Type (internal) conversion
// Licensed under MIT OR Apache-2.0

use tl_ast::TypeExpr;

use crate::{Type, TypeEnv};

/// Convert an AST type expression into the internal Type representation.
pub fn convert_type_expr(texpr: &TypeExpr) -> Type {
    convert_type_expr_with_params(texpr, &[])
}

/// Convert an AST type expression, resolving type aliases from the environment.
pub fn convert_type_expr_with_env(texpr: &TypeExpr, env: &TypeEnv) -> Type {
    convert_type_expr_impl(texpr, &[], Some(env))
}

/// Convert an AST type expression, recognizing type parameters from the given list.
pub fn convert_type_expr_with_params(texpr: &TypeExpr, type_params: &[String]) -> Type {
    convert_type_expr_impl(texpr, type_params, None)
}

fn convert_type_expr_impl(texpr: &TypeExpr, type_params: &[String], env: Option<&TypeEnv>) -> Type {
    match texpr {
        TypeExpr::Named(name) => {
            if type_params.contains(name) {
                Type::TypeParam(name.clone())
            } else if let Some(env) = env {
                if let Some((alias_params, alias_value)) = env.lookup_type_alias(name) {
                    if alias_params.is_empty() {
                        return convert_type_expr_impl(alias_value, type_params, Some(env));
                    }
                }
                convert_named(name)
            } else {
                convert_named(name)
            }
        }
        TypeExpr::Generic { name, args } => {
            // Check if it's a type alias with type params
            if let Some(env) = env {
                if let Some((alias_params, alias_value)) = env.lookup_type_alias(name).cloned() {
                    if !alias_params.is_empty() && alias_params.len() == args.len() {
                        // Substitute type params — for now, just resolve the alias value
                        return convert_type_expr_impl(&alias_value, type_params, Some(env));
                    }
                }
            }
            convert_generic_impl(name, args, type_params, env)
        }
        TypeExpr::Optional(inner) => Type::Option(Box::new(convert_type_expr_impl(inner, type_params, env))),
        TypeExpr::Function {
            params,
            return_type,
        } => Type::Function {
            params: params.iter().map(|p| convert_type_expr_impl(p, type_params, env)).collect(),
            ret: Box::new(convert_type_expr_impl(return_type, type_params, env)),
        },
    }
}

fn convert_named(name: &str) -> Type {
    match name {
        "int" | "int64" => Type::Int,
        "float" | "float64" => Type::Float,
        "string" => Type::String,
        "bool" => Type::Bool,
        "none" => Type::None,
        "any" => Type::Any,
        "unit" | "void" => Type::Unit,
        "table" => Type::Table(None),
        other => {
            // Could be a struct or enum name — treated as struct by default.
            // The checker resolves which it actually is.
            Type::Struct(other.to_string())
        }
    }
}

#[allow(dead_code)]
fn convert_generic(name: &str, args: &[TypeExpr]) -> Type {
    convert_generic_with_params(name, args, &[])
}

fn convert_generic_with_params(name: &str, args: &[TypeExpr], type_params: &[String]) -> Type {
    convert_generic_impl(name, args, type_params, None)
}

fn convert_generic_impl(name: &str, args: &[TypeExpr], type_params: &[String], env: Option<&TypeEnv>) -> Type {
    match name {
        "list" if args.len() == 1 => Type::List(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "map" if args.len() == 1 => Type::Map(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "set" if args.len() == 1 => Type::Set(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "option" if args.len() == 1 => Type::Option(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "result" if args.len() == 2 => Type::Result(
            Box::new(convert_type_expr_impl(&args[0], type_params, env)),
            Box::new(convert_type_expr_impl(&args[1], type_params, env)),
        ),
        "generator" if args.len() == 1 => {
            Type::Generator(Box::new(convert_type_expr_impl(&args[0], type_params, env)))
        }
        "task" if args.len() == 1 => Type::Task(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "channel" if args.len() == 1 => Type::Channel(Box::new(convert_type_expr_impl(&args[0], type_params, env))),
        "table" if args.len() == 1 => {
            if let TypeExpr::Named(schema) = &args[0] {
                Type::Table(Some(schema.clone()))
            } else {
                Type::Table(None)
            }
        }
        _ => {
            // Unknown generic — could be user-defined generic struct.
            Type::Struct(name.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tl_ast::TypeExpr;

    #[test]
    fn test_convert_primitives() {
        assert_eq!(convert_type_expr(&TypeExpr::Named("int".into())), Type::Int);
        assert_eq!(
            convert_type_expr(&TypeExpr::Named("float".into())),
            Type::Float
        );
        assert_eq!(
            convert_type_expr(&TypeExpr::Named("string".into())),
            Type::String
        );
        assert_eq!(
            convert_type_expr(&TypeExpr::Named("bool".into())),
            Type::Bool
        );
    }

    #[test]
    fn test_convert_optional() {
        let texpr = TypeExpr::Optional(Box::new(TypeExpr::Named("int".into())));
        assert_eq!(convert_type_expr(&texpr), Type::Option(Box::new(Type::Int)));
    }

    #[test]
    fn test_convert_generic_list() {
        let texpr = TypeExpr::Generic {
            name: "list".into(),
            args: vec![TypeExpr::Named("int".into())],
        };
        assert_eq!(convert_type_expr(&texpr), Type::List(Box::new(Type::Int)));
    }

    #[test]
    fn test_convert_result() {
        let texpr = TypeExpr::Generic {
            name: "result".into(),
            args: vec![
                TypeExpr::Named("int".into()),
                TypeExpr::Named("string".into()),
            ],
        };
        assert_eq!(
            convert_type_expr(&texpr),
            Type::Result(Box::new(Type::Int), Box::new(Type::String))
        );
    }

    #[test]
    fn test_convert_function_type() {
        let texpr = TypeExpr::Function {
            params: vec![TypeExpr::Named("int".into())],
            return_type: Box::new(TypeExpr::Named("string".into())),
        };
        assert_eq!(
            convert_type_expr(&texpr),
            Type::Function {
                params: vec![Type::Int],
                ret: Box::new(Type::String),
            }
        );
    }
}
