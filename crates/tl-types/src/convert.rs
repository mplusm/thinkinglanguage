// ThinkingLanguage — TypeExpr (AST) to Type (internal) conversion
// Licensed under MIT OR Apache-2.0

use tl_ast::TypeExpr;

use crate::Type;

/// Convert an AST type expression into the internal Type representation.
pub fn convert_type_expr(texpr: &TypeExpr) -> Type {
    match texpr {
        TypeExpr::Named(name) => convert_named(name),
        TypeExpr::Generic { name, args } => convert_generic(name, args),
        TypeExpr::Optional(inner) => Type::Option(Box::new(convert_type_expr(inner))),
        TypeExpr::Function {
            params,
            return_type,
        } => Type::Function {
            params: params.iter().map(convert_type_expr).collect(),
            ret: Box::new(convert_type_expr(return_type)),
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

fn convert_generic(name: &str, args: &[TypeExpr]) -> Type {
    match name {
        "list" if args.len() == 1 => Type::List(Box::new(convert_type_expr(&args[0]))),
        "map" if args.len() == 1 => Type::Map(Box::new(convert_type_expr(&args[0]))),
        "set" if args.len() == 1 => Type::Set(Box::new(convert_type_expr(&args[0]))),
        "option" if args.len() == 1 => Type::Option(Box::new(convert_type_expr(&args[0]))),
        "result" if args.len() == 2 => Type::Result(
            Box::new(convert_type_expr(&args[0])),
            Box::new(convert_type_expr(&args[1])),
        ),
        "generator" if args.len() == 1 => {
            Type::Generator(Box::new(convert_type_expr(&args[0])))
        }
        "task" if args.len() == 1 => Type::Task(Box::new(convert_type_expr(&args[0]))),
        "channel" if args.len() == 1 => Type::Channel(Box::new(convert_type_expr(&args[0]))),
        "table" if args.len() == 1 => {
            if let TypeExpr::Named(schema) = &args[0] {
                Type::Table(Some(schema.clone()))
            } else {
                Type::Table(None)
            }
        }
        _ => {
            // Unknown generic — could be user-defined in future phases.
            // Treat as struct for now.
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
