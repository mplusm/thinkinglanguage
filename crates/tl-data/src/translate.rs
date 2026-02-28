use std::collections::HashMap;
use datafusion::prelude::*;
use tl_ast::{Expr as AstExpr, BinOp, UnaryOp};

/// Values that can be used as literals in translated expressions.
#[derive(Debug, Clone)]
pub enum LocalValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

/// Context for translating TL AST expressions to DataFusion expressions.
/// `locals` maps variable names to their runtime values.
/// Names not in `locals` are treated as column references.
pub struct TranslateContext {
    pub locals: HashMap<String, LocalValue>,
}

impl TranslateContext {
    pub fn new() -> Self {
        TranslateContext {
            locals: HashMap::new(),
        }
    }
}

/// Translate a TL AST expression into a DataFusion Expr.
///
/// Resolution rules:
/// - Identifiers present in `ctx.locals` → `lit(value)`
/// - Identifiers NOT in `ctx.locals` → `col(name)` (column reference)
/// - Binary ops → DataFusion binary expressions
/// - Function calls → aggregate functions (count, sum, avg, min, max) or DataFusion built-in functions
pub fn translate_expr(ast: &AstExpr, ctx: &TranslateContext) -> Result<Expr, String> {
    match ast {
        AstExpr::Int(n) => Ok(lit(*n)),
        AstExpr::Float(f) => Ok(lit(*f)),
        AstExpr::String(s) => Ok(lit(s.clone())),
        AstExpr::Bool(b) => Ok(lit(*b)),
        AstExpr::None => Ok(lit(datafusion::scalar::ScalarValue::Null)),

        AstExpr::Ident(name) => {
            if let Some(local) = ctx.locals.get(name) {
                match local {
                    LocalValue::Int(n) => Ok(lit(*n)),
                    LocalValue::Float(f) => Ok(lit(*f)),
                    LocalValue::String(s) => Ok(lit(s.clone())),
                    LocalValue::Bool(b) => Ok(lit(*b)),
                }
            } else {
                Ok(col(name.as_str()))
            }
        }

        AstExpr::BinOp { left, op, right } => {
            let l = translate_expr(left, ctx)?;
            let r = translate_expr(right, ctx)?;
            match op {
                BinOp::Add => Ok(l + r),
                BinOp::Sub => Ok(l - r),
                BinOp::Mul => Ok(l * r),
                BinOp::Div => Ok(l / r),
                BinOp::Mod => Ok(l % r),
                BinOp::Eq => Ok(l.eq(r)),
                BinOp::Neq => Ok(l.not_eq(r)),
                BinOp::Lt => Ok(l.lt(r)),
                BinOp::Gt => Ok(l.gt(r)),
                BinOp::Lte => Ok(l.lt_eq(r)),
                BinOp::Gte => Ok(l.gt_eq(r)),
                BinOp::And => Ok(l.and(r)),
                BinOp::Or => Ok(l.or(r)),
                BinOp::Pow => Err("Power operator not supported in table expressions".into()),
            }
        }

        AstExpr::UnaryOp { op, expr } => {
            let e = translate_expr(expr, ctx)?;
            match op {
                UnaryOp::Neg => Ok(Expr::Negative(Box::new(e))),
                UnaryOp::Not => Ok(e.not()),
                UnaryOp::Ref => Ok(e), // References are transparent in DataFusion expressions
            }
        }

        AstExpr::Call { function, args } => {
            if let AstExpr::Ident(fname) = function.as_ref() {
                translate_aggregate_or_function(fname, args, ctx)
            } else {
                Err("Only named function calls supported in table expressions".into())
            }
        }

        AstExpr::Member { object, field } => {
            // object.field → col("object.field") — for qualified column names
            if let AstExpr::Ident(obj_name) = object.as_ref() {
                Ok(col(format!("{obj_name}.{field}").as_str()))
            } else {
                Err("Complex member access not supported in table expressions".into())
            }
        }

        _ => Err(format!(
            "Expression type not supported in table context: {:?}",
            std::mem::discriminant(ast)
        )),
    }
}

/// Translate aggregate and scalar function calls.
fn translate_aggregate_or_function(
    name: &str,
    args: &[AstExpr],
    ctx: &TranslateContext,
) -> Result<Expr, String> {
    match name {
        "count" => {
            if args.is_empty() {
                Ok(datafusion::functions_aggregate::expr_fn::count(lit(1)))
            } else {
                let arg = translate_expr(&args[0], ctx)?;
                Ok(datafusion::functions_aggregate::expr_fn::count(arg))
            }
        }
        "sum" => {
            if args.len() != 1 {
                return Err("sum() requires exactly 1 argument".into());
            }
            let arg = translate_expr(&args[0], ctx)?;
            Ok(datafusion::functions_aggregate::expr_fn::sum(arg))
        }
        "avg" => {
            if args.len() != 1 {
                return Err("avg() requires exactly 1 argument".into());
            }
            let arg = translate_expr(&args[0], ctx)?;
            Ok(datafusion::functions_aggregate::expr_fn::avg(arg))
        }
        "min" => {
            if args.len() != 1 {
                return Err("min() requires exactly 1 argument".into());
            }
            let arg = translate_expr(&args[0], ctx)?;
            Ok(datafusion::functions_aggregate::expr_fn::min(arg))
        }
        "max" => {
            if args.len() != 1 {
                return Err("max() requires exactly 1 argument".into());
            }
            let arg = translate_expr(&args[0], ctx)?;
            Ok(datafusion::functions_aggregate::expr_fn::max(arg))
        }
        _ => Err(format!("Unknown function in table expression: {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_column_ref() {
        let ctx = TranslateContext::new();
        let ast = AstExpr::Ident("age".into());
        let expr = translate_expr(&ast, &ctx).unwrap();
        assert_eq!(format!("{expr}"), "age");
    }

    #[test]
    fn test_translate_local_literal() {
        let mut ctx = TranslateContext::new();
        ctx.locals.insert("threshold".into(), LocalValue::Int(25));
        let ast = AstExpr::Ident("threshold".into());
        let expr = translate_expr(&ast, &ctx).unwrap();
        assert_eq!(format!("{expr}"), "Int64(25)");
    }

    #[test]
    fn test_translate_binop() {
        let ctx = TranslateContext::new();
        let ast = AstExpr::BinOp {
            left: Box::new(AstExpr::Ident("age".into())),
            op: BinOp::Gt,
            right: Box::new(AstExpr::Int(25)),
        };
        let expr = translate_expr(&ast, &ctx).unwrap();
        assert_eq!(format!("{expr}"), "age > Int64(25)");
    }

    #[test]
    fn test_translate_aggregate() {
        let ctx = TranslateContext::new();
        let ast = AstExpr::Call {
            function: Box::new(AstExpr::Ident("sum".into())),
            args: vec![AstExpr::Ident("amount".into())],
        };
        let expr = translate_expr(&ast, &ctx).unwrap();
        let s = format!("{expr}");
        assert!(s.contains("sum") || s.contains("SUM"), "Got: {s}");
    }
}
