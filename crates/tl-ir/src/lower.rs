// ThinkingLanguage — IR Lowering
// Converts an optimized QueryPlan tree back into a flat list of
// (op_name, ast_args) pairs that the compiler emits as Op::TablePipe instructions.

use tl_ast::{BinOp, Expr, UnaryOp};

use crate::plan::*;

/// Lower a QueryPlan to a flat sequence of (op_name, ast_args) pairs.
/// The sequence is in execution order (scan first, terminal last).
pub fn lower_plan(plan: &QueryPlan) -> Vec<(String, Vec<Expr>)> {
    let mut ops = Vec::new();
    lower_inner(plan, &mut ops);
    ops
}

fn lower_inner(plan: &QueryPlan, ops: &mut Vec<(String, Vec<Expr>)>) {
    match plan {
        QueryPlan::Scan { .. } => {
            // Scan is handled by the compiler (it compiles the source expression)
        }

        QueryPlan::Filter { predicate, input } => {
            lower_inner(input, ops);
            let expr = ir_scalar_to_ast(predicate);
            ops.push(("filter".to_string(), vec![expr]));
        }

        QueryPlan::Project { columns, input } => {
            lower_inner(input, ops);
            let args: Vec<Expr> = columns.iter().map(|c| ir_scalar_to_select_arg(c)).collect();
            ops.push(("select".to_string(), args));
        }

        QueryPlan::Sort { orders, input } => {
            lower_inner(input, ops);
            let mut args = Vec::new();
            for order in orders {
                args.push(Expr::Ident(order.column.clone()));
                if !order.ascending {
                    args.push(Expr::String("desc".to_string()));
                }
            }
            ops.push(("sort".to_string(), args));
        }

        QueryPlan::WithColumns { columns, input } => {
            lower_inner(input, ops);
            let pairs: Vec<(Expr, Expr)> = columns
                .iter()
                .map(|(name, expr)| (Expr::Ident(name.clone()), ir_scalar_to_ast(expr)))
                .collect();
            ops.push(("with".to_string(), vec![Expr::Map(pairs)]));
        }

        QueryPlan::Aggregate {
            group_by,
            aggregates,
            input,
        } => {
            lower_inner(input, ops);
            let mut args = Vec::new();

            if !group_by.is_empty() {
                let by_value = if group_by.len() == 1 {
                    ir_scalar_to_ast(&group_by[0])
                } else {
                    Expr::List(group_by.iter().map(|c| ir_scalar_to_ast(c)).collect())
                };
                args.push(Expr::NamedArg {
                    name: "by".to_string(),
                    value: Box::new(by_value),
                });
            }

            for agg in aggregates {
                match agg {
                    IrScalar::Alias { expr, name } => {
                        args.push(Expr::NamedArg {
                            name: name.clone(),
                            value: Box::new(ir_scalar_to_ast(expr)),
                        });
                    }
                    other => args.push(ir_scalar_to_ast(other)),
                }
            }

            ops.push(("aggregate".to_string(), args));
        }

        QueryPlan::Join {
            left,
            right,
            kind,
            left_cols,
            right_cols,
        } => {
            lower_inner(left, ops);
            // The right table source is the first arg
            let right_source = get_scan_source(right);
            let mut args = vec![right_source];

            // Add on: clauses
            for (lc, rc) in left_cols.iter().zip(right_cols.iter()) {
                args.push(Expr::NamedArg {
                    name: "on".to_string(),
                    value: Box::new(Expr::BinOp {
                        left: Box::new(Expr::Ident(lc.clone())),
                        op: BinOp::Eq,
                        right: Box::new(Expr::Ident(rc.clone())),
                    }),
                });
            }

            // Add kind
            let kind_str = match kind {
                IrJoinKind::Inner => "inner",
                IrJoinKind::Left => "left",
                IrJoinKind::Right => "right",
                IrJoinKind::Full => "full",
            };
            args.push(Expr::NamedArg {
                name: "kind".to_string(),
                value: Box::new(Expr::String(kind_str.to_string())),
            });

            ops.push(("join".to_string(), args));
        }

        QueryPlan::Limit { count, input } => {
            lower_inner(input, ops);
            ops.push(("limit".to_string(), vec![Expr::Int(*count as i64)]));
        }

        QueryPlan::Collect { input } => {
            lower_inner(input, ops);
            ops.push(("collect".to_string(), vec![]));
        }

        QueryPlan::Show { limit, input } => {
            lower_inner(input, ops);
            ops.push(("show".to_string(), vec![Expr::Int(*limit as i64)]));
        }

        QueryPlan::Describe { input } => {
            lower_inner(input, ops);
            ops.push(("describe".to_string(), vec![]));
        }

        QueryPlan::WriteCsv { path, input } => {
            lower_inner(input, ops);
            ops.push(("write_csv".to_string(), vec![ir_scalar_to_ast(path)]));
        }

        QueryPlan::WriteParquet { path, input } => {
            lower_inner(input, ops);
            ops.push((
                "write_parquet".to_string(),
                vec![ir_scalar_to_ast(path)],
            ));
        }

        QueryPlan::FillNull {
            column,
            strategy,
            value,
            input,
        } => {
            lower_inner(input, ops);
            let mut args = vec![Expr::Ident(column.clone())];
            if let Some(v) = value {
                args.push(ir_scalar_to_ast(v));
            } else {
                args.push(Expr::String(strategy.clone()));
            }
            ops.push(("fill_null".to_string(), args));
        }

        QueryPlan::DropNull { column, input } => {
            lower_inner(input, ops);
            let args = if let Some(col) = column {
                vec![Expr::Ident(col.clone())]
            } else {
                vec![]
            };
            ops.push(("drop_null".to_string(), args));
        }

        QueryPlan::Dedup { columns, input } => {
            lower_inner(input, ops);
            let args: Vec<Expr> = columns.iter().map(|c| Expr::Ident(c.clone())).collect();
            ops.push(("dedup".to_string(), args));
        }

        QueryPlan::Clamp {
            column,
            min,
            max,
            input,
        } => {
            lower_inner(input, ops);
            ops.push((
                "clamp".to_string(),
                vec![
                    Expr::Ident(column.clone()),
                    ir_scalar_to_ast(min),
                    ir_scalar_to_ast(max),
                ],
            ));
        }

        QueryPlan::DataProfile { input } => {
            lower_inner(input, ops);
            ops.push(("data_profile".to_string(), vec![]));
        }

        QueryPlan::RowCount { input } => {
            lower_inner(input, ops);
            ops.push(("row_count".to_string(), vec![]));
        }

        QueryPlan::NullRate { column, input } => {
            lower_inner(input, ops);
            ops.push(("null_rate".to_string(), vec![Expr::Ident(column.clone())]));
        }

        QueryPlan::IsUnique { column, input } => {
            lower_inner(input, ops);
            ops.push((
                "is_unique".to_string(),
                vec![Expr::Ident(column.clone())],
            ));
        }
    }
}

/// Convert an IR scalar back to an AST expression.
pub fn ir_scalar_to_ast(scalar: &IrScalar) -> Expr {
    match scalar {
        IrScalar::Column(name) => Expr::Ident(name.clone()),
        IrScalar::LitInt(v) => Expr::Int(*v),
        IrScalar::LitFloat(bits) => Expr::Float(f64::from_bits(*bits)),
        IrScalar::LitString(s) => Expr::String(s.clone()),
        IrScalar::LitBool(b) => Expr::Bool(*b),
        IrScalar::LitNull => Expr::None,
        IrScalar::BinOp { left, op, right } => Expr::BinOp {
            left: Box::new(ir_scalar_to_ast(left)),
            op: ir_binop_to_ast(op),
            right: Box::new(ir_scalar_to_ast(right)),
        },
        IrScalar::UnaryOp { op, expr } => Expr::UnaryOp {
            op: match op {
                IrUnaryOp::Neg => UnaryOp::Neg,
                IrUnaryOp::Not => UnaryOp::Not,
            },
            expr: Box::new(ir_scalar_to_ast(expr)),
        },
        IrScalar::Aggregate { func, arg } => {
            let fname = match func {
                AggFunc::Count => "count",
                AggFunc::Sum => "sum",
                AggFunc::Avg => "avg",
                AggFunc::Min => "min",
                AggFunc::Max => "max",
            };
            Expr::Call {
                function: Box::new(Expr::Ident(fname.to_string())),
                args: vec![ir_scalar_to_ast(arg)],
            }
        }
        IrScalar::Alias { expr, name } => Expr::NamedArg {
            name: name.clone(),
            value: Box::new(ir_scalar_to_ast(expr)),
        },
        IrScalar::Var(name) => Expr::Ident(name.clone()),
    }
}

/// Convert IrScalar for use as a select() argument.
/// Aliases become NamedArg.
fn ir_scalar_to_select_arg(scalar: &IrScalar) -> Expr {
    match scalar {
        IrScalar::Alias { expr, name } => Expr::NamedArg {
            name: name.clone(),
            value: Box::new(ir_scalar_to_ast(expr)),
        },
        other => ir_scalar_to_ast(other),
    }
}

fn ir_binop_to_ast(op: &IrBinOp) -> BinOp {
    match op {
        IrBinOp::Add => BinOp::Add,
        IrBinOp::Sub => BinOp::Sub,
        IrBinOp::Mul => BinOp::Mul,
        IrBinOp::Div => BinOp::Div,
        IrBinOp::Mod => BinOp::Mod,
        IrBinOp::Pow => BinOp::Pow,
        IrBinOp::Eq => BinOp::Eq,
        IrBinOp::Neq => BinOp::Neq,
        IrBinOp::Lt => BinOp::Lt,
        IrBinOp::Gt => BinOp::Gt,
        IrBinOp::Lte => BinOp::Lte,
        IrBinOp::Gte => BinOp::Gte,
        IrBinOp::And => BinOp::And,
        IrBinOp::Or => BinOp::Or,
    }
}

/// Extract the source expression from a plan (for join right side).
fn get_scan_source(plan: &QueryPlan) -> Expr {
    match plan {
        QueryPlan::Scan { source } => match source {
            TableSource::Variable(name) => Expr::Ident(name.clone()),
            TableSource::AstExpr(expr) => *expr.clone(),
        },
        // If the right side has filters/etc., we need the full plan —
        // for now just get the deepest scan
        other => {
            if let Some(input) = other.input() {
                get_scan_source(input)
            } else {
                Expr::None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::build_query_plan;
    use crate::optimize::optimize;

    #[test]
    fn test_roundtrip_filter() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![(
            "filter".to_string(),
            vec![Expr::BinOp {
                left: Box::new(Expr::Ident("age".to_string())),
                op: BinOp::Gt,
                right: Box::new(Expr::Int(25)),
            }],
        )];

        let plan = build_query_plan(&source, &ops).unwrap();
        let optimized = optimize(plan);
        let lowered = lower_plan(&optimized);

        assert_eq!(lowered.len(), 1);
        assert_eq!(lowered[0].0, "filter");
    }

    #[test]
    fn test_roundtrip_filter_select() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![
            (
                "filter".to_string(),
                vec![Expr::BinOp {
                    left: Box::new(Expr::Ident("age".to_string())),
                    op: BinOp::Gt,
                    right: Box::new(Expr::Int(25)),
                }],
            ),
            (
                "select".to_string(),
                vec![Expr::Ident("name".to_string()), Expr::Ident("age".to_string())],
            ),
        ];

        let plan = build_query_plan(&source, &ops).unwrap();
        let optimized = optimize(plan);
        let lowered = lower_plan(&optimized);

        // After optimization, filter should come before select
        assert_eq!(lowered.len(), 2);
        assert_eq!(lowered[0].0, "filter");
        assert_eq!(lowered[1].0, "select");
    }

    #[test]
    fn test_filter_merge_roundtrip() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![
            (
                "filter".to_string(),
                vec![Expr::BinOp {
                    left: Box::new(Expr::Ident("age".to_string())),
                    op: BinOp::Gt,
                    right: Box::new(Expr::Int(25)),
                }],
            ),
            (
                "filter".to_string(),
                vec![Expr::BinOp {
                    left: Box::new(Expr::Ident("name".to_string())),
                    op: BinOp::Neq,
                    right: Box::new(Expr::String("Bob".to_string())),
                }],
            ),
        ];

        let plan = build_query_plan(&source, &ops).unwrap();
        let optimized = optimize(plan);
        let lowered = lower_plan(&optimized);

        // Two filters should be merged into one
        assert_eq!(lowered.len(), 1);
        assert_eq!(lowered[0].0, "filter");
    }

    #[test]
    fn test_select_then_filter_pushdown() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![
            (
                "select".to_string(),
                vec![Expr::Ident("name".to_string()), Expr::Ident("age".to_string())],
            ),
            (
                "filter".to_string(),
                vec![Expr::BinOp {
                    left: Box::new(Expr::Ident("age".to_string())),
                    op: BinOp::Gt,
                    right: Box::new(Expr::Int(25)),
                }],
            ),
        ];

        let plan = build_query_plan(&source, &ops).unwrap();
        let optimized = optimize(plan);
        let lowered = lower_plan(&optimized);

        // Filter should be pushed before select
        assert_eq!(lowered.len(), 2);
        assert_eq!(lowered[0].0, "filter");
        assert_eq!(lowered[1].0, "select");
    }
}
