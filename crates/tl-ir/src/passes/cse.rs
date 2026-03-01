// ThinkingLanguage — Common Subexpression Elimination Pass
// Detects identical scalar subtrees and factors them into WithColumns.

use std::collections::HashMap;

use crate::plan::*;

/// Eliminate common subexpressions within a plan chain.
/// Finds duplicate IrScalar subtrees across Filter/Project/WithColumns
/// and replaces them with column references to a generated WithColumns.
pub fn eliminate_common_subexprs(plan: QueryPlan) -> QueryPlan {
    // Collect all non-trivial expressions in the plan
    let exprs = collect_plan_expressions(&plan);

    // Count occurrences of each expression
    let mut counts: HashMap<IrScalar, usize> = HashMap::new();
    for expr in &exprs {
        if is_trivial(expr) {
            continue;
        }
        *counts.entry(expr.clone()).or_insert(0) += 1;
    }

    // Find expressions that appear more than once
    let duplicates: Vec<(IrScalar, String)> = counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .enumerate()
        .map(|(i, (expr, _))| {
            let name = format!("__cse_{i}");
            (expr, name)
        })
        .collect();

    if duplicates.is_empty() {
        return plan;
    }

    // Replace duplicate expressions with column references
    let mut result = plan;
    for (expr, alias_name) in &duplicates {
        result = replace_expr_in_plan(result, expr, &IrScalar::Column(alias_name.clone()));
    }

    // Find the deepest point to insert WithColumns (just above the scan or first major op)
    let with_columns: Vec<(String, IrScalar)> = duplicates
        .into_iter()
        .map(|(expr, name)| (name, expr))
        .collect();

    insert_with_columns(result, with_columns)
}

/// Check if an expression is trivial (not worth factoring out).
fn is_trivial(expr: &IrScalar) -> bool {
    matches!(
        expr,
        IrScalar::Column(_)
            | IrScalar::LitInt(_)
            | IrScalar::LitFloat(_)
            | IrScalar::LitString(_)
            | IrScalar::LitBool(_)
            | IrScalar::LitNull
            | IrScalar::Var(_)
    )
}

/// Collect all non-trivial scalar expressions from the plan.
fn collect_plan_expressions(plan: &QueryPlan) -> Vec<IrScalar> {
    let mut exprs = Vec::new();
    collect_plan_expressions_inner(plan, &mut exprs);
    exprs
}

fn collect_plan_expressions_inner(plan: &QueryPlan, exprs: &mut Vec<IrScalar>) {
    match plan {
        QueryPlan::Filter { predicate, input } => {
            collect_subexprs(predicate, exprs);
            collect_plan_expressions_inner(input, exprs);
        }
        QueryPlan::Project { columns, input } => {
            for col in columns {
                collect_subexprs(col, exprs);
            }
            collect_plan_expressions_inner(input, exprs);
        }
        QueryPlan::WithColumns { columns, input } => {
            for (_, expr) in columns {
                collect_subexprs(expr, exprs);
            }
            collect_plan_expressions_inner(input, exprs);
        }
        QueryPlan::Join { left, right, .. } => {
            collect_plan_expressions_inner(left, exprs);
            collect_plan_expressions_inner(right, exprs);
        }
        other => {
            if let Some(input) = other.input() {
                collect_plan_expressions_inner(input, exprs);
            }
        }
    }
}

/// Collect all subexpressions of a scalar.
fn collect_subexprs(scalar: &IrScalar, exprs: &mut Vec<IrScalar>) {
    exprs.push(scalar.clone());
    match scalar {
        IrScalar::BinOp { left, right, .. } => {
            collect_subexprs(left, exprs);
            collect_subexprs(right, exprs);
        }
        IrScalar::UnaryOp { expr, .. } => {
            collect_subexprs(expr, exprs);
        }
        IrScalar::Aggregate { arg, .. } => {
            collect_subexprs(arg, exprs);
        }
        IrScalar::Alias { expr, .. } => {
            collect_subexprs(expr, exprs);
        }
        _ => {}
    }
}

/// Replace all occurrences of `target` with `replacement` in a plan's expressions.
fn replace_expr_in_plan(plan: QueryPlan, target: &IrScalar, replacement: &IrScalar) -> QueryPlan {
    match plan {
        QueryPlan::Filter { predicate, input } => {
            let new_pred = replace_in_scalar(predicate, target, replacement);
            let new_input = replace_expr_in_plan(*input, target, replacement);
            QueryPlan::Filter {
                predicate: new_pred,
                input: Box::new(new_input),
            }
        }
        QueryPlan::Project { columns, input } => {
            let new_cols = columns
                .into_iter()
                .map(|c| replace_in_scalar(c, target, replacement))
                .collect();
            let new_input = replace_expr_in_plan(*input, target, replacement);
            QueryPlan::Project {
                columns: new_cols,
                input: Box::new(new_input),
            }
        }
        QueryPlan::WithColumns { columns, input } => {
            let new_cols = columns
                .into_iter()
                .map(|(name, expr)| (name, replace_in_scalar(expr, target, replacement)))
                .collect();
            let new_input = replace_expr_in_plan(*input, target, replacement);
            QueryPlan::WithColumns {
                columns: new_cols,
                input: Box::new(new_input),
            }
        }
        QueryPlan::Join {
            left,
            right,
            kind,
            left_cols,
            right_cols,
        } => QueryPlan::Join {
            left: Box::new(replace_expr_in_plan(*left, target, replacement)),
            right: Box::new(replace_expr_in_plan(*right, target, replacement)),
            kind,
            left_cols,
            right_cols,
        },
        other => {
            if let Some(input) = other.input().cloned() {
                let new_input = replace_expr_in_plan(input, target, replacement);
                other.with_input(new_input)
            } else {
                other
            }
        }
    }
}

/// Replace `target` with `replacement` in a scalar expression.
fn replace_in_scalar(scalar: IrScalar, target: &IrScalar, replacement: &IrScalar) -> IrScalar {
    if &scalar == target {
        return replacement.clone();
    }
    match scalar {
        IrScalar::BinOp { left, op, right } => IrScalar::BinOp {
            left: Box::new(replace_in_scalar(*left, target, replacement)),
            op,
            right: Box::new(replace_in_scalar(*right, target, replacement)),
        },
        IrScalar::UnaryOp { op, expr } => IrScalar::UnaryOp {
            op,
            expr: Box::new(replace_in_scalar(*expr, target, replacement)),
        },
        IrScalar::Aggregate { func, arg } => IrScalar::Aggregate {
            func,
            arg: Box::new(replace_in_scalar(*arg, target, replacement)),
        },
        IrScalar::Alias { expr, name } => IrScalar::Alias {
            expr: Box::new(replace_in_scalar(*expr, target, replacement)),
            name,
        },
        other => other,
    }
}

/// Insert a WithColumns node just above the first Scan (or at the bottom of the chain).
fn insert_with_columns(plan: QueryPlan, columns: Vec<(String, IrScalar)>) -> QueryPlan {
    match plan {
        QueryPlan::Scan { .. } => QueryPlan::WithColumns {
            columns,
            input: Box::new(plan),
        },
        QueryPlan::Join { .. } => QueryPlan::WithColumns {
            columns,
            input: Box::new(plan),
        },
        other => {
            if let Some(input) = other.input().cloned() {
                let new_input = insert_with_columns(input, columns);
                other.with_input(new_input)
            } else {
                QueryPlan::WithColumns {
                    columns,
                    input: Box::new(other),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan() -> QueryPlan {
        QueryPlan::Scan {
            source: TableSource::Variable("t".to_string()),
        }
    }

    fn col(name: &str) -> IrScalar {
        IrScalar::Column(name.to_string())
    }

    fn price_times_qty() -> IrScalar {
        IrScalar::BinOp {
            left: Box::new(col("price")),
            op: IrBinOp::Mul,
            right: Box::new(col("qty")),
        }
    }

    #[test]
    fn test_cse_detects_duplicate() {
        // Filter(price*qty > 100, Project([total: price*qty], Scan))
        let plan = QueryPlan::Filter {
            predicate: IrScalar::BinOp {
                left: Box::new(price_times_qty()),
                op: IrBinOp::Gt,
                right: Box::new(IrScalar::LitInt(100)),
            },
            input: Box::new(QueryPlan::Project {
                columns: vec![IrScalar::Alias {
                    expr: Box::new(price_times_qty()),
                    name: "total".to_string(),
                }],
                input: Box::new(scan()),
            }),
        };

        let optimized = eliminate_common_subexprs(plan);

        // The plan should now have a WithColumns computing price*qty once
        let display = format!("{optimized}");
        assert!(
            display.contains("WithColumns") || display.contains("__cse_"),
            "Expected CSE to insert WithColumns: {display}"
        );
    }

    #[test]
    fn test_cse_no_duplicates() {
        // Filter(age > 25, Project([name], Scan))
        let plan = QueryPlan::Filter {
            predicate: IrScalar::BinOp {
                left: Box::new(col("age")),
                op: IrBinOp::Gt,
                right: Box::new(IrScalar::LitInt(25)),
            },
            input: Box::new(QueryPlan::Project {
                columns: vec![col("name")],
                input: Box::new(scan()),
            }),
        };

        let optimized = eliminate_common_subexprs(plan);

        // Should be unchanged
        let display = format!("{optimized}");
        assert!(
            !display.contains("__cse_"),
            "Expected no CSE changes: {display}"
        );
    }
}
