// ThinkingLanguage — Predicate Pushdown Pass
// Pushes Filter nodes down through the plan tree to filter earlier.

use std::collections::HashSet;

use crate::plan::*;

/// Push predicates down through the plan tree.
/// Filters closer to the scan means less data flows through expensive ops.
pub fn push_predicates_down(plan: QueryPlan) -> QueryPlan {
    match plan {
        QueryPlan::Filter { predicate, input } => {
            let optimized_input = push_predicates_down(*input);
            try_push_filter(predicate, optimized_input)
        }

        // Recurse into Join children
        QueryPlan::Join {
            left,
            right,
            kind,
            left_cols,
            right_cols,
        } => QueryPlan::Join {
            left: Box::new(push_predicates_down(*left)),
            right: Box::new(push_predicates_down(*right)),
            kind,
            left_cols,
            right_cols,
        },

        // For all other nodes, recurse into input
        other => {
            if let Some(input) = other.input().cloned() {
                let optimized_input = push_predicates_down(input);
                other.with_input(optimized_input)
            } else {
                other
            }
        }
    }
}

/// Try to push a filter predicate down through the given plan node.
fn try_push_filter(predicate: IrScalar, plan: QueryPlan) -> QueryPlan {
    match plan {
        // Push through Sort — filtering before sorting is cheaper
        QueryPlan::Sort { orders, input } => {
            let pushed = try_push_filter(predicate, *input);
            QueryPlan::Sort {
                orders,
                input: Box::new(pushed),
            }
        }

        // Push through Project if predicate only references columns in the input
        QueryPlan::Project { columns, input } => {
            let pred_cols = referenced_columns(&predicate);
            // Get the set of column names available from the project's input
            // A predicate can be pushed if it doesn't reference any aliases
            let alias_names: HashSet<String> = columns
                .iter()
                .filter_map(|c| {
                    if let IrScalar::Alias { name, .. } = c {
                        Some(name.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Can push if predicate doesn't use any alias names
            if pred_cols.is_disjoint(&alias_names) {
                let pushed = try_push_filter(predicate, *input);
                QueryPlan::Project {
                    columns,
                    input: Box::new(pushed),
                }
            } else {
                // Can't push — keep filter above
                QueryPlan::Filter {
                    predicate,
                    input: Box::new(QueryPlan::Project { columns, input }),
                }
            }
        }

        // Push through WithColumns if predicate doesn't reference new columns
        QueryPlan::WithColumns { columns, input } => {
            let pred_cols = referenced_columns(&predicate);
            let new_col_names: HashSet<String> =
                columns.iter().map(|(name, _)| name.clone()).collect();

            if pred_cols.is_disjoint(&new_col_names) {
                let pushed = try_push_filter(predicate, *input);
                QueryPlan::WithColumns {
                    columns,
                    input: Box::new(pushed),
                }
            } else {
                QueryPlan::Filter {
                    predicate,
                    input: Box::new(QueryPlan::WithColumns { columns, input }),
                }
            }
        }

        // Push into Join sides: split conjuncts by column ownership
        QueryPlan::Join {
            left,
            right,
            kind,
            left_cols,
            right_cols,
        } => {
            // Only push into inner joins (outer joins change semantics)
            if kind != IrJoinKind::Inner {
                return QueryPlan::Filter {
                    predicate,
                    input: Box::new(QueryPlan::Join {
                        left,
                        right,
                        kind,
                        left_cols,
                        right_cols,
                    }),
                };
            }

            // Split AND conjuncts
            let conjuncts = split_conjuncts(predicate);
            let mut left_preds = Vec::new();
            let mut right_preds = Vec::new();
            let mut remaining = Vec::new();

            // We use the join column lists as heuristic for left vs right ownership.
            // This is a simplification — a full implementation would use schema info.
            let right_join_cols: HashSet<&String> = right_cols.iter().collect();

            for conj in conjuncts {
                let cols = referenced_columns(&conj);
                let refs_right = cols.iter().any(|c| right_join_cols.contains(c));
                let refs_left = cols.iter().any(|c| !right_join_cols.contains(c));

                if refs_right && !refs_left {
                    right_preds.push(conj);
                } else if refs_left && !refs_right {
                    left_preds.push(conj);
                } else {
                    remaining.push(conj);
                }
            }

            let new_left = if left_preds.is_empty() {
                *left
            } else {
                QueryPlan::Filter {
                    predicate: combine_conjuncts(left_preds),
                    input: left,
                }
            };

            let new_right = if right_preds.is_empty() {
                *right
            } else {
                QueryPlan::Filter {
                    predicate: combine_conjuncts(right_preds),
                    input: right,
                }
            };

            let join = QueryPlan::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                kind,
                left_cols,
                right_cols,
            };

            if remaining.is_empty() {
                join
            } else {
                QueryPlan::Filter {
                    predicate: combine_conjuncts(remaining),
                    input: Box::new(join),
                }
            }
        }

        // Do NOT push past these nodes (changes semantics)
        QueryPlan::Limit { .. }
        | QueryPlan::Aggregate { .. }
        | QueryPlan::Collect { .. }
        | QueryPlan::Show { .. }
        | QueryPlan::Describe { .. }
        | QueryPlan::WriteCsv { .. }
        | QueryPlan::WriteParquet { .. }
        | QueryPlan::DataProfile { .. }
        | QueryPlan::RowCount { .. }
        | QueryPlan::NullRate { .. }
        | QueryPlan::IsUnique { .. } => QueryPlan::Filter {
            predicate,
            input: Box::new(plan),
        },

        // Can push through data quality ops that don't change row count structurally
        QueryPlan::FillNull {
            column,
            strategy,
            value,
            input,
        } => {
            let pred_cols = referenced_columns(&predicate);
            if !pred_cols.contains(&column) {
                let pushed = try_push_filter(predicate, *input);
                QueryPlan::FillNull {
                    column,
                    strategy,
                    value,
                    input: Box::new(pushed),
                }
            } else {
                QueryPlan::Filter {
                    predicate,
                    input: Box::new(QueryPlan::FillNull {
                        column,
                        strategy,
                        value,
                        input,
                    }),
                }
            }
        }

        // DropNull changes rows — don't push past
        QueryPlan::DropNull { .. } | QueryPlan::Dedup { .. } | QueryPlan::Clamp { .. } => {
            QueryPlan::Filter {
                predicate,
                input: Box::new(plan),
            }
        }

        // Can't push past Scan (leaf)
        QueryPlan::Scan { .. } => QueryPlan::Filter {
            predicate,
            input: Box::new(plan),
        },

        // Filter on filter — leave as is (filter_merge handles this)
        QueryPlan::Filter { .. } => QueryPlan::Filter {
            predicate,
            input: Box::new(plan),
        },
    }
}

/// Collect all column names referenced in a scalar expression.
pub fn referenced_columns(scalar: &IrScalar) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_columns(scalar, &mut cols);
    cols
}

fn collect_columns(scalar: &IrScalar, cols: &mut HashSet<String>) {
    match scalar {
        IrScalar::Column(name) => {
            cols.insert(name.clone());
        }
        IrScalar::BinOp { left, right, .. } => {
            collect_columns(left, cols);
            collect_columns(right, cols);
        }
        IrScalar::UnaryOp { expr, .. } => {
            collect_columns(expr, cols);
        }
        IrScalar::Aggregate { arg, .. } => {
            collect_columns(arg, cols);
        }
        IrScalar::Alias { expr, .. } => {
            collect_columns(expr, cols);
        }
        IrScalar::LitInt(_)
        | IrScalar::LitFloat(_)
        | IrScalar::LitString(_)
        | IrScalar::LitBool(_)
        | IrScalar::LitNull
        | IrScalar::Var(_) => {}
    }
}

/// Split a predicate into AND conjuncts.
fn split_conjuncts(scalar: IrScalar) -> Vec<IrScalar> {
    match scalar {
        IrScalar::BinOp {
            left,
            op: IrBinOp::And,
            right,
        } => {
            let mut result = split_conjuncts(*left);
            result.extend(split_conjuncts(*right));
            result
        }
        other => vec![other],
    }
}

/// Combine a list of conjuncts into a single AND expression.
fn combine_conjuncts(mut conjuncts: Vec<IrScalar>) -> IrScalar {
    assert!(!conjuncts.is_empty());
    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }
    let first = conjuncts.remove(0);
    conjuncts.into_iter().fold(first, |acc, c| IrScalar::BinOp {
        left: Box::new(acc),
        op: IrBinOp::And,
        right: Box::new(c),
    })
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

    fn gt(left: IrScalar, right: IrScalar) -> IrScalar {
        IrScalar::BinOp {
            left: Box::new(left),
            op: IrBinOp::Gt,
            right: Box::new(right),
        }
    }

    #[test]
    fn test_push_through_sort() {
        // Filter(age > 25, Sort(Scan))
        let plan = QueryPlan::Filter {
            predicate: gt(col("age"), IrScalar::LitInt(25)),
            input: Box::new(QueryPlan::Sort {
                orders: vec![SortOrder {
                    column: "name".to_string(),
                    ascending: true,
                }],
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should become Sort(Filter(age > 25, Scan))
        if let QueryPlan::Sort { input, .. } = &pushed {
            assert!(matches!(input.as_ref(), QueryPlan::Filter { .. }));
        } else {
            panic!("Expected Sort at top");
        }
    }

    #[test]
    fn test_push_through_project() {
        // Filter(age > 25, Project([name, age], Scan))
        let plan = QueryPlan::Filter {
            predicate: gt(col("age"), IrScalar::LitInt(25)),
            input: Box::new(QueryPlan::Project {
                columns: vec![col("name"), col("age")],
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should become Project(Filter(age > 25, Scan))
        if let QueryPlan::Project { input, .. } = &pushed {
            assert!(matches!(input.as_ref(), QueryPlan::Filter { .. }));
        } else {
            panic!("Expected Project at top");
        }
    }

    #[test]
    fn test_no_push_through_project_alias() {
        // Filter(total > 100, Project([total: price * qty], Scan))
        let plan = QueryPlan::Filter {
            predicate: gt(col("total"), IrScalar::LitInt(100)),
            input: Box::new(QueryPlan::Project {
                columns: vec![IrScalar::Alias {
                    expr: Box::new(IrScalar::BinOp {
                        left: Box::new(col("price")),
                        op: IrBinOp::Mul,
                        right: Box::new(col("qty")),
                    }),
                    name: "total".to_string(),
                }],
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should NOT push: predicate references alias "total"
        assert!(matches!(pushed, QueryPlan::Filter { .. }));
    }

    #[test]
    fn test_push_through_with_columns() {
        // Filter(age > 25, WithColumns([total = price * qty], Scan))
        let plan = QueryPlan::Filter {
            predicate: gt(col("age"), IrScalar::LitInt(25)),
            input: Box::new(QueryPlan::WithColumns {
                columns: vec![(
                    "total".to_string(),
                    IrScalar::BinOp {
                        left: Box::new(col("price")),
                        op: IrBinOp::Mul,
                        right: Box::new(col("qty")),
                    },
                )],
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should push: "age" not in new columns
        if let QueryPlan::WithColumns { input, .. } = &pushed {
            assert!(matches!(input.as_ref(), QueryPlan::Filter { .. }));
        } else {
            panic!("Expected WithColumns at top");
        }
    }

    #[test]
    fn test_no_push_past_limit() {
        let plan = QueryPlan::Filter {
            predicate: gt(col("age"), IrScalar::LitInt(25)),
            input: Box::new(QueryPlan::Limit {
                count: 10,
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should NOT push past Limit
        if let QueryPlan::Filter { input, .. } = &pushed {
            assert!(matches!(input.as_ref(), QueryPlan::Limit { .. }));
        } else {
            panic!("Expected Filter at top");
        }
    }

    #[test]
    fn test_no_push_past_aggregate() {
        let plan = QueryPlan::Filter {
            predicate: gt(col("total"), IrScalar::LitInt(100)),
            input: Box::new(QueryPlan::Aggregate {
                group_by: vec![col("dept")],
                aggregates: vec![IrScalar::Alias {
                    expr: Box::new(IrScalar::Aggregate {
                        func: AggFunc::Sum,
                        arg: Box::new(col("salary")),
                    }),
                    name: "total".to_string(),
                }],
                input: Box::new(scan()),
            }),
        };

        let pushed = push_predicates_down(plan);

        // Should NOT push past Aggregate
        assert!(matches!(pushed, QueryPlan::Filter { .. }));
    }
}
