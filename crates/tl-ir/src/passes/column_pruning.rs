// ThinkingLanguage — Column Pruning Pass
// Merges adjacent Project nodes and eliminates unnecessary columns.

use crate::passes::predicate_pushdown::referenced_columns;
use crate::plan::*;
use std::collections::HashSet;

/// Prune unnecessary columns from the plan.
/// Merges adjacent Project nodes.
pub fn prune_columns(plan: QueryPlan) -> QueryPlan {
    match plan {
        // Merge adjacent Projects: Project(cols1, Project(cols2, input))
        QueryPlan::Project {
            columns: outer_cols,
            input,
        } => {
            let optimized_input = prune_columns(*input);

            if let QueryPlan::Project {
                columns: inner_cols,
                input: inner_input,
            } = optimized_input
            {
                // Compute which inner columns are needed by outer
                let needed = columns_needed_by(&outer_cols);
                let pruned_inner: Vec<IrScalar> = inner_cols
                    .into_iter()
                    .filter(|c| {
                        let col_name = scalar_output_name(c);
                        col_name.is_none_or(|n| needed.contains(&n))
                    })
                    .collect();

                // If outer just selects plain columns that pass through,
                // and inner provides them, we can merge into one Project
                if outer_cols.iter().all(|c| matches!(c, IrScalar::Column(_))) {
                    // Simple case: outer is all column refs, just keep outer
                    QueryPlan::Project {
                        columns: outer_cols,
                        input: inner_input,
                    }
                } else {
                    // Keep both but with pruned inner
                    QueryPlan::Project {
                        columns: outer_cols,
                        input: Box::new(QueryPlan::Project {
                            columns: pruned_inner,
                            input: inner_input,
                        }),
                    }
                }
            } else {
                QueryPlan::Project {
                    columns: outer_cols,
                    input: Box::new(optimized_input),
                }
            }
        }

        // Recurse into Join children
        QueryPlan::Join {
            left,
            right,
            kind,
            left_cols,
            right_cols,
        } => QueryPlan::Join {
            left: Box::new(prune_columns(*left)),
            right: Box::new(prune_columns(*right)),
            kind,
            left_cols,
            right_cols,
        },

        // For all other nodes, recurse
        other => {
            if let Some(input) = other.input().cloned() {
                let optimized = prune_columns(input);
                other.with_input(optimized)
            } else {
                other
            }
        }
    }
}

/// Get the set of column names needed by a list of scalar expressions.
fn columns_needed_by(scalars: &[IrScalar]) -> HashSet<String> {
    let mut needed = HashSet::new();
    for s in scalars {
        needed.extend(referenced_columns(s));
    }
    needed
}

/// Get the output name of a scalar (the column name it produces).
fn scalar_output_name(scalar: &IrScalar) -> Option<String> {
    match scalar {
        IrScalar::Column(name) => Some(name.clone()),
        IrScalar::Alias { name, .. } => Some(name.clone()),
        _ => None,
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

    #[test]
    fn test_merge_adjacent_projects_simple_columns() {
        // Project([name], Project([name, age, email], Scan))
        let plan = QueryPlan::Project {
            columns: vec![col("name")],
            input: Box::new(QueryPlan::Project {
                columns: vec![col("name"), col("age"), col("email")],
                input: Box::new(scan()),
            }),
        };

        let pruned = prune_columns(plan);

        // Should merge into Project([name], Scan)
        if let QueryPlan::Project { columns, input } = &pruned {
            assert_eq!(columns.len(), 1);
            assert!(matches!(input.as_ref(), QueryPlan::Scan { .. }));
        } else {
            panic!("Expected single Project");
        }
    }

    #[test]
    fn test_single_project_unchanged() {
        let plan = QueryPlan::Project {
            columns: vec![col("name"), col("age")],
            input: Box::new(scan()),
        };

        let pruned = prune_columns(plan);

        if let QueryPlan::Project { columns, .. } = &pruned {
            assert_eq!(columns.len(), 2);
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_nested_project_with_alias() {
        // Project([total: x * y], Project([x, y, z], Scan))
        let plan = QueryPlan::Project {
            columns: vec![IrScalar::Alias {
                expr: Box::new(IrScalar::BinOp {
                    left: Box::new(col("x")),
                    op: IrBinOp::Mul,
                    right: Box::new(col("y")),
                }),
                name: "total".to_string(),
            }],
            input: Box::new(QueryPlan::Project {
                columns: vec![col("x"), col("y"), col("z")],
                input: Box::new(scan()),
            }),
        };

        let pruned = prune_columns(plan);

        // Inner should have z pruned
        if let QueryPlan::Project { input, .. } = &pruned {
            if let QueryPlan::Project { columns, .. } = input.as_ref() {
                assert_eq!(columns.len(), 2); // x and y, not z
            } else {
                panic!("Expected inner Project");
            }
        } else {
            panic!("Expected outer Project");
        }
    }
}
