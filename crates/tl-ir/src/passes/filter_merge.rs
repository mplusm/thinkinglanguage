// ThinkingLanguage — Filter Merge Pass
// Combines adjacent Filter nodes: Filter(p1, Filter(p2, input)) → Filter(p1 AND p2, input)

use crate::plan::*;

/// Merge adjacent Filter nodes in the plan tree.
/// Applied bottom-up so chains of 3+ filters are fully merged.
pub fn merge_filters(plan: QueryPlan) -> QueryPlan {
    match plan {
        QueryPlan::Filter {
            predicate: p1,
            input,
        } => {
            // First, recursively optimize the input
            let optimized_input = merge_filters(*input);

            // Check if input is also a Filter — merge them
            if let QueryPlan::Filter {
                predicate: p2,
                input: inner_input,
            } = optimized_input
            {
                QueryPlan::Filter {
                    predicate: IrScalar::BinOp {
                        left: Box::new(p2),
                        op: IrBinOp::And,
                        right: Box::new(p1),
                    },
                    input: inner_input,
                }
            } else {
                QueryPlan::Filter {
                    predicate: p1,
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
            left: Box::new(merge_filters(*left)),
            right: Box::new(merge_filters(*right)),
            kind,
            left_cols,
            right_cols,
        },

        // For all other nodes, recurse into input
        other => {
            if let Some(input) = other.input().cloned() {
                let optimized_input = merge_filters(input);
                other.with_input(optimized_input)
            } else {
                other
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

    fn gt(left: IrScalar, right: IrScalar) -> IrScalar {
        IrScalar::BinOp {
            left: Box::new(left),
            op: IrBinOp::Gt,
            right: Box::new(right),
        }
    }

    fn neq(left: IrScalar, right: IrScalar) -> IrScalar {
        IrScalar::BinOp {
            left: Box::new(left),
            op: IrBinOp::Neq,
            right: Box::new(right),
        }
    }

    #[test]
    fn test_merge_two_adjacent_filters() {
        // Filter(name != "Bob", Filter(age > 25, Scan))
        let plan = QueryPlan::Filter {
            predicate: neq(col("name"), IrScalar::LitString("Bob".to_string())),
            input: Box::new(QueryPlan::Filter {
                predicate: gt(col("age"), IrScalar::LitInt(25)),
                input: Box::new(scan()),
            }),
        };

        let merged = merge_filters(plan);

        // Should become Filter((age > 25) AND (name != "Bob"), Scan)
        if let QueryPlan::Filter { predicate, input } = &merged {
            assert!(matches!(input.as_ref(), QueryPlan::Scan { .. }));
            if let IrScalar::BinOp { op, .. } = predicate {
                assert_eq!(*op, IrBinOp::And);
            } else {
                panic!("Expected AND predicate");
            }
        } else {
            panic!("Expected Filter node");
        }
    }

    #[test]
    fn test_merge_three_adjacent_filters() {
        // Filter(p3, Filter(p2, Filter(p1, Scan)))
        let plan = QueryPlan::Filter {
            predicate: IrScalar::LitBool(true),
            input: Box::new(QueryPlan::Filter {
                predicate: neq(col("name"), IrScalar::LitString("Bob".to_string())),
                input: Box::new(QueryPlan::Filter {
                    predicate: gt(col("age"), IrScalar::LitInt(25)),
                    input: Box::new(scan()),
                }),
            }),
        };

        let merged = merge_filters(plan);

        // Should be a single Filter with nested ANDs, input is Scan
        if let QueryPlan::Filter { input, .. } = &merged {
            assert!(matches!(input.as_ref(), QueryPlan::Scan { .. }));
        } else {
            panic!("Expected single Filter node");
        }
    }

    #[test]
    fn test_non_adjacent_filters_not_merged() {
        // Filter(p2, Sort(Filter(p1, Scan)))
        let plan = QueryPlan::Filter {
            predicate: neq(col("name"), IrScalar::LitString("Bob".to_string())),
            input: Box::new(QueryPlan::Sort {
                orders: vec![SortOrder {
                    column: "age".to_string(),
                    ascending: true,
                }],
                input: Box::new(QueryPlan::Filter {
                    predicate: gt(col("age"), IrScalar::LitInt(25)),
                    input: Box::new(scan()),
                }),
            }),
        };

        let merged = merge_filters(plan);

        // Should remain as Filter(Sort(Filter(Scan))) — not merged
        if let QueryPlan::Filter { input, .. } = &merged {
            if let QueryPlan::Sort { input: inner, .. } = input.as_ref() {
                assert!(matches!(inner.as_ref(), QueryPlan::Filter { .. }));
            } else {
                panic!("Expected Sort node");
            }
        } else {
            panic!("Expected Filter node");
        }
    }
}
