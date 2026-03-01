// ThinkingLanguage — Intermediate Representation
// Phase 29: IR for table pipe chain optimization.
//
// Provides a QueryPlan-based IR that sits between AST and bytecode emission
// for table pipe chains. The optimizer applies predicate pushdown, filter
// merging, column pruning, and CSE before lowering back to flat ops.

pub mod plan;
pub mod display;
pub mod builder;
pub mod optimize;
pub mod lower;
pub mod passes;

// Re-exports for convenience
pub use builder::build_query_plan;
pub use lower::lower_plan;
pub use optimize::optimize;
pub use plan::*;

#[cfg(test)]
mod tests {
    use super::*;
    use tl_ast::{BinOp, Expr};

    // ── Plan construction tests ──

    #[test]
    fn test_scan_display() {
        let plan = QueryPlan::Scan {
            source: TableSource::Variable("users".to_string()),
        };
        assert_eq!(format!("{plan}"), "Scan: users");
    }

    #[test]
    fn test_filter_display() {
        let plan = QueryPlan::Filter {
            predicate: IrScalar::BinOp {
                left: Box::new(IrScalar::Column("age".to_string())),
                op: IrBinOp::Gt,
                right: Box::new(IrScalar::LitInt(25)),
            },
            input: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("users".to_string()),
            }),
        };
        let display = format!("{plan}");
        assert!(display.contains("Filter: (age > 25)"));
        assert!(display.contains("Scan: users"));
    }

    #[test]
    fn test_project_display() {
        let plan = QueryPlan::Project {
            columns: vec![
                IrScalar::Column("name".to_string()),
                IrScalar::Column("age".to_string()),
            ],
            input: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("users".to_string()),
            }),
        };
        let display = format!("{plan}");
        assert!(display.contains("Project: [name, age]"));
    }

    #[test]
    fn test_sort_display() {
        let plan = QueryPlan::Sort {
            orders: vec![
                SortOrder { column: "age".to_string(), ascending: false },
            ],
            input: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("users".to_string()),
            }),
        };
        let display = format!("{plan}");
        assert!(display.contains("Sort: [age DESC]"));
    }

    #[test]
    fn test_join_display() {
        let plan = QueryPlan::Join {
            left: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("users".to_string()),
            }),
            right: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("orders".to_string()),
            }),
            kind: IrJoinKind::Inner,
            left_cols: vec!["id".to_string()],
            right_cols: vec!["user_id".to_string()],
        };
        let display = format!("{plan}");
        assert!(display.contains("Join: INNER on [id = user_id]"));
    }

    #[test]
    fn test_aggregate_display() {
        let plan = QueryPlan::Aggregate {
            group_by: vec![IrScalar::Column("dept".to_string())],
            aggregates: vec![IrScalar::Alias {
                expr: Box::new(IrScalar::Aggregate {
                    func: AggFunc::Sum,
                    arg: Box::new(IrScalar::Column("salary".to_string())),
                }),
                name: "total".to_string(),
            }],
            input: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("employees".to_string()),
            }),
        };
        let display = format!("{plan}");
        assert!(display.contains("Aggregate:"));
        assert!(display.contains("sum(salary) AS total"));
    }

    // ── Builder tests ──

    #[test]
    fn test_build_filter() {
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
        assert!(matches!(plan, QueryPlan::Filter { .. }));
    }

    #[test]
    fn test_build_select() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![(
            "select".to_string(),
            vec![
                Expr::Ident("name".to_string()),
                Expr::Ident("age".to_string()),
            ],
        )];

        let plan = build_query_plan(&source, &ops).unwrap();
        if let QueryPlan::Project { columns, .. } = &plan {
            assert_eq!(columns.len(), 2);
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_build_select_with_alias() {
        let source = Expr::Ident("orders".to_string());
        let ops = vec![(
            "select".to_string(),
            vec![
                Expr::Ident("name".to_string()),
                Expr::NamedArg {
                    name: "total".to_string(),
                    value: Box::new(Expr::BinOp {
                        left: Box::new(Expr::Ident("price".to_string())),
                        op: BinOp::Mul,
                        right: Box::new(Expr::Ident("qty".to_string())),
                    }),
                },
            ],
        )];

        let plan = build_query_plan(&source, &ops).unwrap();
        if let QueryPlan::Project { columns, .. } = &plan {
            assert_eq!(columns.len(), 2);
            assert!(matches!(&columns[1], IrScalar::Alias { name, .. } if name == "total"));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_build_sort() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![(
            "sort".to_string(),
            vec![
                Expr::Ident("age".to_string()),
                Expr::String("desc".to_string()),
            ],
        )];

        let plan = build_query_plan(&source, &ops).unwrap();
        if let QueryPlan::Sort { orders, .. } = &plan {
            assert_eq!(orders.len(), 1);
            assert_eq!(orders[0].column, "age");
            assert!(!orders[0].ascending);
        } else {
            panic!("Expected Sort");
        }
    }

    #[test]
    fn test_build_limit() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![("head".to_string(), vec![Expr::Int(5)])];

        let plan = build_query_plan(&source, &ops).unwrap();
        if let QueryPlan::Limit { count, .. } = &plan {
            assert_eq!(*count, 5);
        } else {
            panic!("Expected Limit");
        }
    }

    #[test]
    fn test_build_chain() {
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
                vec![Expr::Ident("name".to_string())],
            ),
            ("head".to_string(), vec![Expr::Int(10)]),
        ];

        let plan = build_query_plan(&source, &ops).unwrap();
        // Should be Limit(Project(Filter(Scan)))
        assert!(matches!(plan, QueryPlan::Limit { .. }));
    }

    #[test]
    fn test_build_unknown_op_fails() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![("unknown_op".to_string(), vec![])];

        assert!(build_query_plan(&source, &ops).is_err());
    }

    #[test]
    fn test_build_unsupported_expr_fails() {
        let source = Expr::Ident("users".to_string());
        let ops = vec![(
            "filter".to_string(),
            vec![Expr::Closure {
                params: vec![],
                return_type: None,
                body: tl_ast::ClosureBody::Expr(Box::new(Expr::Bool(true))),
            }],
        )];

        assert!(build_query_plan(&source, &ops).is_err());
    }

    // ── Optimizer integration tests ──

    #[test]
    fn test_optimize_filter_merge() {
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

        // Should be single Filter(Scan), not Filter(Filter(Scan))
        if let QueryPlan::Filter { input, .. } = &optimized {
            assert!(matches!(input.as_ref(), QueryPlan::Scan { .. }));
        } else {
            panic!("Expected merged Filter");
        }
    }

    #[test]
    fn test_optimize_predicate_pushdown() {
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

        // Filter should be pushed before Project
        if let QueryPlan::Project { input, .. } = &optimized {
            assert!(matches!(input.as_ref(), QueryPlan::Filter { .. }));
        } else {
            panic!("Expected Project at top after pushdown");
        }
    }

    // ── Lowering round-trip tests ──

    #[test]
    fn test_lower_simple_filter() {
        let plan = QueryPlan::Filter {
            predicate: IrScalar::BinOp {
                left: Box::new(IrScalar::Column("age".to_string())),
                op: IrBinOp::Gt,
                right: Box::new(IrScalar::LitInt(25)),
            },
            input: Box::new(QueryPlan::Scan {
                source: TableSource::Variable("users".to_string()),
            }),
        };

        let ops = lower_plan(&plan);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0, "filter");
    }

    #[test]
    fn test_lower_multi_op_chain() {
        let plan = QueryPlan::Show {
            limit: 20,
            input: Box::new(QueryPlan::Limit {
                count: 10,
                input: Box::new(QueryPlan::Sort {
                    orders: vec![SortOrder { column: "age".to_string(), ascending: false }],
                    input: Box::new(QueryPlan::Filter {
                        predicate: IrScalar::BinOp {
                            left: Box::new(IrScalar::Column("age".to_string())),
                            op: IrBinOp::Gt,
                            right: Box::new(IrScalar::LitInt(25)),
                        },
                        input: Box::new(QueryPlan::Scan {
                            source: TableSource::Variable("users".to_string()),
                        }),
                    }),
                }),
            }),
        };

        let ops = lower_plan(&plan);
        assert_eq!(ops.len(), 4);
        assert_eq!(ops[0].0, "filter");
        assert_eq!(ops[1].0, "sort");
        assert_eq!(ops[2].0, "limit");
        assert_eq!(ops[3].0, "show");
    }
}
