// ThinkingLanguage — AST to IR Builder
// Converts AST pipe chain operations into a QueryPlan tree.

use tl_ast::{BinOp, Expr, UnaryOp};

use crate::plan::*;

/// Build a QueryPlan from a table source expression and a list of
/// (op_name, args) pairs extracted from a pipe chain.
///
/// Returns Err if any operation or expression cannot be represented in IR.
pub fn build_query_plan(source: &Expr, ops: &[(String, Vec<Expr>)]) -> Result<QueryPlan, String> {
    let table_source = expr_to_table_source(source);
    let mut plan = QueryPlan::Scan {
        source: table_source,
    };

    for (op_name, args) in ops {
        plan = build_op(plan, op_name, args)?;
    }

    Ok(plan)
}

fn expr_to_table_source(expr: &Expr) -> TableSource {
    match expr {
        Expr::Ident(name) => TableSource::Variable(name.clone()),
        other => TableSource::AstExpr(Box::new(other.clone())),
    }
}

fn build_op(input: QueryPlan, op_name: &str, args: &[Expr]) -> Result<QueryPlan, String> {
    match op_name {
        "filter" => build_filter(input, args),
        "select" => build_select(input, args),
        "sort" => build_sort(input, args),
        "with" => build_with(input, args),
        "aggregate" => build_aggregate(input, args),
        "join" => build_join(input, args),
        "head" | "limit" => build_limit(input, args),
        "collect" => Ok(QueryPlan::Collect {
            input: Box::new(input),
        }),
        "show" => build_show(input, args),
        "describe" => Ok(QueryPlan::Describe {
            input: Box::new(input),
        }),
        "write_csv" => build_write_csv(input, args),
        "write_parquet" => build_write_parquet(input, args),
        "fill_null" => build_fill_null(input, args),
        "drop_null" => build_drop_null(input, args),
        "dedup" => build_dedup(input, args),
        "clamp" => build_clamp(input, args),
        "data_profile" => Ok(QueryPlan::DataProfile {
            input: Box::new(input),
        }),
        "row_count" => Ok(QueryPlan::RowCount {
            input: Box::new(input),
        }),
        "null_rate" => build_null_rate(input, args),
        "is_unique" => build_is_unique(input, args),
        _ => Err(format!("Unknown table operation: {op_name}")),
    }
}

fn build_filter(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("filter() expects 1 argument".to_string());
    }
    let predicate = ast_to_ir_scalar(&args[0])?;
    Ok(QueryPlan::Filter {
        predicate,
        input: Box::new(input),
    })
}

fn build_select(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.is_empty() {
        return Err("select() expects at least 1 argument".to_string());
    }
    let mut columns = Vec::new();
    for arg in args {
        match arg {
            Expr::NamedArg { name, value } => {
                let expr = ast_to_ir_scalar(value)?;
                columns.push(IrScalar::Alias {
                    expr: Box::new(expr),
                    name: name.clone(),
                });
            }
            other => {
                columns.push(ast_to_ir_scalar(other)?);
            }
        }
    }
    Ok(QueryPlan::Project {
        columns,
        input: Box::new(input),
    })
}

fn build_sort(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.is_empty() {
        return Err("sort() expects at least 1 argument".to_string());
    }
    let mut orders = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let col_name = match &args[i] {
            Expr::Ident(name) => name.clone(),
            Expr::String(name) => name.clone(),
            _ => return Err("sort() column must be an identifier or string".to_string()),
        };
        i += 1;
        let ascending = if i < args.len() {
            match &args[i] {
                Expr::String(dir) if dir == "desc" || dir == "DESC" => {
                    i += 1;
                    false
                }
                Expr::String(dir) if dir == "asc" || dir == "ASC" => {
                    i += 1;
                    true
                }
                _ => true,
            }
        } else {
            true
        };
        orders.push(SortOrder {
            column: col_name,
            ascending,
        });
    }
    Ok(QueryPlan::Sort {
        orders,
        input: Box::new(input),
    })
}

fn build_with(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("with() expects 1 argument (map)".to_string());
    }
    let pairs = match &args[0] {
        Expr::Map(pairs) => pairs,
        _ => return Err("with() expects a map { col = expr, ... }".to_string()),
    };
    let mut columns = Vec::new();
    for (key, value_expr) in pairs {
        let col_name = match key {
            Expr::String(s) => s.clone(),
            Expr::Ident(s) => s.clone(),
            _ => return Err("with() key must be a string or identifier".to_string()),
        };
        let ir_expr = ast_to_ir_scalar(value_expr)?;
        columns.push((col_name, ir_expr));
    }
    Ok(QueryPlan::WithColumns {
        columns,
        input: Box::new(input),
    })
}

fn build_aggregate(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    let mut group_by = Vec::new();
    let mut aggregates = Vec::new();
    for arg in args {
        match arg {
            Expr::NamedArg { name, value } if name == "by" => match value.as_ref() {
                Expr::Ident(col_name) => {
                    group_by.push(IrScalar::Column(col_name.clone()));
                }
                Expr::String(col_name) => {
                    group_by.push(IrScalar::Column(col_name.clone()));
                }
                Expr::List(items) => {
                    for item in items {
                        match item {
                            Expr::Ident(s) => group_by.push(IrScalar::Column(s.clone())),
                            Expr::String(s) => group_by.push(IrScalar::Column(s.clone())),
                            _ => return Err("by: list items must be strings or identifiers".to_string()),
                        }
                    }
                }
                _ => return Err("by: must be a column name or list".to_string()),
            },
            Expr::NamedArg { name, value } => {
                let agg_expr = ast_to_ir_scalar(value)?;
                aggregates.push(IrScalar::Alias {
                    expr: Box::new(agg_expr),
                    name: name.clone(),
                });
            }
            other => {
                aggregates.push(ast_to_ir_scalar(other)?);
            }
        }
    }
    Ok(QueryPlan::Aggregate {
        group_by,
        aggregates,
        input: Box::new(input),
    })
}

fn build_join(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.is_empty() {
        return Err("join() expects at least 1 argument (right table)".to_string());
    }
    let right_source = expr_to_table_source(&args[0]);
    let right_plan = QueryPlan::Scan {
        source: right_source,
    };

    let mut left_cols = Vec::new();
    let mut right_cols = Vec::new();
    let mut kind = IrJoinKind::Inner;

    for arg in &args[1..] {
        match arg {
            Expr::NamedArg { name, value } if name == "on" => {
                if let Expr::BinOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } = value.as_ref()
                {
                    let lc = match left.as_ref() {
                        Expr::Ident(s) | Expr::String(s) => s.clone(),
                        _ => return Err("on: left side must be a column name".to_string()),
                    };
                    let rc = match right.as_ref() {
                        Expr::Ident(s) | Expr::String(s) => s.clone(),
                        _ => return Err("on: right side must be a column name".to_string()),
                    };
                    left_cols.push(lc);
                    right_cols.push(rc);
                }
            }
            Expr::NamedArg { name, value } if name == "kind" => {
                if let Expr::String(kind_str) = value.as_ref() {
                    kind = match kind_str.as_str() {
                        "inner" => IrJoinKind::Inner,
                        "left" => IrJoinKind::Left,
                        "right" => IrJoinKind::Right,
                        "full" => IrJoinKind::Full,
                        _ => return Err(format!("Unknown join type: {kind_str}")),
                    };
                }
            }
            _ => {}
        }
    }

    Ok(QueryPlan::Join {
        left: Box::new(input),
        right: Box::new(right_plan),
        kind,
        left_cols,
        right_cols,
    })
}

fn build_limit(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    let count = match args.first() {
        Some(Expr::Int(n)) => *n as usize,
        None => 10,
        _ => return Err("head/limit expects an integer".to_string()),
    };
    Ok(QueryPlan::Limit {
        count,
        input: Box::new(input),
    })
}

fn build_show(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    let limit = match args.first() {
        Some(Expr::Int(n)) => *n as usize,
        None => 20,
        _ => 20,
    };
    Ok(QueryPlan::Show {
        limit,
        input: Box::new(input),
    })
}

fn build_write_csv(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("write_csv() expects 1 argument (path)".to_string());
    }
    let path = ast_to_ir_scalar(&args[0])?;
    Ok(QueryPlan::WriteCsv {
        path,
        input: Box::new(input),
    })
}

fn build_write_parquet(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("write_parquet() expects 1 argument (path)".to_string());
    }
    let path = ast_to_ir_scalar(&args[0])?;
    Ok(QueryPlan::WriteParquet {
        path,
        input: Box::new(input),
    })
}

fn build_fill_null(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    // fill_null(column, strategy) or fill_null(column, value: expr)
    if args.is_empty() {
        return Err("fill_null() expects at least 1 argument".to_string());
    }
    let column = match &args[0] {
        Expr::Ident(s) | Expr::String(s) => s.clone(),
        _ => return Err("fill_null() first arg must be a column name".to_string()),
    };
    let mut strategy = "value".to_string();
    let mut value = None;
    for arg in &args[1..] {
        match arg {
            Expr::String(s) => strategy = s.clone(),
            Expr::NamedArg { name, value: v } if name == "value" => {
                value = Some(ast_to_ir_scalar(v)?);
            }
            other => {
                value = Some(ast_to_ir_scalar(other)?);
            }
        }
    }
    Ok(QueryPlan::FillNull {
        column,
        strategy,
        value,
        input: Box::new(input),
    })
}

fn build_drop_null(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    let column = match args.first() {
        Some(Expr::Ident(s)) | Some(Expr::String(s)) => Some(s.clone()),
        None => None,
        _ => return Err("drop_null() expects a column name or no args".to_string()),
    };
    Ok(QueryPlan::DropNull {
        column,
        input: Box::new(input),
    })
}

fn build_dedup(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    let mut columns = Vec::new();
    for arg in args {
        match arg {
            Expr::Ident(s) | Expr::String(s) => columns.push(s.clone()),
            _ => return Err("dedup() args must be column names".to_string()),
        }
    }
    Ok(QueryPlan::Dedup {
        columns,
        input: Box::new(input),
    })
}

fn build_clamp(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 3 {
        return Err("clamp() expects 3 arguments (column, min, max)".to_string());
    }
    let column = match &args[0] {
        Expr::Ident(s) | Expr::String(s) => s.clone(),
        _ => return Err("clamp() first arg must be a column name".to_string()),
    };
    let min = ast_to_ir_scalar(&args[1])?;
    let max = ast_to_ir_scalar(&args[2])?;
    Ok(QueryPlan::Clamp {
        column,
        min,
        max,
        input: Box::new(input),
    })
}

fn build_null_rate(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("null_rate() expects 1 argument (column)".to_string());
    }
    let column = match &args[0] {
        Expr::Ident(s) | Expr::String(s) => s.clone(),
        _ => return Err("null_rate() arg must be a column name".to_string()),
    };
    Ok(QueryPlan::NullRate {
        column,
        input: Box::new(input),
    })
}

fn build_is_unique(input: QueryPlan, args: &[Expr]) -> Result<QueryPlan, String> {
    if args.len() != 1 {
        return Err("is_unique() expects 1 argument (column)".to_string());
    }
    let column = match &args[0] {
        Expr::Ident(s) | Expr::String(s) => s.clone(),
        _ => return Err("is_unique() arg must be a column name".to_string()),
    };
    Ok(QueryPlan::IsUnique {
        column,
        input: Box::new(input),
    })
}

/// Convert an AST expression to an IR scalar expression.
/// Returns Err for expressions that can't be represented in the IR
/// (closures, blocks, await, yield, etc.) — triggers fallback to legacy path.
pub fn ast_to_ir_scalar(expr: &Expr) -> Result<IrScalar, String> {
    match expr {
        Expr::Int(v) => Ok(IrScalar::LitInt(*v)),
        Expr::Float(v) => Ok(IrScalar::lit_float(*v)),
        Expr::String(s) => Ok(IrScalar::LitString(s.clone())),
        Expr::Bool(b) => Ok(IrScalar::LitBool(*b)),
        Expr::None => Ok(IrScalar::LitNull),

        Expr::Ident(name) => {
            // In table context, identifiers are column references
            Ok(IrScalar::Column(name.clone()))
        }

        Expr::BinOp { left, op, right } => {
            let ir_left = ast_to_ir_scalar(left)?;
            let ir_right = ast_to_ir_scalar(right)?;
            let ir_op = ast_binop_to_ir(op)?;
            Ok(IrScalar::BinOp {
                left: Box::new(ir_left),
                op: ir_op,
                right: Box::new(ir_right),
            })
        }

        Expr::UnaryOp { op, expr } => {
            let ir_expr = ast_to_ir_scalar(expr)?;
            let ir_op = match op {
                UnaryOp::Neg => IrUnaryOp::Neg,
                UnaryOp::Not => IrUnaryOp::Not,
                UnaryOp::Ref => return Err("Ref not supported in IR scalar".to_string()),
            };
            Ok(IrScalar::UnaryOp {
                op: ir_op,
                expr: Box::new(ir_expr),
            })
        }

        // Aggregate function calls: count(x), sum(x), avg(x), min(x), max(x)
        Expr::Call { function, args } => {
            if let Expr::Ident(fname) = function.as_ref() {
                if let Some(func) = match fname.as_str() {
                    "count" => Some(AggFunc::Count),
                    "sum" => Some(AggFunc::Sum),
                    "avg" => Some(AggFunc::Avg),
                    "min" => Some(AggFunc::Min),
                    "max" => Some(AggFunc::Max),
                    _ => None,
                } {
                    if args.len() != 1 {
                        return Err(format!("{fname}() expects 1 argument"));
                    }
                    let arg = ast_to_ir_scalar(&args[0])?;
                    return Ok(IrScalar::Aggregate {
                        func,
                        arg: Box::new(arg),
                    });
                }
            }
            Err("Unsupported function call in IR scalar".to_string())
        }

        Expr::NamedArg { name, value } => {
            let expr = ast_to_ir_scalar(value)?;
            Ok(IrScalar::Alias {
                expr: Box::new(expr),
                name: name.clone(),
            })
        }

        // Member access like table.column — treat as column ref
        Expr::Member { object: _, field } => Ok(IrScalar::Column(field.clone())),

        // Everything else triggers fallback
        Expr::Closure { .. }
        | Expr::Block { .. }
        | Expr::Await(_)
        | Expr::Yield(_)
        | Expr::Match { .. }
        | Expr::Case { .. }
        | Expr::Pipe { .. }
        | Expr::Index { .. }
        | Expr::List(_)
        | Expr::Map(_)
        | Expr::Range { .. }
        | Expr::NullCoalesce { .. }
        | Expr::Assign { .. }
        | Expr::StructInit { .. }
        | Expr::EnumVariant { .. }
        | Expr::Try(_)
        | Expr::Decimal(_) => Err(format!("Unsupported expression in IR scalar: {:?}", std::mem::discriminant(expr))),
    }
}

fn ast_binop_to_ir(op: &BinOp) -> Result<IrBinOp, String> {
    Ok(match op {
        BinOp::Add => IrBinOp::Add,
        BinOp::Sub => IrBinOp::Sub,
        BinOp::Mul => IrBinOp::Mul,
        BinOp::Div => IrBinOp::Div,
        BinOp::Mod => IrBinOp::Mod,
        BinOp::Pow => IrBinOp::Pow,
        BinOp::Eq => IrBinOp::Eq,
        BinOp::Neq => IrBinOp::Neq,
        BinOp::Lt => IrBinOp::Lt,
        BinOp::Gt => IrBinOp::Gt,
        BinOp::Lte => IrBinOp::Lte,
        BinOp::Gte => IrBinOp::Gte,
        BinOp::And => IrBinOp::And,
        BinOp::Or => IrBinOp::Or,
    })
}
