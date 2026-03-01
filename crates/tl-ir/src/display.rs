// ThinkingLanguage — IR Display implementations
// Human-readable printing of query plans.

use std::fmt;

use crate::plan::*;

impl fmt::Display for IrBinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IrBinOp::Add => write!(f, "+"),
            IrBinOp::Sub => write!(f, "-"),
            IrBinOp::Mul => write!(f, "*"),
            IrBinOp::Div => write!(f, "/"),
            IrBinOp::Mod => write!(f, "%"),
            IrBinOp::Pow => write!(f, "**"),
            IrBinOp::Eq => write!(f, "=="),
            IrBinOp::Neq => write!(f, "!="),
            IrBinOp::Lt => write!(f, "<"),
            IrBinOp::Gt => write!(f, ">"),
            IrBinOp::Lte => write!(f, "<="),
            IrBinOp::Gte => write!(f, ">="),
            IrBinOp::And => write!(f, "AND"),
            IrBinOp::Or => write!(f, "OR"),
        }
    }
}

impl fmt::Display for IrUnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IrUnaryOp::Neg => write!(f, "-"),
            IrUnaryOp::Not => write!(f, "NOT"),
        }
    }
}

impl fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggFunc::Count => write!(f, "count"),
            AggFunc::Sum => write!(f, "sum"),
            AggFunc::Avg => write!(f, "avg"),
            AggFunc::Min => write!(f, "min"),
            AggFunc::Max => write!(f, "max"),
        }
    }
}

impl fmt::Display for IrScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IrScalar::Column(name) => write!(f, "{name}"),
            IrScalar::LitInt(v) => write!(f, "{v}"),
            IrScalar::LitFloat(bits) => write!(f, "{}", f64::from_bits(*bits)),
            IrScalar::LitString(s) => write!(f, "\"{s}\""),
            IrScalar::LitBool(b) => write!(f, "{b}"),
            IrScalar::LitNull => write!(f, "null"),
            IrScalar::BinOp { left, op, right } => write!(f, "({left} {op} {right})"),
            IrScalar::UnaryOp { op, expr } => write!(f, "{op}({expr})"),
            IrScalar::Aggregate { func, arg } => write!(f, "{func}({arg})"),
            IrScalar::Alias { expr, name } => write!(f, "{expr} AS {name}"),
            IrScalar::Var(name) => write!(f, "${name}"),
        }
    }
}

impl fmt::Display for IrJoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IrJoinKind::Inner => write!(f, "INNER"),
            IrJoinKind::Left => write!(f, "LEFT"),
            IrJoinKind::Right => write!(f, "RIGHT"),
            IrJoinKind::Full => write!(f, "FULL"),
        }
    }
}

impl fmt::Display for TableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSource::Variable(name) => write!(f, "{name}"),
            TableSource::AstExpr(_) => write!(f, "<expr>"),
        }
    }
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dir = if self.ascending { "ASC" } else { "DESC" };
        write!(f, "{} {}", self.column, dir)
    }
}

impl QueryPlan {
    /// Format the plan as an indented tree.
    pub fn display_indented(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        match self {
            QueryPlan::Scan { source } => {
                format!("{pad}Scan: {source}")
            }
            QueryPlan::Filter { predicate, input } => {
                format!(
                    "{pad}Filter: {predicate}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Project { columns, input } => {
                let cols: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
                format!(
                    "{pad}Project: [{}]\n{}",
                    cols.join(", "),
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Sort { orders, input } => {
                let ords: Vec<String> = orders.iter().map(|o| o.to_string()).collect();
                format!(
                    "{pad}Sort: [{}]\n{}",
                    ords.join(", "),
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::WithColumns { columns, input } => {
                let cols: Vec<String> =
                    columns.iter().map(|(n, e)| format!("{n} = {e}")).collect();
                format!(
                    "{pad}WithColumns: [{}]\n{}",
                    cols.join(", "),
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Aggregate {
                group_by,
                aggregates,
                input,
            } => {
                let gb: Vec<String> = group_by.iter().map(|c| c.to_string()).collect();
                let aggs: Vec<String> = aggregates.iter().map(|a| a.to_string()).collect();
                format!(
                    "{pad}Aggregate: by=[{}] aggs=[{}]\n{}",
                    gb.join(", "),
                    aggs.join(", "),
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Join {
                left,
                right,
                kind,
                left_cols,
                right_cols,
            } => {
                format!(
                    "{pad}Join: {kind} on [{} = {}]\n{}\n{}",
                    left_cols.join(", "),
                    right_cols.join(", "),
                    left.display_indented(indent + 1),
                    right.display_indented(indent + 1)
                )
            }
            QueryPlan::Limit { count, input } => {
                format!(
                    "{pad}Limit: {count}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Collect { input } => {
                format!("{pad}Collect\n{}", input.display_indented(indent + 1))
            }
            QueryPlan::Show { limit, input } => {
                format!(
                    "{pad}Show: {limit}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Describe { input } => {
                format!("{pad}Describe\n{}", input.display_indented(indent + 1))
            }
            QueryPlan::WriteCsv { path, input } => {
                format!(
                    "{pad}WriteCsv: {path}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::WriteParquet { path, input } => {
                format!(
                    "{pad}WriteParquet: {path}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::FillNull {
                column,
                strategy,
                input,
                ..
            } => {
                format!(
                    "{pad}FillNull: {column} strategy={strategy}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::DropNull { column, input } => {
                let col_str = column.as_deref().unwrap_or("*");
                format!(
                    "{pad}DropNull: {col_str}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Dedup { columns, input } => {
                format!(
                    "{pad}Dedup: [{}]\n{}",
                    columns.join(", "),
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::Clamp {
                column,
                min,
                max,
                input,
            } => {
                format!(
                    "{pad}Clamp: {column} [{min}, {max}]\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::DataProfile { input } => {
                format!("{pad}DataProfile\n{}", input.display_indented(indent + 1))
            }
            QueryPlan::RowCount { input } => {
                format!("{pad}RowCount\n{}", input.display_indented(indent + 1))
            }
            QueryPlan::NullRate { column, input } => {
                format!(
                    "{pad}NullRate: {column}\n{}",
                    input.display_indented(indent + 1)
                )
            }
            QueryPlan::IsUnique { column, input } => {
                format!(
                    "{pad}IsUnique: {column}\n{}",
                    input.display_indented(indent + 1)
                )
            }
        }
    }
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_indented(0))
    }
}
