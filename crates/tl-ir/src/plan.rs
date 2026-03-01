// ThinkingLanguage — IR Plan Types
// Phase 29: Intermediate Representation for table pipe chain optimization.

use tl_ast::Expr;

/// Source of a table in a query plan.
/// Opaque to the optimizer — it only cares about plan structure.
#[derive(Debug, Clone)]
pub enum TableSource {
    /// A variable reference: `users`, `orders`
    Variable(String),
    /// An arbitrary AST expression the optimizer cannot inspect
    AstExpr(Box<Expr>),
}

/// Scalar expression within a query plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IrScalar {
    /// Column reference
    Column(String),
    /// Integer literal
    LitInt(i64),
    /// Float literal (stored as bits for Hash/Eq)
    LitFloat(u64),
    /// String literal
    LitString(String),
    /// Boolean literal
    LitBool(bool),
    /// Null literal
    LitNull,
    /// Binary operation
    BinOp {
        left: Box<IrScalar>,
        op: IrBinOp,
        right: Box<IrScalar>,
    },
    /// Unary operation
    UnaryOp {
        op: IrUnaryOp,
        expr: Box<IrScalar>,
    },
    /// Aggregate function call
    Aggregate {
        func: AggFunc,
        arg: Box<IrScalar>,
    },
    /// Aliased expression: `expr AS name`
    Alias {
        expr: Box<IrScalar>,
        name: String,
    },
    /// Variable reference (not a column — used for runtime values)
    Var(String),
}

impl IrScalar {
    /// Create a float literal from an f64
    pub fn lit_float(v: f64) -> Self {
        IrScalar::LitFloat(v.to_bits())
    }

    /// Get the float value from a LitFloat
    pub fn as_f64(&self) -> Option<f64> {
        if let IrScalar::LitFloat(bits) = self {
            Some(f64::from_bits(*bits))
        } else {
            None
        }
    }
}

/// Binary operators in IR expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IrBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
}

/// Unary operators in IR expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IrUnaryOp {
    Neg,
    Not,
}

/// Aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Sort order for a column
#[derive(Debug, Clone)]
pub struct SortOrder {
    pub column: String,
    pub ascending: bool,
}

/// Join kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IrJoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// A query plan node — represents a relational operation on a table.
/// Built bottom-up: Scan is the leaf, each node wraps its input.
#[derive(Debug, Clone)]
pub enum QueryPlan {
    /// Leaf: read from a table source
    Scan {
        source: TableSource,
    },
    /// Filter rows by predicate
    Filter {
        predicate: IrScalar,
        input: Box<QueryPlan>,
    },
    /// Project (select) specific columns/expressions
    Project {
        columns: Vec<IrScalar>,
        input: Box<QueryPlan>,
    },
    /// Sort by columns
    Sort {
        orders: Vec<SortOrder>,
        input: Box<QueryPlan>,
    },
    /// Add computed columns
    WithColumns {
        columns: Vec<(String, IrScalar)>,
        input: Box<QueryPlan>,
    },
    /// Aggregate with optional grouping
    Aggregate {
        group_by: Vec<IrScalar>,
        aggregates: Vec<IrScalar>,
        input: Box<QueryPlan>,
    },
    /// Join two tables
    Join {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        kind: IrJoinKind,
        left_cols: Vec<String>,
        right_cols: Vec<String>,
    },
    /// Limit number of rows
    Limit {
        count: usize,
        input: Box<QueryPlan>,
    },
    /// Collect to string
    Collect {
        input: Box<QueryPlan>,
    },
    /// Show (print) rows
    Show {
        limit: usize,
        input: Box<QueryPlan>,
    },
    /// Describe schema
    Describe {
        input: Box<QueryPlan>,
    },
    /// Write to CSV file
    WriteCsv {
        path: IrScalar,
        input: Box<QueryPlan>,
    },
    /// Write to Parquet file
    WriteParquet {
        path: IrScalar,
        input: Box<QueryPlan>,
    },
    /// Fill null values
    FillNull {
        column: String,
        strategy: String,
        value: Option<IrScalar>,
        input: Box<QueryPlan>,
    },
    /// Drop rows with null
    DropNull {
        column: Option<String>,
        input: Box<QueryPlan>,
    },
    /// Deduplicate rows
    Dedup {
        columns: Vec<String>,
        input: Box<QueryPlan>,
    },
    /// Clamp column values
    Clamp {
        column: String,
        min: IrScalar,
        max: IrScalar,
        input: Box<QueryPlan>,
    },
    /// Data profiling
    DataProfile {
        input: Box<QueryPlan>,
    },
    /// Row count
    RowCount {
        input: Box<QueryPlan>,
    },
    /// Null rate
    NullRate {
        column: String,
        input: Box<QueryPlan>,
    },
    /// Is unique check
    IsUnique {
        column: String,
        input: Box<QueryPlan>,
    },
}

impl QueryPlan {
    /// Get a reference to the input plan, if any.
    pub fn input(&self) -> Option<&QueryPlan> {
        match self {
            QueryPlan::Scan { .. } => None,
            QueryPlan::Filter { input, .. }
            | QueryPlan::Project { input, .. }
            | QueryPlan::Sort { input, .. }
            | QueryPlan::WithColumns { input, .. }
            | QueryPlan::Aggregate { input, .. }
            | QueryPlan::Limit { input, .. }
            | QueryPlan::Collect { input, .. }
            | QueryPlan::Show { input, .. }
            | QueryPlan::Describe { input, .. }
            | QueryPlan::WriteCsv { input, .. }
            | QueryPlan::WriteParquet { input, .. }
            | QueryPlan::FillNull { input, .. }
            | QueryPlan::DropNull { input, .. }
            | QueryPlan::Dedup { input, .. }
            | QueryPlan::Clamp { input, .. }
            | QueryPlan::DataProfile { input, .. }
            | QueryPlan::RowCount { input, .. }
            | QueryPlan::NullRate { input, .. }
            | QueryPlan::IsUnique { input, .. } => Some(input),
            QueryPlan::Join { left, .. } => Some(left),
        }
    }

    /// Replace the input of this plan node (returns a new node).
    pub fn with_input(self, new_input: QueryPlan) -> QueryPlan {
        match self {
            QueryPlan::Scan { .. } => self,
            QueryPlan::Filter { predicate, .. } => QueryPlan::Filter {
                predicate,
                input: Box::new(new_input),
            },
            QueryPlan::Project { columns, .. } => QueryPlan::Project {
                columns,
                input: Box::new(new_input),
            },
            QueryPlan::Sort { orders, .. } => QueryPlan::Sort {
                orders,
                input: Box::new(new_input),
            },
            QueryPlan::WithColumns { columns, .. } => QueryPlan::WithColumns {
                columns,
                input: Box::new(new_input),
            },
            QueryPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => QueryPlan::Aggregate {
                group_by,
                aggregates,
                input: Box::new(new_input),
            },
            QueryPlan::Limit { count, .. } => QueryPlan::Limit {
                count,
                input: Box::new(new_input),
            },
            QueryPlan::Collect { .. } => QueryPlan::Collect {
                input: Box::new(new_input),
            },
            QueryPlan::Show { limit, .. } => QueryPlan::Show {
                limit,
                input: Box::new(new_input),
            },
            QueryPlan::Describe { .. } => QueryPlan::Describe {
                input: Box::new(new_input),
            },
            QueryPlan::WriteCsv { path, .. } => QueryPlan::WriteCsv {
                path,
                input: Box::new(new_input),
            },
            QueryPlan::WriteParquet { path, .. } => QueryPlan::WriteParquet {
                path,
                input: Box::new(new_input),
            },
            QueryPlan::FillNull {
                column,
                strategy,
                value,
                ..
            } => QueryPlan::FillNull {
                column,
                strategy,
                value,
                input: Box::new(new_input),
            },
            QueryPlan::DropNull { column, .. } => QueryPlan::DropNull {
                column,
                input: Box::new(new_input),
            },
            QueryPlan::Dedup { columns, .. } => QueryPlan::Dedup {
                columns,
                input: Box::new(new_input),
            },
            QueryPlan::Clamp {
                column, min, max, ..
            } => QueryPlan::Clamp {
                column,
                min,
                max,
                input: Box::new(new_input),
            },
            QueryPlan::DataProfile { .. } => QueryPlan::DataProfile {
                input: Box::new(new_input),
            },
            QueryPlan::RowCount { .. } => QueryPlan::RowCount {
                input: Box::new(new_input),
            },
            QueryPlan::NullRate { column, .. } => QueryPlan::NullRate {
                column,
                input: Box::new(new_input),
            },
            QueryPlan::IsUnique { column, .. } => QueryPlan::IsUnique {
                column,
                input: Box::new(new_input),
            },
            QueryPlan::Join {
                right,
                kind,
                left_cols,
                right_cols,
                ..
            } => QueryPlan::Join {
                left: Box::new(new_input),
                right,
                kind,
                left_cols,
                right_cols,
            },
        }
    }
}
