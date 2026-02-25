// ThinkingLanguage — Abstract Syntax Tree
// Licensed under MIT OR Apache-2.0
//
// Defines the tree structure produced by the parser.
// Phase 0 subset: let bindings, functions, if/else, match/case,
// pipe operator, basic types, print.

/// A complete TL program is a list of statements
#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Stmt>,
}

/// Statements
#[derive(Debug, Clone)]
pub enum Stmt {
    /// `let x = expr` or `let mut x: type = expr`
    Let {
        name: String,
        mutable: bool,
        type_ann: Option<TypeExpr>,
        value: Expr,
    },

    /// `fn name(params) -> return_type { body }`
    FnDecl {
        name: String,
        params: Vec<Param>,
        return_type: Option<TypeExpr>,
        body: Vec<Stmt>,
    },

    /// Expression statement (e.g., a function call on its own line)
    Expr(Expr),

    /// `return expr`
    Return(Option<Expr>),

    /// `if cond { body } else if cond { body } else { body }`
    If {
        condition: Expr,
        then_body: Vec<Stmt>,
        else_ifs: Vec<(Expr, Vec<Stmt>)>,
        else_body: Option<Vec<Stmt>>,
    },

    /// `while cond { body }`
    While {
        condition: Expr,
        body: Vec<Stmt>,
    },

    /// `for name in iter { body }`
    For {
        name: String,
        iter: Expr,
        body: Vec<Stmt>,
    },

    /// `schema Name { field: type, ... }`
    Schema {
        name: String,
        fields: Vec<SchemaField>,
    },

    /// `break`
    Break,

    /// `continue`
    Continue,
}

/// Expressions
#[derive(Debug, Clone)]
pub enum Expr {
    // ── Literals ──
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    None,

    /// Variable reference
    Ident(String),

    /// Binary operation: left op right
    BinOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },

    /// Unary operation: op expr
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    /// Function call: name(args)
    Call {
        function: Box<Expr>,
        args: Vec<Expr>,
    },

    /// Named argument in a call: key: value
    NamedArg {
        name: String,
        value: Box<Expr>,
    },

    /// Pipe: left |> right
    Pipe {
        left: Box<Expr>,
        right: Box<Expr>,
    },

    /// Member access: expr.field
    Member {
        object: Box<Expr>,
        field: String,
    },

    /// Index access: expr[index]
    Index {
        object: Box<Expr>,
        index: Box<Expr>,
    },

    /// List literal: [a, b, c]
    List(Vec<Expr>),

    /// Map literal: { key: value, ... }
    Map(Vec<(Expr, Expr)>),

    /// Block expression: { stmts; expr }
    Block {
        stmts: Vec<Stmt>,
        expr: Option<Box<Expr>>,
    },

    /// case { pattern => expr, ... }
    Case {
        arms: Vec<(Expr, Expr)>,
    },

    /// match expr { pattern => expr, ... }
    Match {
        subject: Box<Expr>,
        arms: Vec<(Expr, Expr)>,
    },

    /// Closure: (params) => expr
    Closure {
        params: Vec<Param>,
        body: Box<Expr>,
    },

    /// Range: start..end
    Range {
        start: Box<Expr>,
        end: Box<Expr>,
    },

    /// Null coalesce: expr ?? default
    NullCoalesce {
        expr: Box<Expr>,
        default: Box<Expr>,
    },

    /// Assignment: name = value (for reassigning mut variables)
    Assign {
        target: Box<Expr>,
        value: Box<Expr>,
    },
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    // Comparison
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // Logical
    And,
    Or,
}

impl std::fmt::Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Mod => write!(f, "%"),
            BinOp::Pow => write!(f, "**"),
            BinOp::Eq => write!(f, "=="),
            BinOp::Neq => write!(f, "!="),
            BinOp::Lt => write!(f, "<"),
            BinOp::Gt => write!(f, ">"),
            BinOp::Lte => write!(f, "<="),
            BinOp::Gte => write!(f, ">="),
            BinOp::And => write!(f, "and"),
            BinOp::Or => write!(f, "or"),
        }
    }
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// Function parameter
#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub type_ann: Option<TypeExpr>,
}

/// Schema field definition
#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub type_ann: TypeExpr,
}

/// Type expressions (Phase 0: basic types only)
#[derive(Debug, Clone)]
pub enum TypeExpr {
    /// Named type: int64, string, bool, float64, User
    Named(String),
    /// Generic type: table<User>, list<int64>
    Generic {
        name: String,
        args: Vec<TypeExpr>,
    },
    /// Optional type: T?
    Optional(Box<TypeExpr>),
    /// Function type: fn(int64, int64) -> int64
    Function {
        params: Vec<TypeExpr>,
        return_type: Box<TypeExpr>,
    },
}
