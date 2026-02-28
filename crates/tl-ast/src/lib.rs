// ThinkingLanguage — Abstract Syntax Tree
// Licensed under MIT OR Apache-2.0
//
// Defines the tree structure produced by the parser.
// Phase 0 subset: let bindings, functions, if/else, match/case,
// pipe operator, basic types, print.

use tl_errors::Span;

/// A complete TL program is a list of statements
#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Stmt>,
}

/// A statement with source location information.
#[derive(Debug, Clone)]
pub struct Stmt {
    pub kind: StmtKind,
    pub span: Span,
}

/// A use-import target
#[derive(Debug, Clone)]
pub enum UseItem {
    /// `use data.transforms.clean_users`
    Single(Vec<String>),
    /// `use data.transforms.{clean_users, CleanedUser}`
    Group(Vec<String>, Vec<String>),
    /// `use data.transforms.*`
    Wildcard(Vec<String>),
    /// `use data.connectors.postgres as pg`
    Aliased(Vec<String>, String),
}

/// A trait bound on a type parameter: `T: Comparable + Hashable`
#[derive(Debug, Clone)]
pub struct TraitBound {
    pub type_param: String,
    pub traits: Vec<String>,
}

/// A method signature within a trait definition
#[derive(Debug, Clone)]
pub struct TraitMethod {
    pub name: String,
    pub params: Vec<Param>,
    pub return_type: Option<TypeExpr>,
}

/// Statement variants
#[derive(Debug, Clone)]
pub enum StmtKind {
    /// `let x = expr` or `let mut x: type = expr`
    Let {
        name: String,
        mutable: bool,
        type_ann: Option<TypeExpr>,
        value: Expr,
        is_public: bool,
    },

    /// `fn name<T, U>(params) -> return_type where T: Bound { body }`
    FnDecl {
        name: String,
        type_params: Vec<String>,
        params: Vec<Param>,
        return_type: Option<TypeExpr>,
        bounds: Vec<TraitBound>,
        body: Vec<Stmt>,
        is_generator: bool,
        is_public: bool,
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
        is_public: bool,
    },

    /// `model name = train algorithm { key: value, ... }`
    Train {
        name: String,
        algorithm: String,
        config: Vec<(String, Expr)>,
    },

    /// `pipeline name { extract { ... } transform { ... } load { ... } }`
    Pipeline {
        name: String,
        extract: Vec<Stmt>,
        transform: Vec<Stmt>,
        load: Vec<Stmt>,
        schedule: Option<String>,
        timeout: Option<String>,
        retries: Option<i64>,
        on_failure: Option<Vec<Stmt>>,
        on_success: Option<Vec<Stmt>>,
    },

    /// `stream name { source: expr, window: spec, transform: { ... }, sink: expr }`
    StreamDecl {
        name: String,
        source: Expr,
        transform: Vec<Stmt>,
        sink: Option<Expr>,
        window: Option<WindowSpec>,
        watermark: Option<String>,
    },

    /// `source name = connector TYPE { key: value, ... }`
    SourceDecl {
        name: String,
        connector_type: String,
        config: Vec<(String, Expr)>,
    },

    /// `sink name = connector TYPE { key: value, ... }`
    SinkDecl {
        name: String,
        connector_type: String,
        config: Vec<(String, Expr)>,
    },

    /// `struct Name<T, U> { field: type, ... }`
    StructDecl {
        name: String,
        type_params: Vec<String>,
        fields: Vec<SchemaField>,
        is_public: bool,
    },

    /// `enum Name<T, E> { Variant, Variant(types), ... }`
    EnumDecl {
        name: String,
        type_params: Vec<String>,
        variants: Vec<EnumVariant>,
        is_public: bool,
    },

    /// `impl<T> Type { fn methods... }`
    ImplBlock {
        type_name: String,
        type_params: Vec<String>,
        methods: Vec<Stmt>,
    },

    /// `try { ... } catch e { ... }`
    TryCatch {
        try_body: Vec<Stmt>,
        catch_var: String,
        catch_body: Vec<Stmt>,
    },

    /// `throw expr`
    Throw(Expr),

    /// `import "path.tl"` or `import "path.tl" as name`
    Import {
        path: String,
        alias: Option<String>,
    },

    /// `test "name" { ... }`
    Test {
        name: String,
        body: Vec<Stmt>,
    },

    /// `use data.transforms.clean_users` etc.
    Use {
        item: UseItem,
        is_public: bool,
    },

    /// `mod transforms` or `pub mod transforms`
    ModDecl {
        name: String,
        is_public: bool,
    },

    /// `trait Display<T> { fn show(self) -> string }`
    TraitDef {
        name: String,
        type_params: Vec<String>,
        methods: Vec<TraitMethod>,
        is_public: bool,
    },

    /// `impl Display for Point { fn show(self) -> string { ... } }`
    TraitImpl {
        trait_name: String,
        type_name: String,
        type_params: Vec<String>,
        methods: Vec<Stmt>,
    },

    /// `let { x, y } = expr` or `let [a, b] = expr`
    LetDestructure {
        pattern: Pattern,
        mutable: bool,
        value: Expr,
        is_public: bool,
    },

    /// `break`
    Break,

    /// `continue`
    Continue,
}

/// Enum variant definition
#[derive(Debug, Clone)]
pub struct EnumVariant {
    pub name: String,
    pub fields: Vec<TypeExpr>,
}

/// Window specification for stream processing
#[derive(Debug, Clone)]
pub enum WindowSpec {
    /// `tumbling(duration)` — fixed-size, non-overlapping windows
    Tumbling(String),
    /// `sliding(window_size, slide_interval)` — overlapping windows
    Sliding(String, String),
    /// `session(gap_duration)` — session windows based on activity gap
    Session(String),
}

/// A pattern for match arms and let-destructuring.
/// Identifiers in pattern position are bindings (create new variables),
/// not value references. Use literals, enum variants, or guards for comparison.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// `_` — matches anything, binds nothing
    Wildcard,
    /// Literal value: 1, "hi", true, none
    Literal(Expr),
    /// Binding: `x` — matches anything, binds to name
    Binding(String),
    /// Enum variant: `Color::Red(r, g, b)` or `None`
    Enum {
        type_name: String,
        variant: String,
        args: Vec<Pattern>,
    },
    /// Struct pattern: `Point { x, y }` or `{ x, y }`
    Struct {
        name: Option<String>,
        fields: Vec<StructPatternField>,
    },
    /// List pattern: `[a, b, ...rest]`
    List {
        elements: Vec<Pattern>,
        rest: Option<String>,
    },
    /// OR pattern: `A | B | C`
    Or(Vec<Pattern>),
}

/// A field in a struct destructuring pattern.
#[derive(Debug, Clone)]
pub struct StructPatternField {
    pub name: String,
    /// None = shorthand `{ x }` means `{ x: x }`
    pub pattern: Option<Pattern>,
}

/// A match arm: `pattern [if guard] => body`
#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Pattern,
    pub guard: Option<Expr>,
    pub body: Expr,
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
        arms: Vec<MatchArm>,
    },

    /// match expr { pattern => expr, ... }
    Match {
        subject: Box<Expr>,
        arms: Vec<MatchArm>,
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

    /// Struct initialization: Name { field: value, ... }
    StructInit {
        name: String,
        fields: Vec<(String, Expr)>,
    },

    /// Enum variant: Enum::Variant or Enum::Variant(args)
    EnumVariant {
        enum_name: String,
        variant: String,
        args: Vec<Expr>,
    },

    /// Await expression: `await expr`
    Await(Box<Expr>),

    /// Yield expression: `yield expr` or bare `yield`
    Yield(Option<Box<Expr>>),

    /// Try propagation: `expr?` — unwrap Result/Option or early return
    Try(Box<Expr>),
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
