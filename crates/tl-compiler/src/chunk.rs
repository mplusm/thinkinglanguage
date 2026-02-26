// ThinkingLanguage — Bytecode chunk (compiled function)

use std::sync::Arc;
use tl_ast::Expr as AstExpr;

/// A compiled function / top-level script.
#[derive(Debug, Clone)]
pub struct Prototype {
    /// Bytecode instructions
    pub code: Vec<u32>,
    /// Constant pool
    pub constants: Vec<Constant>,
    /// Source line for each instruction (for error reporting)
    pub lines: Vec<u32>,
    /// Number of parameters
    pub arity: u8,
    /// Number of local variable slots needed
    pub num_locals: u8,
    /// Number of registers needed
    pub num_registers: u8,
    /// Upvalue definitions (how to capture from enclosing scope)
    pub upvalue_defs: Vec<UpvalueDef>,
    /// Function name (for debugging)
    pub name: String,
}

impl Prototype {
    pub fn new(name: String) -> Self {
        Prototype {
            code: Vec::new(),
            constants: Vec::new(),
            lines: Vec::new(),
            arity: 0,
            num_locals: 0,
            num_registers: 0,
            upvalue_defs: Vec::new(),
            name,
        }
    }
}

/// Constant pool entry.
#[derive(Debug, Clone)]
pub enum Constant {
    Int(i64),
    Float(f64),
    String(Arc<str>),
    /// A nested function prototype
    Prototype(Arc<Prototype>),
    /// Raw AST expression — used for table pipe operations
    /// so the VM can pass it to translate_expr at runtime
    AstExpr(Box<AstExpr>),
    /// A list of AST expressions (for table op args)
    AstExprList(Vec<AstExpr>),
}

/// How to capture an upvalue from the enclosing function.
#[derive(Debug, Clone, Copy)]
pub struct UpvalueDef {
    /// If true, capture from enclosing function's locals.
    /// If false, capture from enclosing function's upvalues.
    pub is_local: bool,
    /// Index into enclosing function's locals or upvalues.
    pub index: u8,
}

/// Builtin function identifiers — avoids string comparisons in the VM hot loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BuiltinId {
    Print = 0,
    Println = 1,
    Len = 2,
    Str = 3,
    Int = 4,
    Float = 5,
    Abs = 6,
    Min = 7,
    Max = 8,
    Range = 9,
    Push = 10,
    TypeOf = 11,
    Map = 12,
    Filter = 13,
    Reduce = 14,
    Sum = 15,
    Any = 16,
    All = 17,
    ReadCsv = 18,
    ReadParquet = 19,
    WriteCsv = 20,
    WriteParquet = 21,
    Collect = 22,
    Show = 23,
    Describe = 24,
    Head = 25,
    Postgres = 26,
}

impl BuiltinId {
    pub fn from_name(name: &str) -> Option<BuiltinId> {
        match name {
            "print" => Some(BuiltinId::Print),
            "println" => Some(BuiltinId::Println),
            "len" => Some(BuiltinId::Len),
            "str" => Some(BuiltinId::Str),
            "int" => Some(BuiltinId::Int),
            "float" => Some(BuiltinId::Float),
            "abs" => Some(BuiltinId::Abs),
            "min" => Some(BuiltinId::Min),
            "max" => Some(BuiltinId::Max),
            "range" => Some(BuiltinId::Range),
            "push" => Some(BuiltinId::Push),
            "type_of" => Some(BuiltinId::TypeOf),
            "map" => Some(BuiltinId::Map),
            "filter" => Some(BuiltinId::Filter),
            "reduce" => Some(BuiltinId::Reduce),
            "sum" => Some(BuiltinId::Sum),
            "any" => Some(BuiltinId::Any),
            "all" => Some(BuiltinId::All),
            "read_csv" => Some(BuiltinId::ReadCsv),
            "read_parquet" => Some(BuiltinId::ReadParquet),
            "write_csv" => Some(BuiltinId::WriteCsv),
            "write_parquet" => Some(BuiltinId::WriteParquet),
            "collect" => Some(BuiltinId::Collect),
            "show" => Some(BuiltinId::Show),
            "describe" => Some(BuiltinId::Describe),
            "head" => Some(BuiltinId::Head),
            "postgres" => Some(BuiltinId::Postgres),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            BuiltinId::Print => "print",
            BuiltinId::Println => "println",
            BuiltinId::Len => "len",
            BuiltinId::Str => "str",
            BuiltinId::Int => "int",
            BuiltinId::Float => "float",
            BuiltinId::Abs => "abs",
            BuiltinId::Min => "min",
            BuiltinId::Max => "max",
            BuiltinId::Range => "range",
            BuiltinId::Push => "push",
            BuiltinId::TypeOf => "type_of",
            BuiltinId::Map => "map",
            BuiltinId::Filter => "filter",
            BuiltinId::Reduce => "reduce",
            BuiltinId::Sum => "sum",
            BuiltinId::Any => "any",
            BuiltinId::All => "all",
            BuiltinId::ReadCsv => "read_csv",
            BuiltinId::ReadParquet => "read_parquet",
            BuiltinId::WriteCsv => "write_csv",
            BuiltinId::WriteParquet => "write_parquet",
            BuiltinId::Collect => "collect",
            BuiltinId::Show => "show",
            BuiltinId::Describe => "describe",
            BuiltinId::Head => "head",
            BuiltinId::Postgres => "postgres",
        }
    }
}

/// Table pipe operation descriptor, stored as a constant.
#[derive(Debug, Clone)]
pub struct TableOpDescriptor {
    pub op_name: String,
    pub args: Vec<AstExpr>,
}
