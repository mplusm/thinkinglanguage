// ThinkingLanguage — VM value types
// Optimized for the bytecode VM: Arc<str> strings, Arc<Prototype> functions.

use std::fmt;
use std::sync::Arc;

use tl_data::{ArrowSchema, DataFrame};

use crate::chunk::{BuiltinId, Prototype};

/// Runtime value for the bytecode VM.
#[derive(Clone)]
pub enum VmValue {
    Int(i64),
    Float(f64),
    String(Arc<str>),
    Bool(bool),
    None,
    List(Vec<VmValue>),
    /// A compiled closure (function + captured upvalues)
    Function(Arc<VmClosure>),
    /// A builtin function reference
    Builtin(BuiltinId),
    /// A lazy DataFusion table
    Table(VmTable),
    /// A schema definition
    Schema(VmSchema),
}

/// A closure: compiled function prototype + captured upvalues.
#[derive(Debug)]
pub struct VmClosure {
    pub prototype: Arc<Prototype>,
    pub upvalues: Vec<UpvalueRef>,
}

/// An upvalue reference — either open (pointing at a stack slot) or closed (owns the value).
#[derive(Debug, Clone)]
pub enum UpvalueRef {
    /// Points to a stack slot (base + index). Will be closed when the slot goes out of scope.
    Open { stack_index: usize },
    /// Closed over — owns the value.
    Closed(VmValue),
}

/// Wrapper around DataFusion DataFrame.
#[derive(Clone)]
pub struct VmTable {
    pub df: DataFrame,
}

impl fmt::Debug for VmTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<table>")
    }
}

/// Schema definition.
#[derive(Debug, Clone)]
pub struct VmSchema {
    pub name: Arc<str>,
    pub arrow_schema: Arc<ArrowSchema>,
}

impl VmValue {
    pub fn is_truthy(&self) -> bool {
        match self {
            VmValue::Bool(b) => *b,
            VmValue::Int(n) => *n != 0,
            VmValue::Float(n) => *n != 0.0,
            VmValue::String(s) => !s.is_empty(),
            VmValue::List(items) => !items.is_empty(),
            VmValue::None => false,
            _ => true,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            VmValue::Int(_) => "int64",
            VmValue::Float(_) => "float64",
            VmValue::String(_) => "string",
            VmValue::Bool(_) => "bool",
            VmValue::List(_) => "list",
            VmValue::None => "none",
            VmValue::Function(_) => "function",
            VmValue::Builtin(_) => "builtin",
            VmValue::Table(_) => "table",
            VmValue::Schema(_) => "schema",
        }
    }
}

impl fmt::Debug for VmValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VmValue::Int(n) => write!(f, "Int({n})"),
            VmValue::Float(n) => write!(f, "Float({n})"),
            VmValue::String(s) => write!(f, "String({s:?})"),
            VmValue::Bool(b) => write!(f, "Bool({b})"),
            VmValue::None => write!(f, "None"),
            VmValue::List(items) => write!(f, "List({items:?})"),
            VmValue::Function(c) => write!(f, "<fn {}>", c.prototype.name),
            VmValue::Builtin(id) => write!(f, "<builtin {}>", id.name()),
            VmValue::Table(_) => write!(f, "<table>"),
            VmValue::Schema(s) => write!(f, "<schema {}>", s.name),
        }
    }
}

impl fmt::Display for VmValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VmValue::Int(n) => write!(f, "{n}"),
            VmValue::Float(n) => {
                if n.fract() == 0.0 {
                    write!(f, "{n:.1}")
                } else {
                    write!(f, "{n}")
                }
            }
            VmValue::String(s) => write!(f, "{s}"),
            VmValue::Bool(b) => write!(f, "{b}"),
            VmValue::None => write!(f, "none"),
            VmValue::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            VmValue::Function(c) => write!(f, "<fn {}>", c.prototype.name),
            VmValue::Builtin(id) => write!(f, "<builtin {}>", id.name()),
            VmValue::Table(_) => write!(f, "<table>"),
            VmValue::Schema(s) => write!(f, "<schema {}>", s.name),
        }
    }
}
