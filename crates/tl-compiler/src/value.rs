// ThinkingLanguage — VM value types
// Optimized for the bytecode VM: Arc<str> strings, Arc<Prototype> functions.

use std::fmt;
use std::sync::{Arc, Mutex, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};

use tl_data::{ArrowSchema, DataFrame};
use tl_ai::{TlTensor, TlModel};
use tl_stream::{ConnectorConfig, PipelineDef, StreamDef, PipelineResult};

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
    /// A tensor (ndarray)
    Tensor(Arc<TlTensor>),
    /// A trained model
    Model(Arc<TlModel>),
    /// A connector configuration
    Connector(Arc<ConnectorConfig>),
    /// A pipeline result
    PipelineResult(Arc<PipelineResult>),
    /// A pipeline definition
    PipelineDef(Arc<PipelineDef>),
    /// A stream definition
    StreamDef(Arc<StreamDef>),
    /// A struct type definition
    StructDef(Arc<VmStructDef>),
    /// A struct instance
    StructInstance(Arc<VmStructInstance>),
    /// An enum type definition
    EnumDef(Arc<VmEnumDef>),
    /// An enum instance
    EnumInstance(Arc<VmEnumInstance>),
    /// A module (from import)
    Module(Arc<VmModule>),
    /// An ordered map (string keys)
    Map(Vec<(Arc<str>, VmValue)>),
    /// A spawned task handle
    Task(Arc<VmTask>),
    /// A channel for inter-task communication
    Channel(Arc<VmChannel>),
    /// A generator (lazy iterator)
    Generator(Arc<Mutex<VmGenerator>>),
}

/// Struct type definition
#[derive(Debug, Clone)]
pub struct VmStructDef {
    pub name: Arc<str>,
    pub fields: Vec<Arc<str>>,
}

/// Struct instance
#[derive(Debug, Clone)]
pub struct VmStructInstance {
    pub type_name: Arc<str>,
    pub fields: Vec<(Arc<str>, VmValue)>,
}

/// Enum type definition
#[derive(Debug, Clone)]
pub struct VmEnumDef {
    pub name: Arc<str>,
    pub variants: Vec<(Arc<str>, usize)>, // (variant_name, field_count)
}

/// Enum instance
#[derive(Debug, Clone)]
pub struct VmEnumInstance {
    pub type_name: Arc<str>,
    pub variant: Arc<str>,
    pub fields: Vec<VmValue>,
}

/// Module (imported file's exports)
#[derive(Debug, Clone)]
pub struct VmModule {
    pub name: Arc<str>,
    pub exports: std::collections::HashMap<String, VmValue>,
}

/// Counter for generating unique task IDs
static TASK_COUNTER: AtomicU64 = AtomicU64::new(1);
/// Counter for generating unique channel IDs
static CHANNEL_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A spawned task handle — wraps the receiver for the task result.
pub struct VmTask {
    pub receiver: Mutex<Option<mpsc::Receiver<Result<VmValue, String>>>>,
    pub id: u64,
}

impl VmTask {
    pub fn new(receiver: mpsc::Receiver<Result<VmValue, String>>) -> Self {
        VmTask {
            receiver: Mutex::new(Some(receiver)),
            id: TASK_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for VmTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<task {}>", self.id)
    }
}

/// A channel for inter-task communication.
pub struct VmChannel {
    pub sender: mpsc::SyncSender<VmValue>,
    pub receiver: Arc<Mutex<mpsc::Receiver<VmValue>>>,
    pub id: u64,
}

impl VmChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(capacity);
        VmChannel {
            sender: tx,
            receiver: Arc::new(Mutex::new(rx)),
            id: CHANNEL_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl Clone for VmChannel {
    fn clone(&self) -> Self {
        VmChannel {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            id: self.id,
        }
    }
}

impl fmt::Debug for VmChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<channel {}>", self.id)
    }
}

/// Counter for generating unique generator IDs
static GENERATOR_COUNTER: AtomicU64 = AtomicU64::new(1);

/// The kind of generator — user-defined (via yield) or built-in combinator.
pub enum GeneratorKind {
    /// User-defined generator (from fn with yield)
    UserDefined {
        prototype: Arc<Prototype>,
        upvalues: Vec<UpvalueRef>,
        saved_stack: Vec<VmValue>,
        ip: usize,
    },
    /// Built-in iterator over a list
    ListIter { items: Vec<VmValue>, index: usize },
    /// Take at most N items from a source generator
    Take { source: Arc<Mutex<VmGenerator>>, remaining: usize },
    /// Skip the first N items from a source generator
    Skip { source: Arc<Mutex<VmGenerator>>, remaining: usize },
    /// Map a function over each yielded value
    Map { source: Arc<Mutex<VmGenerator>>, func: VmValue },
    /// Filter values using a predicate function
    Filter { source: Arc<Mutex<VmGenerator>>, func: VmValue },
    /// Chain two generators: yield from first, then second
    Chain { first: Arc<Mutex<VmGenerator>>, second: Arc<Mutex<VmGenerator>>, on_second: bool },
    /// Zip two generators: yield [a, b] pairs
    Zip { first: Arc<Mutex<VmGenerator>>, second: Arc<Mutex<VmGenerator>> },
    /// Enumerate: yield [index, value] pairs
    Enumerate { source: Arc<Mutex<VmGenerator>>, index: usize },
}

impl fmt::Debug for GeneratorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratorKind::UserDefined { .. } => write!(f, "UserDefined"),
            GeneratorKind::ListIter { .. } => write!(f, "ListIter"),
            GeneratorKind::Take { .. } => write!(f, "Take"),
            GeneratorKind::Skip { .. } => write!(f, "Skip"),
            GeneratorKind::Map { .. } => write!(f, "Map"),
            GeneratorKind::Filter { .. } => write!(f, "Filter"),
            GeneratorKind::Chain { .. } => write!(f, "Chain"),
            GeneratorKind::Zip { .. } => write!(f, "Zip"),
            GeneratorKind::Enumerate { .. } => write!(f, "Enumerate"),
        }
    }
}

/// A generator object — wraps a GeneratorKind with done state.
pub struct VmGenerator {
    pub kind: GeneratorKind,
    pub done: bool,
    pub id: u64,
}

impl VmGenerator {
    pub fn new(kind: GeneratorKind) -> Self {
        VmGenerator {
            kind,
            done: false,
            id: GENERATOR_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for VmGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<generator {}>", self.id)
    }
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
            VmValue::Map(pairs) => !pairs.is_empty(),
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
            VmValue::Tensor(_) => "tensor",
            VmValue::Model(_) => "model",
            VmValue::Connector(_) => "connector",
            VmValue::PipelineResult(_) => "pipeline_result",
            VmValue::PipelineDef(_) => "pipeline",
            VmValue::StreamDef(_) => "stream",
            VmValue::StructDef(_) => "struct_def",
            VmValue::StructInstance(_) => "struct",
            VmValue::EnumDef(_) => "enum_def",
            VmValue::EnumInstance(_) => "enum",
            VmValue::Module(_) => "module",
            VmValue::Map(_) => "map",
            VmValue::Task(_) => "task",
            VmValue::Channel(_) => "channel",
            VmValue::Generator(_) => "generator",
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
            VmValue::Tensor(t) => write!(f, "Tensor({t:?})"),
            VmValue::Model(m) => write!(f, "Model({m:?})"),
            VmValue::Connector(c) => write!(f, "<connector {}>", c.name),
            VmValue::PipelineResult(r) => write!(f, "{r:?}"),
            VmValue::PipelineDef(p) => write!(f, "<pipeline {}>", p.name),
            VmValue::StreamDef(s) => write!(f, "<stream {}>", s.name),
            VmValue::StructDef(d) => write!(f, "<struct {}>", d.name),
            VmValue::StructInstance(s) => {
                write!(f, "{} {{ ", s.type_name)?;
                for (i, (k, v)) in s.fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k}: {v:?}")?;
                }
                write!(f, " }}")
            }
            VmValue::EnumDef(d) => write!(f, "<enum {}>", d.name),
            VmValue::EnumInstance(e) => {
                write!(f, "{}::{}", e.type_name, e.variant)?;
                if !e.fields.is_empty() {
                    write!(f, "({:?})", e.fields)?;
                }
                Ok(())
            }
            VmValue::Module(m) => write!(f, "<module {}>", m.name),
            VmValue::Map(pairs) => {
                write!(f, "Map{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k:?}: {v:?}")?;
                }
                write!(f, "}}")
            }
            VmValue::Task(t) => write!(f, "<task {}>", t.id),
            VmValue::Channel(c) => write!(f, "<channel {}>", c.id),
            VmValue::Generator(g) => {
                let guard = g.lock().unwrap();
                write!(f, "<generator {}>", guard.id)
            }
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
            VmValue::Tensor(t) => write!(f, "{t}"),
            VmValue::Model(m) => write!(f, "{m}"),
            VmValue::Connector(c) => write!(f, "{c}"),
            VmValue::PipelineResult(r) => write!(f, "{r}"),
            VmValue::PipelineDef(p) => write!(f, "{p}"),
            VmValue::StreamDef(s) => write!(f, "{s}"),
            VmValue::StructDef(d) => write!(f, "<struct {}>", d.name),
            VmValue::StructInstance(s) => {
                write!(f, "{} {{ ", s.type_name)?;
                for (i, (k, v)) in s.fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, " }}")
            }
            VmValue::EnumDef(d) => write!(f, "<enum {}>", d.name),
            VmValue::EnumInstance(e) => {
                write!(f, "{}::{}", e.type_name, e.variant)?;
                if !e.fields.is_empty() {
                    write!(f, "(")?;
                    for (i, v) in e.fields.iter().enumerate() {
                        if i > 0 { write!(f, ", ")?; }
                        write!(f, "{v}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            VmValue::Module(m) => write!(f, "<module {}>", m.name),
            VmValue::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            VmValue::Task(t) => write!(f, "<task {}>", t.id),
            VmValue::Channel(c) => write!(f, "<channel {}>", c.id),
            VmValue::Generator(g) => {
                let guard = g.lock().unwrap();
                write!(f, "<generator {}>", guard.id)
            }
        }
    }
}
