// ThinkingLanguage — VM value types
// Optimized for the bytecode VM: Arc<str> strings, Arc<Prototype> functions.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

#[cfg(feature = "native")]
use tl_ai::{TlModel, TlTensor};
#[cfg(feature = "native")]
use tl_data::{ArrowSchema, DataFrame};
#[cfg(feature = "gpu")]
use tl_gpu::GpuTensor;
#[cfg(feature = "native")]
use tl_stream::{AgentDef, ConnectorConfig, PipelineDef, PipelineResult, StreamDef};

use crate::chunk::{BuiltinId, Prototype};

/// Runtime value for the bytecode VM.
#[derive(Clone)]
pub enum VmValue {
    Int(i64),
    Float(f64),
    String(Arc<str>),
    Bool(bool),
    None,
    List(Box<Vec<VmValue>>),
    /// A compiled closure (function + captured upvalues)
    Function(Arc<VmClosure>),
    /// A builtin function reference
    Builtin(BuiltinId),
    /// A lazy DataFusion table
    #[cfg(feature = "native")]
    Table(VmTable),
    /// A schema definition
    #[cfg(feature = "native")]
    Schema(VmSchema),
    /// A tensor (ndarray)
    #[cfg(feature = "native")]
    Tensor(Arc<TlTensor>),
    /// A trained model
    #[cfg(feature = "native")]
    Model(Arc<TlModel>),
    /// A connector configuration
    #[cfg(feature = "native")]
    Connector(Arc<ConnectorConfig>),
    /// A pipeline result
    #[cfg(feature = "native")]
    PipelineResult(Arc<PipelineResult>),
    /// A pipeline definition
    #[cfg(feature = "native")]
    PipelineDef(Arc<PipelineDef>),
    /// A stream definition
    #[cfg(feature = "native")]
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
    Map(Box<Vec<(Arc<str>, VmValue)>>),
    /// A spawned task handle
    Task(Arc<VmTask>),
    /// A channel for inter-task communication
    Channel(Arc<VmChannel>),
    /// A generator (lazy iterator)
    Generator(Arc<Mutex<VmGenerator>>),
    /// A set (unique values)
    Set(Box<Vec<VmValue>>),
    /// Fixed-point decimal value
    Decimal(rust_decimal::Decimal),
    /// A datetime value (milliseconds since Unix epoch)
    DateTime(i64),
    /// A secret value (display-redacted)
    Secret(Arc<str>),
    /// An opaque Python object (feature-gated)
    #[cfg(feature = "python")]
    PyObject(Arc<crate::python::PyObjectWrapper>),
    /// A GPU-resident tensor (feature-gated)
    #[cfg(feature = "gpu")]
    GpuTensor(Arc<GpuTensor>),
    /// An agent definition
    #[cfg(feature = "native")]
    AgentDef(Arc<AgentDef>),
    /// Tombstone for a value consumed by pipe-move
    Moved,
    /// Read-only reference wrapper
    Ref(Arc<VmValue>),
}

impl PartialEq for VmValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (VmValue::Int(a), VmValue::Int(b)) => a == b,
            (VmValue::Float(a), VmValue::Float(b)) => a == b,
            (VmValue::String(a), VmValue::String(b)) => a == b,
            (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
            (VmValue::None, VmValue::None) => true,
            (VmValue::Decimal(a), VmValue::Decimal(b)) => a == b,
            (VmValue::DateTime(a), VmValue::DateTime(b)) => a == b,
            (VmValue::DateTime(a), VmValue::Int(b)) | (VmValue::Int(a), VmValue::DateTime(b)) => {
                a == b
            }
            (VmValue::List(a), VmValue::List(b)) => a == b,
            (VmValue::Map(a), VmValue::Map(b)) => a == b,
            (VmValue::Set(a), VmValue::Set(b)) => a == b,
            (VmValue::Ref(a), VmValue::Ref(b)) => a == b,
            (VmValue::Ref(inner), other) | (other, VmValue::Ref(inner)) => inner.as_ref() == other,
            _ => false,
        }
    }
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
///
/// **Note:** Tasks are shared via `Arc<VmTask>`, so cloning the `VmValue::Task`
/// variant creates a second handle to the *same* task. Only the first `await`
/// will receive the result; subsequent awaits on the same or cloned handle
/// return `None` (the receiver is taken).
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
    Take {
        source: Arc<Mutex<VmGenerator>>,
        remaining: usize,
    },
    /// Skip the first N items from a source generator
    Skip {
        source: Arc<Mutex<VmGenerator>>,
        remaining: usize,
    },
    /// Map a function over each yielded value
    Map {
        source: Arc<Mutex<VmGenerator>>,
        func: VmValue,
    },
    /// Filter values using a predicate function
    Filter {
        source: Arc<Mutex<VmGenerator>>,
        func: VmValue,
    },
    /// Chain two generators: yield from first, then second
    Chain {
        first: Arc<Mutex<VmGenerator>>,
        second: Arc<Mutex<VmGenerator>>,
        on_second: bool,
    },
    /// Zip two generators: yield [a, b] pairs
    Zip {
        first: Arc<Mutex<VmGenerator>>,
        second: Arc<Mutex<VmGenerator>>,
    },
    /// Enumerate: yield [index, value] pairs
    Enumerate {
        source: Arc<Mutex<VmGenerator>>,
        index: usize,
    },
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
///
/// **Note:** Generators are shared via `Arc<Mutex<VmGenerator>>`, so cloning
/// the `VmValue::Generator` variant shares iteration state. Both handles
/// advance the same underlying iterator.
///
/// **Known limitation:** `next()` on an exhausted generator returns `VmValue::None`,
/// which is indistinguishable from `yield none`. Use the generator's `done` field
/// or a wrapper to detect exhaustion if this matters.
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
#[cfg(feature = "native")]
#[derive(Clone)]
pub struct VmTable {
    pub df: DataFrame,
}

#[cfg(feature = "native")]
impl fmt::Debug for VmTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<table>")
    }
}

/// Schema definition.
#[cfg(feature = "native")]
#[derive(Debug, Clone)]
pub struct VmSchema {
    pub name: Arc<str>,
    pub version: i64,
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
            VmValue::Set(items) => !items.is_empty(),
            VmValue::None => false,
            VmValue::Decimal(d) => !d.is_zero(),
            #[cfg(feature = "python")]
            VmValue::PyObject(_) => true,
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(_) => true,
            VmValue::Moved => false,
            VmValue::Ref(inner) => inner.is_truthy(),
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
            #[cfg(feature = "native")]
            VmValue::Table(_) => "table",
            #[cfg(feature = "native")]
            VmValue::Schema(_) => "schema",
            #[cfg(feature = "native")]
            VmValue::Tensor(_) => "tensor",
            #[cfg(feature = "native")]
            VmValue::Model(_) => "model",
            #[cfg(feature = "native")]
            VmValue::Connector(_) => "connector",
            #[cfg(feature = "native")]
            VmValue::PipelineResult(_) => "pipeline_result",
            #[cfg(feature = "native")]
            VmValue::PipelineDef(_) => "pipeline",
            #[cfg(feature = "native")]
            VmValue::StreamDef(_) => "stream",
            VmValue::StructDef(_) => "struct_def",
            VmValue::StructInstance(_) => "struct",
            VmValue::EnumDef(_) => "enum_def",
            VmValue::EnumInstance(_) => "enum",
            VmValue::Module(_) => "module",
            VmValue::Map(_) => "map",
            VmValue::Set(_) => "set",
            VmValue::Task(_) => "task",
            VmValue::Channel(_) => "channel",
            VmValue::Generator(_) => "generator",
            VmValue::Decimal(_) => "decimal",
            VmValue::DateTime(_) => "datetime",
            VmValue::Secret(_) => "secret",
            #[cfg(feature = "python")]
            VmValue::PyObject(_) => "pyobject",
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(_) => "gpu_tensor",
            #[cfg(feature = "native")]
            VmValue::AgentDef(_) => "agent",
            VmValue::Moved => "<moved>",
            VmValue::Ref(inner) => inner.type_name(),
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
            #[cfg(feature = "native")]
            VmValue::Table(_) => write!(f, "<table>"),
            #[cfg(feature = "native")]
            VmValue::Schema(s) => write!(f, "<schema {}>", s.name),
            #[cfg(feature = "native")]
            VmValue::Tensor(t) => write!(f, "Tensor({t:?})"),
            #[cfg(feature = "native")]
            VmValue::Model(m) => write!(f, "Model({m:?})"),
            #[cfg(feature = "native")]
            VmValue::Connector(c) => write!(f, "<connector {}>", c.name),
            #[cfg(feature = "native")]
            VmValue::PipelineResult(r) => write!(f, "{r:?}"),
            #[cfg(feature = "native")]
            VmValue::PipelineDef(p) => write!(f, "<pipeline {}>", p.name),
            #[cfg(feature = "native")]
            VmValue::StreamDef(s) => write!(f, "<stream {}>", s.name),
            VmValue::StructDef(d) => write!(f, "<struct {}>", d.name),
            VmValue::StructInstance(s) => {
                write!(f, "{} {{ ", s.type_name)?;
                for (i, (k, v)) in s.fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
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
                    if i > 0 {
                        write!(f, ", ")?;
                    }
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
            VmValue::Set(items) => {
                write!(f, "Set{{")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v:?}")?;
                }
                write!(f, "}}")
            }
            VmValue::Decimal(d) => write!(f, "Decimal({d})"),
            VmValue::DateTime(ms) => {
                use chrono::TimeZone;
                let secs = *ms / 1000;
                let nsecs = ((*ms % 1000) * 1_000_000) as u32;
                match chrono::Utc.timestamp_opt(secs, nsecs).single() {
                    Some(dt) => write!(f, "DateTime({})", dt.format("%Y-%m-%d %H:%M:%S UTC")),
                    None => write!(f, "DateTime({ms}ms)"),
                }
            }
            VmValue::Secret(_) => write!(f, "Secret(***)"),
            #[cfg(feature = "python")]
            VmValue::PyObject(w) => write!(f, "PyObject({w:?})"),
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(t) => write!(f, "{t:?}"),
            #[cfg(feature = "native")]
            VmValue::AgentDef(a) => write!(f, "AgentDef({})", a.name),
            VmValue::Moved => write!(f, "<moved>"),
            VmValue::Ref(inner) => write!(f, "&{inner:?}"),
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
            #[cfg(feature = "native")]
            VmValue::Table(_) => write!(f, "<table>"),
            #[cfg(feature = "native")]
            VmValue::Schema(s) => write!(f, "<schema {}>", s.name),
            #[cfg(feature = "native")]
            VmValue::Tensor(t) => write!(f, "{t}"),
            #[cfg(feature = "native")]
            VmValue::Model(m) => write!(f, "{m}"),
            #[cfg(feature = "native")]
            VmValue::Connector(c) => write!(f, "{c}"),
            #[cfg(feature = "native")]
            VmValue::PipelineResult(r) => write!(f, "{r}"),
            #[cfg(feature = "native")]
            VmValue::PipelineDef(p) => write!(f, "{p}"),
            #[cfg(feature = "native")]
            VmValue::StreamDef(s) => write!(f, "{s}"),
            VmValue::StructDef(d) => write!(f, "<struct {}>", d.name),
            VmValue::StructInstance(s) => {
                write!(f, "{} {{ ", s.type_name)?;
                for (i, (k, v)) in s.fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
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
                        if i > 0 {
                            write!(f, ", ")?;
                        }
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
                    if i > 0 {
                        write!(f, ", ")?;
                    }
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
            VmValue::Set(items) => {
                write!(f, "{{")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "}}")
            }
            VmValue::Decimal(d) => write!(f, "{d}"),
            VmValue::DateTime(ms) => {
                use chrono::TimeZone;
                let secs = *ms / 1000;
                let nsecs = ((*ms % 1000) * 1_000_000) as u32;
                match chrono::Utc.timestamp_opt(secs, nsecs).single() {
                    Some(dt) => write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S")),
                    None => write!(f, "<datetime {ms}ms>"),
                }
            }
            VmValue::Secret(_) => write!(f, "***"),
            #[cfg(feature = "python")]
            VmValue::PyObject(w) => write!(f, "{w}"),
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(t) => write!(f, "{t}"),
            #[cfg(feature = "native")]
            VmValue::AgentDef(a) => write!(f, "<agent {}>", a.name),
            VmValue::Moved => write!(f, "<moved>"),
            VmValue::Ref(inner) => write!(f, "{inner}"),
        }
    }
}
