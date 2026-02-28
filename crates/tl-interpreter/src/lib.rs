// ThinkingLanguage — Tree-Walking Interpreter
// Licensed under MIT OR Apache-2.0
//
// Phase 0: Executes TL programs by walking the AST directly.
// This is slow but correct — used for REPL and initial development.
// Will be replaced by compiled execution in Phase 2.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tl_ast::*;
use tl_errors::{RuntimeError, TlError};
use tl_compiler::security::SecurityPolicy;
use tl_data::translate::{translate_expr, LocalValue, TranslateContext};
use tl_data::{
    ArrowDataType, ArrowField, ArrowSchema,
    DataFrame, DataEngine, JoinType, col,
};
use tl_stream::{ConnectorConfig, PipelineDef, PipelineRunner, PipelineStatus, StreamDef};

/// Wrapper around DataFusion DataFrame that implements Debug + Clone.
#[derive(Clone)]
pub struct TlTable {
    pub df: DataFrame,
}

impl fmt::Debug for TlTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<table>")
    }
}

/// Schema definition: column names and Arrow types.
#[derive(Debug, Clone)]
pub struct TlSchema {
    pub name: String,
    pub arrow_schema: Arc<ArrowSchema>,
}

/// Counter for generating unique task IDs in the interpreter.
static INTERP_TASK_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
/// Counter for generating unique channel IDs in the interpreter.
static INTERP_CHANNEL_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A spawned task handle for the interpreter.
pub struct TlTask {
    pub receiver: Mutex<Option<mpsc::Receiver<Result<Value, String>>>>,
    pub id: u64,
}

impl TlTask {
    pub fn new(receiver: mpsc::Receiver<Result<Value, String>>) -> Self {
        TlTask {
            receiver: Mutex::new(Some(receiver)),
            id: INTERP_TASK_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for TlTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<task {}>", self.id)
    }
}

impl Clone for TlTask {
    fn clone(&self) -> Self {
        // Tasks are not truly cloneable (receiver is single-use), but we need
        // Clone for Value. This creates a "consumed" copy.
        TlTask {
            receiver: Mutex::new(None),
            id: self.id,
        }
    }
}

/// A channel for inter-task communication in the interpreter.
pub struct TlChannel {
    pub sender: mpsc::SyncSender<Value>,
    pub receiver: Arc<Mutex<mpsc::Receiver<Value>>>,
    pub id: u64,
}

impl TlChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(capacity);
        TlChannel {
            sender: tx,
            receiver: Arc::new(Mutex::new(rx)),
            id: INTERP_CHANNEL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl Clone for TlChannel {
    fn clone(&self) -> Self {
        TlChannel {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            id: self.id,
        }
    }
}

impl fmt::Debug for TlChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<channel {}>", self.id)
    }
}

/// Counter for generating unique generator IDs in the interpreter.
static INTERP_GENERATOR_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A generator for the interpreter — thread-based coroutine model.
pub enum TlGeneratorKind {
    /// User-defined generator using thread-based coroutines
    UserDefined {
        /// Receive yielded values from the generator thread
        receiver: Mutex<Option<mpsc::Receiver<Result<Value, String>>>>,
        /// Signal the generator thread to resume
        resume_tx: mpsc::SyncSender<()>,
    },
    /// Built-in iterator over a list
    ListIter { items: Vec<Value>, index: Mutex<usize> },
    /// Take at most N items
    Take { source: Arc<TlGenerator>, remaining: Mutex<usize> },
    /// Skip first N items
    Skip { source: Arc<TlGenerator>, remaining: Mutex<usize> },
    /// Map a function over yielded values
    Map { source: Arc<TlGenerator>, func: Value },
    /// Filter values with predicate
    Filter { source: Arc<TlGenerator>, func: Value },
    /// Chain two generators
    Chain { first: Arc<TlGenerator>, second: Arc<TlGenerator>, on_second: Mutex<bool> },
    /// Zip two generators
    Zip { first: Arc<TlGenerator>, second: Arc<TlGenerator> },
    /// Enumerate values with index
    Enumerate { source: Arc<TlGenerator>, index: Mutex<usize> },
}

pub struct TlGenerator {
    pub kind: TlGeneratorKind,
    pub done: Mutex<bool>,
    pub id: u64,
}

impl TlGenerator {
    pub fn new(kind: TlGeneratorKind) -> Self {
        TlGenerator {
            kind,
            done: Mutex::new(false),
            id: INTERP_GENERATOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for TlGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<generator {}>", self.id)
    }
}

impl Clone for TlGenerator {
    fn clone(&self) -> Self {
        // Generators are not truly cloneable (channels are single-use)
        // but we need Clone for Value. This creates a "consumed" copy.
        TlGenerator {
            kind: TlGeneratorKind::ListIter { items: vec![], index: Mutex::new(0) },
            done: Mutex::new(true),
            id: self.id,
        }
    }
}

/// Wrapper around a Python object for storage in interpreter Value.
#[cfg(feature = "python")]
pub struct InterpPyObjectWrapper {
    pub inner: pyo3::Py<pyo3::PyAny>,
}

#[cfg(feature = "python")]
impl Clone for InterpPyObjectWrapper {
    fn clone(&self) -> Self {
        pyo3::Python::with_gil(|py| InterpPyObjectWrapper {
            inner: self.inner.clone_ref(py),
        })
    }
}

#[cfg(feature = "python")]
impl fmt::Debug for InterpPyObjectWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use pyo3::prelude::*;
        pyo3::Python::with_gil(|py| {
            let obj = self.inner.bind(py);
            match obj.repr() {
                Ok(r) => write!(f, "{}", r),
                Err(_) => write!(f, "<pyobject>"),
            }
        })
    }
}

#[cfg(feature = "python")]
impl fmt::Display for InterpPyObjectWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use pyo3::prelude::*;
        pyo3::Python::with_gil(|py| {
            let obj = self.inner.bind(py);
            match obj.str() {
                Ok(s) => write!(f, "{}", s),
                Err(_) => write!(f, "<pyobject>"),
            }
        })
    }
}

/// Runtime value
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    List(Vec<Value>),
    None,
    /// A function defined in TL code
    Function {
        name: String,
        params: Vec<Param>,
        body: Vec<Stmt>,
        is_generator: bool,
    },
    /// A built-in function
    Builtin(String),
    /// A closure (anonymous function with captured environment)
    Closure {
        params: Vec<Param>,
        body: ClosureBody,
        captured_env: Vec<HashMap<String, Value>>,
    },
    /// A lazy DataFusion table (DataFrame)
    Table(TlTable),
    /// A schema definition
    Schema(TlSchema),
    /// A tensor (ndarray)
    Tensor(tl_ai::TlTensor),
    /// A trained model
    Model(tl_ai::TlModel),
    /// A connector configuration
    Connector(ConnectorConfig),
    /// A pipeline definition
    Pipeline(PipelineDef),
    /// A stream definition
    Stream(StreamDef),
    /// A struct type definition
    StructDef {
        name: String,
        fields: Vec<String>,
    },
    /// A struct instance
    StructInstance {
        type_name: String,
        fields: HashMap<String, Value>,
    },
    /// An enum type definition
    EnumDef {
        name: String,
        variants: Vec<(String, usize)>, // (variant_name, field_count)
    },
    /// An enum instance
    EnumInstance {
        type_name: String,
        variant: String,
        fields: Vec<Value>,
    },
    /// A module (from import)
    Module {
        name: String,
        exports: HashMap<String, Value>,
    },
    /// An ordered map (string keys)
    Map(Vec<(String, Value)>),
    /// A set (unique values)
    Set(Vec<Value>),
    /// A spawned task handle
    Task(Arc<TlTask>),
    /// A channel for inter-task communication
    Channel(Arc<TlChannel>),
    /// A generator (lazy iterator)
    Generator(Arc<TlGenerator>),
    /// A fixed-point decimal value (Phase 22)
    Decimal(rust_decimal::Decimal),
    /// A secret value with redacted display (Phase 23)
    Secret(String),
    /// An opaque Python object (feature-gated)
    #[cfg(feature = "python")]
    PyObject(Arc<InterpPyObjectWrapper>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(n) => write!(f, "{n}"),
            Value::Float(n) => {
                if n.fract() == 0.0 {
                    write!(f, "{n:.1}")
                } else {
                    write!(f, "{n}")
                }
            }
            Value::String(s) => write!(f, "{s}"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::None => write!(f, "none"),
            Value::Function { name, .. } => write!(f, "<fn {name}>"),
            Value::Builtin(name) => write!(f, "<builtin {name}>"),
            Value::Closure { .. } => write!(f, "<closure>"),
            Value::Table(_) => write!(f, "<table>"),
            Value::Schema(s) => write!(f, "<schema {}>", s.name),
            Value::Tensor(t) => write!(f, "{t}"),
            Value::Model(m) => write!(f, "{m}"),
            Value::Connector(c) => write!(f, "{c}"),
            Value::Pipeline(p) => write!(f, "{p}"),
            Value::Stream(s) => write!(f, "{s}"),
            Value::StructDef { name, .. } => write!(f, "<struct {name}>"),
            Value::StructInstance { type_name, fields } => {
                write!(f, "{type_name} {{ ")?;
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, " }}")
            }
            Value::EnumDef { name, .. } => write!(f, "<enum {name}>"),
            Value::EnumInstance { type_name, variant, fields } => {
                write!(f, "{type_name}::{variant}")?;
                if !fields.is_empty() {
                    write!(f, "(")?;
                    for (i, v) in fields.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{v}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Value::Module { name, .. } => write!(f, "<module {name}>"),
            Value::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Value::Set(items) => {
                write!(f, "set{{")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{item}")?;
                }
                write!(f, "}}")
            }
            Value::Task(t) => write!(f, "<task {}>", t.id),
            Value::Channel(c) => write!(f, "<channel {}>", c.id),
            Value::Generator(g) => write!(f, "<generator {}>", g.id),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Secret(_) => write!(f, "***"),
            #[cfg(feature = "python")]
            Value::PyObject(w) => write!(f, "{w}"),
        }
    }
}

impl Value {
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::List(items) => !items.is_empty(),
            Value::Map(pairs) => !pairs.is_empty(),
            Value::Decimal(d) => !d.is_zero(),
            Value::Secret(s) => !s.is_empty(),
            Value::None => false,
            #[cfg(feature = "python")]
            Value::PyObject(_) => true,
            _ => true,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "int64",
            Value::Float(_) => "float64",
            Value::String(_) => "string",
            Value::Bool(_) => "bool",
            Value::List(_) => "list",
            Value::None => "none",
            Value::Function { .. } => "function",
            Value::Builtin(_) => "builtin",
            Value::Closure { .. } => "closure",
            Value::Table(_) => "table",
            Value::Schema(_) => "schema",
            Value::Tensor(_) => "tensor",
            Value::Model(_) => "model",
            Value::Connector(_) => "connector",
            Value::Pipeline(_) => "pipeline",
            Value::Stream(_) => "stream",
            Value::StructDef { .. } => "struct_def",
            Value::StructInstance { type_name, .. } => {
                // We can't return a dynamic string from &'static str,
                // so just return "struct"
                let _ = type_name;
                "struct"
            }
            Value::EnumDef { .. } => "enum_def",
            Value::EnumInstance { .. } => "enum",
            Value::Module { .. } => "module",
            Value::Map(_) => "map",
            Value::Set(_) => "set",
            Value::Task(_) => "task",
            Value::Channel(_) => "channel",
            Value::Generator(_) => "generator",
            Value::Decimal(_) => "decimal",
            Value::Secret(_) => "secret",
            #[cfg(feature = "python")]
            Value::PyObject(_) => "pyobject",
        }
    }
}

/// Control flow signals
enum Signal {
    None,
    Return(Value),
    Break,
    Continue,
    Throw(Value),
    Yield(Value),
}

/// Generator-specific control flow signals (used in exec_stmt_gen).
enum GenSignal {
    None,
    Return(Value),
    Break,
    Continue,
    Throw(Value),
    Yield(Value),
}

/// Variable environment (scope chain)
#[derive(Debug, Clone)]
pub struct Environment {
    scopes: Vec<HashMap<String, Value>>,
}

impl Environment {
    pub fn new() -> Self {
        let mut global = HashMap::new();
        // Register builtins
        global.insert("print".to_string(), Value::Builtin("print".to_string()));
        global.insert("println".to_string(), Value::Builtin("println".to_string()));
        global.insert("len".to_string(), Value::Builtin("len".to_string()));
        global.insert("str".to_string(), Value::Builtin("str".to_string()));
        global.insert("int".to_string(), Value::Builtin("int".to_string()));
        global.insert("float".to_string(), Value::Builtin("float".to_string()));
        global.insert("abs".to_string(), Value::Builtin("abs".to_string()));
        global.insert("min".to_string(), Value::Builtin("min".to_string()));
        global.insert("max".to_string(), Value::Builtin("max".to_string()));
        global.insert("range".to_string(), Value::Builtin("range".to_string()));
        global.insert("push".to_string(), Value::Builtin("push".to_string()));
        global.insert("type_of".to_string(), Value::Builtin("type_of".to_string()));
        global.insert("map".to_string(), Value::Builtin("map".to_string()));
        global.insert("filter".to_string(), Value::Builtin("filter".to_string()));
        global.insert("reduce".to_string(), Value::Builtin("reduce".to_string()));
        global.insert("sum".to_string(), Value::Builtin("sum".to_string()));
        global.insert("any".to_string(), Value::Builtin("any".to_string()));
        global.insert("all".to_string(), Value::Builtin("all".to_string()));
        // Data engine builtins
        global.insert("read_csv".to_string(), Value::Builtin("read_csv".to_string()));
        global.insert("read_parquet".to_string(), Value::Builtin("read_parquet".to_string()));
        global.insert("write_csv".to_string(), Value::Builtin("write_csv".to_string()));
        global.insert("write_parquet".to_string(), Value::Builtin("write_parquet".to_string()));
        global.insert("collect".to_string(), Value::Builtin("collect".to_string()));
        global.insert("show".to_string(), Value::Builtin("show".to_string()));
        global.insert("describe".to_string(), Value::Builtin("describe".to_string()));
        global.insert("head".to_string(), Value::Builtin("head".to_string()));
        global.insert("postgres".to_string(), Value::Builtin("postgres".to_string()));
        // AI builtins
        global.insert("tensor".to_string(), Value::Builtin("tensor".to_string()));
        global.insert("tensor_zeros".to_string(), Value::Builtin("tensor_zeros".to_string()));
        global.insert("tensor_ones".to_string(), Value::Builtin("tensor_ones".to_string()));
        global.insert("tensor_shape".to_string(), Value::Builtin("tensor_shape".to_string()));
        global.insert("tensor_reshape".to_string(), Value::Builtin("tensor_reshape".to_string()));
        global.insert("tensor_transpose".to_string(), Value::Builtin("tensor_transpose".to_string()));
        global.insert("tensor_sum".to_string(), Value::Builtin("tensor_sum".to_string()));
        global.insert("tensor_mean".to_string(), Value::Builtin("tensor_mean".to_string()));
        global.insert("tensor_dot".to_string(), Value::Builtin("tensor_dot".to_string()));
        global.insert("predict".to_string(), Value::Builtin("predict".to_string()));
        global.insert("embed".to_string(), Value::Builtin("embed".to_string()));
        global.insert("similarity".to_string(), Value::Builtin("similarity".to_string()));
        global.insert("ai_complete".to_string(), Value::Builtin("ai_complete".to_string()));
        global.insert("ai_chat".to_string(), Value::Builtin("ai_chat".to_string()));
        global.insert("model_save".to_string(), Value::Builtin("model_save".to_string()));
        global.insert("model_load".to_string(), Value::Builtin("model_load".to_string()));
        global.insert("model_register".to_string(), Value::Builtin("model_register".to_string()));
        global.insert("model_list".to_string(), Value::Builtin("model_list".to_string()));
        global.insert("model_get".to_string(), Value::Builtin("model_get".to_string()));
        // Streaming builtins
        global.insert("alert_slack".to_string(), Value::Builtin("alert_slack".to_string()));
        global.insert("alert_webhook".to_string(), Value::Builtin("alert_webhook".to_string()));
        global.insert("emit".to_string(), Value::Builtin("emit".to_string()));
        global.insert("lineage".to_string(), Value::Builtin("lineage".to_string()));
        global.insert("run_pipeline".to_string(), Value::Builtin("run_pipeline".to_string()));
        // Math builtins
        global.insert("sqrt".to_string(), Value::Builtin("sqrt".to_string()));
        global.insert("pow".to_string(), Value::Builtin("pow".to_string()));
        global.insert("floor".to_string(), Value::Builtin("floor".to_string()));
        global.insert("ceil".to_string(), Value::Builtin("ceil".to_string()));
        global.insert("round".to_string(), Value::Builtin("round".to_string()));
        global.insert("sin".to_string(), Value::Builtin("sin".to_string()));
        global.insert("cos".to_string(), Value::Builtin("cos".to_string()));
        global.insert("tan".to_string(), Value::Builtin("tan".to_string()));
        global.insert("log".to_string(), Value::Builtin("log".to_string()));
        global.insert("log2".to_string(), Value::Builtin("log2".to_string()));
        global.insert("log10".to_string(), Value::Builtin("log10".to_string()));
        global.insert("join".to_string(), Value::Builtin("join".to_string()));
        // Phase 6: Stdlib & Ecosystem
        global.insert("json_parse".to_string(), Value::Builtin("json_parse".to_string()));
        global.insert("json_stringify".to_string(), Value::Builtin("json_stringify".to_string()));
        global.insert("map_from".to_string(), Value::Builtin("map_from".to_string()));
        global.insert("read_file".to_string(), Value::Builtin("read_file".to_string()));
        global.insert("write_file".to_string(), Value::Builtin("write_file".to_string()));
        global.insert("append_file".to_string(), Value::Builtin("append_file".to_string()));
        global.insert("file_exists".to_string(), Value::Builtin("file_exists".to_string()));
        global.insert("list_dir".to_string(), Value::Builtin("list_dir".to_string()));
        global.insert("env_get".to_string(), Value::Builtin("env_get".to_string()));
        global.insert("env_set".to_string(), Value::Builtin("env_set".to_string()));
        global.insert("regex_match".to_string(), Value::Builtin("regex_match".to_string()));
        global.insert("regex_find".to_string(), Value::Builtin("regex_find".to_string()));
        global.insert("regex_replace".to_string(), Value::Builtin("regex_replace".to_string()));
        global.insert("now".to_string(), Value::Builtin("now".to_string()));
        global.insert("date_format".to_string(), Value::Builtin("date_format".to_string()));
        global.insert("date_parse".to_string(), Value::Builtin("date_parse".to_string()));
        global.insert("zip".to_string(), Value::Builtin("zip".to_string()));
        global.insert("enumerate".to_string(), Value::Builtin("enumerate".to_string()));
        global.insert("bool".to_string(), Value::Builtin("bool".to_string()));
        // Assert builtins
        global.insert("assert".to_string(), Value::Builtin("assert".to_string()));
        global.insert("assert_eq".to_string(), Value::Builtin("assert_eq".to_string()));
        // HTTP builtins
        global.insert("http_get".to_string(), Value::Builtin("http_get".to_string()));
        global.insert("http_post".to_string(), Value::Builtin("http_post".to_string()));
        // Concurrency builtins
        global.insert("spawn".to_string(), Value::Builtin("spawn".to_string()));
        global.insert("sleep".to_string(), Value::Builtin("sleep".to_string()));
        global.insert("channel".to_string(), Value::Builtin("channel".to_string()));
        global.insert("send".to_string(), Value::Builtin("send".to_string()));
        global.insert("recv".to_string(), Value::Builtin("recv".to_string()));
        global.insert("try_recv".to_string(), Value::Builtin("try_recv".to_string()));
        global.insert("await_all".to_string(), Value::Builtin("await_all".to_string()));
        global.insert("pmap".to_string(), Value::Builtin("pmap".to_string()));
        global.insert("timeout".to_string(), Value::Builtin("timeout".to_string()));
        // Phase 8: Iterators & Generators
        global.insert("next".to_string(), Value::Builtin("next".to_string()));
        global.insert("is_generator".to_string(), Value::Builtin("is_generator".to_string()));
        global.insert("iter".to_string(), Value::Builtin("iter".to_string()));
        global.insert("take".to_string(), Value::Builtin("take".to_string()));
        global.insert("skip".to_string(), Value::Builtin("skip".to_string()));
        global.insert("gen_collect".to_string(), Value::Builtin("gen_collect".to_string()));
        global.insert("gen_map".to_string(), Value::Builtin("gen_map".to_string()));
        global.insert("gen_filter".to_string(), Value::Builtin("gen_filter".to_string()));
        global.insert("chain".to_string(), Value::Builtin("chain".to_string()));
        global.insert("gen_zip".to_string(), Value::Builtin("gen_zip".to_string()));
        global.insert("gen_enumerate".to_string(), Value::Builtin("gen_enumerate".to_string()));
        // Phase 10: Result builtins
        global.insert("Ok".to_string(), Value::Builtin("Ok".to_string()));
        global.insert("Err".to_string(), Value::Builtin("Err".to_string()));
        global.insert("is_ok".to_string(), Value::Builtin("is_ok".to_string()));
        global.insert("is_err".to_string(), Value::Builtin("is_err".to_string()));
        global.insert("unwrap".to_string(), Value::Builtin("unwrap".to_string()));
        // Phase 10: Set builtins
        global.insert("set_from".to_string(), Value::Builtin("set_from".to_string()));
        global.insert("set_add".to_string(), Value::Builtin("set_add".to_string()));
        global.insert("set_remove".to_string(), Value::Builtin("set_remove".to_string()));
        global.insert("set_contains".to_string(), Value::Builtin("set_contains".to_string()));
        global.insert("set_union".to_string(), Value::Builtin("set_union".to_string()));
        global.insert("set_intersection".to_string(), Value::Builtin("set_intersection".to_string()));
        global.insert("set_difference".to_string(), Value::Builtin("set_difference".to_string()));
        // Phase 15: Data Quality & Connectors
        global.insert("fill_null".to_string(), Value::Builtin("fill_null".to_string()));
        global.insert("drop_null".to_string(), Value::Builtin("drop_null".to_string()));
        global.insert("dedup".to_string(), Value::Builtin("dedup".to_string()));
        global.insert("clamp".to_string(), Value::Builtin("clamp".to_string()));
        global.insert("data_profile".to_string(), Value::Builtin("data_profile".to_string()));
        global.insert("row_count".to_string(), Value::Builtin("row_count".to_string()));
        global.insert("null_rate".to_string(), Value::Builtin("null_rate".to_string()));
        global.insert("is_unique".to_string(), Value::Builtin("is_unique".to_string()));
        global.insert("is_email".to_string(), Value::Builtin("is_email".to_string()));
        global.insert("is_url".to_string(), Value::Builtin("is_url".to_string()));
        global.insert("is_phone".to_string(), Value::Builtin("is_phone".to_string()));
        global.insert("is_between".to_string(), Value::Builtin("is_between".to_string()));
        global.insert("levenshtein".to_string(), Value::Builtin("levenshtein".to_string()));
        global.insert("soundex".to_string(), Value::Builtin("soundex".to_string()));
        global.insert("read_mysql".to_string(), Value::Builtin("read_mysql".to_string()));
        global.insert("redis_connect".to_string(), Value::Builtin("redis_connect".to_string()));
        global.insert("redis_get".to_string(), Value::Builtin("redis_get".to_string()));
        global.insert("redis_set".to_string(), Value::Builtin("redis_set".to_string()));
        global.insert("redis_del".to_string(), Value::Builtin("redis_del".to_string()));
        global.insert("graphql_query".to_string(), Value::Builtin("graphql_query".to_string()));
        global.insert("register_s3".to_string(), Value::Builtin("register_s3".to_string()));
        // Phase 20: Python FFI
        global.insert("py_import".to_string(), Value::Builtin("py_import".to_string()));
        global.insert("py_call".to_string(), Value::Builtin("py_call".to_string()));
        global.insert("py_eval".to_string(), Value::Builtin("py_eval".to_string()));
        global.insert("py_getattr".to_string(), Value::Builtin("py_getattr".to_string()));
        global.insert("py_setattr".to_string(), Value::Builtin("py_setattr".to_string()));
        global.insert("py_to_tl".to_string(), Value::Builtin("py_to_tl".to_string()));
        // Phase 21: Schema Evolution
        global.insert("schema_register".to_string(), Value::Builtin("schema_register".to_string()));
        global.insert("schema_get".to_string(), Value::Builtin("schema_get".to_string()));
        global.insert("schema_latest".to_string(), Value::Builtin("schema_latest".to_string()));
        global.insert("schema_history".to_string(), Value::Builtin("schema_history".to_string()));
        global.insert("schema_check".to_string(), Value::Builtin("schema_check".to_string()));
        global.insert("schema_diff".to_string(), Value::Builtin("schema_diff".to_string()));
        global.insert("schema_versions".to_string(), Value::Builtin("schema_versions".to_string()));
        global.insert("schema_fields".to_string(), Value::Builtin("schema_fields".to_string()));
        // Phase 22: Advanced Types
        global.insert("decimal".to_string(), Value::Builtin("decimal".to_string()));
        // Phase 23: Security & Access Control
        global.insert("secret_get".to_string(), Value::Builtin("secret_get".to_string()));
        global.insert("secret_set".to_string(), Value::Builtin("secret_set".to_string()));
        global.insert("secret_delete".to_string(), Value::Builtin("secret_delete".to_string()));
        global.insert("secret_list".to_string(), Value::Builtin("secret_list".to_string()));
        global.insert("check_permission".to_string(), Value::Builtin("check_permission".to_string()));
        global.insert("mask_email".to_string(), Value::Builtin("mask_email".to_string()));
        global.insert("mask_phone".to_string(), Value::Builtin("mask_phone".to_string()));
        global.insert("mask_cc".to_string(), Value::Builtin("mask_cc".to_string()));
        global.insert("redact".to_string(), Value::Builtin("redact".to_string()));
        global.insert("hash".to_string(), Value::Builtin("hash".to_string()));
        // Phase 25: Async builtins
        global.insert("async_read_file".to_string(), Value::Builtin("async_read_file".to_string()));
        global.insert("async_write_file".to_string(), Value::Builtin("async_write_file".to_string()));
        global.insert("async_http_get".to_string(), Value::Builtin("async_http_get".to_string()));
        global.insert("async_http_post".to_string(), Value::Builtin("async_http_post".to_string()));
        global.insert("async_sleep".to_string(), Value::Builtin("async_sleep".to_string()));
        global.insert("select".to_string(), Value::Builtin("select".to_string()));
        global.insert("race_all".to_string(), Value::Builtin("race_all".to_string()));
        global.insert("async_map".to_string(), Value::Builtin("async_map".to_string()));
        global.insert("async_filter".to_string(), Value::Builtin("async_filter".to_string()));

        Self {
            scopes: vec![global],
        }
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    pub fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(val) = scope.get(name) {
                return Some(val);
            }
        }
        None
    }

    pub fn set(&mut self, name: String, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name, value);
        }
    }

    /// Update an existing variable in the nearest scope that contains it
    pub fn update(&mut self, name: &str, value: Value) -> bool {
        for scope in self.scopes.iter_mut().rev() {
            if scope.contains_key(name) {
                scope.insert(name.to_string(), value);
                return true;
            }
        }
        false
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::new()
    }
}

/// The interpreter
pub struct Interpreter {
    pub env: Environment,
    /// Captured output (for testing)
    pub output: Vec<String>,
    /// Track last expression value for REPL display
    last_expr_value: Option<Value>,
    /// Data engine (lazily initialized)
    data_engine: Option<DataEngine>,
    /// Method table: type_name -> method_name -> function value
    method_table: HashMap<String, HashMap<String, Value>>,
    /// Module cache: file_path -> exported values
    module_cache: HashMap<String, HashMap<String, Value>>,
    /// Files currently being imported (for circular dependency detection)
    importing_files: std::collections::HashSet<String>,
    /// Current file path (for resolving relative imports)
    pub file_path: Option<String>,
    /// Whether we are in test mode (run test blocks)
    pub test_mode: bool,
    /// Package roots: package_name → source directory
    pub package_roots: HashMap<String, std::path::PathBuf>,
    /// Project root (where tl.toml lives)
    pub project_root: Option<std::path::PathBuf>,
    /// Schema registry for versioned schemas
    pub schema_registry: tl_compiler::schema::SchemaRegistry,
    /// Secret vault for credential management (Phase 23)
    pub secret_vault: HashMap<String, String>,
    /// Security policy for sandbox mode (Phase 23)
    pub security_policy: Option<SecurityPolicy>,
    /// Tokio runtime for async builtins (lazily initialized)
    #[cfg(feature = "async-runtime")]
    runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl Interpreter {
    pub fn new() -> Self {
        Self {
            env: Environment::new(),
            output: Vec::new(),
            last_expr_value: None,
            data_engine: None,
            method_table: HashMap::new(),
            module_cache: HashMap::new(),
            importing_files: std::collections::HashSet::new(),
            file_path: None,
            test_mode: false,
            package_roots: HashMap::new(),
            project_root: None,
            schema_registry: tl_compiler::schema::SchemaRegistry::new(),
            secret_vault: HashMap::new(),
            security_policy: None,
            #[cfg(feature = "async-runtime")]
            runtime: None,
        }
    }

    /// Lazily initialize and return the tokio runtime.
    #[cfg(feature = "async-runtime")]
    fn ensure_runtime(&mut self) -> Arc<tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            self.runtime = Some(Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime")
            ));
        }
        self.runtime.as_ref().unwrap().clone()
    }

    /// Get or create the DataEngine (lazy init).
    fn engine(&mut self) -> &DataEngine {
        if self.data_engine.is_none() {
            self.data_engine = Some(DataEngine::new());
        }
        self.data_engine.as_ref().unwrap()
    }

    /// Execute a complete program
    pub fn execute(&mut self, program: &Program) -> Result<Value, TlError> {
        let mut last = Value::None;
        for stmt in &program.statements {
            match self.exec_stmt(stmt)? {
                Signal::Return(val) => return Ok(val),
                Signal::None => {}
                Signal::Throw(val) => {
                    return Err(TlError::Runtime(RuntimeError {
                        message: format!("Unhandled throw: {val}"),
                        span: None,
                        stack_trace: vec![],
                        }))
                }
                Signal::Break | Signal::Continue => {
                    return Err(TlError::Runtime(RuntimeError {
                        message: "break/continue outside of loop".to_string(),
                        span: None,
                        stack_trace: vec![],
                        }))
                }
                Signal::Yield(_) => {} // yield outside generator is a no-op at top level
            }
            // Track last expression value for REPL
            if let StmtKind::Expr(_) = &stmt.kind {
                last = self.last_expr_value.clone().unwrap_or(Value::None);
            }
        }
        Ok(last)
    }

    /// Execute a single statement (for REPL)
    pub fn execute_stmt(&mut self, stmt: &Stmt) -> Result<Value, TlError> {
        self.last_expr_value = None;
        match self.exec_stmt(stmt)? {
            Signal::Return(val) => Ok(val),
            _ => Ok(self.last_expr_value.clone().unwrap_or(Value::None)),
        }
    }

}

impl Default for Interpreter {
    fn default() -> Self {
        Self::new()
    }
}

// ── Statement execution ──────────────────────────────────

impl Interpreter {
    fn exec_stmt(&mut self, stmt: &Stmt) -> Result<Signal, TlError> {
        match &stmt.kind {
            StmtKind::Let {
                name,
                value,
                ..
            } => {
                let val = self.eval_expr(value)?;
                self.env.set(name.clone(), val);
                Ok(Signal::None)
            }
            StmtKind::FnDecl {
                name,
                params,
                body,
                is_generator,
                ..
            } => {
                let func = Value::Function {
                    name: name.clone(),
                    params: params.clone(),
                    body: body.clone(),
                    is_generator: *is_generator,
                };
                self.env.set(name.clone(), func);
                Ok(Signal::None)
            }
            StmtKind::Expr(expr) => {
                let val = self.eval_expr(expr)?;
                self.last_expr_value = Some(val);
                Ok(Signal::None)
            }
            StmtKind::Return(expr) => {
                let val = match expr {
                    Some(e) => self.eval_expr(e)?,
                    None => Value::None,
                };
                Ok(Signal::Return(val))
            }
            StmtKind::If {
                condition,
                then_body,
                else_ifs,
                else_body,
            } => {
                let cond = self.eval_expr(condition)?;
                if cond.is_truthy() {
                    return self.exec_block(then_body);
                }
                for (cond_expr, body) in else_ifs {
                    let cond = self.eval_expr(cond_expr)?;
                    if cond.is_truthy() {
                        return self.exec_block(body);
                    }
                }
                if let Some(body) = else_body {
                    return self.exec_block(body);
                }
                Ok(Signal::None)
            }
            StmtKind::While { condition, body } => {
                loop {
                    let cond = self.eval_expr(condition)?;
                    if !cond.is_truthy() {
                        break;
                    }
                    match self.exec_block(body)? {
                        Signal::Break => break,
                        Signal::Return(v) => return Ok(Signal::Return(v)),
                        Signal::Throw(v) => return Ok(Signal::Throw(v)),
                        Signal::Continue | Signal::None | Signal::Yield(_) => continue,
                    }
                }
                Ok(Signal::None)
            }
            StmtKind::For { name, iter, body } => {
                let iter_val = self.eval_expr(iter)?;
                // Handle generator iteration
                if let Value::Generator(ref g) = iter_val {
                    let g = g.clone();
                    loop {
                        let val = self.interpreter_next(&g)?;
                        if matches!(val, Value::None) { break; }
                        self.env.push_scope();
                        self.env.set(name.clone(), val);
                        let signal = self.exec_block(body)?;
                        self.env.pop_scope();
                        match signal {
                            Signal::Break => break,
                            Signal::Return(v) => return Ok(Signal::Return(v)),
                            Signal::Throw(v) => return Ok(Signal::Throw(v)),
                            Signal::Continue | Signal::None => continue,
                            _ => {}
                        }
                    }
                    return Ok(Signal::None);
                }
                let items = match iter_val {
                    Value::List(items) => items,
                    Value::Map(pairs) => {
                        // Map iteration yields [key, value] pairs
                        pairs.into_iter()
                            .map(|(k, v)| Value::List(vec![Value::String(k), v]))
                            .collect()
                    }
                    Value::Set(items) => items,
                    _ => {
                        return Err(TlError::Runtime(RuntimeError {
                            message: format!("Cannot iterate over {}", iter_val.type_name()),
                            span: None,
                            stack_trace: vec![],
                            }))
                    }
                };
                for item in items {
                    self.env.push_scope();
                    self.env.set(name.clone(), item);
                    let signal = self.exec_block(body)?;
                    self.env.pop_scope();
                    match signal {
                        Signal::Break => break,
                        Signal::Return(v) => return Ok(Signal::Return(v)),
                        Signal::Throw(v) => return Ok(Signal::Throw(v)),
                        Signal::Continue | Signal::None | Signal::Yield(_) => continue,
                    }
                }
                Ok(Signal::None)
            }
            StmtKind::Schema { name, fields, version, .. } => {
                let arrow_fields: Vec<ArrowField> = fields
                    .iter()
                    .map(|f| {
                        let dt = tl_type_to_arrow(&f.type_ann);
                        ArrowField::new(&f.name, dt, true)
                    })
                    .collect();
                let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

                // If versioned, register in schema registry
                if let Some(ver) = version {
                    let mut metadata = tl_compiler::schema::SchemaMetadata::default();
                    // Extract @since from field doc comments
                    for f in fields {
                        if let Some(ref doc) = f.doc_comment {
                            for line in doc.lines() {
                                let trimmed = line.trim();
                                if let Some(rest) = trimmed.strip_prefix("@since") {
                                    if let Ok(v) = rest.trim().parse::<i64>() {
                                        metadata.field_since.insert(f.name.clone(), v);
                                    }
                                } else if let Some(rest) = trimmed.strip_prefix("@deprecated") {
                                    if let Ok(v) = rest.trim().parse::<i64>() {
                                        metadata.field_deprecated.insert(f.name.clone(), v);
                                    }
                                }
                            }
                        }
                        if let Some(ref _def) = f.default_value {
                            metadata.field_defaults.insert(f.name.clone(), format!("{:?}", f.default_value));
                        }
                    }
                    let _ = self.schema_registry.register(name, *ver, arrow_schema.clone(), metadata);
                }

                let schema = TlSchema {
                    name: name.clone(),
                    arrow_schema,
                };
                self.env.set(name.clone(), Value::Schema(schema));
                Ok(Signal::None)
            }
            StmtKind::Train { name, algorithm, config } => {
                self.exec_train(name, algorithm, config)
            }
            StmtKind::Pipeline { name, extract, transform, load, schedule, timeout, retries, on_failure, on_success } => {
                self.exec_pipeline(name, extract, transform, load, schedule, timeout, retries, on_failure, on_success)
            }
            StmtKind::StreamDecl { name, source, transform, sink, window, watermark } => {
                self.exec_stream_decl(name, source, transform, sink, window, watermark)
            }
            StmtKind::SourceDecl { name, connector_type, config } => {
                self.exec_source_decl(name, connector_type, config)
            }
            StmtKind::SinkDecl { name, connector_type, config } => {
                self.exec_sink_decl(name, connector_type, config)
            }
            StmtKind::StructDecl { name, fields, .. } => {
                let field_names: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
                self.env.set(
                    name.clone(),
                    Value::StructDef {
                        name: name.clone(),
                        fields: field_names,
                    },
                );
                Ok(Signal::None)
            }
            StmtKind::EnumDecl { name, variants, .. } => {
                let variant_info: Vec<(String, usize)> = variants
                    .iter()
                    .map(|v| (v.name.clone(), v.fields.len()))
                    .collect();
                self.env.set(
                    name.clone(),
                    Value::EnumDef {
                        name: name.clone(),
                        variants: variant_info,
                    },
                );
                Ok(Signal::None)
            }
            StmtKind::ImplBlock { type_name, methods, .. } => {
                let mut method_map = self
                    .method_table
                    .remove(type_name)
                    .unwrap_or_default();
                for method in methods {
                    if let StmtKind::FnDecl { name, params, body, is_generator, .. } = &method.kind {
                        method_map.insert(
                            name.clone(),
                            Value::Function {
                                name: name.clone(),
                                params: params.clone(),
                                body: body.clone(),
                                is_generator: *is_generator,
                            },
                        );
                    }
                }
                self.method_table.insert(type_name.clone(), method_map);
                Ok(Signal::None)
            }
            StmtKind::TryCatch { try_body, catch_var, catch_body } => {
                self.env.push_scope();
                let mut result = Signal::None;
                let mut caught = None;
                for stmt in try_body {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Throw(val)) => {
                            caught = Some(val);
                            break;
                        }
                        Ok(Signal::Return(v)) => {
                            self.env.pop_scope();
                            return Ok(Signal::Return(v));
                        }
                        Ok(sig) => {
                            result = sig;
                            if matches!(result, Signal::Break | Signal::Continue) {
                                break;
                            }
                        }
                        Err(TlError::Runtime(re)) => {
                            caught = Some(Value::String(re.message.clone()));
                            break;
                        }
                        Err(e) => {
                            self.env.pop_scope();
                            return Err(e);
                        }
                    }
                }
                self.env.pop_scope();
                if let Some(err_val) = caught {
                    self.env.push_scope();
                    self.env.set(catch_var.clone(), err_val);
                    for stmt in catch_body {
                        match self.exec_stmt(stmt)? {
                            Signal::Return(v) => {
                                self.env.pop_scope();
                                return Ok(Signal::Return(v));
                            }
                            Signal::Break | Signal::Continue => {
                                let sig = self.exec_stmt(stmt)?;
                                self.env.pop_scope();
                                return Ok(sig);
                            }
                            _ => {}
                        }
                    }
                    self.env.pop_scope();
                }
                Ok(result)
            }
            StmtKind::Throw(expr) => {
                let val = self.eval_expr(expr)?;
                Ok(Signal::Throw(val))
            }
            StmtKind::Import { path, alias } => {
                self.exec_import(path, alias.as_deref())
            }
            StmtKind::Test { name, body } => {
                if self.test_mode {
                    self.env.push_scope();
                    let mut failed = false;
                    for stmt in body {
                        match self.exec_stmt(stmt) {
                            Ok(Signal::Throw(val)) => {
                                let msg = format!("Test '{}' failed: throw {}", name, val);
                                self.output.push(msg.clone());
                                println!("{msg}");
                                failed = true;
                                break;
                            }
                            Ok(Signal::Return(_)) => break,
                            Ok(_) => {}
                            Err(e) => {
                                let msg = format!("Test '{}' failed: {}", name, e);
                                self.output.push(msg.clone());
                                println!("{msg}");
                                failed = true;
                                break;
                            }
                        }
                    }
                    self.env.pop_scope();
                    if !failed {
                        let msg = format!("Test '{}' passed", name);
                        self.output.push(msg.clone());
                        println!("{msg}");
                    }
                }
                Ok(Signal::None)
            }
            StmtKind::Use { item, .. } => {
                self.exec_use(item)
            }
            StmtKind::ModDecl { .. } => {
                // ModDecl is handled at module load time
                Ok(Signal::None)
            }
            StmtKind::TraitDef { .. } => {
                // Trait definitions are type-checker only; no runtime effect
                Ok(Signal::None)
            }
            StmtKind::TraitImpl { type_name, methods, .. } => {
                // Execute as a regular impl block — trait impls are type-erased at runtime
                let mut method_map = self
                    .method_table
                    .remove(type_name)
                    .unwrap_or_default();
                for method in methods {
                    if let StmtKind::FnDecl { name, params, body, is_generator, .. } = &method.kind {
                        method_map.insert(
                            name.clone(),
                            Value::Function {
                                name: name.clone(),
                                params: params.clone(),
                                body: body.clone(),
                                is_generator: *is_generator,
                            },
                        );
                    }
                }
                self.method_table.insert(type_name.clone(), method_map);
                Ok(Signal::None)
            }
            StmtKind::LetDestructure { pattern, value, .. } => {
                let val = self.eval_expr(value)?;
                let bindings = self.match_pattern(pattern, &val).unwrap_or_default();
                for (name, bval) in bindings {
                    self.env.set(name, bval);
                }
                Ok(Signal::None)
            }
            StmtKind::TypeAlias { .. } => {
                // Type aliases are type-checker only; no runtime effect
                Ok(Signal::None)
            }
            StmtKind::Migrate { schema_name, from_version, to_version, operations } => {
                self.exec_migrate(schema_name, *from_version, *to_version, operations)
            }
            StmtKind::Break => Ok(Signal::Break),
            StmtKind::Continue => Ok(Signal::Continue),
        }
    }

    /// Execute a migrate block — apply migration operations to schema registry
    fn exec_migrate(&mut self, schema_name: &str, from_version: i64, to_version: i64, operations: &[MigrateOp]) -> Result<Signal, TlError> {
        let ops: Vec<tl_compiler::schema::MigrationOp> = operations.iter().map(|op| {
            match op {
                MigrateOp::AddColumn { name, type_ann, default } => {
                    tl_compiler::schema::MigrationOp::AddColumn {
                        name: name.clone(),
                        type_name: format!("{:?}", type_ann),
                        default: default.as_ref().map(|d| format!("{:?}", d)),
                    }
                }
                MigrateOp::DropColumn { name } => {
                    tl_compiler::schema::MigrationOp::DropColumn { name: name.clone() }
                }
                MigrateOp::RenameColumn { from, to } => {
                    tl_compiler::schema::MigrationOp::RenameColumn { from: from.clone(), to: to.clone() }
                }
                MigrateOp::AlterType { column, new_type } => {
                    tl_compiler::schema::MigrationOp::AlterType {
                        column: column.clone(),
                        new_type: format!("{:?}", new_type),
                    }
                }
                MigrateOp::AddConstraint { .. } | MigrateOp::DropConstraint { .. } => {
                    // Constraints are metadata-only; no Arrow schema change
                    tl_compiler::schema::MigrationOp::AddColumn { name: String::new(), type_name: String::new(), default: None }
                }
            }
        }).collect();

        self.schema_registry.apply_migration(schema_name, from_version, to_version, &ops)
            .map_err(|e| TlError::Runtime(RuntimeError {
                message: format!("Migration error: {}", e),
                span: None,
                stack_trace: vec![],
            }))?;
        Ok(Signal::None)
    }

    /// Evaluate a closure body (either expr or block).
    fn eval_closure_body(&mut self, body: &ClosureBody) -> Result<Value, TlError> {
        match body {
            ClosureBody::Expr(e) => self.eval_expr(e),
            ClosureBody::Block { stmts, expr } => {
                for s in stmts {
                    match self.exec_stmt(s)? {
                        Signal::Return(val) => return Ok(val),
                        Signal::Throw(val) => return Err(TlError::Runtime(RuntimeError {
                            message: format!("{}", val),
                            span: None,
                            stack_trace: vec![],
                        })),
                        _ => {}
                    }
                }
                if let Some(e) = expr {
                    self.eval_expr(e)
                } else {
                    Ok(Value::None)
                }
            }
        }
    }

    /// Match a pattern against a value. Returns Some(bindings) if matched, None if not.
    fn match_pattern(&self, pattern: &Pattern, value: &Value) -> Option<Vec<(String, Value)>> {
        match pattern {
            Pattern::Wildcard => Some(vec![]),
            Pattern::Binding(name) => Some(vec![(name.clone(), value.clone())]),
            Pattern::Literal(expr) => {
                // Compare the literal expression value against the subject
                let pat_val = match expr {
                    Expr::Int(n) => Value::Int(*n),
                    Expr::Float(n) => Value::Float(*n),
                    Expr::Decimal(s) => {
                        use std::str::FromStr;
                        let cleaned = s.trim_end_matches('d');
                        match rust_decimal::Decimal::from_str(cleaned) {
                            Ok(d) => Value::Decimal(d),
                            Err(_) => return None,
                        }
                    }
                    Expr::String(s) => Value::String(s.clone()),
                    Expr::Bool(b) => Value::Bool(*b),
                    Expr::None => Value::None,
                    _ => return None,
                };
                if values_equal(value, &pat_val) {
                    Some(vec![])
                } else {
                    None
                }
            }
            Pattern::Enum { type_name: _, variant, args } => {
                if let Value::EnumInstance { variant: sv, fields, .. } = value {
                    if variant == sv {
                        let mut bindings = vec![];
                        for (i, arg_pat) in args.iter().enumerate() {
                            let field_val = fields.get(i).cloned().unwrap_or(Value::None);
                            match self.match_pattern(arg_pat, &field_val) {
                                Some(sub_bindings) => bindings.extend(sub_bindings),
                                None => return None,
                            }
                        }
                        return Some(bindings);
                    }
                }
                None
            }
            Pattern::Struct { name: struct_name, fields } => {
                // Check it's a struct instance
                if let Value::StructInstance { type_name, fields: sfields } = value {
                    if let Some(expected) = struct_name {
                        if expected != type_name {
                            return None;
                        }
                    }
                    let mut bindings = vec![];
                    for field in fields {
                        let field_val = sfields.get(&field.name)
                            .cloned()
                            .unwrap_or(Value::None);
                        match &field.pattern {
                            None => {
                                // Shorthand: { x } binds x
                                bindings.push((field.name.clone(), field_val));
                            }
                            Some(sub_pat) => {
                                match self.match_pattern(sub_pat, &field_val) {
                                    Some(sub_bindings) => bindings.extend(sub_bindings),
                                    None => return None,
                                }
                            }
                        }
                    }
                    return Some(bindings);
                }
                None
            }
            Pattern::List { elements, rest } => {
                if let Value::List(items) = value {
                    if rest.is_some() {
                        if items.len() < elements.len() {
                            return None;
                        }
                    } else if items.len() != elements.len() {
                        return None;
                    }
                    let mut bindings = vec![];
                    for (i, elem_pat) in elements.iter().enumerate() {
                        let item_val = items.get(i).cloned().unwrap_or(Value::None);
                        match self.match_pattern(elem_pat, &item_val) {
                            Some(sub_bindings) => bindings.extend(sub_bindings),
                            None => return None,
                        }
                    }
                    if let Some(rest_name) = rest {
                        let rest_items = items[elements.len()..].to_vec();
                        bindings.push((rest_name.clone(), Value::List(rest_items)));
                    }
                    return Some(bindings);
                }
                None
            }
            Pattern::Or(patterns) => {
                for sub_pat in patterns {
                    if let Some(bindings) = self.match_pattern(sub_pat, value) {
                        return Some(bindings);
                    }
                }
                None
            }
        }
    }

    fn exec_block(&mut self, stmts: &[Stmt]) -> Result<Signal, TlError> {
        self.env.push_scope();
        let mut result = Signal::None;
        for stmt in stmts {
            result = self.exec_stmt(stmt)?;
            match &result {
                Signal::Return(_) | Signal::Break | Signal::Continue | Signal::Throw(_) | Signal::Yield(_) => {
                    self.env.pop_scope();
                    return Ok(result);
                }
                Signal::None => {}
            }
        }
        self.env.pop_scope();
        Ok(result)
    }

    // ── Expression evaluation ────────────────────────────────

    fn eval_expr(&mut self, expr: &Expr) -> Result<Value, TlError> {
        match expr {
            Expr::Int(n) => Ok(Value::Int(*n)),
            Expr::Float(n) => Ok(Value::Float(*n)),
            Expr::Decimal(s) => {
                use std::str::FromStr;
                let cleaned = s.trim_end_matches('d');
                let d = rust_decimal::Decimal::from_str(cleaned)
                    .map_err(|e| runtime_err(format!("Invalid decimal: {e}")))?;
                Ok(Value::Decimal(d))
            }
            Expr::String(s) => Ok(Value::String(self.interpolate_string(s)?)),
            Expr::Bool(b) => Ok(Value::Bool(*b)),
            Expr::None => Ok(Value::None),

            Expr::Ident(name) => self.env.get(name).cloned().ok_or_else(|| {
                TlError::Runtime(RuntimeError {
                    message: format!("Undefined variable: `{name}`"),
                    span: None,
                    stack_trace: vec![],
                    })
            }),

            Expr::BinOp { left, op, right } => {
                let l = self.eval_expr(left)?;
                let r = self.eval_expr(right)?;
                self.eval_binop(&l, op, &r)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval_expr(expr)?;
                match op {
                    UnaryOp::Neg => match val {
                        Value::Int(n) => Ok(Value::Int(-n)),
                        Value::Float(n) => Ok(Value::Float(-n)),
                        Value::Decimal(d) => Ok(Value::Decimal(-d)),
                        _ => Err(runtime_err(format!(
                            "Cannot negate {}",
                            val.type_name()
                        ))),
                    },
                    UnaryOp::Not => Ok(Value::Bool(!val.is_truthy())),
                }
            }

            Expr::Call { function, args } => {
                // Method call: obj.method(args) — where function is Member { object, field }
                if let Expr::Member { object, field } = function.as_ref() {
                    let obj = self.eval_expr(object)?;
                    let mut eval_args = Vec::new();
                    for arg in args {
                        eval_args.push(self.eval_expr(arg)?);
                    }
                    return self.call_method(&obj, field, &eval_args);
                }
                let func = self.eval_expr(function)?;
                let mut eval_args = Vec::new();
                for arg in args {
                    eval_args.push(self.eval_expr(arg)?);
                }
                self.call_function(&func, &eval_args)
            }

            Expr::Member { object, field } => {
                let obj = self.eval_expr(object)?;
                match &obj {
                    Value::StructInstance { fields, .. } => {
                        fields.get(field).cloned().ok_or_else(|| {
                            runtime_err(format!("Struct has no field `{field}`"))
                        })
                    }
                    Value::Module { exports, name } => {
                        exports.get(field).cloned().ok_or_else(|| {
                            runtime_err(format!("Module `{name}` has no export `{field}`"))
                        })
                    }
                    Value::Map(pairs) => {
                        Ok(pairs.iter()
                            .find(|(k, _)| k == field)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(Value::None))
                    }
                    #[cfg(feature = "python")]
                    Value::PyObject(wrapper) => {
                        Ok(interp_py_get_member(wrapper, field))
                    }
                    _ => Err(runtime_err(format!(
                        "Cannot access field `{field}` on {}",
                        obj.type_name()
                    ))),
                }
            }

            Expr::Pipe { left, right } => {
                let left_val = self.eval_expr(left)?;
                // Table-aware pipe: if left is a Table, dispatch to table operations
                if let Value::Table(ref tl_table) = left_val {
                    return self.eval_table_pipe(tl_table.df.clone(), right);
                }
                // Regular pipe: left_val becomes the first argument to the right-side call
                match right.as_ref() {
                    Expr::Call { function, args } => {
                        let func = self.eval_expr(function)?;
                        let mut all_args = vec![left_val];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        self.call_function(&func, &all_args)
                    }
                    Expr::Ident(name) => {
                        let func = self.env.get(name).cloned().ok_or_else(|| {
                            TlError::Runtime(RuntimeError {
                                message: format!("Undefined function: `{name}`"),
                                span: None,
                                stack_trace: vec![],
                                })
                        })?;
                        self.call_function(&func, &[left_val])
                    }
                    _ => Err(runtime_err(
                        "Right side of |> must be a function call".to_string(),
                    )),
                }
            }

            Expr::List(elements) => {
                let mut items = Vec::new();
                for el in elements {
                    items.push(self.eval_expr(el)?);
                }
                Ok(Value::List(items))
            }

            Expr::Index { object, index } => {
                let obj = self.eval_expr(object)?;
                let idx = self.eval_expr(index)?;
                match (&obj, &idx) {
                    (Value::List(items), Value::Int(i)) => {
                        let i = *i as usize;
                        items.get(i).cloned().ok_or_else(|| {
                            runtime_err(format!(
                                "Index {i} out of bounds for list of length {}",
                                items.len()
                            ))
                        })
                    }
                    (Value::Map(pairs), Value::String(key)) => {
                        Ok(pairs.iter()
                            .find(|(k, _)| k == key)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(Value::None))
                    }
                    _ => Err(runtime_err(format!(
                        "Cannot index {} with {}",
                        obj.type_name(),
                        idx.type_name()
                    ))),
                }
            }

            Expr::Case { arms } => {
                for arm in arms {
                    match &arm.pattern {
                        Pattern::Wildcard | Pattern::Binding(_) => {
                            return self.eval_expr(&arm.body);
                        }
                        Pattern::Literal(expr) => {
                            let val = self.eval_expr(expr)?;
                            if val.is_truthy() {
                                return self.eval_expr(&arm.body);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Value::None)
            }

            Expr::Match { subject, arms } => {
                let subject_val = self.eval_expr(subject)?;
                for arm in arms {
                    if let Some(bindings) = self.match_pattern(&arm.pattern, &subject_val) {
                        self.env.push_scope();
                        for (name, val) in &bindings {
                            self.env.set(name.clone(), val.clone());
                        }
                        // Check guard
                        if let Some(guard) = &arm.guard {
                            let guard_val = self.eval_expr(guard)?;
                            if !guard_val.is_truthy() {
                                self.env.pop_scope();
                                continue;
                            }
                        }
                        let result = self.eval_expr(&arm.body);
                        self.env.pop_scope();
                        return result;
                    }
                }
                Ok(Value::None)
            }

            Expr::NullCoalesce { expr, default } => {
                let val = self.eval_expr(expr)?;
                if matches!(val, Value::None) {
                    self.eval_expr(default)
                } else {
                    Ok(val)
                }
            }

            Expr::Closure { params, body, .. } => {
                Ok(Value::Closure {
                    params: params.clone(),
                    body: body.clone(),
                    captured_env: self.env.scopes.clone(),
                })
            }

            Expr::Assign { target, value } => {
                let val = self.eval_expr(value)?;
                match target.as_ref() {
                    Expr::Ident(name) => {
                        if self.env.update(name, val.clone()) {
                            Ok(val)
                        } else {
                            Err(runtime_err(format!("Undefined variable: `{name}`")))
                        }
                    }
                    Expr::Member { object, field } => {
                        // Struct field assignment: s.x = val
                        if let Expr::Ident(name) = object.as_ref() {
                            let obj = self.env.get(name).cloned();
                            match obj {
                                Some(Value::StructInstance { type_name, mut fields }) => {
                                    fields.insert(field.clone(), val.clone());
                                    self.env.update(name, Value::StructInstance { type_name, fields });
                                    Ok(val)
                                }
                                Some(Value::Map(mut pairs)) => {
                                    if let Some(entry) = pairs.iter_mut().find(|(k, _)| k == field) {
                                        entry.1 = val.clone();
                                    } else {
                                        pairs.push((field.clone(), val.clone()));
                                    }
                                    self.env.update(name, Value::Map(pairs));
                                    Ok(val)
                                }
                                _ => Err(runtime_err(format!("Cannot set field on {}", name))),
                            }
                        } else {
                            Err(runtime_err("Invalid assignment target".to_string()))
                        }
                    }
                    Expr::Index { object, index } => {
                        // Map/list index assignment: m["key"] = val, list[0] = val
                        if let Expr::Ident(name) = object.as_ref() {
                            let idx = self.eval_expr(index)?;
                            let obj = self.env.get(name).cloned();
                            match (obj, idx) {
                                (Some(Value::Map(mut pairs)), Value::String(key)) => {
                                    if let Some(entry) = pairs.iter_mut().find(|(k, _)| k == &key) {
                                        entry.1 = val.clone();
                                    } else {
                                        pairs.push((key, val.clone()));
                                    }
                                    self.env.update(name, Value::Map(pairs));
                                    Ok(val)
                                }
                                (Some(Value::List(mut items)), Value::Int(i)) => {
                                    let i = i as usize;
                                    if i < items.len() {
                                        items[i] = val.clone();
                                        self.env.update(name, Value::List(items));
                                        Ok(val)
                                    } else {
                                        Err(runtime_err(format!("Index {i} out of bounds")))
                                    }
                                }
                                _ => Err(runtime_err("Invalid index assignment target".to_string())),
                            }
                        } else {
                            Err(runtime_err("Invalid assignment target".to_string()))
                        }
                    }
                    _ => Err(runtime_err("Invalid assignment target".to_string())),
                }
            }

            Expr::StructInit { name, fields } => {
                let def = self.env.get(name).cloned();
                match def {
                    Some(Value::StructDef { name: type_name, fields: def_fields }) => {
                        let mut field_map = HashMap::new();
                        for (fname, fexpr) in fields {
                            let fval = self.eval_expr(fexpr)?;
                            if !def_fields.contains(fname) {
                                return Err(runtime_err(format!(
                                    "Unknown field `{fname}` on struct `{type_name}`"
                                )));
                            }
                            field_map.insert(fname.clone(), fval);
                        }
                        Ok(Value::StructInstance {
                            type_name,
                            fields: field_map,
                        })
                    }
                    _ => Err(runtime_err(format!("Unknown struct type: `{name}`"))),
                }
            }

            Expr::EnumVariant { enum_name, variant, args } => {
                let def = self.env.get(enum_name).cloned();
                match def {
                    Some(Value::EnumDef { name: type_name, variants }) => {
                        if let Some((_, expected_count)) = variants.iter().find(|(v, _)| v == variant) {
                            let mut eval_args = Vec::new();
                            for arg in args {
                                eval_args.push(self.eval_expr(arg)?);
                            }
                            if eval_args.len() != *expected_count {
                                return Err(runtime_err(format!(
                                    "Enum variant {}::{} expects {} arguments, got {}",
                                    type_name, variant, expected_count, eval_args.len()
                                )));
                            }
                            Ok(Value::EnumInstance {
                                type_name,
                                variant: variant.clone(),
                                fields: eval_args,
                            })
                        } else {
                            Err(runtime_err(format!(
                                "Unknown variant `{variant}` on enum `{type_name}`"
                            )))
                        }
                    }
                    _ => Err(runtime_err(format!("Unknown enum type: `{enum_name}`"))),
                }
            }

            Expr::Yield(_) => {
                // Yield should not be directly evaluated in interpreter — it's handled
                // by the generator thread infrastructure. If we get here, yield was used
                // outside a generator context.
                Err(runtime_err("yield used outside of a generator function".to_string()))
            }
            Expr::Await(inner) => {
                let val = self.eval_expr(inner)?;
                match val {
                    Value::Task(task) => {
                        let rx = {
                            let mut guard = task.receiver.lock().unwrap();
                            guard.take()
                        };
                        match rx {
                            Some(receiver) => {
                                match receiver.recv() {
                                    Ok(Ok(result)) => Ok(result),
                                    Ok(Err(err_msg)) => Err(runtime_err(err_msg)),
                                    Err(_) => Err(runtime_err("Task channel disconnected".to_string())),
                                }
                            }
                            None => Err(runtime_err("Task already awaited".to_string())),
                        }
                    }
                    // Non-task values pass through
                    other => Ok(other),
                }
            }

            Expr::Try(inner) => {
                let val = self.eval_expr(inner)?;
                match val {
                    Value::EnumInstance { ref type_name, ref variant, ref fields } if type_name == "Result" => {
                        if variant == "Ok" && !fields.is_empty() {
                            Ok(fields[0].clone())
                        } else if variant == "Err" {
                            // Propagate: signal early return with Err value
                            let err_msg = if fields.is_empty() { "error".to_string() } else { format!("{}", fields[0]) };
                            Err(TlError::Runtime(tl_errors::RuntimeError {
                                message: format!("__try_propagate__:{err_msg}"),
                                span: None,
                                stack_trace: vec![],
                            }))
                        } else {
                            Ok(val)
                        }
                    }
                    Value::None => {
                        // Propagate: early return None
                        Err(TlError::Runtime(tl_errors::RuntimeError {
                            message: "__try_propagate_none__".to_string(),
                            span: None,
                            stack_trace: vec![],
                        }))
                    }
                    _ => Ok(val), // passthrough
                }
            }

            _ => Err(runtime_err(format!("Unsupported expression: {expr:?}"))),
        }
    }

    fn eval_binop(&self, left: &Value, op: &BinOp, right: &Value) -> Result<Value, TlError> {
        match (left, right) {
            // Int operations
            (Value::Int(a), Value::Int(b)) => match op {
                BinOp::Add => Ok(Value::Int(a + b)),
                BinOp::Sub => Ok(Value::Int(a - b)),
                BinOp::Mul => Ok(Value::Int(a * b)),
                BinOp::Div => {
                    if *b == 0 {
                        Err(runtime_err("Division by zero".to_string()))
                    } else {
                        Ok(Value::Int(a / b))
                    }
                }
                BinOp::Mod => {
                    if *b == 0 {
                        Err(runtime_err("Modulo by zero".to_string()))
                    } else {
                        Ok(Value::Int(a % b))
                    }
                }
                BinOp::Pow => Ok(Value::Int(a.pow(*b as u32)),),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                BinOp::Lt => Ok(Value::Bool(a < b)),
                BinOp::Gt => Ok(Value::Bool(a > b)),
                BinOp::Lte => Ok(Value::Bool(a <= b)),
                BinOp::Gte => Ok(Value::Bool(a >= b)),
                BinOp::And => Ok(Value::Bool(*a != 0 && *b != 0)),
                BinOp::Or => Ok(Value::Bool(*a != 0 || *b != 0)),
            },

            // Float operations
            (Value::Float(a), Value::Float(b)) => match op {
                BinOp::Add => Ok(Value::Float(a + b)),
                BinOp::Sub => Ok(Value::Float(a - b)),
                BinOp::Mul => Ok(Value::Float(a * b)),
                BinOp::Div => Ok(Value::Float(a / b)),
                BinOp::Mod => Ok(Value::Float(a % b)),
                BinOp::Pow => Ok(Value::Float(a.powf(*b))),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                BinOp::Lt => Ok(Value::Bool(a < b)),
                BinOp::Gt => Ok(Value::Bool(a > b)),
                BinOp::Lte => Ok(Value::Bool(a <= b)),
                BinOp::Gte => Ok(Value::Bool(a >= b)),
                _ => Err(runtime_err(format!("Unsupported op: float {op} float"))),
            },

            // Int-Float mixed (promote int to float)
            (Value::Int(a), Value::Float(b)) => {
                self.eval_binop(&Value::Float(*a as f64), op, &Value::Float(*b))
            }
            (Value::Float(a), Value::Int(b)) => {
                self.eval_binop(&Value::Float(*a), op, &Value::Float(*b as f64))
            }

            // String concatenation
            (Value::String(a), Value::String(b)) if *op == BinOp::Add => {
                Ok(Value::String(format!("{a}{b}")))
            }

            // String repeat
            (Value::String(a), Value::Int(b)) if *op == BinOp::Mul => {
                Ok(Value::String(a.repeat(*b as usize)))
            }

            // Boolean logic
            (Value::Bool(a), Value::Bool(b)) => match op {
                BinOp::And => Ok(Value::Bool(*a && *b)),
                BinOp::Or => Ok(Value::Bool(*a || *b)),
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                _ => Err(runtime_err(format!("Unsupported op: bool {op} bool"))),
            },

            // String equality
            (Value::String(a), Value::String(b)) => match op {
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                _ => Err(runtime_err(format!(
                    "Unsupported op: string {op} string"
                ))),
            },

            // Tensor arithmetic
            (Value::Tensor(a), Value::Tensor(b)) => match op {
                BinOp::Add => {
                    let result = a.add(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Sub => {
                    let result = a.sub(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Mul => {
                    let result = a.mul(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                BinOp::Div => {
                    let result = a.div(b).map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(result))
                }
                _ => Err(runtime_err(format!("Unsupported op: tensor {op} tensor"))),
            },

            // Tensor * scalar
            (Value::Tensor(t), Value::Float(s)) | (Value::Float(s), Value::Tensor(t)) if *op == BinOp::Mul => {
                Ok(Value::Tensor(t.scale(*s)))
            }

            // Decimal arithmetic (Phase 22)
            (Value::Decimal(a), Value::Decimal(b)) => match op {
                BinOp::Add => Ok(Value::Decimal(a + b)),
                BinOp::Sub => Ok(Value::Decimal(a - b)),
                BinOp::Mul => Ok(Value::Decimal(a * b)),
                BinOp::Div => {
                    if b.is_zero() {
                        Err(runtime_err("Division by zero".to_string()))
                    } else {
                        Ok(Value::Decimal(a / b))
                    }
                }
                BinOp::Eq => Ok(Value::Bool(a == b)),
                BinOp::Neq => Ok(Value::Bool(a != b)),
                BinOp::Lt => Ok(Value::Bool(a < b)),
                BinOp::Gt => Ok(Value::Bool(a > b)),
                BinOp::Lte => Ok(Value::Bool(a <= b)),
                BinOp::Gte => Ok(Value::Bool(a >= b)),
                _ => Err(runtime_err(format!("Unsupported op: decimal {op} decimal"))),
            },
            // Decimal + Int -> Decimal
            (Value::Decimal(a), Value::Int(b)) => {
                let b_dec = rust_decimal::Decimal::from(*b);
                self.eval_binop(&Value::Decimal(*a), op, &Value::Decimal(b_dec))
            }
            (Value::Int(a), Value::Decimal(b)) => {
                let a_dec = rust_decimal::Decimal::from(*a);
                self.eval_binop(&Value::Decimal(a_dec), op, &Value::Decimal(*b))
            }
            // Decimal + Float -> Float
            (Value::Decimal(a), Value::Float(b)) => {
                use rust_decimal::prelude::ToPrimitive;
                let a_f = a.to_f64().unwrap_or(0.0);
                self.eval_binop(&Value::Float(a_f), op, &Value::Float(*b))
            }
            (Value::Float(a), Value::Decimal(b)) => {
                use rust_decimal::prelude::ToPrimitive;
                let b_f = b.to_f64().unwrap_or(0.0);
                self.eval_binop(&Value::Float(*a), op, &Value::Float(b_f))
            }

            _ => Err(runtime_err(format!(
                "Cannot apply `{op}` to {} and {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }

    fn call_function(&mut self, func: &Value, args: &[Value]) -> Result<Value, TlError> {
        match func {
            Value::Builtin(name) => self.call_builtin(name, args),
            Value::Function {
                params, body, is_generator, ..
            } => {
                if args.len() != params.len() {
                    return Err(runtime_err(format!(
                        "Expected {} arguments, got {}",
                        params.len(),
                        args.len()
                    )));
                }

                // If this is a generator function, create a generator coroutine
                if *is_generator {
                    return self.create_generator(params, body, args);
                }

                self.env.push_scope();
                for (param, arg) in params.iter().zip(args) {
                    self.env.set(param.name.clone(), arg.clone());
                }
                let mut result = Value::None;
                for stmt in body {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Return(val)) => {
                            result = val;
                            break;
                        }
                        Ok(Signal::None) => {
                            if let Some(val) = &self.last_expr_value {
                                result = val.clone();
                            }
                        }
                        Err(TlError::Runtime(ref e)) if e.message.starts_with("__try_propagate__:") => {
                            // ? operator hit an Err — return the Err as this function's return value
                            let err_msg = e.message.strip_prefix("__try_propagate__:").unwrap_or("error");
                            self.env.pop_scope();
                            return Ok(Value::EnumInstance {
                                type_name: "Result".to_string(),
                                variant: "Err".to_string(),
                                fields: vec![Value::String(err_msg.to_string())],
                            });
                        }
                        Err(TlError::Runtime(ref e)) if e.message == "__try_propagate_none__" => {
                            // ? operator hit None — return None as this function's return value
                            self.env.pop_scope();
                            return Ok(Value::None);
                        }
                        Err(e) => {
                            self.env.pop_scope();
                            return Err(e);
                        }
                        Ok(_) => {}
                    }
                }
                self.env.pop_scope();
                Ok(result)
            }
            Value::Closure {
                params,
                body,
                captured_env,
            } => {
                if args.len() != params.len() {
                    return Err(runtime_err(format!(
                        "Closure expected {} arguments, got {}",
                        params.len(),
                        args.len()
                    )));
                }
                // Save current env, swap in captured env
                let saved_env = std::mem::replace(&mut self.env.scopes, captured_env.clone());
                self.env.push_scope();
                for (param, arg) in params.iter().zip(args) {
                    self.env.set(param.name.clone(), arg.clone());
                }
                let result = match &body {
                    ClosureBody::Expr(e) => self.eval_expr(e),
                    ClosureBody::Block { stmts, expr } => {
                        let mut early_return = None;
                        for s in stmts {
                            match self.exec_stmt(s) {
                                Ok(Signal::Return(val)) => {
                                    early_return = Some(val);
                                    break;
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    self.env.scopes = saved_env;
                                    return Err(e);
                                }
                            }
                        }
                        if let Some(val) = early_return {
                            Ok(val)
                        } else if let Some(e) = expr {
                            self.eval_expr(e)
                        } else {
                            Ok(Value::None)
                        }
                    }
                };
                // Restore original env
                self.env.scopes = saved_env;
                result
            }
            _ => Err(runtime_err(format!(
                "Cannot call {}",
                func.type_name()
            ))),
        }
    }

    /// Create a generator from a function with yield.
    /// Spawns a thread that executes the function body, pausing at each yield.
    fn create_generator(&mut self, params: &[Param], body: &[Stmt], args: &[Value]) -> Result<Value, TlError> {
        let params = params.to_vec();
        let body = body.to_vec();
        let args = args.to_vec();
        let env_scopes = self.env.scopes.clone();
        let method_table = self.method_table.clone();

        // Channel for yielded values: generator thread → consumer
        let (yield_tx, yield_rx) = mpsc::channel::<Result<Value, String>>();
        // Channel for resume signals: consumer → generator thread
        let (resume_tx, resume_rx) = mpsc::sync_channel::<()>(0);

        std::thread::spawn(move || {
            let mut interp = Interpreter::new();
            interp.env.scopes = env_scopes;
            interp.method_table = method_table;

            // Override yield behavior: instead of Expr::Yield erroring,
            // we handle Signal::Yield from exec_stmt
            interp.env.push_scope();
            for (param, arg) in params.iter().zip(&args) {
                interp.env.set(param.name.clone(), arg.clone());
            }

            // Wait for first next() call before starting
            if resume_rx.recv().is_err() {
                return; // Consumer dropped — generator abandoned
            }

            for stmt in &body {
                match interp.exec_stmt_gen(stmt, &yield_tx, &resume_rx) {
                    Ok(GenSignal::None) => {}
                    Ok(GenSignal::Return(_)) => break,
                    Ok(GenSignal::Break) | Ok(GenSignal::Continue) => {}
                    Ok(GenSignal::Yield(_)) => {
                        // Already handled inside exec_stmt_gen
                    }
                    Ok(GenSignal::Throw(v)) => {
                        let _ = yield_tx.send(Err(format!("{v}")));
                        return;
                    }
                    Err(e) => {
                        let _ = yield_tx.send(Err(format!("{e}")));
                        return;
                    }
                }
            }
            interp.env.pop_scope();
            // Generator function completed — channel closes naturally
        });

        let gn = TlGenerator::new(TlGeneratorKind::UserDefined {
            receiver: Mutex::new(Some(yield_rx)),
            resume_tx,
        });
        Ok(Value::Generator(Arc::new(gn)))
    }

    /// Advance a generator by one step.
    fn interpreter_next(&mut self, gen_arc: &Arc<TlGenerator>) -> Result<Value, TlError> {
        let done = *gen_arc.done.lock().unwrap();
        if done {
            return Ok(Value::None);
        }

        match &gen_arc.kind {
            TlGeneratorKind::UserDefined { receiver, resume_tx } => {
                // Signal the generator thread to continue
                if resume_tx.send(()).is_err() {
                    *gen_arc.done.lock().unwrap() = true;
                    return Ok(Value::None);
                }
                // Wait for the next yielded value
                let rx_guard = receiver.lock().unwrap();
                if let Some(rx) = rx_guard.as_ref() {
                    match rx.recv() {
                        Ok(Ok(val)) => Ok(val),
                        Ok(Err(err_msg)) => {
                            *gen_arc.done.lock().unwrap() = true;
                            Err(runtime_err(err_msg))
                        }
                        Err(_) => {
                            // Channel closed — generator exhausted
                            *gen_arc.done.lock().unwrap() = true;
                            Ok(Value::None)
                        }
                    }
                } else {
                    *gen_arc.done.lock().unwrap() = true;
                    Ok(Value::None)
                }
            }
            TlGeneratorKind::ListIter { items, index } => {
                let mut idx = index.lock().unwrap();
                if *idx < items.len() {
                    let val = items[*idx].clone();
                    *idx += 1;
                    Ok(val)
                } else {
                    *gen_arc.done.lock().unwrap() = true;
                    Ok(Value::None)
                }
            }
            TlGeneratorKind::Take { source, remaining } => {
                let mut rem = remaining.lock().unwrap();
                if *rem == 0 {
                    *gen_arc.done.lock().unwrap() = true;
                    return Ok(Value::None);
                }
                *rem -= 1;
                drop(rem);
                let val = self.interpreter_next(source)?;
                if matches!(val, Value::None) {
                    *gen_arc.done.lock().unwrap() = true;
                }
                Ok(val)
            }
            TlGeneratorKind::Skip { source, remaining } => {
                let mut rem = remaining.lock().unwrap();
                let skip_n = *rem;
                *rem = 0;
                drop(rem);
                for _ in 0..skip_n {
                    let val = self.interpreter_next(source)?;
                    if matches!(val, Value::None) {
                        *gen_arc.done.lock().unwrap() = true;
                        return Ok(Value::None);
                    }
                }
                let val = self.interpreter_next(source)?;
                if matches!(val, Value::None) {
                    *gen_arc.done.lock().unwrap() = true;
                }
                Ok(val)
            }
            TlGeneratorKind::Map { source, func } => {
                let val = self.interpreter_next(source)?;
                if matches!(val, Value::None) {
                    *gen_arc.done.lock().unwrap() = true;
                    return Ok(Value::None);
                }
                self.call_function(func, &[val])
            }
            TlGeneratorKind::Filter { source, func } => {
                loop {
                    let val = self.interpreter_next(source)?;
                    if matches!(val, Value::None) {
                        *gen_arc.done.lock().unwrap() = true;
                        return Ok(Value::None);
                    }
                    let test = self.call_function(func, &[val.clone()])?;
                    if test.is_truthy() {
                        return Ok(val);
                    }
                }
            }
            TlGeneratorKind::Chain { first, second, on_second } => {
                let is_second = *on_second.lock().unwrap();
                if !is_second {
                    let val = self.interpreter_next(first)?;
                    if matches!(val, Value::None) {
                        *on_second.lock().unwrap() = true;
                        return self.interpreter_next(second);
                    }
                    Ok(val)
                } else {
                    let val = self.interpreter_next(second)?;
                    if matches!(val, Value::None) {
                        *gen_arc.done.lock().unwrap() = true;
                    }
                    Ok(val)
                }
            }
            TlGeneratorKind::Zip { first, second } => {
                let val1 = self.interpreter_next(first)?;
                let val2 = self.interpreter_next(second)?;
                if matches!(val1, Value::None) || matches!(val2, Value::None) {
                    *gen_arc.done.lock().unwrap() = true;
                    return Ok(Value::None);
                }
                Ok(Value::List(vec![val1, val2]))
            }
            TlGeneratorKind::Enumerate { source, index } => {
                let mut idx = index.lock().unwrap();
                let cur_idx = *idx;
                *idx += 1;
                drop(idx);
                let val = self.interpreter_next(source)?;
                if matches!(val, Value::None) {
                    *gen_arc.done.lock().unwrap() = true;
                    return Ok(Value::None);
                }
                Ok(Value::List(vec![Value::Int(cur_idx as i64), val]))
            }
        }
    }

    fn call_builtin(&mut self, name: &str, args: &[Value]) -> Result<Value, TlError> {
        match name {
            "print" | "println" => {
                // If any arg is a table, auto-collect and display it
                let mut parts = Vec::new();
                for a in args {
                    match a {
                        Value::Table(t) => {
                            let batches = self.engine().collect(t.df.clone()).map_err(|e| runtime_err(e))?;
                            let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                            parts.push(formatted);
                        }
                        _ => parts.push(format!("{a}")),
                    }
                }
                let line = parts.join(" ");
                println!("{line}");
                self.output.push(line);
                Ok(Value::None)
            }
            "len" => match args.first() {
                Some(Value::String(s)) => Ok(Value::Int(s.len() as i64)),
                Some(Value::List(l)) => Ok(Value::Int(l.len() as i64)),
                Some(Value::Map(pairs)) => Ok(Value::Int(pairs.len() as i64)),
                Some(Value::Set(items)) => Ok(Value::Int(items.len() as i64)),
                _ => Err(runtime_err("len() expects a string, list, map, or set".to_string())),
            },
            "str" => Ok(Value::String(
                args.first().map(|v| format!("{v}")).unwrap_or_default(),
            )),
            "int" => match args.first() {
                Some(Value::Float(f)) => Ok(Value::Int(*f as i64)),
                Some(Value::String(s)) => s
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to int"))),
                Some(Value::Int(n)) => Ok(Value::Int(*n)),
                Some(Value::Bool(b)) => Ok(Value::Int(if *b { 1 } else { 0 })),
                _ => Err(runtime_err("int() expects a number, string, or bool".to_string())),
            },
            "float" => match args.first() {
                Some(Value::Int(n)) => Ok(Value::Float(*n as f64)),
                Some(Value::String(s)) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to float"))),
                Some(Value::Float(n)) => Ok(Value::Float(*n)),
                Some(Value::Bool(b)) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
                _ => Err(runtime_err("float() expects a number, string, or bool".to_string())),
            },
            "abs" => match args.first() {
                Some(Value::Int(n)) => Ok(Value::Int(n.abs())),
                Some(Value::Float(n)) => Ok(Value::Float(n.abs())),
                _ => Err(runtime_err("abs() expects a number".to_string())),
            },
            "min" => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.min(b))),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.min(*b))),
                        _ => Err(runtime_err("min() expects two numbers".to_string())),
                    }
                } else {
                    Err(runtime_err("min() expects 2 arguments".to_string()))
                }
            }
            "max" => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.max(b))),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.max(*b))),
                        _ => Err(runtime_err("max() expects two numbers".to_string())),
                    }
                } else {
                    Err(runtime_err("max() expects 2 arguments".to_string()))
                }
            }
            "range" => {
                if args.len() == 1 {
                    if let Value::Int(n) = &args[0] {
                        Ok(Value::List((0..*n).map(Value::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects an integer".to_string()))
                    }
                } else if args.len() == 2 {
                    if let (Value::Int(start), Value::Int(end)) = (&args[0], &args[1]) {
                        Ok(Value::List((*start..*end).map(Value::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects integers".to_string()))
                    }
                } else if args.len() == 3 {
                    if let (Value::Int(start), Value::Int(end), Value::Int(step)) = (&args[0], &args[1], &args[2]) {
                        if *step == 0 { return Err(runtime_err("range() step cannot be zero".to_string())); }
                        let mut result = Vec::new();
                        let mut i = *start;
                        if *step > 0 {
                            while i < *end { result.push(Value::Int(i)); i += step; }
                        } else {
                            while i > *end { result.push(Value::Int(i)); i += step; }
                        }
                        Ok(Value::List(result))
                    } else {
                        Err(runtime_err("range() expects integers".to_string()))
                    }
                } else {
                    Err(runtime_err("range() expects 1, 2, or 3 arguments".to_string()))
                }
            }
            "push" => {
                if args.len() == 2 {
                    if let Value::List(mut items) = args[0].clone() {
                        items.push(args[1].clone());
                        Ok(Value::List(items))
                    } else {
                        Err(runtime_err("push() first arg must be a list".to_string()))
                    }
                } else {
                    Err(runtime_err("push() expects 2 arguments".to_string()))
                }
            }
            "type_of" => Ok(Value::String(
                args.first()
                    .map(|v| v.type_name().to_string())
                    .unwrap_or_else(|| "none".to_string()),
            )),
            "map" => {
                if args.len() != 2 {
                    return Err(runtime_err("map() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("map() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                let mut result = Vec::new();
                for item in items {
                    result.push(self.call_function(&func, &[item])?);
                }
                Ok(Value::List(result))
            }
            "filter" => {
                if args.len() != 2 {
                    return Err(runtime_err("filter() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("filter() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_function(&func, &[item.clone()])?;
                    if val.is_truthy() {
                        result.push(item);
                    }
                }
                Ok(Value::List(result))
            }
            "reduce" => {
                if args.len() != 3 {
                    return Err(runtime_err("reduce() expects 3 arguments (list, init, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("reduce() first arg must be a list".to_string())),
                };
                let mut acc = args[1].clone();
                let func = args[2].clone();
                for item in items {
                    acc = self.call_function(&func, &[acc, item])?;
                }
                Ok(acc)
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(runtime_err("sum() expects 1 argument (list)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("sum() expects a list".to_string())),
                };
                let mut total: i64 = 0;
                let mut is_float = false;
                let mut total_f: f64 = 0.0;
                for item in &items {
                    match item {
                        Value::Int(n) => {
                            if is_float {
                                total_f += *n as f64;
                            } else {
                                total += n;
                            }
                        }
                        Value::Float(n) => {
                            if !is_float {
                                total_f = total as f64;
                                is_float = true;
                            }
                            total_f += n;
                        }
                        _ => return Err(runtime_err("sum() list must contain numbers".to_string())),
                    }
                }
                if is_float {
                    Ok(Value::Float(total_f))
                } else {
                    Ok(Value::Int(total))
                }
            }
            "any" => {
                if args.len() != 2 {
                    return Err(runtime_err("any() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("any() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_function(&func, &[item])?;
                    if val.is_truthy() {
                        return Ok(Value::Bool(true));
                    }
                }
                Ok(Value::Bool(false))
            }
            "all" => {
                if args.len() != 2 {
                    return Err(runtime_err("all() expects 2 arguments (list, fn)".to_string()));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err("all() first arg must be a list".to_string())),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_function(&func, &[item])?;
                    if !val.is_truthy() {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            // ── Data engine builtins ──
            "read_csv" => {
                if args.len() != 1 {
                    return Err(runtime_err("read_csv() expects 1 argument (path)".into()));
                }
                let path = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("read_csv() path must be a string".into())),
                };
                let df = self.engine().read_csv(&path).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            "read_parquet" => {
                if args.len() != 1 {
                    return Err(runtime_err("read_parquet() expects 1 argument (path)".into()));
                }
                let path = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("read_parquet() path must be a string".into())),
                };
                let df = self.engine().read_parquet(&path).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            "write_csv" => {
                if args.len() != 2 {
                    return Err(runtime_err("write_csv() expects 2 arguments (table, path)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_csv() first arg must be a table".into())),
                };
                let path = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("write_csv() path must be a string".into())),
                };
                self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "write_parquet" => {
                if args.len() != 2 {
                    return Err(runtime_err("write_parquet() expects 2 arguments (table, path)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_parquet() first arg must be a table".into())),
                };
                let path = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("write_parquet() path must be a string".into())),
                };
                self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "collect" => {
                if args.len() != 1 {
                    return Err(runtime_err("collect() expects 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("collect() expects a table".into())),
                };
                let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                Ok(Value::String(formatted))
            }
            "show" => {
                let df = match args.first() {
                    Some(Value::Table(t)) => t.df.clone(),
                    _ => return Err(runtime_err("show() expects a table".into())),
                };
                let limit = match args.get(1) {
                    Some(Value::Int(n)) => *n as usize,
                    None => 20,
                    _ => return Err(runtime_err("show() second arg must be an int".into())),
                };
                let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                println!("{formatted}");
                self.output.push(formatted.clone());
                Ok(Value::None)
            }
            "describe" => {
                if args.len() != 1 {
                    return Err(runtime_err("describe() expects 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("describe() expects a table".into())),
                };
                let schema = df.schema();
                let mut lines = Vec::new();
                lines.push("Columns:".to_string());
                for (qualifier, field) in schema.iter() {
                    let prefix = match qualifier {
                        Some(q) => format!("{q}."),
                        None => String::new(),
                    };
                    lines.push(format!("  {}{}: {}", prefix, field.name(), field.data_type()));
                }
                let output = lines.join("\n");
                println!("{output}");
                self.output.push(output.clone());
                Ok(Value::String(output))
            }
            "head" => {
                if args.is_empty() {
                    return Err(runtime_err("head() expects at least 1 argument (table)".into()));
                }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("head() first arg must be a table".into())),
                };
                let n = match args.get(1) {
                    Some(Value::Int(n)) => *n as usize,
                    None => 10,
                    _ => return Err(runtime_err("head() second arg must be an int".into())),
                };
                let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(Value::Table(TlTable { df: limited }))
            }
            "postgres" => {
                if args.len() != 2 {
                    return Err(runtime_err("postgres() expects 2 arguments (conn_str, table_name)".into()));
                }
                let conn_str = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("postgres() conn_str must be a string".into())),
                };
                let table_name = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err("postgres() table_name must be a string".into())),
                };
                let df = self.engine().read_postgres(&conn_str, &table_name)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df }))
            }
            // ── AI builtins ──
            "tensor" => self.builtin_tensor(args),
            "tensor_zeros" => self.builtin_tensor_zeros(args),
            "tensor_ones" => self.builtin_tensor_ones(args),
            "tensor_shape" => self.builtin_tensor_shape(args),
            "tensor_reshape" => self.builtin_tensor_reshape(args),
            "tensor_transpose" => self.builtin_tensor_transpose(args),
            "tensor_sum" => self.builtin_tensor_sum(args),
            "tensor_mean" => self.builtin_tensor_mean(args),
            "tensor_dot" => self.builtin_tensor_dot(args),
            "predict" => self.builtin_predict(args),
            "similarity" => self.builtin_similarity(args),
            "ai_complete" => self.builtin_ai_complete(args),
            "ai_chat" => self.builtin_ai_chat(args),
            "model_save" => self.builtin_model_save(args),
            "model_load" => self.builtin_model_load(args),
            "model_register" => self.builtin_model_register(args),
            "model_list" => self.builtin_model_list(args),
            "model_get" => self.builtin_model_get(args),
            "embed" | _ if name == "embed" => {
                Err(runtime_err("embed() requires an API key. Set TL_OPENAI_KEY env var.".to_string()))
            }
            // Streaming builtins
            "alert_slack" => {
                if args.len() != 2 {
                    return Err(runtime_err("alert_slack(url, message) requires 2 args".to_string()));
                }
                let url = match &args[0] { Value::String(s) => s.clone(), _ => return Err(runtime_err("alert_slack: url must be a string".to_string())) };
                let msg = match &args[1] { Value::String(s) => s.clone(), _ => format!("{}", args[1]) };
                tl_stream::send_alert(&tl_stream::AlertTarget::Slack(url), &msg)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "alert_webhook" => {
                if args.len() != 2 {
                    return Err(runtime_err("alert_webhook(url, message) requires 2 args".to_string()));
                }
                let url = match &args[0] { Value::String(s) => s.clone(), _ => return Err(runtime_err("alert_webhook: url must be a string".to_string())) };
                let msg = match &args[1] { Value::String(s) => s.clone(), _ => format!("{}", args[1]) };
                tl_stream::send_alert(&tl_stream::AlertTarget::Webhook(url), &msg)
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "emit" => {
                // emit(value) — output a value in a stream context
                if args.is_empty() {
                    return Err(runtime_err("emit() requires at least 1 argument".to_string()));
                }
                let val = &args[0];
                self.output.push(format!("emit: {val}"));
                Ok(val.clone())
            }
            "lineage" => {
                // lineage() — create a new lineage tracker
                // For now, return a string representation
                Ok(Value::String("lineage_tracker".to_string()))
            }
            "run_pipeline" => {
                if args.is_empty() {
                    return Err(runtime_err("run_pipeline() requires a pipeline name".to_string()));
                }
                if let Value::Pipeline(ref def) = args[0] {
                    Ok(Value::String(format!("Pipeline '{}' triggered", def.name)))
                } else {
                    Err(runtime_err("run_pipeline: argument must be a pipeline".to_string()))
                }
            }
            // Math builtins
            "sqrt" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.sqrt())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).sqrt())),
                _ => Err(runtime_err_s("sqrt() expects a number")),
            },
            "pow" => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.powf(*b))),
                        (Value::Int(a), Value::Int(b)) => Ok(Value::Float((*a as f64).powf(*b as f64))),
                        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a.powf(*b as f64))),
                        (Value::Int(a), Value::Float(b)) => Ok(Value::Float((*a as f64).powf(*b))),
                        _ => Err(runtime_err_s("pow() expects two numbers")),
                    }
                } else {
                    Err(runtime_err_s("pow() expects 2 arguments"))
                }
            }
            "floor" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.floor())),
                Some(Value::Int(n)) => Ok(Value::Int(*n)),
                _ => Err(runtime_err_s("floor() expects a number")),
            },
            "ceil" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.ceil())),
                Some(Value::Int(n)) => Ok(Value::Int(*n)),
                _ => Err(runtime_err_s("ceil() expects a number")),
            },
            "round" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.round())),
                Some(Value::Int(n)) => Ok(Value::Int(*n)),
                _ => Err(runtime_err_s("round() expects a number")),
            },
            "sin" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.sin())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).sin())),
                _ => Err(runtime_err_s("sin() expects a number")),
            },
            "cos" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.cos())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).cos())),
                _ => Err(runtime_err_s("cos() expects a number")),
            },
            "tan" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.tan())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).tan())),
                _ => Err(runtime_err_s("tan() expects a number")),
            },
            "log" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.ln())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).ln())),
                _ => Err(runtime_err_s("log() expects a number")),
            },
            "log2" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.log2())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).log2())),
                _ => Err(runtime_err_s("log2() expects a number")),
            },
            "log10" => match args.first() {
                Some(Value::Float(n)) => Ok(Value::Float(n.log10())),
                Some(Value::Int(n)) => Ok(Value::Float((*n as f64).log10())),
                _ => Err(runtime_err_s("log10() expects a number")),
            },
            "join" => {
                if args.len() == 2 {
                    if let (Value::String(sep), Value::List(items)) = (&args[0], &args[1]) {
                        let parts: Vec<String> = items.iter().map(|v| format!("{v}")).collect();
                        Ok(Value::String(parts.join(sep.as_str())))
                    } else {
                        Err(runtime_err_s("join() expects a separator string and a list"))
                    }
                } else {
                    Err(runtime_err_s("join() expects 2 arguments"))
                }
            }
            // Assert builtins
            "assert" => {
                if args.is_empty() {
                    return Err(runtime_err_s("assert() expects at least 1 argument"));
                }
                if !args[0].is_truthy() {
                    let msg = if args.len() > 1 {
                        format!("{}", args[1])
                    } else {
                        "Assertion failed".to_string()
                    };
                    Err(runtime_err(msg))
                } else {
                    Ok(Value::None)
                }
            }
            "assert_eq" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("assert_eq() expects 2 arguments"));
                }
                let eq = match (&args[0], &args[1]) {
                    (Value::Int(a), Value::Int(b)) => a == b,
                    (Value::Float(a), Value::Float(b)) => a == b,
                    (Value::String(a), Value::String(b)) => a == b,
                    (Value::Bool(a), Value::Bool(b)) => a == b,
                    (Value::None, Value::None) => true,
                    _ => false,
                };
                if !eq {
                    Err(runtime_err(format!(
                        "Assertion failed: {} != {}",
                        args[0], args[1]
                    )))
                } else {
                    Ok(Value::None)
                }
            }
            // HTTP builtins
            "http_get" => {
                if args.is_empty() {
                    return Err(runtime_err_s("http_get() expects a URL string"));
                }
                if let Value::String(url) = &args[0] {
                    let body = reqwest::blocking::get(url.as_str())
                        .map_err(|e| runtime_err(format!("HTTP GET error: {e}")))?
                        .text()
                        .map_err(|e| runtime_err(format!("HTTP response error: {e}")))?;
                    Ok(Value::String(body))
                } else {
                    Err(runtime_err_s("http_get() expects a string URL"))
                }
            }
            "http_post" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("http_post() expects a URL and body string"));
                }
                if let (Value::String(url), Value::String(body_str)) = (&args[0], &args[1]) {
                    let client = reqwest::blocking::Client::new();
                    let resp = client
                        .post(url.as_str())
                        .header("Content-Type", "application/json")
                        .body(body_str.clone())
                        .send()
                        .map_err(|e| runtime_err(format!("HTTP POST error: {e}")))?
                        .text()
                        .map_err(|e| runtime_err(format!("HTTP response error: {e}")))?;
                    Ok(Value::String(resp))
                } else {
                    Err(runtime_err_s("http_post() expects string URL and body"))
                }
            }
            // ── Phase 6: Stdlib & Ecosystem builtins ──
            "json_parse" => {
                if args.is_empty() { return Err(runtime_err_s("json_parse() expects a string")); }
                if let Value::String(s) = &args[0] {
                    let json_val: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| runtime_err(format!("JSON parse error: {e}")))?;
                    Ok(json_to_value(&json_val))
                } else {
                    Err(runtime_err_s("json_parse() expects a string"))
                }
            }
            "json_stringify" => {
                if args.is_empty() { return Err(runtime_err_s("json_stringify() expects a value")); }
                let json = value_to_json(&args[0]);
                Ok(Value::String(json.to_string()))
            }
            "map_from" => {
                if args.len() % 2 != 0 {
                    return Err(runtime_err_s("map_from() expects even number of arguments (key, value pairs)"));
                }
                let mut pairs = Vec::new();
                for chunk in args.chunks(2) {
                    let key = match &chunk[0] {
                        Value::String(s) => s.clone(),
                        other => format!("{other}"),
                    };
                    pairs.push((key, chunk[1].clone()));
                }
                Ok(Value::Map(pairs))
            }
            "read_file" => {
                if args.is_empty() { return Err(runtime_err_s("read_file() expects a path")); }
                if let Value::String(path) = &args[0] {
                    let content = std::fs::read_to_string(path)
                        .map_err(|e| runtime_err(format!("read_file error: {e}")))?;
                    Ok(Value::String(content))
                } else {
                    Err(runtime_err_s("read_file() expects a string path"))
                }
            }
            "write_file" => {
                if args.len() < 2 { return Err(runtime_err_s("write_file() expects path and content")); }
                if let (Value::String(path), Value::String(content)) = (&args[0], &args[1]) {
                    std::fs::write(path, content)
                        .map_err(|e| runtime_err(format!("write_file error: {e}")))?;
                    Ok(Value::None)
                } else {
                    Err(runtime_err_s("write_file() expects string path and content"))
                }
            }
            "append_file" => {
                if args.len() < 2 { return Err(runtime_err_s("append_file() expects path and content")); }
                if let (Value::String(path), Value::String(content)) = (&args[0], &args[1]) {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .create(true).append(true).open(path)
                        .map_err(|e| runtime_err(format!("append_file error: {e}")))?;
                    file.write_all(content.as_bytes())
                        .map_err(|e| runtime_err(format!("append_file error: {e}")))?;
                    Ok(Value::None)
                } else {
                    Err(runtime_err_s("append_file() expects string path and content"))
                }
            }
            "file_exists" => {
                if args.is_empty() { return Err(runtime_err_s("file_exists() expects a path")); }
                if let Value::String(path) = &args[0] {
                    Ok(Value::Bool(std::path::Path::new(path).exists()))
                } else {
                    Err(runtime_err_s("file_exists() expects a string path"))
                }
            }
            "list_dir" => {
                if args.is_empty() { return Err(runtime_err_s("list_dir() expects a path")); }
                if let Value::String(path) = &args[0] {
                    let entries: Vec<Value> = std::fs::read_dir(path)
                        .map_err(|e| runtime_err(format!("list_dir error: {e}")))?
                        .filter_map(|e| e.ok())
                        .map(|e| Value::String(e.file_name().to_string_lossy().to_string()))
                        .collect();
                    Ok(Value::List(entries))
                } else {
                    Err(runtime_err_s("list_dir() expects a string path"))
                }
            }
            "env_get" => {
                if args.is_empty() { return Err(runtime_err_s("env_get() expects a name")); }
                if let Value::String(name) = &args[0] {
                    match std::env::var(name) {
                        Ok(val) => Ok(Value::String(val)),
                        Err(_) => Ok(Value::None),
                    }
                } else {
                    Err(runtime_err_s("env_get() expects a string"))
                }
            }
            "env_set" => {
                if args.len() < 2 { return Err(runtime_err_s("env_set() expects name and value")); }
                if let (Value::String(name), Value::String(val)) = (&args[0], &args[1]) {
                    unsafe { std::env::set_var(name, val); }
                    Ok(Value::None)
                } else {
                    Err(runtime_err_s("env_set() expects two strings"))
                }
            }
            "regex_match" => {
                if args.len() < 2 { return Err(runtime_err_s("regex_match() expects pattern and string")); }
                if let (Value::String(pattern), Value::String(text)) = (&args[0], &args[1]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    Ok(Value::Bool(re.is_match(text)))
                } else {
                    Err(runtime_err_s("regex_match() expects string pattern and string"))
                }
            }
            "regex_find" => {
                if args.len() < 2 { return Err(runtime_err_s("regex_find() expects pattern and string")); }
                if let (Value::String(pattern), Value::String(text)) = (&args[0], &args[1]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    let matches: Vec<Value> = re.find_iter(text)
                        .map(|m| Value::String(m.as_str().to_string()))
                        .collect();
                    Ok(Value::List(matches))
                } else {
                    Err(runtime_err_s("regex_find() expects string pattern and string"))
                }
            }
            "regex_replace" => {
                if args.len() < 3 { return Err(runtime_err_s("regex_replace() expects pattern, string, replacement")); }
                if let (Value::String(pattern), Value::String(text), Value::String(replacement)) = (&args[0], &args[1], &args[2]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    Ok(Value::String(re.replace_all(text, replacement.as_str()).to_string()))
                } else {
                    Err(runtime_err_s("regex_replace() expects three strings"))
                }
            }
            "now" => {
                let ts = chrono::Utc::now().timestamp_millis();
                Ok(Value::Int(ts))
            }
            "date_format" => {
                if args.len() < 2 { return Err(runtime_err_s("date_format() expects timestamp_ms and format")); }
                if let (Value::Int(ts), Value::String(fmt)) = (&args[0], &args[1]) {
                    use chrono::TimeZone;
                    let secs = *ts / 1000;
                    let nsecs = ((*ts % 1000) * 1_000_000) as u32;
                    let dt = chrono::Utc.timestamp_opt(secs, nsecs)
                        .single()
                        .ok_or_else(|| runtime_err_s("Invalid timestamp"))?;
                    Ok(Value::String(dt.format(fmt).to_string()))
                } else {
                    Err(runtime_err_s("date_format() expects int timestamp and string format"))
                }
            }
            "date_parse" => {
                if args.len() < 2 { return Err(runtime_err_s("date_parse() expects string and format")); }
                if let (Value::String(s), Value::String(fmt)) = (&args[0], &args[1]) {
                    let dt = chrono::NaiveDateTime::parse_from_str(s, fmt)
                        .map_err(|e| runtime_err(format!("date_parse error: {e}")))?;
                    let ts = dt.and_utc().timestamp_millis();
                    Ok(Value::Int(ts))
                } else {
                    Err(runtime_err_s("date_parse() expects two strings"))
                }
            }
            "zip" => {
                if args.len() < 2 { return Err(runtime_err_s("zip() expects two lists")); }
                if let (Value::List(a), Value::List(b)) = (&args[0], &args[1]) {
                    let pairs: Vec<Value> = a.iter().zip(b.iter())
                        .map(|(x, y)| Value::List(vec![x.clone(), y.clone()]))
                        .collect();
                    Ok(Value::List(pairs))
                } else {
                    Err(runtime_err_s("zip() expects two lists"))
                }
            }
            "enumerate" => {
                if args.is_empty() { return Err(runtime_err_s("enumerate() expects a list")); }
                if let Value::List(items) = &args[0] {
                    let pairs: Vec<Value> = items.iter().enumerate()
                        .map(|(i, v)| Value::List(vec![Value::Int(i as i64), v.clone()]))
                        .collect();
                    Ok(Value::List(pairs))
                } else {
                    Err(runtime_err_s("enumerate() expects a list"))
                }
            }
            "bool" => {
                if args.is_empty() { return Err(runtime_err_s("bool() expects a value")); }
                Ok(Value::Bool(args[0].is_truthy()))
            }

            // Phase 7: Concurrency builtins
            "spawn" => {
                if args.is_empty() {
                    return Err(runtime_err_s("spawn() expects a function argument"));
                }
                match &args[0] {
                    Value::Function { params, body, name, .. } => {
                        let params = params.clone();
                        let body = body.clone();
                        let _name = name.clone();
                        let (tx, rx) = mpsc::channel::<Result<Value, String>>();
                        // Capture the global scope for the spawned thread
                        let env_scopes = self.env.scopes.clone();
                        let method_table = self.method_table.clone();
                        std::thread::spawn(move || {
                            let mut interp = Interpreter::new();
                            interp.env.scopes = env_scopes;
                            interp.method_table = method_table;
                            // Execute function body
                            interp.env.push_scope();
                            for param in &params {
                                interp.env.set(param.name.clone(), Value::None);
                            }
                            let mut result = Value::None;
                            let mut err = None;
                            for stmt in &body {
                                match interp.exec_stmt(stmt) {
                                    Ok(Signal::Return(val)) => { result = val; break; }
                                    Ok(Signal::None) => {
                                        if let Some(val) = &interp.last_expr_value {
                                            result = val.clone();
                                        }
                                    }
                                    Ok(Signal::Throw(val)) => {
                                        err = Some(format!("{val}"));
                                        break;
                                    }
                                    Err(e) => { err = Some(format!("{e}")); break; }
                                    _ => {}
                                }
                            }
                            interp.env.pop_scope();
                            let _ = tx.send(match err {
                                Some(e) => Err(e),
                                None => Ok(result),
                            });
                        });
                        Ok(Value::Task(Arc::new(TlTask::new(rx))))
                    }
                    Value::Closure { params, body, captured_env } => {
                        let params = params.clone();
                        let body = body.clone();
                        let captured_env = captured_env.clone();
                        let (tx, rx) = mpsc::channel::<Result<Value, String>>();
                        let method_table = self.method_table.clone();
                        std::thread::spawn(move || {
                            let mut interp = Interpreter::new();
                            interp.env.scopes = captured_env;
                            interp.method_table = method_table;
                            interp.env.push_scope();
                            for param in &params {
                                interp.env.set(param.name.clone(), Value::None);
                            }
                            let result = interp.eval_closure_body(&body);
                            interp.env.pop_scope();
                            let _ = tx.send(result.map_err(|e| format!("{e}")));
                        });
                        Ok(Value::Task(Arc::new(TlTask::new(rx))))
                    }
                    _ => Err(runtime_err_s("spawn() expects a function")),
                }
            }
            "sleep" => {
                if args.is_empty() {
                    return Err(runtime_err_s("sleep() expects a duration in milliseconds"));
                }
                match &args[0] {
                    Value::Int(ms) => {
                        std::thread::sleep(Duration::from_millis(*ms as u64));
                        Ok(Value::None)
                    }
                    _ => Err(runtime_err_s("sleep() expects an integer (milliseconds)")),
                }
            }
            "channel" => {
                let capacity = match args.first() {
                    Some(Value::Int(n)) => *n as usize,
                    None => 64,
                    _ => return Err(runtime_err_s("channel() expects an optional integer capacity")),
                };
                Ok(Value::Channel(Arc::new(TlChannel::new(capacity))))
            }
            "send" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("send() expects a channel and a value"));
                }
                match &args[0] {
                    Value::Channel(ch) => {
                        ch.sender.send(args[1].clone())
                            .map_err(|_| runtime_err_s("Channel disconnected"))?;
                        Ok(Value::None)
                    }
                    _ => Err(runtime_err_s("send() expects a channel as first argument")),
                }
            }
            "recv" => {
                if args.is_empty() {
                    return Err(runtime_err_s("recv() expects a channel"));
                }
                match &args[0] {
                    Value::Channel(ch) => {
                        let guard = ch.receiver.lock().unwrap();
                        match guard.recv() {
                            Ok(val) => Ok(val),
                            Err(_) => Ok(Value::None),
                        }
                    }
                    _ => Err(runtime_err_s("recv() expects a channel")),
                }
            }
            "try_recv" => {
                if args.is_empty() {
                    return Err(runtime_err_s("try_recv() expects a channel"));
                }
                match &args[0] {
                    Value::Channel(ch) => {
                        let guard = ch.receiver.lock().unwrap();
                        match guard.try_recv() {
                            Ok(val) => Ok(val),
                            Err(_) => Ok(Value::None),
                        }
                    }
                    _ => Err(runtime_err_s("try_recv() expects a channel")),
                }
            }
            "await_all" => {
                if args.is_empty() {
                    return Err(runtime_err_s("await_all() expects a list of tasks"));
                }
                match &args[0] {
                    Value::List(tasks) => {
                        let mut results = Vec::with_capacity(tasks.len());
                        for task in tasks {
                            match task {
                                Value::Task(t) => {
                                    let rx = {
                                        let mut guard = t.receiver.lock().unwrap();
                                        guard.take()
                                    };
                                    match rx {
                                        Some(receiver) => {
                                            match receiver.recv() {
                                                Ok(Ok(val)) => results.push(val),
                                                Ok(Err(e)) => return Err(runtime_err(e)),
                                                Err(_) => return Err(runtime_err_s("Task channel disconnected")),
                                            }
                                        }
                                        None => return Err(runtime_err_s("Task already awaited")),
                                    }
                                }
                                other => results.push(other.clone()),
                            }
                        }
                        Ok(Value::List(results))
                    }
                    _ => Err(runtime_err_s("await_all() expects a list")),
                }
            }
            "pmap" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("pmap() expects a list and a function"));
                }
                let items = match &args[0] {
                    Value::List(items) => items.clone(),
                    _ => return Err(runtime_err_s("pmap() expects a list as first argument")),
                };

                let env_scopes = self.env.scopes.clone();
                let method_table = self.method_table.clone();

                match &args[1] {
                    Value::Function { params, body, .. } => {
                        let params = params.clone();
                        let body = body.clone();
                        let mut handles = Vec::with_capacity(items.len());
                        for item in items {
                            let params = params.clone();
                            let body = body.clone();
                            let env_scopes = env_scopes.clone();
                            let method_table = method_table.clone();
                            let handle = std::thread::spawn(move || {
                                let mut interp = Interpreter::new();
                                interp.env.scopes = env_scopes;
                                interp.method_table = method_table;
                                interp.env.push_scope();
                                if let Some(p) = params.first() {
                                    interp.env.set(p.name.clone(), item);
                                }
                                let mut result = Value::None;
                                for stmt in &body {
                                    match interp.exec_stmt(stmt) {
                                        Ok(Signal::Return(val)) => { result = val; break; }
                                        Ok(Signal::None) => {
                                            if let Some(val) = &interp.last_expr_value {
                                                result = val.clone();
                                            }
                                        }
                                        Ok(Signal::Throw(val)) => {
                                            interp.env.pop_scope();
                                            return Err(format!("{val}"));
                                        }
                                        Err(e) => { interp.env.pop_scope(); return Err(format!("{e}")); }
                                        _ => {}
                                    }
                                }
                                interp.env.pop_scope();
                                Ok(result)
                            });
                            handles.push(handle);
                        }
                        let mut results = Vec::with_capacity(handles.len());
                        for handle in handles {
                            match handle.join() {
                                Ok(Ok(val)) => results.push(val),
                                Ok(Err(e)) => return Err(runtime_err(e)),
                                Err(_) => return Err(runtime_err_s("pmap() thread panicked")),
                            }
                        }
                        Ok(Value::List(results))
                    }
                    Value::Closure { params, body, captured_env } => {
                        let params = params.clone();
                        let body = body.clone();
                        let captured_env = captured_env.clone();
                        let mut handles = Vec::with_capacity(items.len());
                        for item in items {
                            let params = params.clone();
                            let body = body.clone();
                            let captured_env = captured_env.clone();
                            let method_table = method_table.clone();
                            let handle = std::thread::spawn(move || {
                                let mut interp = Interpreter::new();
                                interp.env.scopes = captured_env;
                                interp.method_table = method_table;
                                interp.env.push_scope();
                                if let Some(p) = params.first() {
                                    interp.env.set(p.name.clone(), item);
                                }
                                let result = interp.eval_closure_body(&body);
                                interp.env.pop_scope();
                                result.map_err(|e| format!("{e}"))
                            });
                            handles.push(handle);
                        }
                        let mut results = Vec::with_capacity(handles.len());
                        for handle in handles {
                            match handle.join() {
                                Ok(Ok(val)) => results.push(val),
                                Ok(Err(e)) => return Err(runtime_err(e)),
                                Err(_) => return Err(runtime_err_s("pmap() thread panicked")),
                            }
                        }
                        Ok(Value::List(results))
                    }
                    _ => Err(runtime_err_s("pmap() expects a function as second argument")),
                }
            }
            "timeout" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("timeout() expects a task and a duration in milliseconds"));
                }
                let ms = match &args[1] {
                    Value::Int(n) => *n as u64,
                    _ => return Err(runtime_err_s("timeout() expects an integer duration")),
                };
                match &args[0] {
                    Value::Task(task) => {
                        let rx = {
                            let mut guard = task.receiver.lock().unwrap();
                            guard.take()
                        };
                        match rx {
                            Some(receiver) => {
                                match receiver.recv_timeout(Duration::from_millis(ms)) {
                                    Ok(Ok(val)) => Ok(val),
                                    Ok(Err(e)) => Err(runtime_err(e)),
                                    Err(mpsc::RecvTimeoutError::Timeout) => {
                                        Err(runtime_err_s("Task timed out"))
                                    }
                                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                                        Err(runtime_err_s("Task channel disconnected"))
                                    }
                                }
                            }
                            None => Err(runtime_err_s("Task already awaited")),
                        }
                    }
                    _ => Err(runtime_err_s("timeout() expects a task as first argument")),
                }
            }

            // Phase 8: Iterators & Generators
            "next" => {
                if args.is_empty() {
                    return Err(runtime_err_s("next() expects a generator"));
                }
                match &args[0] {
                    Value::Generator(g) => self.interpreter_next(g),
                    _ => Err(runtime_err_s("next() expects a generator")),
                }
            }
            "is_generator" => {
                let val = args.first().unwrap_or(&Value::None);
                Ok(Value::Bool(matches!(val, Value::Generator(_))))
            }
            "iter" => {
                if args.is_empty() {
                    return Err(runtime_err_s("iter() expects a list"));
                }
                match &args[0] {
                    Value::List(items) => {
                        let g = TlGenerator::new(TlGeneratorKind::ListIter {
                            items: items.clone(),
                            index: Mutex::new(0),
                        });
                        Ok(Value::Generator(Arc::new(g)))
                    }
                    _ => Err(runtime_err_s("iter() expects a list")),
                }
            }
            "take" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("take() expects a generator and a count"));
                }
                let g = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("take() expects a generator")),
                };
                let n = match &args[1] {
                    Value::Int(n) => *n as usize,
                    _ => return Err(runtime_err_s("take() expects an integer count")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Take {
                    source: g,
                    remaining: Mutex::new(n),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "skip" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("skip() expects a generator and a count"));
                }
                let g = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("skip() expects a generator")),
                };
                let n = match &args[1] {
                    Value::Int(n) => *n as usize,
                    _ => return Err(runtime_err_s("skip() expects an integer count")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Skip {
                    source: g,
                    remaining: Mutex::new(n),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "gen_collect" => {
                if args.is_empty() {
                    return Err(runtime_err_s("gen_collect() expects a generator"));
                }
                match &args[0] {
                    Value::Generator(g) => {
                        let mut items = Vec::new();
                        loop {
                            let val = self.interpreter_next(g)?;
                            if matches!(val, Value::None) {
                                break;
                            }
                            items.push(val);
                        }
                        Ok(Value::List(items))
                    }
                    _ => Err(runtime_err_s("gen_collect() expects a generator")),
                }
            }
            "gen_map" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("gen_map() expects a generator and a function"));
                }
                let g = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("gen_map() expects a generator")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Map {
                    source: g,
                    func: args[1].clone(),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "gen_filter" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("gen_filter() expects a generator and a function"));
                }
                let g = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("gen_filter() expects a generator")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Filter {
                    source: g,
                    func: args[1].clone(),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "chain" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("chain() expects two generators"));
                }
                let first = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("chain() expects generators")),
                };
                let second = match &args[1] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("chain() expects generators")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Chain {
                    first,
                    second,
                    on_second: Mutex::new(false),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "gen_zip" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("gen_zip() expects two generators"));
                }
                let first = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("gen_zip() expects generators")),
                };
                let second = match &args[1] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("gen_zip() expects generators")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Zip { first, second });
                Ok(Value::Generator(Arc::new(gn)))
            }
            "gen_enumerate" => {
                if args.is_empty() {
                    return Err(runtime_err_s("gen_enumerate() expects a generator"));
                }
                let g = match &args[0] {
                    Value::Generator(g) => g.clone(),
                    _ => return Err(runtime_err_s("gen_enumerate() expects a generator")),
                };
                let gn = TlGenerator::new(TlGeneratorKind::Enumerate {
                    source: g,
                    index: Mutex::new(0),
                });
                Ok(Value::Generator(Arc::new(gn)))
            }

            // Phase 10: Result builtins
            "Ok" => {
                let val = if args.is_empty() { Value::None } else { args[0].clone() };
                Ok(Value::EnumInstance {
                    type_name: "Result".to_string(),
                    variant: "Ok".to_string(),
                    fields: vec![val],
                })
            }
            "Err" => {
                let val = if args.is_empty() { Value::String("error".to_string()) } else { args[0].clone() };
                Ok(Value::EnumInstance {
                    type_name: "Result".to_string(),
                    variant: "Err".to_string(),
                    fields: vec![val],
                })
            }
            "is_ok" => {
                if args.is_empty() {
                    return Err(runtime_err_s("is_ok() expects an argument"));
                }
                match &args[0] {
                    Value::EnumInstance { type_name, variant, .. } if type_name == "Result" => {
                        Ok(Value::Bool(variant == "Ok"))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            "is_err" => {
                if args.is_empty() {
                    return Err(runtime_err_s("is_err() expects an argument"));
                }
                match &args[0] {
                    Value::EnumInstance { type_name, variant, .. } if type_name == "Result" => {
                        Ok(Value::Bool(variant == "Err"))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            "unwrap" => {
                if args.is_empty() {
                    return Err(runtime_err_s("unwrap() expects an argument"));
                }
                match &args[0] {
                    Value::EnumInstance { type_name, variant, fields } if type_name == "Result" => {
                        if variant == "Ok" && !fields.is_empty() {
                            Ok(fields[0].clone())
                        } else if variant == "Err" {
                            let msg = if fields.is_empty() {
                                "error".to_string()
                            } else {
                                format!("{}", fields[0])
                            };
                            Err(runtime_err(format!("unwrap() called on Err({msg})")))
                        } else {
                            Ok(Value::None)
                        }
                    }
                    Value::None => Err(runtime_err_s("unwrap() called on none")),
                    other => Ok(other.clone()),
                }
            }
            "set_from" => {
                if args.is_empty() {
                    return Ok(Value::Set(Vec::new()));
                }
                match &args[0] {
                    Value::List(items) => {
                        let mut result = Vec::new();
                        for item in items {
                            if !result.iter().any(|x| values_equal(x, item)) {
                                result.push(item.clone());
                            }
                        }
                        Ok(Value::Set(result))
                    }
                    _ => Err(runtime_err_s("set_from() expects a list")),
                }
            }
            "set_add" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_add() expects (set, value)"));
                }
                match &args[0] {
                    Value::Set(items) => {
                        let val = &args[1];
                        let mut new_items = items.clone();
                        if !new_items.iter().any(|x| values_equal(x, val)) {
                            new_items.push(val.clone());
                        }
                        Ok(Value::Set(new_items))
                    }
                    _ => Err(runtime_err_s("set_add() expects a set as first argument")),
                }
            }
            "set_remove" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_remove() expects (set, value)"));
                }
                match &args[0] {
                    Value::Set(items) => {
                        let val = &args[1];
                        let new_items: Vec<Value> = items.iter()
                            .filter(|x| !values_equal(x, val))
                            .cloned()
                            .collect();
                        Ok(Value::Set(new_items))
                    }
                    _ => Err(runtime_err_s("set_remove() expects a set as first argument")),
                }
            }
            "set_contains" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_contains() expects (set, value)"));
                }
                match &args[0] {
                    Value::Set(items) => {
                        let val = &args[1];
                        Ok(Value::Bool(items.iter().any(|x| values_equal(x, val))))
                    }
                    _ => Err(runtime_err_s("set_contains() expects a set as first argument")),
                }
            }
            "set_union" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_union() expects (set, set)"));
                }
                match (&args[0], &args[1]) {
                    (Value::Set(a), Value::Set(b)) => {
                        let mut result = a.clone();
                        for item in b {
                            if !result.iter().any(|x| values_equal(x, item)) {
                                result.push(item.clone());
                            }
                        }
                        Ok(Value::Set(result))
                    }
                    _ => Err(runtime_err_s("set_union() expects two sets")),
                }
            }
            "set_intersection" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_intersection() expects (set, set)"));
                }
                match (&args[0], &args[1]) {
                    (Value::Set(a), Value::Set(b)) => {
                        let result: Vec<Value> = a.iter()
                            .filter(|x| b.iter().any(|y| values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(Value::Set(result))
                    }
                    _ => Err(runtime_err_s("set_intersection() expects two sets")),
                }
            }
            "set_difference" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("set_difference() expects (set, set)"));
                }
                match (&args[0], &args[1]) {
                    (Value::Set(a), Value::Set(b)) => {
                        let result: Vec<Value> = a.iter()
                            .filter(|x| !b.iter().any(|y| values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(Value::Set(result))
                    }
                    _ => Err(runtime_err_s("set_difference() expects two sets")),
                }
            }

            // ── Phase 15: Data Quality & Connectors ──
            "fill_null" => {
                if args.len() < 2 { return Err(runtime_err_s("fill_null() expects (table, column, [strategy], [value])")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("fill_null() first arg must be a table")),
                };
                let column = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("fill_null() column must be a string")),
                };
                let strategy = if args.len() > 2 {
                    match &args[2] { Value::String(s) => s.clone(), _ => "value".to_string() }
                } else { "value".to_string() };
                let fill_value = if args.len() > 3 {
                    match &args[3] {
                        Value::Int(n) => Some(*n as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    }
                } else if args.len() > 2 && strategy == "value" {
                    match &args[2] {
                        Value::Int(n) => { let r = self.engine().fill_null(df, &column, "value", Some(*n as f64)).map_err(|e| runtime_err(e))?; return Ok(Value::Table(TlTable { df: r })); }
                        Value::Float(f) => { let r = self.engine().fill_null(df, &column, "value", Some(*f)).map_err(|e| runtime_err(e))?; return Ok(Value::Table(TlTable { df: r })); }
                        _ => None,
                    }
                } else { None };
                let result = self.engine().fill_null(df, &column, &strategy, fill_value).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df: result }))
            }
            "drop_null" => {
                if args.len() < 2 { return Err(runtime_err_s("drop_null() expects (table, column)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("drop_null() first arg must be a table")),
                };
                let column = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("drop_null() column must be a string")),
                };
                let result = self.engine().drop_null(df, &column).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df: result }))
            }
            "dedup" => {
                if args.is_empty() { return Err(runtime_err_s("dedup() expects (table, [columns...])")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("dedup() first arg must be a table")),
                };
                let columns: Vec<String> = args[1..].iter().filter_map(|a| {
                    if let Value::String(s) = a { Some(s.clone()) } else { None }
                }).collect();
                let result = self.engine().dedup(df, &columns).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df: result }))
            }
            "clamp" => {
                if args.len() < 4 { return Err(runtime_err_s("clamp() expects (table, column, min, max)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("clamp() first arg must be a table")),
                };
                let column = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("clamp() column must be a string")),
                };
                let min_val = match &args[2] {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => return Err(runtime_err_s("clamp() min must be a number")),
                };
                let max_val = match &args[3] {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => return Err(runtime_err_s("clamp() max must be a number")),
                };
                let result = self.engine().clamp(df, &column, min_val, max_val).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df: result }))
            }
            "data_profile" => {
                if args.is_empty() { return Err(runtime_err_s("data_profile() expects (table)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("data_profile() arg must be a table")),
                };
                let result = self.engine().data_profile(df).map_err(|e| runtime_err(e))?;
                Ok(Value::Table(TlTable { df: result }))
            }
            "row_count" => {
                if args.is_empty() { return Err(runtime_err_s("row_count() expects (table)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("row_count() arg must be a table")),
                };
                let count = self.engine().row_count(df).map_err(|e| runtime_err(e))?;
                Ok(Value::Int(count))
            }
            "null_rate" => {
                if args.len() < 2 { return Err(runtime_err_s("null_rate() expects (table, column)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("null_rate() first arg must be a table")),
                };
                let column = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("null_rate() column must be a string")),
                };
                let rate = self.engine().null_rate(df, &column).map_err(|e| runtime_err(e))?;
                Ok(Value::Float(rate))
            }
            "is_unique" => {
                if args.len() < 2 { return Err(runtime_err_s("is_unique() expects (table, column)")); }
                let df = match &args[0] {
                    Value::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err_s("is_unique() first arg must be a table")),
                };
                let column = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("is_unique() column must be a string")),
                };
                let unique = self.engine().is_unique(df, &column).map_err(|e| runtime_err(e))?;
                Ok(Value::Bool(unique))
            }
            "is_email" => {
                if args.is_empty() { return Err(runtime_err_s("is_email() expects 1 argument")); }
                let s = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("is_email() arg must be a string")),
                };
                Ok(Value::Bool(tl_data::validate::is_email(&s)))
            }
            "is_url" => {
                if args.is_empty() { return Err(runtime_err_s("is_url() expects 1 argument")); }
                let s = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("is_url() arg must be a string")),
                };
                Ok(Value::Bool(tl_data::validate::is_url(&s)))
            }
            "is_phone" => {
                if args.is_empty() { return Err(runtime_err_s("is_phone() expects 1 argument")); }
                let s = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("is_phone() arg must be a string")),
                };
                Ok(Value::Bool(tl_data::validate::is_phone(&s)))
            }
            "is_between" => {
                if args.len() < 3 { return Err(runtime_err_s("is_between() expects (value, low, high)")); }
                let val = match &args[0] {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => return Err(runtime_err_s("is_between() value must be a number")),
                };
                let low = match &args[1] {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => return Err(runtime_err_s("is_between() low must be a number")),
                };
                let high = match &args[2] {
                    Value::Int(n) => *n as f64,
                    Value::Float(f) => *f,
                    _ => return Err(runtime_err_s("is_between() high must be a number")),
                };
                Ok(Value::Bool(tl_data::validate::is_between(val, low, high)))
            }
            "levenshtein" => {
                if args.len() < 2 { return Err(runtime_err_s("levenshtein() expects (str_a, str_b)")); }
                let a = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("levenshtein() args must be strings")),
                };
                let b = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("levenshtein() args must be strings")),
                };
                Ok(Value::Int(tl_data::validate::levenshtein(&a, &b) as i64))
            }
            "soundex" => {
                if args.is_empty() { return Err(runtime_err_s("soundex() expects 1 argument")); }
                let s = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("soundex() arg must be a string")),
                };
                Ok(Value::String(tl_data::validate::soundex(&s)))
            }
            "read_mysql" => {
                #[cfg(feature = "mysql")]
                {
                    if args.len() < 2 { return Err(runtime_err_s("read_mysql() expects (conn_str, query)")); }
                    let conn_str = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("read_mysql() conn_str must be a string")),
                    };
                    let query = match &args[1] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("read_mysql() query must be a string")),
                    };
                    let df = self.engine().read_mysql(&conn_str, &query).map_err(|e| runtime_err(e))?;
                    Ok(Value::Table(TlTable { df }))
                }
                #[cfg(not(feature = "mysql"))]
                Err(runtime_err_s("read_mysql() requires the 'mysql' feature"))
            }
            "redis_connect" => {
                #[cfg(feature = "redis")]
                {
                    if args.is_empty() { return Err(runtime_err_s("redis_connect() expects (url)")); }
                    let url = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_connect() url must be a string")),
                    };
                    let result = tl_data::redis_conn::redis_connect(&url).map_err(|e| runtime_err(e))?;
                    Ok(Value::String(result))
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err_s("redis_connect() requires the 'redis' feature"))
            }
            "redis_get" => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 2 { return Err(runtime_err_s("redis_get() expects (url, key)")); }
                    let url = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_get() url must be a string")),
                    };
                    let key = match &args[1] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_get() key must be a string")),
                    };
                    match tl_data::redis_conn::redis_get(&url, &key).map_err(|e| runtime_err(e))? {
                        Some(v) => Ok(Value::String(v)),
                        None => Ok(Value::None),
                    }
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err_s("redis_get() requires the 'redis' feature"))
            }
            "redis_set" => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 3 { return Err(runtime_err_s("redis_set() expects (url, key, value)")); }
                    let url = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_set() url must be a string")),
                    };
                    let key = match &args[1] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_set() key must be a string")),
                    };
                    let value = match &args[2] {
                        Value::String(s) => s.clone(),
                        _ => format!("{}", &args[2]),
                    };
                    tl_data::redis_conn::redis_set(&url, &key, &value).map_err(|e| runtime_err(e))?;
                    Ok(Value::None)
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err_s("redis_set() requires the 'redis' feature"))
            }
            "redis_del" => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 2 { return Err(runtime_err_s("redis_del() expects (url, key)")); }
                    let url = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_del() url must be a string")),
                    };
                    let key = match &args[1] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("redis_del() key must be a string")),
                    };
                    let deleted = tl_data::redis_conn::redis_del(&url, &key).map_err(|e| runtime_err(e))?;
                    Ok(Value::Bool(deleted))
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err_s("redis_del() requires the 'redis' feature"))
            }
            "graphql_query" => {
                if args.len() < 2 { return Err(runtime_err_s("graphql_query() expects (endpoint, query, [variables])")); }
                let endpoint = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("graphql_query() endpoint must be a string")),
                };
                let query = match &args[1] {
                    Value::String(s) => s.clone(),
                    _ => return Err(runtime_err_s("graphql_query() query must be a string")),
                };
                let variables = if args.len() > 2 {
                    value_to_json(&args[2])
                } else {
                    serde_json::Value::Null
                };
                let mut body = serde_json::Map::new();
                body.insert("query".to_string(), serde_json::Value::String(query));
                if !variables.is_null() {
                    body.insert("variables".to_string(), variables);
                }
                let client = reqwest::blocking::Client::new();
                let resp = client.post(&endpoint)
                    .header("Content-Type", "application/json")
                    .json(&body)
                    .send()
                    .map_err(|e| runtime_err(format!("graphql_query() request error: {e}")))?;
                let text = resp.text().map_err(|e| runtime_err(format!("graphql_query() response error: {e}")))?;
                let json: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| runtime_err(format!("graphql_query() JSON parse error: {e}")))?;
                Ok(json_to_value(&json))
            }
            "register_s3" => {
                #[cfg(feature = "s3")]
                {
                    if args.len() < 2 { return Err(runtime_err_s("register_s3() expects (bucket, region, [access_key], [secret_key], [endpoint])")); }
                    let bucket = match &args[0] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("register_s3() bucket must be a string")),
                    };
                    let region = match &args[1] {
                        Value::String(s) => s.clone(),
                        _ => return Err(runtime_err_s("register_s3() region must be a string")),
                    };
                    let access_key = args.get(2).and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None });
                    let secret_key = args.get(3).and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None });
                    let endpoint = args.get(4).and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None });
                    self.engine().register_s3(
                        &bucket, &region,
                        access_key.as_deref(), secret_key.as_deref(), endpoint.as_deref(),
                    ).map_err(|e| runtime_err(e))?;
                    Ok(Value::None)
                }
                #[cfg(not(feature = "s3"))]
                Err(runtime_err_s("register_s3() requires the 's3' feature"))
            }

            // Phase 20: Python FFI
            "py_import" => {
                #[cfg(feature = "python")]
                { self.interp_py_import(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_import() requires the 'python' feature"))
            }
            "py_call" => {
                #[cfg(feature = "python")]
                { self.interp_py_call(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_call() requires the 'python' feature"))
            }
            "py_eval" => {
                #[cfg(feature = "python")]
                { self.interp_py_eval(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_eval() requires the 'python' feature"))
            }
            "py_getattr" => {
                #[cfg(feature = "python")]
                { self.interp_py_getattr(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_getattr() requires the 'python' feature"))
            }
            "py_setattr" => {
                #[cfg(feature = "python")]
                { self.interp_py_setattr(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_setattr() requires the 'python' feature"))
            }
            "py_to_tl" => {
                #[cfg(feature = "python")]
                { self.interp_py_to_tl(args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err_s("py_to_tl() requires the 'python' feature"))
            }

            // Phase 21: Schema Evolution
            "schema_register" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_register: need name")) };
                let version = match args.get(1) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_register: need version")) };
                let fields = match args.get(2) {
                    Some(Value::Map(pairs)) => {
                        let mut arrow_fields = Vec::new();
                        for (k, v) in pairs {
                            let ftype = match v { Value::String(s) => s.clone(), _ => "string".to_string() };
                            arrow_fields.push(ArrowField::new(k, tl_compiler::schema::type_name_to_arrow_pub(&ftype), true));
                        }
                        arrow_fields
                    }
                    _ => return Err(runtime_err_s("schema_register: third arg must be a map")),
                };
                let schema = Arc::new(ArrowSchema::new(fields));
                self.schema_registry.register(&name, version, schema, tl_compiler::schema::SchemaMetadata::default())
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            "schema_get" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_get: need name")) };
                let version = match args.get(1) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_get: need version")) };
                match self.schema_registry.get(&name, version) {
                    Some(vs) => {
                        let fields: Vec<Value> = vs.schema.fields().iter().map(|f: &std::sync::Arc<ArrowField>| {
                            Value::String(format!("{}: {}", f.name(), f.data_type()))
                        }).collect();
                        Ok(Value::List(fields))
                    }
                    None => Ok(Value::None),
                }
            }
            "schema_latest" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_latest: need name")) };
                match self.schema_registry.latest(&name) {
                    Some(vs) => Ok(Value::Int(vs.version)),
                    None => Ok(Value::None),
                }
            }
            "schema_history" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_history: need name")) };
                let versions = self.schema_registry.versions(&name);
                Ok(Value::List(versions.into_iter().map(Value::Int).collect()))
            }
            "schema_check" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_check: need name")) };
                let v1 = match args.get(1) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_check: need v1")) };
                let v2 = match args.get(2) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_check: need v2")) };
                let mode_str = match args.get(3) { Some(Value::String(s)) => s.clone(), _ => "backward".to_string() };
                let mode = tl_compiler::schema::CompatibilityMode::from_str(&mode_str);
                let issues = self.schema_registry.check_compatibility(&name, v1, v2, mode);
                Ok(Value::List(issues.into_iter().map(|i: tl_compiler::schema::CompatIssue| Value::String(i.to_string())).collect()))
            }
            "schema_diff" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_diff: need name")) };
                let v1 = match args.get(1) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_diff: need v1")) };
                let v2 = match args.get(2) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_diff: need v2")) };
                let diffs = self.schema_registry.diff(&name, v1, v2);
                Ok(Value::List(diffs.into_iter().map(|d: tl_compiler::schema::SchemaDiff| Value::String(d.to_string())).collect()))
            }
            "schema_versions" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_versions: need name")) };
                let versions = self.schema_registry.versions(&name);
                Ok(Value::List(versions.into_iter().map(Value::Int).collect()))
            }
            "schema_fields" => {
                let name = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("schema_fields: need name")) };
                let version = match args.get(1) { Some(Value::Int(v)) => *v, _ => return Err(runtime_err_s("schema_fields: need version")) };
                let fields = self.schema_registry.fields(&name, version);
                Ok(Value::List(fields.into_iter().map(|(n, t)| Value::String(format!("{}: {}", n, t))).collect()))
            }

            // Phase 22: decimal() builtin
            "decimal" => {
                use std::str::FromStr;
                match args.first() {
                    Some(Value::String(s)) => {
                        let cleaned = s.trim_end_matches('d');
                        let d = rust_decimal::Decimal::from_str(cleaned)
                            .map_err(|e| runtime_err(format!("Invalid decimal: {e}")))?;
                        Ok(Value::Decimal(d))
                    }
                    Some(Value::Int(n)) => Ok(Value::Decimal(rust_decimal::Decimal::from(*n))),
                    Some(Value::Float(n)) => {
                        use rust_decimal::prelude::FromPrimitive;
                        Ok(Value::Decimal(rust_decimal::Decimal::from_f64(*n).unwrap_or_default()))
                    }
                    Some(Value::Decimal(d)) => Ok(Value::Decimal(*d)),
                    _ => Err(runtime_err_s("decimal() expects a string, int, or float")),
                }
            }

            // Phase 23: Secret vault
            "secret_get" => {
                let key = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("secret_get: need key")) };
                if let Some(val) = self.secret_vault.get(&key) {
                    Ok(Value::Secret(val.clone()))
                } else {
                    // Fallback to env var
                    let env_key = format!("TL_SECRET_{}", key.to_uppercase());
                    match std::env::var(&env_key) {
                        Ok(val) => Ok(Value::Secret(val)),
                        Err(_) => Ok(Value::None),
                    }
                }
            }
            "secret_set" => {
                let key = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("secret_set: need key")) };
                let val = match args.get(1) {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Secret(s)) => s.clone(),
                    _ => return Err(runtime_err_s("secret_set: need string value")),
                };
                self.secret_vault.insert(key, val);
                Ok(Value::None)
            }
            "secret_delete" => {
                let key = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("secret_delete: need key")) };
                self.secret_vault.remove(&key);
                Ok(Value::None)
            }
            "secret_list" => {
                let keys: Vec<Value> = self.secret_vault.keys().map(|k| Value::String(k.clone())).collect();
                Ok(Value::List(keys))
            }
            "check_permission" => {
                let perm = match args.first() { Some(Value::String(s)) => s.clone(), _ => return Err(runtime_err_s("check_permission: need permission string")) };
                let allowed = match &self.security_policy {
                    Some(policy) => policy.check(&perm),
                    None => true,
                };
                Ok(Value::Bool(allowed))
            }

            // Phase 23: Data masking
            "mask_email" => {
                let email = match args.first() { Some(Value::String(s)) => s.clone(), Some(Value::Secret(s)) => s.clone(), _ => return Err(runtime_err_s("mask_email: need string")) };
                let masked = if let Some(at_pos) = email.find('@') {
                    let local = &email[..at_pos];
                    let domain = &email[at_pos..];
                    if local.len() <= 1 {
                        format!("*{domain}")
                    } else {
                        format!("{}***{domain}", &local[..1])
                    }
                } else {
                    "***".to_string()
                };
                Ok(Value::String(masked))
            }
            "mask_phone" => {
                let phone = match args.first() { Some(Value::String(s)) => s.clone(), Some(Value::Secret(s)) => s.clone(), _ => return Err(runtime_err_s("mask_phone: need string")) };
                let digits: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    let last4 = &digits[digits.len()-4..];
                    Ok(Value::String(format!("***-***-{last4}")))
                } else {
                    Ok(Value::String("***".to_string()))
                }
            }
            "mask_cc" => {
                let cc = match args.first() { Some(Value::String(s)) => s.clone(), Some(Value::Secret(s)) => s.clone(), _ => return Err(runtime_err_s("mask_cc: need string")) };
                let digits: String = cc.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    let last4 = &digits[digits.len()-4..];
                    Ok(Value::String(format!("****-****-****-{last4}")))
                } else {
                    Ok(Value::String("****-****-****-****".to_string()))
                }
            }
            "redact" => {
                let val = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Secret(s)) => s.clone(),
                    Some(v) => format!("{v}"),
                    None => return Err(runtime_err_s("redact: need value")),
                };
                let policy = match args.get(1) { Some(Value::String(s)) => s.as_str(), _ => "full" };
                let result = match policy {
                    "partial" => {
                        if val.len() <= 2 { "***".to_string() }
                        else { format!("{}***{}", &val[..1], &val[val.len()-1..]) }
                    }
                    "hash" => {
                        use sha2::Digest;
                        let hash = sha2::Sha256::digest(val.as_bytes());
                        format!("{:x}", hash)
                    }
                    _ => "***".to_string(), // "full"
                };
                Ok(Value::String(result))
            }
            "hash" => {
                let val = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Secret(s)) => s.clone(),
                    Some(v) => format!("{v}"),
                    None => return Err(runtime_err_s("hash: need value")),
                };
                let algo = match args.get(1) { Some(Value::String(s)) => s.as_str(), _ => "sha256" };
                let result = match algo {
                    "sha256" => {
                        use sha2::Digest;
                        let hash = sha2::Sha256::digest(val.as_bytes());
                        format!("{:x}", hash)
                    }
                    "sha512" => {
                        use sha2::Digest;
                        let hash = sha2::Sha512::digest(val.as_bytes());
                        format!("{:x}", hash)
                    }
                    "md5" => {
                        use md5::Digest;
                        let hash = md5::Md5::digest(val.as_bytes());
                        format!("{:x}", hash)
                    }
                    _ => return Err(runtime_err(format!("hash: unknown algorithm '{algo}', use sha256/sha512/md5"))),
                };
                Ok(Value::String(result))
            }

            // ── Phase 25: Async builtins ──
            #[cfg(feature = "async-runtime")]
            "async_read_file" => {
                let path = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_read_file() expects a string path")),
                };
                if let Some(policy) = &self.security_policy {
                    if !policy.check("file_read") {
                        return Err(runtime_err_s("async_read_file: file_read not allowed by security policy"));
                    }
                }
                let rt = self.ensure_runtime();
                let (tx, rx) = mpsc::channel();
                rt.spawn(async move {
                    let result = tokio::fs::read_to_string(&path).await;
                    let _ = tx.send(result
                        .map(|s| Value::String(s))
                        .map_err(|e| format!("async_read_file error: {e}")));
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_write_file" => {
                let path = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_write_file() expects a string path")),
                };
                let content = match args.get(1) {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_write_file() expects string content")),
                };
                if let Some(policy) = &self.security_policy {
                    if !policy.check("file_write") {
                        return Err(runtime_err_s("async_write_file: file_write not allowed by security policy"));
                    }
                }
                let rt = self.ensure_runtime();
                let (tx, rx) = mpsc::channel();
                rt.spawn(async move {
                    let result = tokio::fs::write(&path, content.as_bytes()).await;
                    let _ = tx.send(result
                        .map(|_| Value::None)
                        .map_err(|e| format!("async_write_file error: {e}")));
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_http_get" => {
                let url = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_http_get() expects a string URL")),
                };
                if let Some(policy) = &self.security_policy {
                    if !policy.check("network") {
                        return Err(runtime_err_s("async_http_get: network not allowed by security policy"));
                    }
                }
                let rt = self.ensure_runtime();
                let (tx, rx) = mpsc::channel();
                rt.spawn(async move {
                    let result: Result<Value, String> = async {
                        let body = reqwest::get(&url).await
                            .map_err(|e| format!("async_http_get error: {e}"))?
                            .text().await
                            .map_err(|e| format!("async_http_get response error: {e}"))?;
                        Ok(Value::String(body))
                    }.await;
                    let _ = tx.send(result);
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_http_post" => {
                let url = match args.first() {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_http_post() expects a string URL")),
                };
                let body = match args.get(1) {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(runtime_err_s("async_http_post() expects string body")),
                };
                if let Some(policy) = &self.security_policy {
                    if !policy.check("network") {
                        return Err(runtime_err_s("async_http_post: network not allowed by security policy"));
                    }
                }
                let rt = self.ensure_runtime();
                let (tx, rx) = mpsc::channel();
                rt.spawn(async move {
                    let result: Result<Value, String> = async {
                        let resp = reqwest::Client::new()
                            .post(&url)
                            .body(body)
                            .send().await
                            .map_err(|e| format!("async_http_post error: {e}"))?
                            .text().await
                            .map_err(|e| format!("async_http_post response error: {e}"))?;
                        Ok(Value::String(resp))
                    }.await;
                    let _ = tx.send(result);
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_sleep" => {
                let ms = match args.first() {
                    Some(Value::Int(n)) => *n as u64,
                    _ => return Err(runtime_err_s("async_sleep() expects an integer (milliseconds)")),
                };
                let rt = self.ensure_runtime();
                let (tx, rx) = mpsc::channel();
                rt.spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
                    let _ = tx.send(Ok(Value::None));
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "select" => {
                if args.len() < 2 {
                    return Err(runtime_err_s("select() expects at least 2 task arguments"));
                }
                let mut receivers = Vec::new();
                for (i, arg) in args.iter().enumerate() {
                    match arg {
                        Value::Task(task) => {
                            let rx = task.receiver.lock().unwrap().take();
                            match rx {
                                Some(r) => receivers.push(r),
                                None => return Err(runtime_err(format!("select: task {} already consumed", i))),
                            }
                        }
                        _ => return Err(runtime_err(format!("select: argument {} is not a task", i))),
                    }
                }
                let (winner_tx, winner_rx) = mpsc::channel::<Result<Value, String>>();
                for rx in receivers {
                    let tx = winner_tx.clone();
                    std::thread::spawn(move || {
                        if let Ok(result) = rx.recv() {
                            let _ = tx.send(result);
                        }
                    });
                }
                drop(winner_tx);
                Ok(Value::Task(Arc::new(TlTask::new(winner_rx))))
            }
            #[cfg(feature = "async-runtime")]
            "race_all" => {
                let tasks = match args.first() {
                    Some(Value::List(list)) => list.clone(),
                    _ => return Err(runtime_err_s("race_all() expects a list of tasks")),
                };
                if tasks.is_empty() {
                    return Err(runtime_err_s("race_all() expects a non-empty list of tasks"));
                }
                let mut receivers = Vec::new();
                for (i, task_val) in tasks.iter().enumerate() {
                    match task_val {
                        Value::Task(task) => {
                            let rx = task.receiver.lock().unwrap().take();
                            match rx {
                                Some(r) => receivers.push(r),
                                None => return Err(runtime_err(format!("race_all: task {} already consumed", i))),
                            }
                        }
                        _ => return Err(runtime_err(format!("race_all: element {} is not a task", i))),
                    }
                }
                let (winner_tx, winner_rx) = mpsc::channel::<Result<Value, String>>();
                for rx in receivers {
                    let tx = winner_tx.clone();
                    std::thread::spawn(move || {
                        if let Ok(result) = rx.recv() {
                            let _ = tx.send(result);
                        }
                    });
                }
                drop(winner_tx);
                Ok(Value::Task(Arc::new(TlTask::new(winner_rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_map" => {
                let items = match args.first() {
                    Some(Value::List(list)) => list.clone(),
                    _ => return Err(runtime_err_s("async_map() expects a list as first argument")),
                };
                let (closure_params, closure_body, closure_env) = match args.get(1) {
                    Some(Value::Closure { params, body, captured_env }) => {
                        (params.clone(), body.clone(), captured_env.clone())
                    }
                    Some(Value::Function { params, body, .. }) => {
                        (params.clone(), ClosureBody::Block { stmts: body.clone(), expr: None }, self.env.scopes.clone())
                    }
                    _ => return Err(runtime_err_s("async_map() expects a function as second argument")),
                };
                let method_table = self.method_table.clone();
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let mut results = Vec::new();
                    let mut handles = Vec::new();
                    for item in items {
                        let params = closure_params.clone();
                        let body = closure_body.clone();
                        let env = closure_env.clone();
                        let mt = method_table.clone();
                        let handle = std::thread::spawn(move || {
                            let mut interp = Interpreter::new();
                            interp.env.scopes = env;
                            interp.method_table = mt;
                            interp.env.push_scope();
                            if let Some(p) = params.first() {
                                interp.env.set(p.name.clone(), item);
                            }
                            let result = interp.eval_closure_body(&body);
                            interp.env.pop_scope();
                            result.map_err(|e| format!("{e}"))
                        });
                        handles.push(handle);
                    }
                    for handle in handles {
                        match handle.join() {
                            Ok(Ok(val)) => results.push(val),
                            Ok(Err(e)) => {
                                let _ = tx.send(Err(format!("async_map error: {e}")));
                                return;
                            }
                            Err(_) => {
                                let _ = tx.send(Err("async_map: thread panicked".to_string()));
                                return;
                            }
                        }
                    }
                    let _ = tx.send(Ok(Value::List(results)));
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(feature = "async-runtime")]
            "async_filter" => {
                let items = match args.first() {
                    Some(Value::List(list)) => list.clone(),
                    _ => return Err(runtime_err_s("async_filter() expects a list as first argument")),
                };
                let (closure_params, closure_body, closure_env) = match args.get(1) {
                    Some(Value::Closure { params, body, captured_env }) => {
                        (params.clone(), body.clone(), captured_env.clone())
                    }
                    Some(Value::Function { params, body, .. }) => {
                        (params.clone(), ClosureBody::Block { stmts: body.clone(), expr: None }, self.env.scopes.clone())
                    }
                    _ => return Err(runtime_err_s("async_filter() expects a function as second argument")),
                };
                let method_table = self.method_table.clone();
                let items_clone = items.clone();
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let mut handles = Vec::new();
                    for item in items {
                        let params = closure_params.clone();
                        let body = closure_body.clone();
                        let env = closure_env.clone();
                        let mt = method_table.clone();
                        let handle = std::thread::spawn(move || {
                            let mut interp = Interpreter::new();
                            interp.env.scopes = env;
                            interp.method_table = mt;
                            interp.env.push_scope();
                            if let Some(p) = params.first() {
                                interp.env.set(p.name.clone(), item);
                            }
                            let result = interp.eval_closure_body(&body);
                            interp.env.pop_scope();
                            result.map_err(|e| format!("{e}"))
                        });
                        handles.push(handle);
                    }
                    let mut results = Vec::new();
                    for (i, handle) in handles.into_iter().enumerate() {
                        match handle.join() {
                            Ok(Ok(val)) => {
                                if matches!(&val, Value::Bool(true)) {
                                    results.push(items_clone[i].clone());
                                }
                            }
                            Ok(Err(e)) => {
                                let _ = tx.send(Err(format!("async_filter error: {e}")));
                                return;
                            }
                            Err(_) => {
                                let _ = tx.send(Err("async_filter: thread panicked".to_string()));
                                return;
                            }
                        }
                    }
                    let _ = tx.send(Ok(Value::List(results)));
                });
                Ok(Value::Task(Arc::new(TlTask::new(rx))))
            }
            #[cfg(not(feature = "async-runtime"))]
            "async_read_file" | "async_write_file" | "async_http_get" | "async_http_post" |
            "async_sleep" | "select" | "race_all" | "async_map" | "async_filter" => {
                Err(runtime_err(format!("{name}: async builtins require the 'async-runtime' feature")))
            }

            _ => Err(runtime_err(format!("Unknown builtin: {name}"))),
        }
    }

    /// Call a method on an object via dot notation
    fn call_method(&mut self, obj: &Value, method: &str, args: &[Value]) -> Result<Value, TlError> {
        // String methods
        if let Value::String(s) = obj {
            return match method {
                "len" => Ok(Value::Int(s.len() as i64)),
                "split" => {
                    let sep = match args.first() {
                        Some(Value::String(sep)) => sep.as_str().to_string(),
                        _ => return Err(runtime_err_s("split() expects a string separator")),
                    };
                    let parts: Vec<Value> = s.split(&sep).map(|p| Value::String(p.to_string())).collect();
                    Ok(Value::List(parts))
                }
                "trim" => Ok(Value::String(s.trim().to_string())),
                "contains" => {
                    let needle = match args.first() {
                        Some(Value::String(n)) => n.as_str(),
                        _ => return Err(runtime_err_s("contains() expects a string")),
                    };
                    Ok(Value::Bool(s.contains(needle)))
                }
                "replace" => {
                    if args.len() < 2 {
                        return Err(runtime_err_s("replace() expects 2 arguments"));
                    }
                    if let (Value::String(from), Value::String(to)) = (&args[0], &args[1]) {
                        Ok(Value::String(s.replace(from.as_str(), to.as_str())))
                    } else {
                        Err(runtime_err_s("replace() expects string arguments"))
                    }
                }
                "starts_with" => {
                    let prefix = match args.first() {
                        Some(Value::String(p)) => p.as_str(),
                        _ => return Err(runtime_err_s("starts_with() expects a string")),
                    };
                    Ok(Value::Bool(s.starts_with(prefix)))
                }
                "ends_with" => {
                    let suffix = match args.first() {
                        Some(Value::String(su)) => su.as_str(),
                        _ => return Err(runtime_err_s("ends_with() expects a string")),
                    };
                    Ok(Value::Bool(s.ends_with(suffix)))
                }
                "to_upper" => Ok(Value::String(s.to_uppercase())),
                "to_lower" => Ok(Value::String(s.to_lowercase())),
                "chars" => {
                    let chars: Vec<Value> = s.chars().map(|c| Value::String(c.to_string())).collect();
                    Ok(Value::List(chars))
                }
                "repeat" => {
                    let n = match args.first() {
                        Some(Value::Int(n)) => *n as usize,
                        _ => return Err(runtime_err_s("repeat() expects an integer")),
                    };
                    Ok(Value::String(s.repeat(n)))
                }
                "index_of" => {
                    let needle = match args.first() {
                        Some(Value::String(n)) => n.as_str(),
                        _ => return Err(runtime_err_s("index_of() expects a string")),
                    };
                    Ok(Value::Int(s.find(needle).map(|i| i as i64).unwrap_or(-1)))
                }
                "substring" => {
                    if args.len() < 2 { return Err(runtime_err_s("substring() expects start and end")); }
                    let start = match &args[0] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("substring() expects integers")) };
                    let end = match &args[1] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("substring() expects integers")) };
                    let end = end.min(s.len());
                    let start = start.min(end);
                    Ok(Value::String(s[start..end].to_string()))
                }
                "pad_left" => {
                    if args.is_empty() { return Err(runtime_err_s("pad_left() expects width")); }
                    let width = match &args[0] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("pad_left() expects integer width")) };
                    let ch = match args.get(1) {
                        Some(Value::String(c)) => c.chars().next().unwrap_or(' '),
                        _ => ' ',
                    };
                    if s.len() >= width { Ok(Value::String(s.clone())) }
                    else { Ok(Value::String(format!("{}{}", std::iter::repeat(ch).take(width - s.len()).collect::<String>(), s))) }
                }
                "pad_right" => {
                    if args.is_empty() { return Err(runtime_err_s("pad_right() expects width")); }
                    let width = match &args[0] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("pad_right() expects integer width")) };
                    let ch = match args.get(1) {
                        Some(Value::String(c)) => c.chars().next().unwrap_or(' '),
                        _ => ' ',
                    };
                    if s.len() >= width { Ok(Value::String(s.clone())) }
                    else { Ok(Value::String(format!("{}{}", s, std::iter::repeat(ch).take(width - s.len()).collect::<String>()))) }
                }
                "join" => {
                    if let Some(Value::List(items)) = args.first() {
                        let parts: Vec<String> = items.iter().map(|v| format!("{v}")).collect();
                        Ok(Value::String(parts.join(s.as_str())))
                    } else {
                        Err(runtime_err_s("join() expects a list argument"))
                    }
                }
                _ => Err(runtime_err(format!("String has no method `{method}`"))),
            };
        }

        // List methods
        if let Value::List(items) = obj {
            return match method {
                "len" => Ok(Value::Int(items.len() as i64)),
                "push" => {
                    let mut new_items = items.clone();
                    for arg in args {
                        new_items.push(arg.clone());
                    }
                    Ok(Value::List(new_items))
                }
                "map" => {
                    if let Some(func) = args.first() {
                        let mut result = Vec::new();
                        for item in items {
                            result.push(self.call_function(func, &[item.clone()])?);
                        }
                        Ok(Value::List(result))
                    } else {
                        Err(runtime_err_s("map() expects a function argument"))
                    }
                }
                "filter" => {
                    if let Some(func) = args.first() {
                        let mut result = Vec::new();
                        for item in items {
                            let keep = self.call_function(func, &[item.clone()])?;
                            if keep.is_truthy() {
                                result.push(item.clone());
                            }
                        }
                        Ok(Value::List(result))
                    } else {
                        Err(runtime_err_s("filter() expects a function argument"))
                    }
                }
                "reduce" => {
                    if args.len() < 2 {
                        return Err(runtime_err_s("reduce() expects a function and initial value"));
                    }
                    let func = &args[0];
                    let mut acc = args[1].clone();
                    for item in items {
                        acc = self.call_function(func, &[acc, item.clone()])?;
                    }
                    Ok(acc)
                }
                "sort" => {
                    let mut sorted = items.clone();
                    sorted.sort_by(|a, b| {
                        match (a, b) {
                            (Value::Int(x), Value::Int(y)) => x.cmp(y),
                            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
                            (Value::String(x), Value::String(y)) => x.cmp(y),
                            _ => std::cmp::Ordering::Equal,
                        }
                    });
                    Ok(Value::List(sorted))
                }
                "reverse" => {
                    let mut reversed = items.clone();
                    reversed.reverse();
                    Ok(Value::List(reversed))
                }
                "contains" => {
                    if args.is_empty() { return Err(runtime_err_s("contains() expects a value")); }
                    let needle = &args[0];
                    let found = items.iter().any(|item| {
                        match (item, needle) {
                            (Value::Int(a), Value::Int(b)) => a == b,
                            (Value::Float(a), Value::Float(b)) => a == b,
                            (Value::String(a), Value::String(b)) => a == b,
                            (Value::Bool(a), Value::Bool(b)) => a == b,
                            (Value::None, Value::None) => true,
                            _ => false,
                        }
                    });
                    Ok(Value::Bool(found))
                }
                "index_of" => {
                    if args.is_empty() { return Err(runtime_err_s("index_of() expects a value")); }
                    let needle = &args[0];
                    let idx = items.iter().position(|item| {
                        match (item, needle) {
                            (Value::Int(a), Value::Int(b)) => a == b,
                            (Value::Float(a), Value::Float(b)) => a == b,
                            (Value::String(a), Value::String(b)) => a == b,
                            (Value::Bool(a), Value::Bool(b)) => a == b,
                            (Value::None, Value::None) => true,
                            _ => false,
                        }
                    });
                    Ok(Value::Int(idx.map(|i| i as i64).unwrap_or(-1)))
                }
                "slice" => {
                    if args.len() < 2 { return Err(runtime_err_s("slice() expects start and end")); }
                    let start = match &args[0] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("slice() expects integers")) };
                    let end = match &args[1] { Value::Int(n) => *n as usize, _ => return Err(runtime_err_s("slice() expects integers")) };
                    let end = end.min(items.len());
                    let start = start.min(end);
                    Ok(Value::List(items[start..end].to_vec()))
                }
                "flat_map" => {
                    if args.is_empty() { return Err(runtime_err_s("flat_map() expects a function")); }
                    let func = &args[0];
                    let mut result = Vec::new();
                    for item in items {
                        let val = self.call_function(func, &[item.clone()])?;
                        match val {
                            Value::List(sub) => result.extend(sub),
                            other => result.push(other),
                        }
                    }
                    Ok(Value::List(result))
                }
                _ => Err(runtime_err(format!("List has no method `{method}`"))),
            };
        }

        // Map methods
        if let Value::Map(pairs) = obj {
            return match method {
                "len" => Ok(Value::Int(pairs.len() as i64)),
                "keys" => {
                    Ok(Value::List(pairs.iter().map(|(k, _)| Value::String(k.clone())).collect()))
                }
                "values" => {
                    Ok(Value::List(pairs.iter().map(|(_, v)| v.clone()).collect()))
                }
                "contains_key" => {
                    if args.is_empty() { return Err(runtime_err_s("contains_key() expects a key")); }
                    if let Value::String(key) = &args[0] {
                        Ok(Value::Bool(pairs.iter().any(|(k, _)| k == key)))
                    } else {
                        Err(runtime_err_s("contains_key() expects a string key"))
                    }
                }
                "remove" => {
                    if args.is_empty() { return Err(runtime_err_s("remove() expects a key")); }
                    if let Value::String(key) = &args[0] {
                        let new_pairs: Vec<(String, Value)> = pairs.iter()
                            .filter(|(k, _)| k != key)
                            .cloned()
                            .collect();
                        Ok(Value::Map(new_pairs))
                    } else {
                        Err(runtime_err_s("remove() expects a string key"))
                    }
                }
                _ => Err(runtime_err(format!("Map has no method `{method}`"))),
            };
        }

        // Set methods
        if let Value::Set(items) = obj {
            return match method {
                "len" => Ok(Value::Int(items.len() as i64)),
                "contains" => {
                    if args.is_empty() { return Err(runtime_err_s("contains() expects a value")); }
                    Ok(Value::Bool(items.iter().any(|x| values_equal(x, &args[0]))))
                }
                "add" => {
                    if args.is_empty() { return Err(runtime_err_s("add() expects a value")); }
                    let mut new_items = items.clone();
                    if !new_items.iter().any(|x| values_equal(x, &args[0])) {
                        new_items.push(args[0].clone());
                    }
                    Ok(Value::Set(new_items))
                }
                "remove" => {
                    if args.is_empty() { return Err(runtime_err_s("remove() expects a value")); }
                    let new_items: Vec<Value> = items.iter()
                        .filter(|x| !values_equal(x, &args[0]))
                        .cloned()
                        .collect();
                    Ok(Value::Set(new_items))
                }
                "to_list" => Ok(Value::List(items.clone())),
                "union" => {
                    if args.is_empty() { return Err(runtime_err_s("union() expects a set")); }
                    if let Value::Set(b) = &args[0] {
                        let mut result = items.clone();
                        for item in b {
                            if !result.iter().any(|x| values_equal(x, item)) {
                                result.push(item.clone());
                            }
                        }
                        Ok(Value::Set(result))
                    } else {
                        Err(runtime_err_s("union() expects a set"))
                    }
                }
                "intersection" => {
                    if args.is_empty() { return Err(runtime_err_s("intersection() expects a set")); }
                    if let Value::Set(b) = &args[0] {
                        let result: Vec<Value> = items.iter()
                            .filter(|x| b.iter().any(|y| values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(Value::Set(result))
                    } else {
                        Err(runtime_err_s("intersection() expects a set"))
                    }
                }
                "difference" => {
                    if args.is_empty() { return Err(runtime_err_s("difference() expects a set")); }
                    if let Value::Set(b) = &args[0] {
                        let result: Vec<Value> = items.iter()
                            .filter(|x| !b.iter().any(|y| values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(Value::Set(result))
                    } else {
                        Err(runtime_err_s("difference() expects a set"))
                    }
                }
                _ => Err(runtime_err(format!("Set has no method `{method}`"))),
            };
        }

        // Generator method dispatch
        if let Value::Generator(gen_arc) = obj {
            let gen_arc = gen_arc.clone();
            return match method {
                "next" => self.interpreter_next(&gen_arc),
                "collect" => {
                    let mut items = Vec::new();
                    loop {
                        let val = self.interpreter_next(&gen_arc)?;
                        if matches!(val, Value::None) {
                            break;
                        }
                        items.push(val);
                    }
                    Ok(Value::List(items))
                }
                _ => Err(runtime_err(format!("Generator has no method `{method}`"))),
            };
        }

        // Module method dispatch (for aliased use: m.compute())
        if let Value::Module { name, exports } = obj {
            if let Some(func) = exports.get(method) {
                return self.call_function(func, args);
            } else {
                return Err(runtime_err(format!(
                    "Module '{}' has no export '{}'", name, method
                )));
            }
        }

        // Struct/impl method dispatch
        if let Value::StructInstance { type_name, .. } = obj {
            if let Some(methods) = self.method_table.get(type_name) {
                if let Some(func) = methods.get(method) {
                    let func = func.clone();
                    // Prepend self to args
                    let mut all_args = vec![obj.clone()];
                    all_args.extend_from_slice(args);
                    return self.call_function(&func, &all_args);
                }
            }
        }

        // Python method dispatch
        #[cfg(feature = "python")]
        if let Value::PyObject(wrapper) = obj {
            return interp_py_call_method(wrapper, method, args);
        }

        Err(runtime_err(format!(
            "No method `{method}` on {}",
            obj.type_name()
        )))
    }

    /// Execute a use statement (placeholder — full impl in Step 5)
    fn exec_use(&mut self, item: &tl_ast::UseItem) -> Result<Signal, TlError> {
        use tl_ast::UseItem;
        match item {
            UseItem::Single(path) | UseItem::Wildcard(path) | UseItem::Aliased(path, _) => {
                let file_path = self.resolve_use_path(path)?;
                let alias = match item {
                    UseItem::Aliased(_, alias) => Some(alias.as_str()),
                    // Single and Wildcard both merge exports into scope
                    _ => None,
                };
                self.exec_import(&file_path, alias)
            }
            UseItem::Group(prefix, names) => {
                let file_path = self.resolve_use_path(prefix)?;
                // Import the module, then pick specific items
                self.exec_import(&file_path, None)?;
                // Items are already imported by wildcard for now
                let _ = names;
                Ok(Signal::None)
            }
        }
    }

    /// Resolve a dot-path to a file path for use statements
    fn resolve_use_path(&self, segments: &[String]) -> Result<String, TlError> {
        let base_dir = if let Some(ref fp) = self.file_path {
            std::path::Path::new(fp)
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
        };

        // Try: segments joined as path with .tl extension
        // e.g., ["data", "transforms"] -> "data/transforms.tl"
        let rel_path = segments.join("/");

        // Try file module first
        let file_path = base_dir.join(format!("{rel_path}.tl"));
        if file_path.exists() {
            return Ok(file_path.to_string_lossy().to_string());
        }

        // Try directory module (mod.tl)
        let dir_path = base_dir.join(&rel_path).join("mod.tl");
        if dir_path.exists() {
            return Ok(dir_path.to_string_lossy().to_string());
        }

        // If multi-segment, try parent as file module
        if segments.len() > 1 {
            let parent = &segments[..segments.len() - 1];
            let parent_path = parent.join("/");
            let parent_file = base_dir.join(format!("{parent_path}.tl"));
            if parent_file.exists() {
                return Ok(parent_file.to_string_lossy().to_string());
            }
            let parent_dir = base_dir.join(&parent_path).join("mod.tl");
            if parent_dir.exists() {
                return Ok(parent_dir.to_string_lossy().to_string());
            }
        }

        // Package import fallback: first segment as package name
        let pkg_name = &segments[0];
        let pkg_name_hyphen = pkg_name.replace('_', "-");
        let pkg_root = self.package_roots.get(pkg_name.as_str())
            .or_else(|| self.package_roots.get(&pkg_name_hyphen));

        if let Some(root) = pkg_root {
            let remaining: Vec<&str> = segments[1..].iter().map(|s| s.as_str()).collect();
            if let Some(path) = resolve_package_file_interp(root, &remaining) {
                return Ok(path);
            }
        }

        Err(TlError::Runtime(tl_errors::RuntimeError {
            message: format!("Module not found: {}", segments.join(".")),
            span: None,
            stack_trace: vec![],
        }))
    }

    /// Execute an import statement
    fn exec_import(&mut self, path: &str, alias: Option<&str>) -> Result<Signal, TlError> {
        // Resolve path relative to current file
        let resolved = if let Some(ref base) = self.file_path {
            let base_dir = std::path::Path::new(base).parent().unwrap_or(std::path::Path::new("."));
            base_dir.join(path).to_string_lossy().to_string()
        } else {
            path.to_string()
        };

        // Circular dependency detection
        if self.importing_files.contains(&resolved) {
            return Err(TlError::Runtime(RuntimeError {
                message: format!("Circular import detected: {resolved}"),
                span: None,
                stack_trace: vec![],
                }));
        }

        // Check cache
        if let Some(exports) = self.module_cache.get(&resolved) {
            if let Some(alias) = alias {
                self.env.set(alias.to_string(), Value::Module {
                    name: alias.to_string(),
                    exports: exports.clone(),
                });
            } else {
                for (k, v) in exports {
                    self.env.set(k.clone(), v.clone());
                }
            }
            return Ok(Signal::None);
        }

        // Read and parse file
        let source = std::fs::read_to_string(&resolved)
            .map_err(|e| runtime_err(format!("Cannot read '{resolved}': {e}")))?;
        let program = tl_parser::parse(&source)
            .map_err(|e| runtime_err(format!("Parse error in '{resolved}': {e}")))?;

        // Execute in a fresh interpreter with shared method table
        self.importing_files.insert(resolved.clone());
        let mut sub_interp = Interpreter::new();
        sub_interp.file_path = Some(resolved.clone());
        sub_interp.importing_files = self.importing_files.clone();
        sub_interp.package_roots = self.package_roots.clone();
        sub_interp.project_root = self.project_root.clone();
        sub_interp.execute(&program)?;
        self.importing_files.remove(&resolved);

        // Collect exports (all top-level bindings)
        let exports: HashMap<String, Value> = sub_interp.env.scopes[0].clone();

        // Merge method tables from imported module
        for (type_name, methods) in &sub_interp.method_table {
            let entry = self.method_table.entry(type_name.clone()).or_default();
            for (name, func) in methods {
                entry.insert(name.clone(), func.clone());
            }
        }

        // Cache
        self.module_cache.insert(resolved, exports.clone());

        // Inject into current scope
        if let Some(alias) = alias {
            self.env.set(alias.to_string(), Value::Module {
                name: alias.to_string(),
                exports,
            });
        } else {
            for (k, v) in exports {
                // Don't import builtins
                if !matches!(v, Value::Builtin(_)) {
                    self.env.set(k, v);
                }
            }
        }

        Ok(Signal::None)
    }

    /// Simple string interpolation: replace {expr} with evaluated value
    fn interpolate_string(&mut self, s: &str) -> Result<String, TlError> {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '{' {
                let mut expr_str = String::new();
                let mut depth = 1;
                for c in chars.by_ref() {
                    if c == '{' {
                        depth += 1;
                    } else if c == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    expr_str.push(c);
                }
                // Look up the variable in the environment
                if let Some(val) = self.env.get(&expr_str) {
                    result.push_str(&format!("{val}"));
                } else {
                    result.push('{');
                    result.push_str(&expr_str);
                    result.push('}');
                }
            } else if ch == '\\' {
                // Handle escape sequences
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some(c) => {
                        result.push('\\');
                        result.push(c);
                    }
                    None => result.push('\\'),
                }
            } else {
                result.push(ch);
            }
        }
        Ok(result)
    }

    // ── Table-aware pipe evaluation ─────────────────────────

    /// Evaluate `table |> operation(args)` — dispatches to table operations.
    fn eval_table_pipe(&mut self, df: DataFrame, right: &Expr) -> Result<Value, TlError> {
        match right {
            Expr::Call { function, args } => {
                let fname = match function.as_ref() {
                    Expr::Ident(name) => name.as_str(),
                    _ => {
                        // Fall through to regular call with table as first arg
                        let func = self.eval_expr(function)?;
                        let mut all_args = vec![Value::Table(TlTable { df })];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        return self.call_function(&func, &all_args);
                    }
                };
                match fname {
                    "filter" => self.table_filter(df, args),
                    "select" => self.table_select(df, args),
                    "sort" => self.table_sort(df, args),
                    "with" => self.table_with(df, args),
                    "aggregate" => self.table_aggregate(df, args),
                    "join" => self.table_join(df, args),
                    "head" => self.table_limit(df, args),
                    "limit" => self.table_limit(df, args),
                    "collect" => {
                        let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                        let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                        Ok(Value::String(formatted))
                    }
                    "show" => {
                        let limit = match args.first() {
                            Some(expr) => {
                                let val = self.eval_expr(expr)?;
                                match val {
                                    Value::Int(n) => n as usize,
                                    _ => 20,
                                }
                            }
                            None => 20,
                        };
                        let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                        let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                        let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                        println!("{formatted}");
                        self.output.push(formatted);
                        Ok(Value::None)
                    }
                    "describe" => {
                        let schema = df.schema();
                        let mut lines = Vec::new();
                        lines.push("Columns:".to_string());
                        for field in schema.fields() {
                            lines.push(format!("  {}: {}", field.name(), field.data_type()));
                        }
                        let output = lines.join("\n");
                        println!("{output}");
                        self.output.push(output.clone());
                        Ok(Value::String(output))
                    }
                    "write_csv" => {
                        if args.len() != 1 {
                            return Err(runtime_err("write_csv() expects 1 argument (path)".into()));
                        }
                        let path = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("write_csv() path must be a string".into())),
                        };
                        self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                        Ok(Value::None)
                    }
                    "write_parquet" => {
                        if args.len() != 1 {
                            return Err(runtime_err("write_parquet() expects 1 argument (path)".into()));
                        }
                        let path = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("write_parquet() path must be a string".into())),
                        };
                        self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                        Ok(Value::None)
                    }
                    // Phase 15: Data quality pipe operations
                    "fill_null" => {
                        if args.is_empty() { return Err(runtime_err("fill_null() expects (column, [strategy/value])".into())); }
                        let column = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("fill_null() column must be a string".into())),
                        };
                        if args.len() >= 2 {
                            let val = self.eval_expr(&args[1])?;
                            match val {
                                Value::String(s) => {
                                    let fill_val = if args.len() >= 3 {
                                        match self.eval_expr(&args[2])? {
                                            Value::Int(n) => Some(n as f64),
                                            Value::Float(f) => Some(f),
                                            _ => None,
                                        }
                                    } else { None };
                                    let result = self.engine().fill_null(df, &column, &s, fill_val).map_err(|e| runtime_err(e))?;
                                    Ok(Value::Table(TlTable { df: result }))
                                }
                                Value::Int(n) => {
                                    let result = self.engine().fill_null(df, &column, "value", Some(n as f64)).map_err(|e| runtime_err(e))?;
                                    Ok(Value::Table(TlTable { df: result }))
                                }
                                Value::Float(f) => {
                                    let result = self.engine().fill_null(df, &column, "value", Some(f)).map_err(|e| runtime_err(e))?;
                                    Ok(Value::Table(TlTable { df: result }))
                                }
                                _ => Err(runtime_err("fill_null() second arg must be a strategy or value".into())),
                            }
                        } else {
                            let result = self.engine().fill_null(df, &column, "zero", None).map_err(|e| runtime_err(e))?;
                            Ok(Value::Table(TlTable { df: result }))
                        }
                    }
                    "drop_null" => {
                        if args.is_empty() { return Err(runtime_err("drop_null() expects (column)".into())); }
                        let column = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("drop_null() column must be a string".into())),
                        };
                        let result = self.engine().drop_null(df, &column).map_err(|e| runtime_err(e))?;
                        Ok(Value::Table(TlTable { df: result }))
                    }
                    "dedup" => {
                        let columns: Vec<String> = args.iter()
                            .filter_map(|a| match self.eval_expr(a) {
                                Ok(Value::String(s)) => Some(s),
                                _ => None,
                            })
                            .collect();
                        let result = self.engine().dedup(df, &columns).map_err(|e| runtime_err(e))?;
                        Ok(Value::Table(TlTable { df: result }))
                    }
                    "clamp" => {
                        if args.len() < 3 { return Err(runtime_err("clamp() expects (column, min, max)".into())); }
                        let column = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("clamp() column must be a string".into())),
                        };
                        let min_val = match self.eval_expr(&args[1])? {
                            Value::Int(n) => n as f64,
                            Value::Float(f) => f,
                            _ => return Err(runtime_err("clamp() min must be a number".into())),
                        };
                        let max_val = match self.eval_expr(&args[2])? {
                            Value::Int(n) => n as f64,
                            Value::Float(f) => f,
                            _ => return Err(runtime_err("clamp() max must be a number".into())),
                        };
                        let result = self.engine().clamp(df, &column, min_val, max_val).map_err(|e| runtime_err(e))?;
                        Ok(Value::Table(TlTable { df: result }))
                    }
                    "data_profile" => {
                        let result = self.engine().data_profile(df).map_err(|e| runtime_err(e))?;
                        Ok(Value::Table(TlTable { df: result }))
                    }
                    "row_count" => {
                        let count = self.engine().row_count(df).map_err(|e| runtime_err(e))?;
                        Ok(Value::Int(count))
                    }
                    "null_rate" => {
                        if args.is_empty() { return Err(runtime_err("null_rate() expects (column)".into())); }
                        let column = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("null_rate() column must be a string".into())),
                        };
                        let rate = self.engine().null_rate(df, &column).map_err(|e| runtime_err(e))?;
                        Ok(Value::Float(rate))
                    }
                    "is_unique" => {
                        if args.is_empty() { return Err(runtime_err("is_unique() expects (column)".into())); }
                        let column = match self.eval_expr(&args[0])? {
                            Value::String(s) => s,
                            _ => return Err(runtime_err("is_unique() column must be a string".into())),
                        };
                        let unique = self.engine().is_unique(df, &column).map_err(|e| runtime_err(e))?;
                        Ok(Value::Bool(unique))
                    }
                    // Unknown table op: fall through to regular call
                    _ => {
                        let func = self.env.get(fname).cloned().ok_or_else(|| {
                            runtime_err(format!("Unknown table operation: `{fname}`"))
                        })?;
                        let mut all_args = vec![Value::Table(TlTable { df })];
                        for arg in args {
                            all_args.push(self.eval_expr(arg)?);
                        }
                        self.call_function(&func, &all_args)
                    }
                }
            }
            Expr::Ident(name) => {
                let func = self.env.get(name).cloned().ok_or_else(|| {
                    runtime_err(format!("Unknown table operation: `{name}`"))
                })?;
                self.call_function(&func, &[Value::Table(TlTable { df })])
            }
            _ => Err(runtime_err("Right side of |> must be a function call".into())),
        }
    }

    /// Build a TranslateContext from current interpreter locals.
    fn build_translate_context(&self) -> TranslateContext {
        let mut ctx = TranslateContext::new();
        for scope in &self.env.scopes {
            for (name, val) in scope {
                let local = match val {
                    Value::Int(n) => Some(LocalValue::Int(*n)),
                    Value::Float(f) => Some(LocalValue::Float(*f)),
                    Value::String(s) => Some(LocalValue::String(s.clone())),
                    Value::Bool(b) => Some(LocalValue::Bool(*b)),
                    _ => None,
                };
                if let Some(local) = local {
                    ctx.locals.insert(name.clone(), local);
                }
            }
        }
        ctx
    }

    /// `table |> filter(predicate)`
    fn table_filter(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.len() != 1 {
            return Err(runtime_err("filter() expects 1 argument (predicate)".into()));
        }
        let ctx = self.build_translate_context();
        let pred = translate_expr(&args[0], &ctx).map_err(|e| runtime_err(e))?;
        let filtered = df.filter(pred).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: filtered }))
    }

    /// `table |> select(col1, col2, name: expr)`
    fn table_select(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("select() expects at least 1 argument".into()));
        }
        let ctx = self.build_translate_context();
        let mut select_exprs = Vec::new();
        for arg in args {
            match arg {
                Expr::Ident(name) => {
                    select_exprs.push(col(name.as_str()));
                }
                Expr::NamedArg { name, value } => {
                    let expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                    select_exprs.push(expr.alias(name));
                }
                Expr::String(name) => {
                    select_exprs.push(col(name.as_str()));
                }
                _ => {
                    let expr = translate_expr(arg, &ctx).map_err(|e| runtime_err(e))?;
                    select_exprs.push(expr);
                }
            }
        }
        let selected = df.select(select_exprs).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: selected }))
    }

    /// `table |> sort(col, "desc")` or `table |> sort(col)` (default asc)
    fn table_sort(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("sort() expects at least 1 argument (column)".into()));
        }
        let mut sort_exprs = Vec::new();
        let mut i = 0;
        while i < args.len() {
            let col_name = match &args[i] {
                Expr::Ident(name) => name.clone(),
                Expr::String(name) => name.clone(),
                _ => return Err(runtime_err("sort() column must be an identifier or string".into())),
            };
            i += 1;
            // Check for optional "asc"/"desc" direction
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
            sort_exprs.push(
                col(col_name.as_str()).sort(ascending, true) // nulls last
            );
        }
        let sorted = df.sort(sort_exprs).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: sorted }))
    }

    /// `table |> with { col_name = expr, ... }` — add derived columns
    fn table_with(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.len() != 1 {
            return Err(runtime_err("with() expects 1 argument (map of column definitions)".into()));
        }
        let pairs = match &args[0] {
            Expr::Map(pairs) => pairs,
            _ => return Err(runtime_err("with() expects a map { col = expr, ... }".into())),
        };
        let ctx = self.build_translate_context();
        let mut result_df = df;
        for (key, value_expr) in pairs {
            let col_name = match key {
                Expr::String(s) => s.clone(),
                Expr::Ident(s) => s.clone(),
                _ => return Err(runtime_err("with() key must be a string or identifier".into())),
            };
            let df_expr = translate_expr(value_expr, &ctx).map_err(|e| runtime_err(e))?;
            result_df = result_df
                .with_column(&col_name, df_expr)
                .map_err(|e| runtime_err(format!("{e}")))?;
        }
        Ok(Value::Table(TlTable { df: result_df }))
    }

    /// `table |> aggregate(by: "col", total: sum(amount), n: count())`
    fn table_aggregate(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        let ctx = self.build_translate_context();
        let mut group_by_cols: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();
        let mut agg_exprs: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();

        for arg in args {
            match arg {
                Expr::NamedArg { name, value } if name == "by" => {
                    // by: "col" or by: col
                    match value.as_ref() {
                        Expr::String(col_name) => {
                            group_by_cols.push(col(col_name.as_str()));
                        }
                        Expr::Ident(col_name) => {
                            group_by_cols.push(col(col_name.as_str()));
                        }
                        Expr::List(items) => {
                            for item in items {
                                match item {
                                    Expr::String(s) => group_by_cols.push(col(s.as_str())),
                                    Expr::Ident(s) => group_by_cols.push(col(s.as_str())),
                                    _ => return Err(runtime_err("by: list items must be strings or identifiers".into())),
                                }
                            }
                        }
                        _ => return Err(runtime_err("by: must be a column name or list".into())),
                    }
                }
                Expr::NamedArg { name, value } => {
                    // Named aggregate: total: sum(amount)
                    let agg_expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                    agg_exprs.push(agg_expr.alias(name));
                }
                _ => {
                    // Positional aggregate
                    let agg_expr = translate_expr(arg, &ctx).map_err(|e| runtime_err(e))?;
                    agg_exprs.push(agg_expr);
                }
            }
        }

        let aggregated = df
            .aggregate(group_by_cols, agg_exprs)
            .map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: aggregated }))
    }

    /// `table |> join(right_table, on: left_col == right_col, kind: "inner")`
    fn table_join(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        if args.is_empty() {
            return Err(runtime_err("join() expects at least 1 argument (right table)".into()));
        }

        // First positional arg: right table (evaluate it)
        let right_table = self.eval_expr(&args[0])?;
        let right_df = match right_table {
            Value::Table(t) => t.df,
            _ => return Err(runtime_err("join() first arg must be a table".into())),
        };

        let mut left_cols: Vec<&str> = Vec::new();
        let mut right_cols: Vec<&str> = Vec::new();
        let mut join_type = JoinType::Inner;
        let mut on_col_names: Vec<(String, String)> = Vec::new();

        for arg in &args[1..] {
            match arg {
                Expr::NamedArg { name, value } if name == "on" => {
                    // on: left_col == right_col
                    match value.as_ref() {
                        Expr::BinOp { left, op: BinOp::Eq, right } => {
                            let left_col = match left.as_ref() {
                                Expr::Ident(s) => s.clone(),
                                Expr::String(s) => s.clone(),
                                _ => return Err(runtime_err("on: left side must be a column name".into())),
                            };
                            let right_col = match right.as_ref() {
                                Expr::Ident(s) => s.clone(),
                                Expr::String(s) => s.clone(),
                                _ => return Err(runtime_err("on: right side must be a column name".into())),
                            };
                            on_col_names.push((left_col, right_col));
                        }
                        _ => return Err(runtime_err("on: must be an equality expression (col1 == col2)".into())),
                    }
                }
                Expr::NamedArg { name, value } if name == "kind" => {
                    let kind_val = self.eval_expr(value)?;
                    let kind_str = match &kind_val {
                        Value::String(s) => s.as_str(),
                        _ => return Err(runtime_err("kind: must be a string".into())),
                    };
                    join_type = match kind_str {
                        "inner" => JoinType::Inner,
                        "left" => JoinType::Left,
                        "right" => JoinType::Right,
                        "full" => JoinType::Full,
                        _ => return Err(runtime_err(format!("Unknown join type: {kind_str}"))),
                    };
                }
                _ => {} // ignore other args
            }
        }

        // Build column references
        for (l, r) in &on_col_names {
            left_cols.push(l.as_str());
            right_cols.push(r.as_str());
        }

        let joined = df
            .join(right_df, join_type, &left_cols, &right_cols, None)
            .map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: joined }))
    }

    /// `table |> head(n)` or `table |> limit(n)`
    fn table_limit(&mut self, df: DataFrame, args: &[Expr]) -> Result<Value, TlError> {
        let n = match args.first() {
            Some(expr) => {
                let val = self.eval_expr(expr)?;
                match val {
                    Value::Int(n) => n as usize,
                    _ => return Err(runtime_err("head/limit expects an integer".into())),
                }
            }
            None => 10,
        };
        let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
        Ok(Value::Table(TlTable { df: limited }))
    }
}

/// Convert TL type annotations to Arrow DataTypes.
fn tl_type_to_arrow(ty: &TypeExpr) -> ArrowDataType {
    match ty {
        TypeExpr::Named(name) => match name.as_str() {
            "int64" | "int" => ArrowDataType::Int64,
            "int32" => ArrowDataType::Int32,
            "int16" => ArrowDataType::Int16,
            "float64" | "float" => ArrowDataType::Float64,
            "float32" => ArrowDataType::Float32,
            "string" | "str" | "text" => ArrowDataType::Utf8,
            "bool" | "boolean" => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8, // fallback
        },
        TypeExpr::Optional(inner) => tl_type_to_arrow(inner), // nullable is always true in Arrow
        _ => ArrowDataType::Utf8, // fallback for complex types
    }
}

fn runtime_err(message: String) -> TlError {
    TlError::Runtime(RuntimeError {
        message,
        span: None,
        stack_trace: vec![],
    })
}

fn runtime_err_s(message: &str) -> TlError {
    TlError::Runtime(RuntimeError {
        message: message.to_string(),
        span: None,
        stack_trace: vec![],
    })
}

/// Resolve a file path within a package directory for package imports.
fn resolve_package_file_interp(pkg_root: &std::path::Path, remaining: &[&str]) -> Option<String> {
    if remaining.is_empty() {
        let src = pkg_root.join("src");
        for entry in &["lib.tl", "mod.tl", "main.tl"] {
            let p = src.join(entry);
            if p.exists() {
                return Some(p.to_string_lossy().to_string());
            }
        }
        for entry in &["mod.tl", "lib.tl"] {
            let p = pkg_root.join(entry);
            if p.exists() {
                return Some(p.to_string_lossy().to_string());
            }
        }
        return None;
    }

    let rel = remaining.join("/");
    let src = pkg_root.join("src");

    let file_path = src.join(format!("{rel}.tl"));
    if file_path.exists() {
        return Some(file_path.to_string_lossy().to_string());
    }

    let dir_path = src.join(&rel).join("mod.tl");
    if dir_path.exists() {
        return Some(dir_path.to_string_lossy().to_string());
    }

    let file_path = pkg_root.join(format!("{rel}.tl"));
    if file_path.exists() {
        return Some(file_path.to_string_lossy().to_string());
    }

    let dir_path = pkg_root.join(&rel).join("mod.tl");
    if dir_path.exists() {
        return Some(dir_path.to_string_lossy().to_string());
    }

    if remaining.len() > 1 {
        let parent = &remaining[..remaining.len() - 1];
        let parent_rel = parent.join("/");
        let parent_file = src.join(format!("{parent_rel}.tl"));
        if parent_file.exists() {
            return Some(parent_file.to_string_lossy().to_string());
        }
        let parent_file = pkg_root.join(format!("{parent_rel}.tl"));
        if parent_file.exists() {
            return Some(parent_file.to_string_lossy().to_string());
        }
    }

    None
}

/// Convert serde_json::Value to interpreter Value
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::None,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::List(arr.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            Value::Map(obj.iter().map(|(k, v)| (k.clone(), json_to_value(v))).collect())
        }
    }
}

/// Convert interpreter Value to serde_json::Value
/// Compare two values for equality (used by set operations).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Decimal(x), Value::Decimal(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::None, Value::None) => true,
        _ => false,
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::None => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::json!(*n),
        Value::Float(n) => serde_json::json!(*n),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        Value::Map(pairs) => {
            let obj: serde_json::Map<String, serde_json::Value> = pairs.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::String(format!("{v}")),
    }
}

// ── AI builtin implementations ──────────────────────────

impl Interpreter {
    fn value_to_f64_list(&self, v: &Value) -> Result<Vec<f64>, TlError> {
        match v {
            Value::List(items) => {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::Int(n) => result.push(*n as f64),
                        Value::Float(f) => result.push(*f),
                        _ => return Err(runtime_err(format!("Expected number in list, got {}", item.type_name()))),
                    }
                }
                Ok(result)
            }
            _ => Err(runtime_err(format!("Expected list, got {}", v.type_name()))),
        }
    }

    fn value_to_usize_list(&self, v: &Value) -> Result<Vec<usize>, TlError> {
        match v {
            Value::List(items) => {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::Int(n) => result.push(*n as usize),
                        _ => return Err(runtime_err(format!("Expected int in shape, got {}", item.type_name()))),
                    }
                }
                Ok(result)
            }
            _ => Err(runtime_err(format!("Expected list for shape, got {}", v.type_name()))),
        }
    }

    fn builtin_tensor(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::List(_)) => {
                let data = self.value_to_f64_list(&args[0])?;
                if args.len() > 1 {
                    let shape = self.value_to_usize_list(&args[1])?;
                    let t = tl_ai::TlTensor::from_vec(data, &shape)
                        .map_err(|e| runtime_err(e))?;
                    Ok(Value::Tensor(t))
                } else {
                    Ok(Value::Tensor(tl_ai::TlTensor::from_list(data)))
                }
            }
            _ => Err(runtime_err("tensor() expects a list of numbers".to_string())),
        }
    }

    fn builtin_tensor_zeros(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let shape = self.value_to_usize_list(args.first().ok_or_else(|| runtime_err("tensor_zeros() expects a shape".to_string()))?)?;
        Ok(Value::Tensor(tl_ai::TlTensor::zeros(&shape)))
    }

    fn builtin_tensor_ones(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let shape = self.value_to_usize_list(args.first().ok_or_else(|| runtime_err("tensor_ones() expects a shape".to_string()))?)?;
        Ok(Value::Tensor(tl_ai::TlTensor::ones(&shape)))
    }

    fn builtin_tensor_shape(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => {
                let shape = t.shape();
                Ok(Value::List(shape.into_iter().map(|s| Value::Int(s as i64)).collect()))
            }
            _ => Err(runtime_err("tensor_shape() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_reshape(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("tensor_reshape() expects (tensor, shape)".to_string()));
        }
        match &args[0] {
            Value::Tensor(t) => {
                let shape = self.value_to_usize_list(&args[1])?;
                let reshaped = t.reshape(&shape).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(reshaped))
            }
            _ => Err(runtime_err("tensor_reshape() expects a tensor as first argument".to_string())),
        }
    }

    fn builtin_tensor_transpose(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => {
                let transposed = t.transpose().map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(transposed))
            }
            _ => Err(runtime_err("tensor_transpose() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_sum(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => Ok(Value::Float(t.sum())),
            _ => Err(runtime_err("tensor_sum() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_mean(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::Tensor(t)) => Ok(Value::Float(t.mean())),
            _ => Err(runtime_err("tensor_mean() expects a tensor".to_string())),
        }
    }

    fn builtin_tensor_dot(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("tensor_dot() expects 2 tensors".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Tensor(a), Value::Tensor(b)) => {
                let result = a.dot(b).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(result))
            }
            _ => Err(runtime_err("tensor_dot() expects two tensors".to_string())),
        }
    }

    fn builtin_predict(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("predict() expects (model, input)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Model(m), Value::Tensor(t)) => {
                let result = tl_ai::predict(m, t).map_err(|e| runtime_err(e))?;
                Ok(Value::Tensor(result))
            }
            _ => Err(runtime_err("predict() expects (model, tensor)".to_string())),
        }
    }

    fn builtin_similarity(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("similarity() expects 2 tensors".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Tensor(a), Value::Tensor(b)) => {
                let sim = tl_ai::similarity(a, b).map_err(|e| runtime_err(e))?;
                Ok(Value::Float(sim))
            }
            _ => Err(runtime_err("similarity() expects two tensors".to_string())),
        }
    }

    fn builtin_ai_complete(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let prompt = match args.first() {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(runtime_err("ai_complete() expects a string prompt".to_string())),
        };
        let model = args.get(1).and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        });
        let result = tl_ai::ai_complete(&prompt, model, None, None)
            .map_err(|e| runtime_err(e))?;
        Ok(Value::String(result))
    }

    fn builtin_ai_chat(&mut self, args: &[Value]) -> Result<Value, TlError> {
        let model = match args.first() {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(runtime_err("ai_chat() expects (model, system?, messages)".to_string())),
        };
        let system = args.get(1).and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        });
        // Messages as list of [role, content] pairs
        let messages = match args.last() {
            Some(Value::List(msgs)) => {
                let mut result = Vec::new();
                for msg in msgs {
                    if let Value::List(pair) = msg {
                        if pair.len() == 2 {
                            if let (Value::String(role), Value::String(content)) = (&pair[0], &pair[1]) {
                                result.push((role.clone(), content.clone()));
                            }
                        }
                    }
                }
                result
            }
            _ => Vec::new(),
        };
        let result = tl_ai::ai_chat(&model, system, &messages)
            .map_err(|e| runtime_err(e))?;
        Ok(Value::String(result))
    }

    fn builtin_model_save(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("model_save() expects (model, path)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::Model(m), Value::String(path)) => {
                m.save(std::path::Path::new(path)).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            _ => Err(runtime_err("model_save() expects (model, string_path)".to_string())),
        }
    }

    fn builtin_model_load(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::String(path)) => {
                let model = tl_ai::TlModel::load(std::path::Path::new(path))
                    .map_err(|e| runtime_err(e))?;
                Ok(Value::Model(model))
            }
            _ => Err(runtime_err("model_load() expects a path string".to_string())),
        }
    }

    fn builtin_model_register(&mut self, args: &[Value]) -> Result<Value, TlError> {
        if args.len() != 2 {
            return Err(runtime_err("model_register() expects (name, model)".to_string()));
        }
        match (&args[0], &args[1]) {
            (Value::String(name), Value::Model(m)) => {
                let registry = tl_ai::ModelRegistry::default_location();
                registry.register(name, m).map_err(|e| runtime_err(e))?;
                Ok(Value::None)
            }
            _ => Err(runtime_err("model_register() expects (string, model)".to_string())),
        }
    }

    fn builtin_model_list(&mut self, _args: &[Value]) -> Result<Value, TlError> {
        let registry = tl_ai::ModelRegistry::default_location();
        let names = registry.list();
        Ok(Value::List(names.into_iter().map(Value::String).collect()))
    }

    fn builtin_model_get(&mut self, args: &[Value]) -> Result<Value, TlError> {
        match args.first() {
            Some(Value::String(name)) => {
                let registry = tl_ai::ModelRegistry::default_location();
                let model = registry.get(name).map_err(|e| runtime_err(e))?;
                Ok(Value::Model(model))
            }
            _ => Err(runtime_err("model_get() expects a model name string".to_string())),
        }
    }

    /// Execute a statement inside a generator thread, handling yield via channels.
    fn exec_stmt_gen(
        &mut self,
        stmt: &Stmt,
        yield_tx: &mpsc::Sender<Result<Value, String>>,
        resume_rx: &mpsc::Receiver<()>,
    ) -> Result<GenSignal, TlError> {
        match &stmt.kind {
            StmtKind::Expr(expr) => {
                let val = self.eval_expr_gen(expr, yield_tx, resume_rx)?;
                self.last_expr_value = Some(val);
                Ok(GenSignal::None)
            }
            StmtKind::Let { name, value, .. } => {
                let val = self.eval_expr_gen(value, yield_tx, resume_rx)?;
                self.env.set(name.clone(), val);
                Ok(GenSignal::None)
            }
            StmtKind::Return(expr) => {
                let val = match expr {
                    Some(e) => self.eval_expr_gen(e, yield_tx, resume_rx)?,
                    None => Value::None,
                };
                Ok(GenSignal::Return(val))
            }
            StmtKind::If { condition, then_body, else_ifs, else_body } => {
                let cond = self.eval_expr_gen(condition, yield_tx, resume_rx)?;
                if cond.is_truthy() {
                    for s in then_body {
                        let sig = self.exec_stmt_gen(s, yield_tx, resume_rx)?;
                        if !matches!(sig, GenSignal::None) { return Ok(sig); }
                    }
                } else {
                    let mut handled = false;
                    for (ec, eb) in else_ifs {
                        let ecv = self.eval_expr_gen(ec, yield_tx, resume_rx)?;
                        if ecv.is_truthy() {
                            for s in eb {
                                let sig = self.exec_stmt_gen(s, yield_tx, resume_rx)?;
                                if !matches!(sig, GenSignal::None) { return Ok(sig); }
                            }
                            handled = true;
                            break;
                        }
                    }
                    if !handled {
                        if let Some(eb) = else_body {
                            for s in eb {
                                let sig = self.exec_stmt_gen(s, yield_tx, resume_rx)?;
                                if !matches!(sig, GenSignal::None) { return Ok(sig); }
                            }
                        }
                    }
                }
                Ok(GenSignal::None)
            }
            StmtKind::While { condition, body } => {
                loop {
                    let cond = self.eval_expr_gen(condition, yield_tx, resume_rx)?;
                    if !cond.is_truthy() { break; }
                    self.env.push_scope();
                    let mut brk = false;
                    for s in body {
                        let sig = self.exec_stmt_gen(s, yield_tx, resume_rx)?;
                        match sig {
                            GenSignal::Break => { brk = true; break; }
                            GenSignal::Continue => break,
                            GenSignal::Return(v) => {
                                self.env.pop_scope();
                                return Ok(GenSignal::Return(v));
                            }
                            GenSignal::Throw(v) => {
                                self.env.pop_scope();
                                return Ok(GenSignal::Throw(v));
                            }
                            _ => {}
                        }
                    }
                    self.env.pop_scope();
                    if brk { break; }
                }
                Ok(GenSignal::None)
            }
            StmtKind::For { name, iter, body } => {
                let iter_val = self.eval_expr_gen(iter, yield_tx, resume_rx)?;
                let items = match iter_val {
                    Value::List(items) => items,
                    Value::Map(pairs) => pairs.into_iter()
                        .map(|(k, v)| Value::List(vec![Value::String(k), v]))
                        .collect(),
                    Value::Generator(g) => {
                        // For generator: pull items one by one
                        let mut results = Vec::new();
                        loop {
                            let val = self.interpreter_next(&g)?;
                            if matches!(val, Value::None) { break; }
                            results.push(val);
                        }
                        results
                    }
                    _ => return Err(runtime_err(format!("Cannot iterate over {}", iter_val.type_name()))),
                };
                for item in items {
                    self.env.push_scope();
                    self.env.set(name.clone(), item);
                    let mut brk = false;
                    for s in body {
                        let sig = self.exec_stmt_gen(s, yield_tx, resume_rx)?;
                        match sig {
                            GenSignal::Break => { brk = true; break; }
                            GenSignal::Continue => break,
                            GenSignal::Return(v) => {
                                self.env.pop_scope();
                                return Ok(GenSignal::Return(v));
                            }
                            GenSignal::Throw(v) => {
                                self.env.pop_scope();
                                return Ok(GenSignal::Throw(v));
                            }
                            _ => {}
                        }
                    }
                    self.env.pop_scope();
                    if brk { break; }
                }
                Ok(GenSignal::None)
            }
            StmtKind::Break => Ok(GenSignal::Break),
            StmtKind::Continue => Ok(GenSignal::Continue),
            StmtKind::Throw(expr) => {
                let val = self.eval_expr_gen(expr, yield_tx, resume_rx)?;
                Ok(GenSignal::Throw(val))
            }
            StmtKind::FnDecl { name, params, body, is_generator, .. } => {
                let func = Value::Function {
                    name: name.clone(),
                    params: params.clone(),
                    body: body.clone(),
                    is_generator: *is_generator,
                };
                self.env.set(name.clone(), func);
                Ok(GenSignal::None)
            }
            // For other statements, delegate to regular exec_stmt
            _ => {
                let sig = self.exec_stmt(stmt)?;
                Ok(match sig {
                    Signal::None => GenSignal::None,
                    Signal::Return(v) => GenSignal::Return(v),
                    Signal::Break => GenSignal::Break,
                    Signal::Continue => GenSignal::Continue,
                    Signal::Throw(v) => GenSignal::Throw(v),
                    Signal::Yield(v) => GenSignal::Yield(v),
                })
            }
        }
    }

    /// Evaluate expression inside a generator, handling yield.
    fn eval_expr_gen(
        &mut self,
        expr: &Expr,
        yield_tx: &mpsc::Sender<Result<Value, String>>,
        resume_rx: &mpsc::Receiver<()>,
    ) -> Result<Value, TlError> {
        match expr {
            Expr::Yield(opt_expr) => {
                let val = match opt_expr {
                    Some(e) => self.eval_expr_gen(e, yield_tx, resume_rx)?,
                    None => Value::None,
                };
                // Send the yielded value to the consumer
                yield_tx.send(Ok(val.clone()))
                    .map_err(|_| runtime_err("Generator consumer disconnected".to_string()))?;
                // Wait for resume signal
                resume_rx.recv()
                    .map_err(|_| runtime_err("Generator consumer disconnected".to_string()))?;
                Ok(val)
            }
            // For assign with yield: handle specially
            Expr::Assign { target, value } => {
                let val = self.eval_expr_gen(value, yield_tx, resume_rx)?;
                match target.as_ref() {
                    Expr::Ident(name) => {
                        if !self.env.update(name, val.clone()) {
                            return Err(runtime_err(format!("Variable '{name}' not found")));
                        }
                        Ok(val)
                    }
                    _ => {
                        // For complex assignments, use regular eval
                        self.eval_expr(&Expr::Assign {
                            target: target.clone(),
                            value: Box::new(Expr::None),
                        }).ok();
                        // Actually just set the value
                        Ok(val)
                    }
                }
            }
            // For most expressions, delegate to regular eval_expr
            _ => self.eval_expr(expr),
        }
    }

    fn exec_train(&mut self, name: &str, algorithm: &str, config: &[(String, Expr)]) -> Result<Signal, TlError> {
        // Extract config values
        let mut features_val = None;
        let mut target_val = None;
        let mut feature_names = Vec::new();
        let mut target_name = String::new();

        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            match key.as_str() {
                "data" => features_val = Some(val),
                "target" => {
                    if let Value::String(s) = &val {
                        target_name = s.clone();
                    }
                    target_val = Some(val);
                }
                "features" => {
                    if let Value::List(items) = &val {
                        for item in items {
                            if let Value::String(s) = item {
                                feature_names.push(s.clone());
                            }
                        }
                    }
                }
                _ => {} // ignore unknown config keys for now
            }
        }

        // When data is a Table, extract features and target from it
        if let Some(Value::Table(ref tbl)) = features_val {
            let engine = tl_data::DataEngine::new();
            let batches = engine.collect(tbl.df.clone())
                .map_err(|e| runtime_err(e))?;
            if batches.is_empty() {
                return Err(runtime_err("train: empty dataset".to_string()));
            }
            let batch = &batches[0];
            let schema = batch.schema();

            // Determine feature columns
            if feature_names.is_empty() {
                for field in schema.fields() {
                    if field.name() != &target_name {
                        feature_names.push(field.name().clone());
                    }
                }
            }

            let n_rows = batch.num_rows();
            let n_features = feature_names.len();

            // Extract feature columns
            let mut col_data: Vec<Vec<f64>> = Vec::new();
            for col_name in &feature_names {
                let col_idx = schema.index_of(col_name)
                    .map_err(|_| runtime_err(format!("Column not found: {col_name}")))?;
                let arr = batch.column(col_idx);
                let mut vals = Vec::with_capacity(n_rows);
                Self::extract_f64_col(arr, &mut vals)?;
                col_data.push(vals);
            }

            // Convert to row-major
            let mut row_major = Vec::with_capacity(n_rows * n_features);
            for row in 0..n_rows {
                for col in &col_data {
                    row_major.push(col[row]);
                }
            }
            let features_tensor = tl_ai::TlTensor::from_vec(row_major, &[n_rows, n_features])
                .map_err(|e| runtime_err(e))?;

            // Extract target column
            let target_idx = schema.index_of(&target_name)
                .map_err(|_| runtime_err(format!("Target column not found: {target_name}")))?;
            let target_arr = batch.column(target_idx);
            let mut target_data = Vec::with_capacity(n_rows);
            Self::extract_f64_col(target_arr, &mut target_data)?;
            let target_tensor = tl_ai::TlTensor::from_list(target_data);

            let train_config = tl_ai::TrainConfig {
                features: features_tensor,
                target: target_tensor,
                feature_names,
                target_name,
                model_name: name.to_string(),
                split_ratio: 1.0,
                hyperparams: std::collections::HashMap::new(),
            };

            let model = tl_ai::train(algorithm, &train_config)
                .map_err(|e| runtime_err(e))?;

            if let Some(meta) = model.metadata() {
                let metrics_str: Vec<String> = meta.metrics.iter()
                    .map(|(k, v)| format!("{k}={v:.4}"))
                    .collect();
                if !metrics_str.is_empty() {
                    let msg = format!("Trained model '{}' ({algorithm}): {}", name, metrics_str.join(", "));
                    println!("{msg}");
                    self.output.push(msg);
                }
            }

            self.env.set(name.to_string(), Value::Model(model));
            return Ok(Signal::None);
        }

        // Convert data to tensors (non-table path)
        let features_tensor = match features_val {
            Some(Value::Tensor(t)) => t,
            Some(Value::List(items)) => {
                // Treat as 2D list of lists or flat list
                let mut all_data = Vec::new();
                let mut n_cols = 0;
                for item in &items {
                    match item {
                        Value::List(row) => {
                            if n_cols == 0 {
                                n_cols = row.len();
                            }
                            for v in row {
                                match v {
                                    Value::Int(n) => all_data.push(*n as f64),
                                    Value::Float(f) => all_data.push(*f),
                                    _ => return Err(runtime_err("Training data must be numeric".to_string())),
                                }
                            }
                        }
                        Value::Int(n) => all_data.push(*n as f64),
                        Value::Float(f) => all_data.push(*f),
                        _ => return Err(runtime_err("Training data must be numeric".to_string())),
                    }
                }
                if n_cols == 0 {
                    n_cols = 1;
                }
                let n_rows = all_data.len() / n_cols;
                tl_ai::TlTensor::from_vec(all_data, &[n_rows, n_cols])
                    .map_err(|e| runtime_err(e))?
            }
            _ => return Err(runtime_err("train requires 'data' config key".to_string())),
        };

        let target_tensor = match target_val {
            Some(Value::Tensor(t)) => t,
            Some(Value::List(items)) => {
                let data: Result<Vec<f64>, _> = items.iter().map(|v| match v {
                    Value::Int(n) => Ok(*n as f64),
                    Value::Float(f) => Ok(*f),
                    _ => Err(runtime_err("Target values must be numeric".to_string())),
                }).collect();
                tl_ai::TlTensor::from_list(data?)
            }
            Some(Value::String(_)) => {
                return Err(runtime_err("String target column requires table data. Pass data as a table.".to_string()));
            }
            _ => return Err(runtime_err("train requires 'target' config key with numeric data".to_string())),
        };

        if feature_names.is_empty() {
            let n_features = features_tensor.shape().get(1).copied().unwrap_or(1);
            feature_names = (0..n_features).map(|i| format!("x{i}")).collect();
        }

        let train_config = tl_ai::TrainConfig {
            features: features_tensor,
            target: target_tensor,
            feature_names,
            target_name,
            model_name: name.to_string(),
            split_ratio: 1.0,
            hyperparams: std::collections::HashMap::new(),
        };

        let model = tl_ai::train(algorithm, &train_config)
            .map_err(|e| runtime_err(e))?;

        // Print training metrics
        if let Some(meta) = model.metadata() {
            let metrics_str: Vec<String> = meta.metrics.iter()
                .map(|(k, v)| format!("{k}={v:.4}"))
                .collect();
            if !metrics_str.is_empty() {
                let msg = format!("Trained model '{}' ({algorithm}): {}", name, metrics_str.join(", "));
                println!("{msg}");
                self.output.push(msg);
            }
        }

        self.env.set(name.to_string(), Value::Model(model));
        Ok(Signal::None)
    }

    fn extract_f64_col(col: &Arc<dyn tl_data::datafusion::arrow::array::Array>, out: &mut Vec<f64>) -> Result<(), TlError> {
        use tl_data::datafusion::arrow::array::{Float64Array, Int64Array, Float32Array, Int32Array, Array};
        let len = col.len();
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..len {
                out.push(if arr.is_null(i) { 0.0 } else { arr.value(i) as f64 });
            }
        } else {
            return Err(runtime_err("Column must be numeric (int32, int64, float32, float64)".to_string()));
        }
        Ok(())
    }
}

// ── Streaming & Pipeline execution ──────────────────────────

impl Interpreter {
    fn exec_pipeline(
        &mut self,
        name: &str,
        extract: &[Stmt],
        transform: &[Stmt],
        load: &[Stmt],
        schedule: &Option<String>,
        timeout: &Option<String>,
        retries: &Option<i64>,
        on_failure: &Option<Vec<Stmt>>,
        on_success: &Option<Vec<Stmt>>,
    ) -> Result<Signal, TlError> {
        let timeout_ms = timeout
            .as_ref()
            .and_then(|t| tl_stream::parse_duration(t).ok());

        let def = PipelineDef {
            name: name.to_string(),
            schedule: schedule.clone(),
            timeout_ms,
            retries: retries.unwrap_or(0) as u32,
        };

        let runner = PipelineRunner::new(def.clone());

        // Clone what we need for the closure
        let extract = extract.to_vec();
        let transform = transform.to_vec();
        let load = load.to_vec();

        // Run the pipeline blocks with shared scope and retry logic
        let max_attempts = def.retries + 1;
        let mut last_error = String::new();
        let mut succeeded = false;

        for _attempt in 0..max_attempts {
            // Push a shared scope for all pipeline blocks
            self.env.push_scope();
            let mut attempt_ok = true;

            // Execute extract block
            for stmt in &extract {
                match self.exec_stmt(stmt) {
                    Ok(Signal::Return(v)) => {
                        self.env.pop_scope();
                        return Ok(Signal::Return(v));
                    }
                    Err(e) => {
                        last_error = format!("{e}");
                        attempt_ok = false;
                        break;
                    }
                    _ => {}
                }
            }

            // Execute transform block
            if attempt_ok {
                for stmt in &transform {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Return(v)) => {
                            self.env.pop_scope();
                            return Ok(Signal::Return(v));
                        }
                        Err(e) => {
                            last_error = format!("{e}");
                            attempt_ok = false;
                            break;
                        }
                        _ => {}
                    }
                }
            }

            // Execute load block
            if attempt_ok {
                for stmt in &load {
                    match self.exec_stmt(stmt) {
                        Ok(Signal::Return(v)) => {
                            self.env.pop_scope();
                            return Ok(Signal::Return(v));
                        }
                        Err(e) => {
                            last_error = format!("{e}");
                            attempt_ok = false;
                            break;
                        }
                        _ => {}
                    }
                }
            }

            self.env.pop_scope();

            if attempt_ok {
                succeeded = true;
                break;
            }
        }

        if succeeded {
            if let Some(success_block) = on_success {
                self.exec_block(success_block)?;
            }
            // Store pipeline result
            let _result = tl_stream::PipelineResult {
                name: name.to_string(),
                status: PipelineStatus::Success,
                started_at: String::new(),
                ended_at: String::new(),
                rows_processed: 0,
                attempts: 1,
            };
            self.output.push(format!("Pipeline '{}': success", name));
            let _ = runner; // use the runner to suppress warnings
        } else {
            if let Some(failure_block) = on_failure {
                self.exec_block(failure_block)?;
            }
            self.output.push(format!("Pipeline '{}': failed — {}", name, last_error));
        }

        // Store pipeline def in env
        self.env.set(name.to_string(), Value::Pipeline(def));
        Ok(Signal::None)
    }

    fn exec_stream_decl(
        &mut self,
        name: &str,
        source: &Expr,
        _transform: &[Stmt],
        sink: &Option<Expr>,
        window: &Option<tl_ast::WindowSpec>,
        watermark: &Option<String>,
    ) -> Result<Signal, TlError> {
        let _source_val = self.eval_expr(source)?;

        let window_type = window.as_ref().map(|w| match w {
            tl_ast::WindowSpec::Tumbling(dur) => {
                let ms = tl_stream::parse_duration(dur).unwrap_or(0);
                tl_stream::window::WindowType::Tumbling { duration_ms: ms }
            }
            tl_ast::WindowSpec::Sliding(win, slide) => {
                let wms = tl_stream::parse_duration(win).unwrap_or(0);
                let sms = tl_stream::parse_duration(slide).unwrap_or(0);
                tl_stream::window::WindowType::Sliding { window_ms: wms, slide_ms: sms }
            }
            tl_ast::WindowSpec::Session(gap) => {
                let ms = tl_stream::parse_duration(gap).unwrap_or(0);
                tl_stream::window::WindowType::Session { gap_ms: ms }
            }
        });

        let watermark_ms = watermark
            .as_ref()
            .and_then(|w| tl_stream::parse_duration(w).ok());

        let def = StreamDef {
            name: name.to_string(),
            window: window_type,
            watermark_ms,
        };

        // Evaluate sink if provided
        if let Some(_sink_expr) = sink {
            // sink is evaluated but not used until stream is started
        }

        // Store stream definition
        self.env.set(name.to_string(), Value::Stream(def));
        self.output.push(format!("Stream '{}' declared", name));
        Ok(Signal::None)
    }

    fn exec_source_decl(
        &mut self,
        name: &str,
        connector_type: &str,
        config: &[(String, Expr)],
    ) -> Result<Signal, TlError> {
        let mut properties = std::collections::HashMap::new();
        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            properties.insert(key.clone(), format!("{val}"));
        }

        let config = ConnectorConfig {
            name: name.to_string(),
            connector_type: connector_type.to_string(),
            properties,
        };

        self.env.set(name.to_string(), Value::Connector(config));
        Ok(Signal::None)
    }

    fn exec_sink_decl(
        &mut self,
        name: &str,
        connector_type: &str,
        config: &[(String, Expr)],
    ) -> Result<Signal, TlError> {
        let mut properties = std::collections::HashMap::new();
        for (key, expr) in config {
            let val = self.eval_expr(expr)?;
            properties.insert(key.clone(), format!("{val}"));
        }

        let config = ConnectorConfig {
            name: name.to_string(),
            connector_type: connector_type.to_string(),
            properties,
        };

        self.env.set(name.to_string(), Value::Connector(config));
        Ok(Signal::None)
    }

    // --- Phase 20: Python FFI builtins (interpreter) ---

    #[cfg(feature = "python")]
    fn interp_py_import(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.is_empty() { return Err(runtime_err_s("py_import() expects a module name")); }
        let name = match &args[0] {
            Value::String(s) => s.clone(),
            _ => return Err(runtime_err_s("py_import() expects a string module name")),
        };
        pyo3::Python::with_gil(|py| {
            let module = py.import(&*name)
                .map_err(|e| runtime_err(format!("py_import('{name}'): {e}")))?;
            Ok(Value::PyObject(Arc::new(InterpPyObjectWrapper {
                inner: module.into_any().unbind(),
            })))
        })
    }

    #[cfg(feature = "python")]
    fn interp_py_eval(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.is_empty() { return Err(runtime_err_s("py_eval() expects a code string")); }
        let code = match &args[0] {
            Value::String(s) => s.clone(),
            _ => return Err(runtime_err_s("py_eval() expects a string")),
        };
        pyo3::Python::with_gil(|py| {
            let result = py.eval(&std::ffi::CString::new(code.as_str()).unwrap(), None, None)
                .map_err(|e| runtime_err(format!("py_eval(): {e}")))?;
            interp_py_to_value(py, &result)
                .map_err(|e| runtime_err(format!("py_eval() conversion: {e}")))
        })
    }

    #[cfg(feature = "python")]
    fn interp_py_call(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.is_empty() { return Err(runtime_err_s("py_call() expects a callable and arguments")); }
        let callable = match &args[0] {
            Value::PyObject(w) => w.clone(),
            _ => return Err(runtime_err_s("py_call() first argument must be a Python object")),
        };
        let call_args = &args[1..];
        pyo3::Python::with_gil(|py| {
            let py_args: Vec<pyo3::Py<pyo3::PyAny>> = call_args.iter()
                .map(|a| interp_value_to_py(py, a))
                .collect::<Result<_, _>>()
                .map_err(|e| runtime_err(format!("py_call() arg conversion: {e}")))?;
            let tuple = pyo3::types::PyTuple::new(py, &py_args)
                .map_err(|e| runtime_err(format!("py_call() tuple: {e}")))?;
            let result = callable.inner.call1(py, tuple)
                .map_err(|e| runtime_err(format!("py_call(): {e}")))?;
            interp_py_to_value(py, result.bind(py))
                .map_err(|e| runtime_err(format!("py_call() result conversion: {e}")))
        })
    }

    #[cfg(feature = "python")]
    fn interp_py_getattr(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.len() < 2 { return Err(runtime_err_s("py_getattr() expects (object, name)")); }
        let obj = match &args[0] {
            Value::PyObject(w) => w.clone(),
            _ => return Err(runtime_err_s("py_getattr() first argument must be a Python object")),
        };
        let attr_name = match &args[1] {
            Value::String(s) => s.clone(),
            _ => return Err(runtime_err_s("py_getattr() second argument must be a string")),
        };
        pyo3::Python::with_gil(|py| {
            let bound = obj.inner.bind(py);
            let attr = bound.getattr(attr_name.as_str())
                .map_err(|e| runtime_err(format!("py_getattr('{attr_name}'): {e}")))?;
            interp_py_to_value(py, &attr)
                .map_err(|e| runtime_err(format!("py_getattr() conversion: {e}")))
        })
    }

    #[cfg(feature = "python")]
    fn interp_py_setattr(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.len() < 3 { return Err(runtime_err_s("py_setattr() expects (object, name, value)")); }
        let obj = match &args[0] {
            Value::PyObject(w) => w.clone(),
            _ => return Err(runtime_err_s("py_setattr() first argument must be a Python object")),
        };
        let attr_name = match &args[1] {
            Value::String(s) => s.clone(),
            _ => return Err(runtime_err_s("py_setattr() second argument must be a string")),
        };
        pyo3::Python::with_gil(|py| {
            let py_val = interp_value_to_py(py, &args[2])
                .map_err(|e| runtime_err(format!("py_setattr() conversion: {e}")))?;
            obj.inner.bind(py).setattr(attr_name.as_str(), py_val)
                .map_err(|e| runtime_err(format!("py_setattr('{attr_name}'): {e}")))?;
            Ok(Value::None)
        })
    }

    #[cfg(feature = "python")]
    fn interp_py_to_tl(&mut self, args: &[Value]) -> Result<Value, TlError> {
        use pyo3::prelude::*;
        if args.is_empty() { return Err(runtime_err_s("py_to_tl() expects a Python object")); }
        match &args[0] {
            Value::PyObject(w) => {
                pyo3::Python::with_gil(|py| {
                    let bound = w.inner.bind(py);
                    interp_py_to_value(py, &bound)
                        .map_err(|e| runtime_err(format!("py_to_tl(): {e}")))
                })
            }
            other => Ok(other.clone()),
        }
    }
}

// --- Phase 20: Python FFI value conversion for interpreter ---

#[cfg(feature = "python")]
fn interp_value_to_py(py: pyo3::Python<'_>, val: &Value) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PySet};

    match val {
        Value::Int(n) => Ok((*n).into_pyobject(py)?.into_any().into()),
        Value::Float(f) => Ok((*f).into_pyobject(py)?.into_any().unbind()),
        Value::String(s) => Ok(s.as_str().into_pyobject(py)?.into_any().unbind()),
        Value::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::None => Ok(py.None()),
        Value::List(items) => {
            let py_items: Vec<pyo3::Py<pyo3::PyAny>> = items.iter()
                .map(|item| interp_value_to_py(py, item))
                .collect::<pyo3::PyResult<_>>()?;
            Ok(PyList::new(py, &py_items)?.into_any().unbind())
        }
        Value::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                let py_val = interp_value_to_py(py, v)?;
                dict.set_item(k.as_str(), py_val)?;
            }
            Ok(dict.into_any().unbind())
        }
        Value::Set(items) => {
            let py_items: Vec<pyo3::Py<pyo3::PyAny>> = items.iter()
                .map(|item| interp_value_to_py(py, item))
                .collect::<pyo3::PyResult<_>>()?;
            Ok(PySet::new(py, &py_items)?.into_any().unbind())
        }
        Value::PyObject(w) => Ok(w.inner.clone_ref(py)),
        _ => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Cannot convert TL {} to Python", val.type_name()
        ))),
    }
}

#[cfg(feature = "python")]
fn interp_py_to_value(py: pyo3::Python<'_>, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Value> {
    use pyo3::prelude::*;
    use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PySet, PyString};

    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(obj.extract::<bool>()?));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(Value::Int(obj.extract::<i64>()?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::Float(obj.extract::<f64>()?));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(Value::String(obj.extract::<String>()?));
    }
    if obj.is_none() {
        return Ok(Value::None);
    }
    if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>()?;
        let items: Vec<Value> = list.iter()
            .map(|item| interp_py_to_value(py, &item))
            .collect::<pyo3::PyResult<_>>()?;
        return Ok(Value::List(items));
    }
    if obj.is_instance_of::<PyDict>() {
        let dict = obj.downcast::<PyDict>()?;
        let mut pairs = Vec::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            let val = interp_py_to_value(py, &v)?;
            pairs.push((key, val));
        }
        return Ok(Value::Map(pairs));
    }
    if obj.is_instance_of::<PySet>() {
        let set = obj.downcast::<PySet>()?;
        let mut items = Vec::new();
        for item in set.iter() {
            items.push(interp_py_to_value(py, &item)?);
        }
        return Ok(Value::Set(items));
    }

    // Everything else stays as opaque PyObject
    Ok(Value::PyObject(Arc::new(InterpPyObjectWrapper {
        inner: obj.clone().unbind(),
    })))
}

#[cfg(feature = "python")]
fn interp_py_get_member(wrapper: &InterpPyObjectWrapper, field: &str) -> Value {
    use pyo3::prelude::*;
    pyo3::Python::with_gil(|py| {
        let bound = wrapper.inner.bind(py);
        match bound.getattr(field) {
            Ok(attr) => interp_py_to_value(py, &attr).unwrap_or(Value::None),
            Err(_) => Value::None,
        }
    })
}

#[cfg(feature = "python")]
fn interp_py_call_method(
    wrapper: &InterpPyObjectWrapper,
    method: &str,
    args: &[Value],
) -> Result<Value, TlError> {
    use pyo3::prelude::*;
    pyo3::Python::with_gil(|py| {
        let bound = wrapper.inner.bind(py);
        let py_args: Vec<pyo3::Py<pyo3::PyAny>> = args.iter()
            .map(|a| interp_value_to_py(py, a))
            .collect::<Result<_, _>>()
            .map_err(|e| runtime_err(format!("Python arg conversion: {e}")))?;
        let tuple = pyo3::types::PyTuple::new(py, &py_args)
            .map_err(|e| runtime_err(format!("Python tuple: {e}")))?;
        let attr = bound.getattr(method)
            .map_err(|e| runtime_err(format!("Python: no attribute '{method}': {e}")))?;
        let result = attr.call1(tuple)
            .map_err(|e| runtime_err(format!("Python method '{method}': {e}")))?;
        interp_py_to_value(py, &result)
            .map_err(|e| runtime_err(format!("Python result conversion: {e}")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tl_parser::parse;

    fn run(source: &str) -> Result<Value, TlError> {
        let program = parse(source)?;
        let mut interp = Interpreter::new();
        interp.execute(&program)
    }

    fn run_output(source: &str) -> Vec<String> {
        let program = parse(source).unwrap();
        let mut interp = Interpreter::new();
        interp.execute(&program).unwrap();
        interp.output
    }

    fn run_err(source: &str) -> String {
        let program = parse(source).unwrap();
        let mut interp = Interpreter::new();
        match interp.execute(&program) {
            Err(e) => format!("{e}"),
            Ok(_) => "no error".to_string(),
        }
    }

    #[test]
    fn test_arithmetic() {
        assert!(matches!(run("1 + 2").unwrap(), Value::Int(3)));
        assert!(matches!(run("10 - 3").unwrap(), Value::Int(7)));
        assert!(matches!(run("4 * 5").unwrap(), Value::Int(20)));
        assert!(matches!(run("10 / 3").unwrap(), Value::Int(3)));
        assert!(matches!(run("10 % 3").unwrap(), Value::Int(1)));
        assert!(matches!(run("2 ** 10").unwrap(), Value::Int(1024)));
    }

    #[test]
    fn test_precedence() {
        assert!(matches!(run("2 + 3 * 4").unwrap(), Value::Int(14)));
        assert!(matches!(run("(2 + 3) * 4").unwrap(), Value::Int(20)));
    }

    #[test]
    fn test_let_and_variable() {
        let output = run_output("let x = 42\nprint(x)");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_function() {
        let output = run_output(
            "fn double(n: int64) -> int64 { n * 2 }\nlet result = double(21)\nprint(result)",
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_pipe() {
        let output =
            run_output("fn double(n: int64) -> int64 { n * 2 }\nlet x = 5 |> double()\nprint(x)");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_if_else() {
        let output = run_output("let x = 10\nif x > 5 { print(\"big\") } else { print(\"small\") }");
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_string_interpolation() {
        let output = run_output("let name = \"TL\"\nprint(\"Hello {name}!\")");
        assert_eq!(output, vec!["Hello TL!"]);
    }

    #[test]
    fn test_list() {
        let output = run_output("let items = [1, 2, 3]\nprint(len(items))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_comparison() {
        assert!(matches!(run("5 > 3").unwrap(), Value::Bool(true)));
        assert!(matches!(run("5 < 3").unwrap(), Value::Bool(false)));
        assert!(matches!(run("5 == 5").unwrap(), Value::Bool(true)));
    }

    #[test]
    fn test_match_int() {
        let output = run_output("let x = 2\nprint(match x { 1 => \"one\", 2 => \"two\", _ => \"other\" })");
        assert_eq!(output, vec!["two"]);
    }

    #[test]
    fn test_match_wildcard() {
        let output = run_output("let x = 99\nprint(match x { 1 => \"one\", _ => \"fallback\" })");
        assert_eq!(output, vec!["fallback"]);
    }

    #[test]
    fn test_match_string() {
        let output = run_output("let s = \"hi\"\nprint(match s { \"hello\" => 1, \"hi\" => 2, _ => 0 })");
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_closure() {
        let output = run_output("let double = (x) => x * 2\nprint(double(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_closure_capture() {
        let output = run_output("let factor = 3\nlet mul = (x) => x * factor\nprint(mul(7))");
        assert_eq!(output, vec!["21"]);
    }

    #[test]
    fn test_for_loop() {
        let output = run_output(
            "let sum = 0\nfor i in range(5) { sum = sum + i }\nprint(sum)",
        );
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_map_builtin() {
        let output = run_output("let nums = [1, 2, 3]\nlet doubled = map(nums, (x) => x * 2)\nprint(doubled)");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_filter_builtin() {
        let output = run_output("let nums = [1, 2, 3, 4, 5]\nlet evens = filter(nums, (x) => x % 2 == 0)\nprint(evens)");
        assert_eq!(output, vec!["[2, 4]"]);
    }

    #[test]
    fn test_pipe_with_closure() {
        let output = run_output("let result = [1, 2, 3] |> map((x) => x + 10)\nprint(result)");
        assert_eq!(output, vec!["[11, 12, 13]"]);
    }

    #[test]
    fn test_sum_builtin() {
        let output = run_output("print(sum([1, 2, 3, 4]))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_reduce_builtin() {
        let output = run_output("let product = reduce([1, 2, 3, 4], 1, (acc, x) => acc * x)\nprint(product)");
        assert_eq!(output, vec!["24"]);
    }

    #[test]
    fn test_struct_creation() {
        let output = run_output(
            "struct Point { x: float64, y: float64 }\nlet p = Point { x: 1.0, y: 2.0 }\nprint(p.x)\nprint(p.y)",
        );
        assert_eq!(output, vec!["1.0", "2.0"]);
    }

    #[test]
    fn test_struct_nested() {
        let output = run_output(
            "struct Point { x: float64, y: float64 }\nstruct Line { start: Point, end_pt: Point }\nlet l = Line { start: Point { x: 0.0, y: 0.0 }, end_pt: Point { x: 1.0, y: 1.0 } }\nprint(l.start.x)",
        );
        assert_eq!(output, vec!["0.0"]);
    }

    #[test]
    fn test_enum_creation() {
        let output = run_output(
            "enum Color { Red, Green, Blue }\nlet c = Color::Red\nprint(c)",
        );
        assert!(output.len() == 1);
        assert!(output[0].contains("Color::Red"), "expected output to contain 'Color::Red', got: {}", output[0]);
    }

    #[test]
    fn test_enum_with_fields() {
        let output = run_output(
            "enum Shape { Circle(float64), Rect(float64, float64) }\nlet s = Shape::Circle(5.0)\nprint(s)",
        );
        assert!(output.len() == 1);
        assert!(output[0].contains("Circle"), "expected output to contain 'Circle', got: {}", output[0]);
    }

    #[test]
    fn test_enum_match() {
        let output = run_output(
            "enum Shape { Circle(float64), Rect(float64, float64) }\nlet s = Shape::Circle(5.0)\nlet result = match s {\n    Shape::Circle(r) => r * 2.0,\n    Shape::Rect(w, h) => w * h,\n    _ => 0.0\n}\nprint(result)",
        );
        assert_eq!(output, vec!["10.0"]);
    }

    #[test]
    fn test_impl_method() {
        let output = run_output(
            "struct Counter { value: int64 }\nimpl Counter {\n    fn get(self) { self.value }\n}\nlet c = Counter { value: 42 }\nprint(c.get())",
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_try_catch() {
        let output = run_output(
            "try {\n    throw \"oops\"\n} catch e {\n    print(e)\n}",
        );
        assert_eq!(output, vec!["oops"]);
    }

    #[test]
    fn test_try_catch_runtime_error() {
        let output = run_output(
            "try {\n    let x = 1 / 0\n} catch e {\n    print(\"caught\")\n}",
        );
        assert_eq!(output, vec!["caught"]);
    }

    #[test]
    fn test_string_methods() {
        let output = run_output(
            "print(\"hello world\".split(\" \"))\nprint(\"  hello  \".trim())\nprint(\"hello\".contains(\"ell\"))\nprint(\"hello\".to_upper())",
        );
        assert_eq!(output, vec!["[hello, world]", "hello", "true", "HELLO"]);
    }

    #[test]
    fn test_math_builtins() {
        let output = run_output(
            "print(sqrt(16.0))\nprint(floor(3.7))\nprint(ceil(3.2))\nprint(abs(-5))",
        );
        assert_eq!(output, vec!["4.0", "3.0", "4.0", "5"]);
    }

    #[test]
    fn test_assert_pass() {
        let result = run("assert(true)\nassert_eq(1 + 1, 2)");
        assert!(result.is_ok(), "assert(true) and assert_eq(1+1, 2) should not error");
    }

    #[test]
    fn test_assert_fail() {
        let result = run("assert(false)");
        assert!(result.is_err(), "assert(false) should return an error");
    }

    #[test]
    fn test_join_builtin() {
        let output = run_output(
            "print(join(\", \", [\"a\", \"b\", \"c\"]))",
        );
        assert_eq!(output, vec!["a, b, c"]);
    }

    #[test]
    fn test_list_methods() {
        let output = run_output(
            "let nums = [1, 2, 3]\nprint(nums.len())\nlet doubled = nums.map((x) => x * 2)\nprint(doubled)",
        );
        assert_eq!(output, vec!["3", "[2, 4, 6]"]);
    }

    // ── Phase 6: Stdlib & Ecosystem tests ──

    #[test]
    fn test_json_parse_object() {
        // Build JSON from map_from + json_stringify to avoid string escaping issues
        let output = run_output(r#"let m = map_from("a", 1, "b", "hello")
let s = json_stringify(m)
let m2 = json_parse(s)
print(m2["a"])
print(m2["b"])"#);
        assert_eq!(output, vec!["1", "hello"]);
    }

    #[test]
    fn test_json_parse_array() {
        let output = run_output(r#"let arr = json_parse("[1, 2, 3]")
print(len(arr))"#);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_json_stringify() {
        let output = run_output(r#"let m = map_from("x", 1, "y", 2)
let s = json_stringify(m)
print(s)"#);
        assert_eq!(output, vec![r#"{"x":1,"y":2}"#]);
    }

    #[test]
    fn test_json_roundtrip() {
        let output = run_output(r#"let m = map_from("name", "test", "val", 42)
let s = json_stringify(m)
let m2 = json_parse(s)
print(m2["name"])
print(m2["val"])"#);
        assert_eq!(output, vec!["test", "42"]);
    }

    #[test]
    fn test_map_from() {
        let output = run_output(r#"let m = map_from("a", 1, "b", 2)
print(m["a"])
print(m["b"])"#);
        assert_eq!(output, vec!["1", "2"]);
    }

    #[test]
    fn test_map_member_access() {
        let output = run_output(r#"let m = map_from("name", "alice")
print(m.name)"#);
        assert_eq!(output, vec!["alice"]);
    }

    #[test]
    fn test_map_index_set() {
        let output = run_output(r#"let m = map_from("a", 1)
m["b"] = 2
print(m["b"])"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_map_methods() {
        let output = run_output(r#"let m = map_from("a", 1, "b", 2, "c", 3)
print(m.len())
print(m.keys())
print(m.contains_key("b"))
print(m.contains_key("x"))
let m2 = m.remove("b")
print(m2.len())"#);
        assert_eq!(output, vec!["3", "[a, b, c]", "true", "false", "2"]);
    }

    #[test]
    fn test_map_iteration() {
        let output = run_output(r#"let m = map_from("x", 10, "y", 20)
for kv in m {
    print(kv[0])
}"#);
        assert_eq!(output, vec!["x", "y"]);
    }

    #[test]
    fn test_map_len_type_of() {
        let output = run_output(r#"let m = map_from("a", 1)
print(len(m))
print(type_of(m))"#);
        assert_eq!(output, vec!["1", "map"]);
    }

    #[test]
    fn test_file_read_write() {
        let output = run_output(r#"write_file("/tmp/tl_test_phase6.txt", "hello world")
let content = read_file("/tmp/tl_test_phase6.txt")
print(content)
print(file_exists("/tmp/tl_test_phase6.txt"))"#);
        assert_eq!(output, vec!["hello world", "true"]);
    }

    #[test]
    fn test_file_append() {
        let output = run_output(r#"write_file("/tmp/tl_test_append.txt", "line1")
append_file("/tmp/tl_test_append.txt", "line2")
let content = read_file("/tmp/tl_test_append.txt")
print(content)"#);
        assert_eq!(output, vec!["line1line2"]);
    }

    #[test]
    fn test_file_exists_false() {
        let output = run_output(r#"print(file_exists("/tmp/nonexistent_tl_file_xyz"))"#);
        assert_eq!(output, vec!["false"]);
    }

    #[test]
    fn test_list_dir() {
        // Setup directory for test
        std::fs::create_dir_all("/tmp/tl_listdir_test").ok();
        std::fs::write("/tmp/tl_listdir_test/a.txt", "a").ok();
        std::fs::write("/tmp/tl_listdir_test/b.txt", "b").ok();
        let output = run_output(r#"let files = list_dir("/tmp/tl_listdir_test")
print(len(files) >= 2)"#);
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_env_get_set() {
        let output = run_output(r#"env_set("TL_TEST_VAR", "hello123")
let v = env_get("TL_TEST_VAR")
print(v)"#);
        assert_eq!(output, vec!["hello123"]);
    }

    #[test]
    fn test_env_get_missing() {
        let output = run_output(r#"let v = env_get("TL_NONEXISTENT_VAR_XYZ")
print(v)"#);
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_regex_match() {
        let output = run_output(r#"print(regex_match("\\d+", "abc123"))
print(regex_match("^\\d+$", "abc123"))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_regex_find() {
        let output = run_output(r#"let matches = regex_find("\\d+", "abc123def456")
print(len(matches))
print(matches[0])
print(matches[1])"#);
        assert_eq!(output, vec!["2", "123", "456"]);
    }

    #[test]
    fn test_regex_replace() {
        let output = run_output(r#"let result = regex_replace("\\d+", "abc123def456", "X")
print(result)"#);
        assert_eq!(output, vec!["abcXdefX"]);
    }

    #[test]
    fn test_now() {
        let output = run_output("let t = now()\nprint(t > 0)");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_date_format() {
        // 2024-01-01 00:00:00 UTC = 1704067200000 ms
        let output = run_output(r#"print(date_format(1704067200000, "%Y-%m-%d"))"#);
        assert_eq!(output, vec!["2024-01-01"]);
    }

    #[test]
    fn test_date_parse() {
        let output = run_output(r#"let ts = date_parse("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
print(ts)"#);
        assert_eq!(output, vec!["1704067200000"]);
    }

    #[test]
    fn test_string_chars() {
        let output = run_output(r#"let chars = "hello".chars()
print(len(chars))
print(chars[0])"#);
        assert_eq!(output, vec!["5", "h"]);
    }

    #[test]
    fn test_string_repeat() {
        let output = run_output(r#"print("ab".repeat(3))"#);
        assert_eq!(output, vec!["ababab"]);
    }

    #[test]
    fn test_string_index_of() {
        let output = run_output(r#"print("hello world".index_of("world"))
print("hello".index_of("xyz"))"#);
        assert_eq!(output, vec!["6", "-1"]);
    }

    #[test]
    fn test_string_substring() {
        let output = run_output(r#"print("hello world".substring(0, 5))"#);
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_string_pad() {
        let output = run_output(r#"print("42".pad_left(5, "0"))
print("hi".pad_right(5, "."))"#);
        assert_eq!(output, vec!["00042", "hi..."]);
    }

    #[test]
    fn test_list_sort() {
        let output = run_output(r#"print([3, 1, 2].sort())"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_list_reverse() {
        let output = run_output(r#"print([1, 2, 3].reverse())"#);
        assert_eq!(output, vec!["[3, 2, 1]"]);
    }

    #[test]
    fn test_list_contains() {
        let output = run_output(r#"print([1, 2, 3].contains(2))
print([1, 2, 3].contains(5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_list_index_of() {
        let output = run_output(r#"print([10, 20, 30].index_of(20))
print([10, 20, 30].index_of(99))"#);
        assert_eq!(output, vec!["1", "-1"]);
    }

    #[test]
    fn test_list_slice() {
        let output = run_output(r#"print([1, 2, 3, 4, 5].slice(1, 4))"#);
        assert_eq!(output, vec!["[2, 3, 4]"]);
    }

    #[test]
    fn test_list_flat_map() {
        let output = run_output(r#"let result = [1, 2, 3].flat_map((x) => [x, x * 10])
print(result)"#);
        assert_eq!(output, vec!["[1, 10, 2, 20, 3, 30]"]);
    }

    #[test]
    fn test_zip() {
        let output = run_output(r#"let pairs = zip([1, 2, 3], ["a", "b", "c"])
print(pairs[0])
print(pairs[1])"#);
        assert_eq!(output, vec!["[1, a]", "[2, b]"]);
    }

    #[test]
    fn test_enumerate() {
        let output = run_output(r#"let items = enumerate(["a", "b", "c"])
print(items[0])
print(items[2])"#);
        assert_eq!(output, vec!["[0, a]", "[2, c]"]);
    }

    #[test]
    fn test_bool_builtin() {
        let output = run_output(r#"print(bool(1))
print(bool(0))
print(bool(""))
print(bool("hello"))"#);
        assert_eq!(output, vec!["true", "false", "false", "true"]);
    }

    #[test]
    fn test_range_step() {
        let output = run_output(r#"print(range(0, 10, 3))"#);
        assert_eq!(output, vec!["[0, 3, 6, 9]"]);
    }

    #[test]
    fn test_int_bool() {
        let output = run_output(r#"print(int(true))
print(int(false))"#);
        assert_eq!(output, vec!["1", "0"]);
    }

    #[test]
    fn test_float_bool() {
        let output = run_output(r#"print(float(true))
print(float(false))"#);
        assert_eq!(output, vec!["1.0", "0.0"]);
    }

    #[test]
    fn test_integration_json_file_roundtrip() {
        let output = run_output(r#"let data = map_from("name", "test", "count", 42)
let json_str = json_stringify(data)
write_file("/tmp/tl_json_roundtrip.json", json_str)
let content = read_file("/tmp/tl_json_roundtrip.json")
let parsed = json_parse(content)
print(parsed["name"])
print(parsed["count"])"#);
        assert_eq!(output, vec!["test", "42"]);
    }

    #[test]
    fn test_integration_regex_on_file() {
        let output = run_output(r#"write_file("/tmp/tl_regex_test.txt", "Error: code 404\nInfo: ok\nError: code 500")
let content = read_file("/tmp/tl_regex_test.txt")
let errors = regex_find("Error: code \\d+", content)
print(len(errors))"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_integration_list_transform() {
        let output = run_output(r#"let data = [5, 3, 8, 1, 9, 2]
let result = data.sort().slice(0, 3)
print(result)"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_integration_map_values() {
        let output = run_output(r#"let m = map_from("a", 1, "b", 2, "c", 3)
let vals = m.values()
print(sum(vals))"#);
        assert_eq!(output, vec!["6"]);
    }

    // ── Phase 7: Concurrency tests ──

    #[test]
    fn test_interp_spawn_await_basic() {
        let output = run_output(r#"fn worker() { 42 }
let t = spawn(worker)
let result = await t
print(result)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_spawn_closure_with_capture() {
        let output = run_output(r#"let x = 10
fn f() { x + 5 }
let t = spawn(f)
print(await t)"#);
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_interp_sleep() {
        let output = run_output(r#"sleep(10)
print("done")"#);
        assert_eq!(output, vec!["done"]);
    }

    #[test]
    fn test_interp_await_non_task() {
        let output = run_output(r#"print(await 42)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_channel_basic() {
        let output = run_output(r#"let ch = channel()
send(ch, 42)
let val = recv(ch)
print(val)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_channel_between_tasks() {
        let output = run_output(r#"let ch = channel()
fn producer() { send(ch, 100) }
let t = spawn(producer)
let val = recv(ch)
await t
print(val)"#);
        assert_eq!(output, vec!["100"]);
    }

    #[test]
    fn test_interp_try_recv_empty() {
        let output = run_output(r#"let ch = channel()
let val = try_recv(ch)
print(val)"#);
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_interp_channel_multiple_values() {
        let output = run_output(r#"let ch = channel()
send(ch, 1)
send(ch, 2)
send(ch, 3)
print(recv(ch))
print(recv(ch))
print(recv(ch))"#);
        assert_eq!(output, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_interp_channel_producer_consumer() {
        let output = run_output(r#"let ch = channel()
fn producer() {
    send(ch, 10)
    send(ch, 20)
    send(ch, 30)
}
let t = spawn(producer)
let a = recv(ch)
let b = recv(ch)
let c = recv(ch)
await t
print(a + b + c)"#);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_await_all() {
        let output = run_output(r#"fn w1() { 10 }
fn w2() { 20 }
fn w3() { 30 }
let t1 = spawn(w1)
let t2 = spawn(w2)
let t3 = spawn(w3)
let results = await_all([t1, t2, t3])
print(sum(results))"#);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_pmap_basic() {
        let output = run_output(r#"fn double(x) { x * 2 }
let results = pmap([1, 2, 3], double)
print(results)"#);
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_interp_pmap_order() {
        let output = run_output(r#"fn inc(x) { x + 1 }
let results = pmap([10, 20, 30], inc)
print(results)"#);
        assert_eq!(output, vec!["[11, 21, 31]"]);
    }

    #[test]
    fn test_interp_timeout_success() {
        let output = run_output(r#"fn worker() { 42 }
let t = spawn(worker)
let result = timeout(t, 5000)
print(result)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_timeout_failure() {
        let output = run_output(r#"fn slow() { sleep(10000) }
let t = spawn(slow)
let result = "ok"
try {
    result = timeout(t, 50)
} catch e {
    result = e
}
print(result)"#);
        assert_eq!(output, vec!["Task timed out"]);
    }

    #[test]
    fn test_interp_spawn_error_propagation() {
        let output = run_output(r#"fn bad() { throw "bad thing" }
let result = "ok"
try {
    let t = spawn(bad)
    result = await t
} catch e {
    result = e
}
print(result)"#);
        assert_eq!(output, vec!["bad thing"]);
    }

    #[test]
    fn test_interp_spawn_multiple_collect() {
        let output = run_output(r#"fn w1() { 1 }
fn w2() { 2 }
fn w3() { 3 }
let t1 = spawn(w1)
let t2 = spawn(w2)
let t3 = spawn(w3)
let a = await t1
let b = await t2
let c = await t3
print(a + b + c)"#);
        assert_eq!(output, vec!["6"]);
    }

    #[test]
    fn test_interp_type_of_task_channel() {
        let output = run_output(r#"fn worker() { 1 }
let t = spawn(worker)
let ch = channel()
print(type_of(t))
print(type_of(ch))
await t"#);
        assert_eq!(output, vec!["task", "channel"]);
    }

    #[test]
    fn test_interp_producer_consumer_pipeline() {
        let output = run_output(r#"let ch = channel()
fn producer() {
    let mut i = 0
    while i < 5 {
        send(ch, i * 10)
        i = i + 1
    }
}
let t = spawn(producer)
let mut total = 0
let mut count = 0
while count < 5 {
    total = total + recv(ch)
    count = count + 1
}
await t
print(total)"#);
        assert_eq!(output, vec!["100"]);
    }

    // ── Phase 8: Generators & Iterators ──────────────────────

    #[test]
    fn test_interp_basic_generator() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
    yield 3
}
let g = gen()
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["1", "2", "3", "none"]);
    }

    #[test]
    fn test_interp_generator_exhaustion() {
        let output = run_output(r#"fn gen() { yield 42 }
let g = gen()
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["42", "none", "none"]);
    }

    #[test]
    fn test_interp_generator_with_loop() {
        let output = run_output(r#"fn counter() {
    let mut i = 0
    while i < 5 {
        yield i
        i = i + 1
    }
}
print(gen_collect(counter()))"#);
        assert_eq!(output, vec!["[0, 1, 2, 3, 4]"]);
    }

    #[test]
    fn test_interp_generator_with_args() {
        let output = run_output(r#"fn range_gen(start, end) {
    let mut i = start
    while i < end {
        yield i
        i = i + 1
    }
}
let g = range_gen(3, 7)
print(next(g))
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["3", "4", "5", "6", "none"]);
    }

    #[test]
    fn test_interp_generator_yield_none() {
        let output = run_output(r#"fn gen() {
    yield
    yield 5
}
let g = gen()
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["none", "5", "none"]);
    }

    #[test]
    fn test_interp_is_generator() {
        let output = run_output(r#"fn gen() { yield 1 }
let g = gen()
print(is_generator(g))
print(is_generator(42))
print(is_generator(none))"#);
        assert_eq!(output, vec!["true", "false", "false"]);
    }

    #[test]
    fn test_interp_multiple_generators() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
}
let g1 = gen()
let g2 = gen()
print(next(g1))
print(next(g2))
print(next(g1))
print(next(g2))"#);
        assert_eq!(output, vec!["1", "1", "2", "2"]);
    }

    #[test]
    fn test_interp_for_over_generator() {
        let output = run_output(r#"fn gen() {
    yield 10
    yield 20
    yield 30
}
let mut sum = 0
for x in gen() {
    sum = sum + x
}
print(sum)"#);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_iter_builtin() {
        let output = run_output(r#"let g = iter([10, 20, 30])
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["10", "20", "30", "none"]);
    }

    #[test]
    fn test_interp_take_builtin() {
        let output = run_output(r#"fn naturals() {
    let mut n = 0
    while true {
        yield n
        n = n + 1
    }
}
print(gen_collect(take(naturals(), 5)))"#);
        assert_eq!(output, vec!["[0, 1, 2, 3, 4]"]);
    }

    #[test]
    fn test_interp_skip_builtin() {
        let output = run_output(r#"let g = skip(iter([1, 2, 3, 4, 5]), 3)
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[4, 5]"]);
    }

    #[test]
    fn test_interp_gen_collect() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
    yield 3
}
print(gen_collect(gen()))"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_interp_gen_map() {
        let output = run_output(r#"let g = gen_map(iter([1, 2, 3]), (x) => x * 10)
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[10, 20, 30]"]);
    }

    #[test]
    fn test_interp_gen_filter() {
        let output = run_output(r#"let g = gen_filter(iter([1, 2, 3, 4, 5, 6]), (x) => x % 2 == 0)
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_interp_chain() {
        let output = run_output(r#"let g = chain(iter([1, 2]), iter([3, 4]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[1, 2, 3, 4]"]);
    }

    #[test]
    fn test_interp_gen_zip() {
        let output = run_output(r#"let g = gen_zip(iter([1, 2, 3]), iter([10, 20]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[[1, 10], [2, 20]]"]);
    }

    #[test]
    fn test_interp_gen_enumerate() {
        let output = run_output(r#"let g = gen_enumerate(iter([10, 20, 30]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[[0, 10], [1, 20], [2, 30]]"]);
    }

    #[test]
    fn test_interp_combinator_chaining() {
        let output = run_output(r#"fn naturals() {
    let mut n = 0
    while true {
        yield n
        n = n + 1
    }
}
let result = gen_collect(gen_map(gen_filter(take(naturals(), 10), (x) => x % 2 == 0), (x) => x * x))
print(result)"#);
        assert_eq!(output, vec!["[0, 4, 16, 36, 64]"]);
    }

    #[test]
    fn test_interp_for_over_take() {
        let output = run_output(r#"fn naturals() {
    let mut n = 0
    while true {
        yield n
        n = n + 1
    }
}
let mut sum = 0
for x in take(naturals(), 5) {
    sum = sum + x
}
print(sum)"#);
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_interp_fibonacci_generator() {
        let output = run_output(r#"fn fib() {
    let mut a = 0
    let mut b = 1
    while true {
        yield a
        let tmp = a + b
        a = b
        b = tmp
    }
}
print(gen_collect(take(fib(), 8)))"#);
        assert_eq!(output, vec!["[0, 1, 1, 2, 3, 5, 8, 13]"]);
    }

    #[test]
    fn test_interp_generator_method_syntax() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
    yield 3
}
let g = gen()
print(g.next())
print(g.next())
print(g.collect())"#);
        assert_eq!(output, vec!["1", "2", "[3]"]);
    }

    // ── Phase 10: Result/Option + ? operator tests ──

    #[test]
    fn test_interp_ok_err_builtins() {
        let output = run_output("let r = Ok(42)\nprint(r)");
        assert_eq!(output, vec!["Result::Ok(42)"]);

        let output = run_output("let r = Err(\"fail\")\nprint(r)");
        assert_eq!(output, vec!["Result::Err(fail)"]);
    }

    #[test]
    fn test_interp_is_ok_is_err() {
        let output = run_output("print(is_ok(Ok(42)))");
        assert_eq!(output, vec!["true"]);
        let output = run_output("print(is_err(Err(\"fail\")))");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_interp_unwrap_ok() {
        let output = run_output("print(unwrap(Ok(42)))");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_unwrap_err_panics() {
        let result = run("unwrap(Err(\"fail\"))");
        assert!(result.is_err());
    }

    #[test]
    fn test_interp_try_on_ok() {
        let output = run_output(r#"fn get_val() { Ok(42) }
fn process() { let v = get_val()? + 1
Ok(v) }
print(process())"#);
        assert_eq!(output, vec!["Result::Ok(43)"]);
    }

    #[test]
    fn test_interp_try_on_err_propagates() {
        let output = run_output(r#"fn failing() { Err("oops") }
fn process() { let v = failing()?
Ok(v) }
print(process())"#);
        assert_eq!(output, vec!["Result::Err(oops)"]);
    }

    #[test]
    fn test_interp_try_on_none_propagates() {
        let output = run_output(r#"fn get_none() { none }
fn process() { let v = get_none()?
42 }
print(process())"#);
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_interp_result_match() {
        let output = run_output(r#"let r = Ok(42)
match r {
    Result::Ok(v) => print(v),
    Result::Err(e) => print(e)
}"#);
        assert_eq!(output, vec!["42"]);
    }

    // ── Set tests ──

    #[test]
    fn test_interp_set_from_dedup() {
        let output = run_output(r#"let s = set_from([1, 2, 3, 2, 1])
print(len(s))
print(type_of(s))"#);
        assert_eq!(output, vec!["3", "set"]);
    }

    #[test]
    fn test_interp_set_add() {
        let output = run_output(r#"let s = set_from([1, 2])
let s2 = set_add(s, 3)
let s3 = set_add(s2, 2)
print(len(s2))
print(len(s3))"#);
        assert_eq!(output, vec!["3", "3"]);
    }

    #[test]
    fn test_interp_set_remove() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
let s2 = set_remove(s, 2)
print(len(s2))
print(set_contains(s2, 2))"#);
        assert_eq!(output, vec!["2", "false"]);
    }

    #[test]
    fn test_interp_set_contains() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
print(set_contains(s, 2))
print(set_contains(s, 5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_interp_set_union() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([3, 4, 5])
let c = set_union(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["5"]);
    }

    #[test]
    fn test_interp_set_intersection() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
let c = set_intersection(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_interp_set_difference() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
let c = set_difference(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_interp_set_for_loop() {
        let output = run_output(r#"let s = set_from([10, 20, 30])
let total = 0
for item in s {
    total = total + item
}
print(total)"#);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_set_to_list() {
        let output = run_output(r#"let s = set_from([3, 1, 2])
let lst = s.to_list()
print(type_of(lst))
print(len(lst))"#);
        assert_eq!(output, vec!["list", "3"]);
    }

    #[test]
    fn test_interp_set_method_contains() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
print(s.contains(2))
print(s.contains(5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_interp_set_method_add_remove() {
        let output = run_output(r#"let s = set_from([1, 2])
let s2 = s.add(3)
print(s2.len())
let s3 = s2.remove(1)
print(s3.len())"#);
        assert_eq!(output, vec!["3", "2"]);
    }

    #[test]
    fn test_interp_set_method_union_intersection_difference() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
print(a.union(b).len())
print(a.intersection(b).len())
print(a.difference(b).len())"#);
        assert_eq!(output, vec!["4", "2", "1"]);
    }

    #[test]
    fn test_interp_set_empty() {
        let output = run_output(r#"let s = set_from([])
print(len(s))
let s2 = s.add(1)
print(len(s2))"#);
        assert_eq!(output, vec!["0", "1"]);
    }

    #[test]
    fn test_interp_set_string_values() {
        let output = run_output(r#"let s = set_from(["a", "b", "a", "c"])
print(len(s))
print(s.contains("b"))"#);
        assert_eq!(output, vec!["3", "true"]);
    }

    // ── Phase 11: Module System Interpreter Tests ──

    #[test]
    fn test_interp_use_single_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("math.tl"), "fn add(a, b) { a + b }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use math\nprint(add(1, 2))";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["3"]);
    }

    #[test]
    fn test_interp_use_wildcard() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("helpers.tl"), "fn greet() { \"hello\" }\nfn farewell() { \"bye\" }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use helpers.*\nprint(greet())\nprint(farewell())";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["hello", "bye"]);
    }

    #[test]
    fn test_interp_use_aliased() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mylib.tl"), "fn compute() { 42 }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use mylib as m\nprint(m.compute())";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["42"]);
    }

    #[test]
    fn test_interp_use_directory_module() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("utils")).unwrap();
        std::fs::write(dir.path().join("utils/mod.tl"), "fn helper() { 99 }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use utils\nprint(helper())";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["99"]);
    }

    #[test]
    fn test_interp_use_nested_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/transforms.tl"), "fn clean(x) { x + 1 }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use data.transforms\nprint(clean(41))";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["42"]);
    }

    #[test]
    fn test_interp_pub_fn() {
        let output = run_output("pub fn add(a, b) { a + b }\nprint(add(1, 2))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_interp_module_caching() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cached.tl"), "let X = 42").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use cached\nuse cached\nprint(X)";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["42"]);
    }

    #[test]
    fn test_interp_circular_import() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.tl");
        let b_path = dir.path().join("b.tl");
        std::fs::write(&a_path, &format!("import \"{}\"", b_path.to_string_lossy())).unwrap();
        std::fs::write(&b_path, &format!("import \"{}\"", a_path.to_string_lossy())).unwrap();

        let source = std::fs::read_to_string(&a_path).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(a_path.to_string_lossy().to_string());
        let result = interp.execute(&program);
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("Circular"));
    }

    #[test]
    fn test_interp_existing_import_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let lib_path = dir.path().join("lib.tl");
        std::fs::write(&lib_path, "fn imported_fn() { 123 }").unwrap();

        let main_src = format!("import \"{}\"\nprint(imported_fn())", lib_path.to_string_lossy());
        let program = tl_parser::parse(&main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["123"]);
    }

    #[test]
    fn test_interp_use_group() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("lib.tl"), "fn foo() { 1 }\nfn bar() { 2 }\nfn baz() { 3 }").unwrap();

        let main_path = dir.path().join("main.tl");
        let main_src = "use lib.{foo, bar}\nprint(foo())\nprint(bar())";
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let mut interp = Interpreter::new();
        interp.file_path = Some(main_path.to_string_lossy().to_string());
        interp.execute(&program).unwrap();
        assert_eq!(interp.output, vec!["1", "2"]);
    }

    // ── Phase 12: Generics & Traits (Interpreter) ──────────

    #[test]
    fn test_interp_generic_fn() {
        let output = run_output("fn identity<T>(x: T) -> T { x }\nprint(identity(42))");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_generic_fn_string() {
        let output = run_output("fn identity<T>(x: T) -> T { x }\nprint(identity(\"hello\"))");
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_interp_generic_struct() {
        let output = run_output("struct Pair<A, B> { first: A, second: B }\nlet p = Pair { first: 1, second: \"hi\" }\nprint(p.first)\nprint(p.second)");
        assert_eq!(output, vec!["1", "hi"]);
    }

    #[test]
    fn test_interp_trait_def_noop() {
        let output = run_output("trait Display { fn show(self) -> string }\nprint(\"ok\")");
        assert_eq!(output, vec!["ok"]);
    }

    #[test]
    fn test_interp_trait_impl_methods() {
        let output = run_output(
            "struct Point { x: int, y: int }\nimpl Display for Point { fn show(self) -> string { \"point\" } }\nlet p = Point { x: 1, y: 2 }\nprint(p.show())"
        );
        assert_eq!(output, vec!["point"]);
    }

    #[test]
    fn test_interp_generic_enum() {
        // Generic enum declaration works — type params are erased at runtime
        let output = run_output("enum MyOpt<T> { Some(T), Nothing }\nlet x = MyOpt::Some(42)\nlet y = MyOpt::Nothing\nprint(type_of(x))\nprint(type_of(y))");
        assert_eq!(output, vec!["enum", "enum"]);
    }

    #[test]
    fn test_interp_where_clause_runtime() {
        let output = run_output("fn compare<T>(x: T) where T: Comparable { x }\nprint(compare(10))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_interp_trait_impl_self_method() {
        let output = run_output(
            "struct Counter { value: int }\nimpl Incrementable for Counter { fn inc(self) { self.value + 1 } }\nlet c = Counter { value: 5 }\nprint(c.inc())"
        );
        assert_eq!(output, vec!["6"]);
    }

    // ── Phase 12: Integration tests ──────────────────────────

    #[test]
    fn test_interp_generic_fn_with_type_inference() {
        let output = run_output("fn first<T>(xs: list<T>) -> T { xs[0] }\nprint(first([1, 2, 3]))\nprint(first([\"a\", \"b\"]))");
        assert_eq!(output, vec!["1", "a"]);
    }

    #[test]
    fn test_interp_generic_struct_with_methods() {
        let output = run_output(
            "struct Box<T> { val: T }\nimpl Box { fn get(self) { self.val } }\nlet b = Box { val: 42 }\nprint(b.get())"
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_trait_def_impl_call() {
        let output = run_output(
            "trait Greetable { fn greet(self) -> string }\nstruct Person { name: string }\nimpl Greetable for Person { fn greet(self) -> string { self.name } }\nlet p = Person { name: \"Alice\" }\nprint(p.greet())"
        );
        assert_eq!(output, vec!["Alice"]);
    }

    #[test]
    fn test_interp_multiple_generic_params() {
        let output = run_output("fn pair<A, B>(a: A, b: B) { [a, b] }\nlet p = pair(1, \"two\")\nprint(len(p))");
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_interp_backward_compat_non_generic() {
        let output = run_output("fn add(a, b) { a + b }\nstruct Point { x: int, y: int }\nimpl Point { fn sum(self) { self.x + self.y } }\nlet p = Point { x: 3, y: 4 }\nprint(add(1, 2))\nprint(p.sum())");
        assert_eq!(output, vec!["3", "7"]);
    }

    // ── Phase 16: Package import resolution tests ──

    #[test]
    fn test_interp_package_import() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("mylib");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn greet() { print(\"hello from pkg\") }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"mylib\"\nversion = \"1.0.0\"\n").unwrap();

        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use mylib\ngreet()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();

        let mut interp = Interpreter::new();
        interp.file_path = Some(main_file.to_string_lossy().to_string());
        interp.package_roots.insert("mylib".into(), pkg_dir);
        interp.execute(&program).unwrap();

        assert_eq!(interp.output, vec!["hello from pkg"]);
    }

    #[test]
    fn test_interp_package_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("utils");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/math.tl"), "pub fn triple(x) { x * 3 }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"utils\"\nversion = \"1.0.0\"\n").unwrap();

        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use utils.math\nprint(triple(10))").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();

        let mut interp = Interpreter::new();
        interp.file_path = Some(main_file.to_string_lossy().to_string());
        interp.package_roots.insert("utils".into(), pkg_dir);
        interp.execute(&program).unwrap();

        assert_eq!(interp.output, vec!["30"]);
    }

    #[test]
    fn test_interp_package_underscore_to_hyphen() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("my-lib");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn val() { print(77) }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"my-lib\"\nversion = \"1.0.0\"\n").unwrap();

        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use my_lib\nval()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();

        let mut interp = Interpreter::new();
        interp.file_path = Some(main_file.to_string_lossy().to_string());
        interp.package_roots.insert("my-lib".into(), pkg_dir);
        interp.execute(&program).unwrap();

        assert_eq!(interp.output, vec!["77"]);
    }

    // ── Phase 17: Pattern Matching & Destructuring ──

    #[test]
    fn test_interp_match_binding() {
        let output = run_output("let x = 42\nprint(match x { val => val + 1 })");
        assert_eq!(output, vec!["43"]);
    }

    #[test]
    fn test_interp_match_guard() {
        let output = run_output("let x = 5\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["pos"]);
    }

    #[test]
    fn test_interp_match_guard_negative() {
        let output = run_output("let x = -3\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["neg"]);
    }

    #[test]
    fn test_interp_match_guard_zero() {
        let output = run_output("let x = 0\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["zero"]);
    }

    #[test]
    fn test_interp_match_enum_destructure() {
        let src = r#"
enum Shape { Circle(r), Rect(w, h) }
let s = Shape::Circle(5)
print(match s { Shape::Circle(r) => r * r, Shape::Rect(w, h) => w * h, _ => 0 })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["25"]);
    }

    #[test]
    fn test_interp_match_enum_rect() {
        let src = r#"
enum Shape { Circle(r), Rect(w, h) }
let s = Shape::Rect(3, 4)
print(match s { Shape::Circle(r) => r * r, Shape::Rect(w, h) => w * h, _ => 0 })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["12"]);
    }

    #[test]
    fn test_interp_match_or_pattern() {
        let output = run_output("let x = 2\nprint(match x { 1 or 2 or 3 => \"small\", _ => \"big\" })");
        assert_eq!(output, vec!["small"]);
    }

    #[test]
    fn test_interp_match_or_pattern_no_match() {
        let output = run_output("let x = 10\nprint(match x { 1 or 2 or 3 => \"small\", _ => \"big\" })");
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_interp_match_struct_pattern() {
        let src = r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 10, y: 20 }
print(match p { Point { x, y } => x + y, _ => 0 })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["30"]);
    }

    #[test]
    fn test_interp_match_list_pattern() {
        let src = r#"
let lst = [10, 20, 30]
print(match lst { [a, b, c] => a + b + c, _ => 0 })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_match_list_rest() {
        let src = r#"
let lst = [1, 2, 3, 4, 5]
print(match lst { [head, ...tail] => head, _ => 0 })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_interp_match_empty_list() {
        let src = r#"
let lst = []
print(match lst { [] => "empty", _ => "nonempty" })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["empty"]);
    }

    #[test]
    fn test_interp_let_destructure_list() {
        let output = run_output("let [a, b, c] = [10, 20, 30]\nprint(a + b + c)");
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_interp_let_destructure_list_rest() {
        let output = run_output("let [head, ...tail] = [1, 2, 3, 4]\nprint(head)\nprint(len(tail))");
        assert_eq!(output, vec!["1", "3"]);
    }

    #[test]
    fn test_interp_let_destructure_struct() {
        let src = r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 5, y: 10 }
let Point { x, y } = p
print(x + y)
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_interp_match_guard_enum() {
        let src = r#"
enum Result { Ok(v), Err(e) }
let r = Result::Ok(42)
print(match r { Result::Ok(v) if v > 100 => "big", Result::Ok(v) => v, Result::Err(e) => e, _ => "other" })
"#;
        let output = run_output(src);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_interp_match_negative_literal() {
        let output = run_output("let x = -5\nprint(match x { -5 => \"neg five\", _ => \"other\" })");
        assert_eq!(output, vec!["neg five"]);
    }

    #[test]
    fn test_interp_case_with_pattern() {
        let output = run_output("let x = 15\nprint(case { x > 10 => \"big\", x > 0 => \"small\", _ => \"other\" })");
        assert_eq!(output, vec!["big"]);
    }

    // ── Phase 18: Closures & Lambdas Improvements ────────────────

    #[test]
    fn test_interp_block_body_closure() {
        let output = run_output("let f = (x: int64) -> int64 { let y = x * 2\n y + 1 }\nprint(f(5))");
        assert_eq!(output, vec!["11"]);
    }

    #[test]
    fn test_interp_block_body_closure_captured_var() {
        let output = run_output("let offset = 10\nlet f = (x) -> int64 { let y = x + offset\n y }\nprint(f(5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_interp_block_body_closure_as_hof_arg() {
        let output = run_output("let nums = [1, 2, 3]\nlet result = map(nums, (x) -> int64 { let doubled = x * 2\n doubled + 1 })\nprint(result)");
        assert_eq!(output, vec!["[3, 5, 7]"]);
    }

    #[test]
    fn test_interp_type_alias_noop() {
        let output = run_output("type Mapper = fn(int64) -> int64\nlet f: Mapper = (x) => x * 2\nprint(f(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_interp_type_alias_in_function_sig() {
        let output = run_output("type Mapper = fn(int64) -> int64\nfn apply(f: Mapper, x: int64) -> int64 { f(x) }\nprint(apply((x) => x + 10, 5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_interp_shorthand_closure() {
        let output = run_output("let double = x => x * 2\nprint(double(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_interp_iife() {
        let output = run_output("let r = ((x) => x * 2)(5)\nprint(r)");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_interp_closure_as_return_value() {
        let output = run_output("fn make_adder(n) { (x) => x + n }\nlet add5 = make_adder(5)\nprint(add5(3))");
        assert_eq!(output, vec!["8"]);
    }

    #[test]
    fn test_interp_hof_apply() {
        let output = run_output("fn apply(f, x) { f(x) }\nprint(apply((x) => x + 10, 5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_interp_nested_closures() {
        let output = run_output("let add = (a) => (b) => a + b\nlet add3 = add(3)\nprint(add3(4))");
        assert_eq!(output, vec!["7"]);
    }

    #[test]
    fn test_interp_recursive_closure() {
        let output = run_output("fn fact(n) { if n <= 1 { 1 } else { n * fact(n - 1) } }\nprint(fact(5))");
        assert_eq!(output, vec!["120"]);
    }

    #[test]
    fn test_interp_block_body_closure_with_return() {
        let output = run_output("let classify = (x) -> string { if x > 0 { return \"positive\" }\n \"non-positive\" }\nprint(classify(5))\nprint(classify(-1))");
        assert_eq!(output, vec!["positive", "non-positive"]);
    }

    #[test]
    fn test_interp_shorthand_in_filter() {
        let output = run_output("let nums = [1, 2, 3, 4, 5, 6]\nlet evens = filter(nums, x => x % 2 == 0)\nprint(evens)");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_interp_both_backends_expr() {
        let output = run_output("let f = (x) => x * 3 + 1\nprint(f(4))");
        assert_eq!(output, vec!["13"]);
    }

    // Phase 21: Schema Evolution — interpreter tests

    #[test]
    fn test_interp_versioned_schema_registration() {
        // Parse and execute a versioned schema definition
        let output = run_output(r#"
/// User schema
/// @version 1
schema User {
    id: int64
    name: string
}
print(schema_latest("User"))
"#);
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_interp_schema_v1_v2_migration() {
        let output = run_output(r#"
/// @version 1
schema User {
    id: int64
    name: string
}
/// @version 2
schema UserV2 {
    id: int64
    name: string
    email: string
}
schema_register("User", 2, map_from("id", "int64", "name", "string", "email", "string"))
print(schema_latest("User"))
"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_interp_schema_latest() {
        let output = run_output(r#"
schema_register("Order", 1, map_from("id", "int64"))
schema_register("Order", 2, map_from("id", "int64", "total", "float64"))
schema_register("Order", 3, map_from("id", "int64", "total", "float64", "status", "string"))
print(schema_latest("Order"))
"#);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_interp_schema_history() {
        let output = run_output(r#"
schema_register("Event", 1, map_from("id", "int64"))
schema_register("Event", 2, map_from("id", "int64", "name", "string"))
print(schema_history("Event"))
"#);
        assert_eq!(output, vec!["[1, 2]"]);
    }

    #[test]
    fn test_interp_schema_check_backward_compat() {
        let output = run_output(r#"
schema_register("T", 1, map_from("id", "int64"))
schema_register("T", 2, map_from("id", "int64", "name", "string"))
let issues = schema_check("T", 1, 2, "backward")
print(len(issues))
"#);
        // Adding a column is backward compatible
        assert_eq!(output, vec!["0"]);
    }

    #[test]
    fn test_interp_migrate_add_column() {
        let output = run_output(r#"
/// @version 1
schema Product {
    id: int64
    name: string
}
migrate Product from 1 to 2 {
    add_column(price: float64, default: "0.0")
}
print(schema_latest("Product"))
let fields = schema_fields("Product", 2)
print(len(fields))
"#);
        assert_eq!(output, vec!["2", "3"]);
    }

    #[test]
    fn test_interp_migrate_rename_column() {
        let output = run_output(r#"
/// @version 1
schema Item {
    id: int64
    item_name: string
}
migrate Item from 1 to 2 {
    rename_column(item_name, name)
}
let fields = schema_fields("Item", 2)
print(fields)
"#);
        let output_str = &output[0];
        assert!(output_str.contains("name"), "Expected 'name' in fields, got: {}", output_str);
        assert!(!output_str.contains("item_name"), "Unexpected 'item_name' in fields, got: {}", output_str);
    }

    #[test]
    fn test_interp_schema_diff() {
        let output = run_output(r#"
schema_register("D", 1, map_from("id", "int64", "name", "string"))
schema_register("D", 2, map_from("id", "int64", "name", "string", "email", "string"))
let d = schema_diff("D", 1, 2)
print(len(d))
print(d)
"#);
        assert_eq!(output[0], "1");
        assert!(output[1].contains("added"), "Expected 'added' in diff, got: {}", output[1]);
    }

    #[test]
    fn test_interp_schema_versions() {
        let output = run_output(r#"
schema_register("V", 1, map_from("a", "int64"))
schema_register("V", 3, map_from("a", "int64", "b", "string"))
schema_register("V", 2, map_from("a", "int64", "c", "float64"))
print(schema_versions("V"))
"#);
        // Should be sorted
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_interp_schema_fields() {
        let output = run_output(r#"
schema_register("F", 1, map_from("id", "int64", "name", "string"))
let f = schema_fields("F", 1)
print(len(f))
"#);
        assert_eq!(output, vec!["2"]);
    }

    // ── Phase 22: Decimal Tests ────────────────────────────────────

    #[test]
    fn test_interp_decimal_literal() {
        let output = run_output(r#"
let x = 3.14d
print(x)
"#);
        assert_eq!(output, vec!["3.14"]);
    }

    #[test]
    fn test_interp_decimal_arithmetic() {
        let output = run_output(r#"
let a = 10.50d
let b = 3.25d
print(a + b)
print(a - b)
print(a * b)
"#);
        assert_eq!(output, vec!["13.75", "7.25", "34.1250"]);
    }

    #[test]
    fn test_interp_decimal_int_mixed() {
        let output = run_output(r#"
let d = 5.5d
let i = 2
print(d + i)
"#);
        assert_eq!(output, vec!["7.5"]);
    }

    #[test]
    fn test_interp_decimal_comparison() {
        let output = run_output(r#"
let a = 1.0d
let b = 2.0d
print(a < b)
print(a == a)
"#);
        assert_eq!(output, vec!["true", "true"]);
    }

    #[test]
    fn test_interp_decimal_negation() {
        let output = run_output(r#"
let x = 5.0d
print(-x)
"#);
        assert_eq!(output, vec!["-5.0"]);
    }

    #[test]
    fn test_interp_decimal_builtin() {
        let output = run_output(r#"
let x = decimal("99.99")
print(x)
let y = decimal(42)
print(y)
"#);
        assert_eq!(output, vec!["99.99", "42"]);
    }

    #[test]
    fn test_interp_decimal_type_of() {
        let output = run_output(r#"
let x = 1.0d
print(type_of(x))
"#);
        assert_eq!(output, vec!["decimal"]);
    }

    // ── Phase 23: Security Tests ───────────────────────────────────

    #[test]
    fn test_interp_secret_set_get() {
        let output = run_output(r#"
secret_set("api_key", "abc123")
let s = secret_get("api_key")
print(s)
"#);
        // Secret display is redacted
        assert_eq!(output, vec!["***"]);
    }

    #[test]
    fn test_interp_secret_list_delete() {
        let output = run_output(r#"
secret_set("k1", "v1")
secret_set("k2", "v2")
print(len(secret_list()))
secret_delete("k1")
print(len(secret_list()))
"#);
        assert_eq!(output, vec!["2", "1"]);
    }

    #[test]
    fn test_interp_mask_email() {
        let output = run_output(r#"
print(mask_email("alice@example.com"))
"#);
        assert_eq!(output, vec!["a***@example.com"]);
    }

    #[test]
    fn test_interp_mask_phone() {
        let output = run_output(r#"
print(mask_phone("555-123-4567"))
"#);
        assert_eq!(output, vec!["***-***-4567"]);
    }

    #[test]
    fn test_interp_mask_cc() {
        let output = run_output(r#"
print(mask_cc("4111111111111111"))
"#);
        assert_eq!(output, vec!["****-****-****-1111"]);
    }

    #[test]
    fn test_interp_redact() {
        let output = run_output(r#"
print(redact("sensitive", "full"))
print(redact("secret", "partial"))
"#);
        assert_eq!(output, vec!["***", "s***t"]);
    }

    #[test]
    fn test_interp_hash_sha256() {
        let output = run_output(r#"
let h = hash("hello", "sha256")
print(len(h))
"#);
        assert_eq!(output, vec!["64"]);
    }

    #[test]
    fn test_interp_hash_md5() {
        let output = run_output(r#"
let h = hash("hello", "md5")
print(len(h))
"#);
        assert_eq!(output, vec!["32"]);
    }

    #[test]
    fn test_interp_check_permission() {
        let output = run_output(r#"
print(check_permission("network"))
print(check_permission("file_write"))
"#);
        // Without sandbox, everything allowed
        assert_eq!(output, vec!["true", "true"]);
    }

    // ── Phase 25: Async Runtime Tests (feature-gated) ──────────────

    #[cfg(not(feature = "async-runtime"))]
    #[test]
    fn test_interp_async_builtins_require_feature() {
        let err = run_err(r#"let t = async_read_file("test.txt")"#);
        assert!(err.contains("async"), "Expected async feature error, got: {err}");
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_read_write_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("interp_async.txt");
        let path_str = path.to_str().unwrap().replace('\\', "/");
        let source = format!(
            r#"let wt = async_write_file("{path_str}", "interp async")
let wr = await(wt)
let rt = async_read_file("{path_str}")
let content = await(rt)
print(content)"#
        );
        let output = run_output(&source);
        assert_eq!(output, vec!["interp async"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_sleep() {
        let output = run_output(r#"
let t = async_sleep(10)
let r = await(t)
print(r)
"#);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_select() {
        let output = run_output(r#"
let fast = async_sleep(10)
let slow = async_sleep(5000)
let winner = select(fast, slow)
let r = await(winner)
print(r)
"#);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_race_all() {
        let output = run_output(r#"
let t1 = async_sleep(10)
let t2 = async_sleep(5000)
let winner = race_all([t1, t2])
let r = await(winner)
print(r)
"#);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_map() {
        let output = run_output(r#"
let t = async_map([1, 2, 3], (x) => x * 10)
let result = await(t)
print(result)
"#);
        assert_eq!(output, vec!["[10, 20, 30]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_filter() {
        let output = run_output(r#"
let t = async_filter([1, 2, 3, 4, 5], (x) => x > 3)
let result = await(t)
print(result)
"#);
        assert_eq!(output, vec!["[4, 5]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_map_empty() {
        let output = run_output(r#"
let t = async_map([], (x) => x)
let result = await(t)
print(result)
"#);
        assert_eq!(output, vec!["[]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_interp_async_filter_none_match() {
        let output = run_output(r#"
let t = async_filter([1, 2, 3], (x) => x > 100)
let result = await(t)
print(result)
"#);
        assert_eq!(output, vec!["[]"]);
    }
}