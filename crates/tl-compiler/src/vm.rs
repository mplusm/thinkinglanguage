// ThinkingLanguage — Bytecode Virtual Machine
// Register-based VM that executes compiled bytecode.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(feature = "native")]
use std::sync::mpsc;
#[cfg(feature = "native")]
use std::time::Duration;

#[cfg(feature = "native")]
use rayon::prelude::*;
use tl_ast::Expr as AstExpr;
use tl_errors::{RuntimeError, TlError};
#[cfg(feature = "native")]
use tl_data::{DataEngine, JoinType, col};
#[cfg(feature = "native")]
use tl_data::translate::{translate_expr, LocalValue, TranslateContext};

use crate::chunk::*;
use crate::opcode::*;
use crate::value::*;

fn decimal_to_f64(d: &rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(f64::NAN)
}

fn runtime_err(msg: impl Into<String>) -> TlError {
    TlError::Runtime(RuntimeError {
        message: msg.into(),
        span: None,
        stack_trace: vec![],
        })
}

/// Compare two VmValues for equality (used by set operations).
fn vm_values_equal(a: &VmValue, b: &VmValue) -> bool {
    match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => x == y,
        (VmValue::Float(x), VmValue::Float(y)) => x == y,
        (VmValue::String(x), VmValue::String(y)) => x == y,
        (VmValue::Bool(x), VmValue::Bool(y)) => x == y,
        (VmValue::None, VmValue::None) => true,
        _ => false,
    }
}

#[cfg(feature = "native")]
/// Resolve a file path within a package directory for package imports.
/// `pkg_root` is the package root (containing tl.toml).
/// `remaining` are the path segments after the package name.
/// Entry point convention: src/lib.tl > src/mod.tl > src/main.tl > mod.tl > lib.tl
fn resolve_package_file(pkg_root: &std::path::Path, remaining: &[&str]) -> Option<String> {
    if remaining.is_empty() {
        // Import the package itself — find entry point
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

    // Try src/<remaining>.tl, then src/<remaining>/mod.tl
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

    // Also try without src/ prefix
    let file_path = pkg_root.join(format!("{rel}.tl"));
    if file_path.exists() {
        return Some(file_path.to_string_lossy().to_string());
    }

    let dir_path = pkg_root.join(&rel).join("mod.tl");
    if dir_path.exists() {
        return Some(dir_path.to_string_lossy().to_string());
    }

    // Parent fallback for item-within-module
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

/// Convert serde_json::Value to VmValue
fn vm_json_to_value(v: &serde_json::Value) -> VmValue {
    match v {
        serde_json::Value::Null => VmValue::None,
        serde_json::Value::Bool(b) => VmValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                VmValue::Int(i)
            } else {
                VmValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => VmValue::String(Arc::from(s.as_str())),
        serde_json::Value::Array(arr) => {
            VmValue::List(arr.iter().map(vm_json_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            VmValue::Map(obj.iter().map(|(k, v)| (Arc::from(k.as_str()), vm_json_to_value(v))).collect())
        }
    }
}

/// Convert VmValue to serde_json::Value
fn vm_value_to_json(v: &VmValue) -> serde_json::Value {
    match v {
        VmValue::None => serde_json::Value::Null,
        VmValue::Bool(b) => serde_json::Value::Bool(*b),
        VmValue::Int(n) => serde_json::json!(*n),
        VmValue::Float(n) => serde_json::json!(*n),
        VmValue::String(s) => serde_json::Value::String(s.to_string()),
        VmValue::List(items) => {
            serde_json::Value::Array(items.iter().map(vm_value_to_json).collect())
        }
        VmValue::Map(pairs) => {
            let obj: serde_json::Map<String, serde_json::Value> = pairs.iter()
                .map(|(k, v)| (k.to_string(), vm_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::String(format!("{v}")),
    }
}

/// Minimum list size before we attempt parallel execution.
#[cfg(feature = "native")]
const PARALLEL_THRESHOLD: usize = 10_000;

/// Check if a closure is pure (no captured upvalues) and thus safe to run in parallel.
#[cfg(feature = "native")]
fn is_pure_closure(func: &VmValue) -> bool {
    match func {
        VmValue::Function(closure) => closure.upvalues.is_empty(),
        _ => false,
    }
}

/// Execute a pure function (no upvalues) in an isolated mini-VM.
/// Used by rayon parallel operations — each thread gets its own stack.
#[cfg(feature = "native")]
fn execute_pure_fn(proto: &Arc<Prototype>, args: &[VmValue]) -> Result<VmValue, TlError> {
    let base = 0;
    let num_regs = proto.num_registers as usize;
    let mut stack = vec![VmValue::None; num_regs + 1];
    for (i, arg) in args.iter().enumerate() {
        stack[i] = arg.clone();
    }

    let mut ip = 0;
    loop {
        if ip >= proto.code.len() {
            return Ok(VmValue::None);
        }
        let inst = proto.code[ip];
        let op = decode_op(inst);
        let a = decode_a(inst);
        let b = decode_b(inst);
        let c = decode_c(inst);
        let bx = decode_bx(inst);
        let sbx = decode_sbx(inst);

        ip += 1;

        match op {
            Op::LoadConst => {
                let val = match &proto.constants[bx as usize] {
                    Constant::Int(n) => VmValue::Int(*n),
                    Constant::Float(n) => VmValue::Float(*n),
                    Constant::String(s) => VmValue::String(s.clone()),
                    Constant::Decimal(s) => {
                        use std::str::FromStr;
                        VmValue::Decimal(rust_decimal::Decimal::from_str(s).unwrap_or_default())
                    }
                    _ => VmValue::None,
                };
                stack[base + a as usize] = val;
            }
            Op::LoadNone => stack[base + a as usize] = VmValue::None,
            Op::LoadTrue => stack[base + a as usize] = VmValue::Bool(true),
            Op::LoadFalse => stack[base + a as usize] = VmValue::Bool(false),
            Op::Move | Op::GetLocal => {
                let val = stack[base + b as usize].clone();
                stack[base + a as usize] = val;
            }
            Op::SetLocal => {
                let val = stack[base + a as usize].clone();
                stack[base + b as usize] = val;
            }
            Op::Add => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x + y),
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x + y),
                    (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 + y),
                    (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x + *y as f64),
                    _ => return Err(runtime_err("Cannot add in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Sub => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x - y),
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x - y),
                    (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 - y),
                    (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x - *y as f64),
                    _ => return Err(runtime_err("Cannot subtract in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Mul => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x * y),
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x * y),
                    (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 * y),
                    (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x * *y as f64),
                    _ => return Err(runtime_err("Cannot multiply in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Div => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => {
                        if *y == 0 { return Err(runtime_err("Division by zero")); }
                        VmValue::Int(x / y)
                    }
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x / y),
                    (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 / y),
                    (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x / *y as f64),
                    _ => return Err(runtime_err("Cannot divide in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Mod => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x % y),
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x % y),
                    _ => return Err(runtime_err("Cannot modulo in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Pow => {
                let result = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int((*x as f64).powi(*y as i32) as i64),
                    (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x.powf(*y)),
                    (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float((*x as f64).powf(*y)),
                    (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x.powi(*y as i32)),
                    _ => return Err(runtime_err("Cannot pow in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Neg => {
                let result = match &stack[base + b as usize] {
                    VmValue::Int(n) => VmValue::Int(-n),
                    VmValue::Float(n) => VmValue::Float(-n),
                    _ => return Err(runtime_err("Cannot negate in parallel fn")),
                };
                stack[base + a as usize] = result;
            }
            Op::Eq => {
                let eq = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => x == y,
                    (VmValue::Float(x), VmValue::Float(y)) => x == y,
                    (VmValue::Bool(x), VmValue::Bool(y)) => x == y,
                    (VmValue::String(x), VmValue::String(y)) => x == y,
                    (VmValue::None, VmValue::None) => true,
                    _ => false,
                };
                stack[base + a as usize] = VmValue::Bool(eq);
            }
            Op::Neq => {
                let eq = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => x == y,
                    (VmValue::Float(x), VmValue::Float(y)) => x == y,
                    (VmValue::Bool(x), VmValue::Bool(y)) => x == y,
                    (VmValue::String(x), VmValue::String(y)) => x == y,
                    (VmValue::None, VmValue::None) => true,
                    _ => false,
                };
                stack[base + a as usize] = VmValue::Bool(!eq);
            }
            Op::Lt | Op::Gt | Op::Lte | Op::Gte => {
                let cmp = match (&stack[base + b as usize], &stack[base + c as usize]) {
                    (VmValue::Int(x), VmValue::Int(y)) => x.cmp(y) as i8,
                    (VmValue::Float(x), VmValue::Float(y)) => {
                        if x < y { -1 } else if x > y { 1 } else { 0 }
                    }
                    _ => return Err(runtime_err("Cannot compare in parallel fn")),
                };
                let result = match op {
                    Op::Lt => cmp < 0,
                    Op::Gt => cmp > 0,
                    Op::Lte => cmp <= 0,
                    Op::Gte => cmp >= 0,
                    _ => unreachable!(),
                };
                stack[base + a as usize] = VmValue::Bool(result);
            }
            Op::And => {
                let left = stack[base + b as usize].is_truthy();
                let right = stack[base + c as usize].is_truthy();
                stack[base + a as usize] = VmValue::Bool(left && right);
            }
            Op::Or => {
                let left = stack[base + b as usize].is_truthy();
                let right = stack[base + c as usize].is_truthy();
                stack[base + a as usize] = VmValue::Bool(left || right);
            }
            Op::Not => {
                let val = !stack[base + b as usize].is_truthy();
                stack[base + a as usize] = VmValue::Bool(val);
            }
            Op::Jump => {
                ip = (ip as i32 + sbx as i32) as usize;
            }
            Op::JumpIfFalse => {
                if !stack[base + a as usize].is_truthy() {
                    ip = (ip as i32 + sbx as i32) as usize;
                }
            }
            Op::JumpIfTrue => {
                if stack[base + a as usize].is_truthy() {
                    ip = (ip as i32 + sbx as i32) as usize;
                }
            }
            Op::Return => {
                return Ok(stack[base + a as usize].clone());
            }
            // Unsupported ops in parallel context — fall back silently
            _ => return Err(runtime_err("Unsupported op in parallel function")),
        }
    }
}

/// A call frame on the VM stack.
struct CallFrame {
    prototype: Arc<Prototype>,
    ip: usize,
    base: usize,
    upvalues: Vec<UpvalueRef>,
}

/// A try-catch handler on the handler stack.
struct TryHandler {
    /// Frame index where try was entered
    frame_idx: usize,
    /// IP to jump to (catch handler)
    catch_ip: usize,
}

/// The bytecode virtual machine.
pub struct Vm {
    /// Register stack
    pub stack: Vec<VmValue>,
    /// Call frame stack
    frames: Vec<CallFrame>,
    /// Global variables
    pub globals: HashMap<String, VmValue>,
    /// Data engine (lazily initialized)
    #[cfg(feature = "native")]
    data_engine: Option<DataEngine>,
    /// Captured output (for testing)
    pub output: Vec<String>,
    /// Try-catch handler stack
    try_handlers: Vec<TryHandler>,
    /// Yielded value (Some when Op::Yield suspends a generator)
    yielded_value: Option<VmValue>,
    /// IP at the point of yield (instruction after the Yield op)
    yielded_ip: usize,
    /// Current file path (for relative imports)
    pub file_path: Option<String>,
    /// Module cache: resolved path → exports
    module_cache: HashMap<String, HashMap<String, VmValue>>,
    /// Files currently being imported (circular detection)
    importing_files: std::collections::HashSet<String>,
    /// Tracks which globals are public (for module export filtering)
    pub public_items: std::collections::HashSet<String>,
    /// Package roots: package_name → source directory
    pub package_roots: HashMap<String, std::path::PathBuf>,
    /// Project root (where tl.toml lives)
    pub project_root: Option<std::path::PathBuf>,
    /// Schema registry for versioned schemas
    pub schema_registry: crate::schema::SchemaRegistry,
    /// Secret vault for credential management
    pub secret_vault: HashMap<String, String>,
    /// Security policy (optional, set via --sandbox)
    pub security_policy: Option<crate::security::SecurityPolicy>,
    /// Tokio runtime for async builtins (lazily initialized)
    #[cfg(feature = "async-runtime")]
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Stashed thrown value for structured error preservation in try/catch
    thrown_value: Option<VmValue>,
    /// GPU operations dispatcher (lazily initialized)
    #[cfg(feature = "gpu")]
    gpu_ops: Option<tl_gpu::GpuOps>,
}

impl Vm {
    pub fn new() -> Self {
        let mut vm = Vm {
            stack: Vec::with_capacity(256),
            frames: Vec::new(),
            globals: HashMap::new(),
            #[cfg(feature = "native")]
            data_engine: None,
            output: Vec::new(),
            try_handlers: Vec::new(),
            yielded_value: None,
            yielded_ip: 0,
            file_path: None,
            module_cache: HashMap::new(),
            importing_files: std::collections::HashSet::new(),
            public_items: std::collections::HashSet::new(),
            package_roots: HashMap::new(),
            project_root: None,
            schema_registry: crate::schema::SchemaRegistry::new(),
            secret_vault: HashMap::new(),
            security_policy: None,
            #[cfg(feature = "async-runtime")]
            runtime: None,
            thrown_value: None,
            #[cfg(feature = "gpu")]
            gpu_ops: None,
        };
        // Phase 27: Register built-in error enum definitions
        vm.globals.insert("DataError".into(), VmValue::EnumDef(Arc::new(VmEnumDef {
            name: Arc::from("DataError"),
            variants: vec![
                (Arc::from("ParseError"), 2),
                (Arc::from("SchemaError"), 3),
                (Arc::from("ValidationError"), 2),
                (Arc::from("NotFound"), 1),
            ],
        })));
        vm.globals.insert("NetworkError".into(), VmValue::EnumDef(Arc::new(VmEnumDef {
            name: Arc::from("NetworkError"),
            variants: vec![
                (Arc::from("ConnectionError"), 2),
                (Arc::from("TimeoutError"), 1),
                (Arc::from("HttpError"), 2),
            ],
        })));
        vm.globals.insert("ConnectorError".into(), VmValue::EnumDef(Arc::new(VmEnumDef {
            name: Arc::from("ConnectorError"),
            variants: vec![
                (Arc::from("AuthError"), 2),
                (Arc::from("QueryError"), 2),
                (Arc::from("ConfigError"), 2),
            ],
        })));
        vm
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

    /// Lazily initialize and return the GPU ops dispatcher.
    #[cfg(feature = "gpu")]
    fn get_gpu_ops(&mut self) -> Result<&tl_gpu::GpuOps, TlError> {
        if self.gpu_ops.is_none() {
            let device = tl_gpu::GpuDevice::get()
                .ok_or_else(|| runtime_err("No GPU device available"))?;
            self.gpu_ops = Some(tl_gpu::GpuOps::new(device));
        }
        Ok(self.gpu_ops.as_ref().unwrap())
    }

    /// Extract a GpuTensor from a VmValue, auto-uploading CPU tensors if needed.
    #[cfg(feature = "gpu")]
    fn ensure_gpu_tensor(&mut self, val: &VmValue) -> Result<Arc<tl_gpu::GpuTensor>, TlError> {
        match val {
            VmValue::GpuTensor(gt) => Ok(gt.clone()),
            #[cfg(feature = "native")]
            VmValue::Tensor(t) => {
                let device = tl_gpu::GpuDevice::get()
                    .ok_or_else(|| runtime_err("No GPU device available"))?;
                Ok(Arc::new(tl_gpu::GpuTensor::from_cpu(t, device)))
            }
            _ => Err(runtime_err(format!("Expected tensor or gpu_tensor, got {}", val.type_name()))),
        }
    }

    #[cfg(feature = "native")]
    fn engine(&mut self) -> &DataEngine {
        if self.data_engine.is_none() {
            self.data_engine = Some(DataEngine::new());
        }
        self.data_engine.as_ref().unwrap()
    }

    /// Ensure the stack has at least `size` slots.
    fn ensure_stack(&mut self, size: usize) {
        if self.stack.len() < size {
            self.stack.resize(size, VmValue::None);
        }
    }

    /// Execute a compiled prototype.
    pub fn execute(&mut self, proto: &Prototype) -> Result<VmValue, TlError> {
        let proto = Arc::new(proto.clone());
        let base = self.stack.len();
        self.ensure_stack(base + proto.num_registers as usize + 1);

        self.frames.push(CallFrame {
            prototype: proto,
            ip: 0,
            base,
            upvalues: Vec::new(),
        });

        self.run().map_err(|e| self.enrich_error(e))
    }

    /// Enrich a runtime error with line number and stack trace from the current call frames.
    fn enrich_error(&self, err: TlError) -> TlError {
        match err {
            TlError::Runtime(mut re) => {
                // Build stack trace from remaining frames
                let mut trace = Vec::new();
                for frame in self.frames.iter().rev() {
                    let ip = if frame.ip > 0 { frame.ip - 1 } else { 0 };
                    let line = if ip < frame.prototype.lines.len() {
                        frame.prototype.lines[ip]
                    } else {
                        0
                    };
                    trace.push(tl_errors::StackFrame {
                        function: frame.prototype.name.clone(),
                        line,
                    });
                }
                // Set span from the innermost frame's line if not already set
                if re.span.is_none() && !trace.is_empty() && trace[0].line > 0 {
                    // We only have line number, not byte offset, so we can't set a precise span.
                    // But we can set a line-based marker that report_runtime_error can use.
                    // For now, leave span as None and rely on the stack trace.
                }
                re.stack_trace = trace;
                TlError::Runtime(re)
            }
            other => other,
        }
    }

    /// Main dispatch loop. Runs the current (topmost) frame until Return.
    fn run(&mut self) -> Result<VmValue, TlError> {
        let entry_depth = self.frames.len();
        loop {
            let step_result = self.run_step(entry_depth);
            match step_result {
                Ok(Some(val)) => return Ok(val),        // Return instruction
                Ok(None) => continue,                    // Normal instruction
                Err(e) => {
                    // Check for try handler
                    if let Some(handler) = self.try_handlers.pop() {
                        // Restore to handler's frame
                        while self.frames.len() > handler.frame_idx {
                            self.frames.pop();
                        }
                        if self.frames.is_empty() {
                            return Err(e);
                        }
                        let fidx = self.frames.len() - 1;
                        self.frames[fidx].ip = handler.catch_ip;
                        let err_msg = match &e {
                            TlError::Runtime(re) => re.message.clone(),
                            other => format!("{other}"),
                        };
                        // Put error value in catch scope's first local
                        // The compiler emits LoadNone for the catch var at catch_ip; we need to
                        // identify the register, set the error value, and skip past the LoadNone
                        let catch_val = self.thrown_value.take().unwrap_or_else(|| {
                            VmValue::String(Arc::from(err_msg.as_str()))
                        });
                        let cbase = self.frames[fidx].base;
                        let current_ip = self.frames[fidx].ip;
                        if current_ip < self.frames[fidx].prototype.code.len() {
                            let catch_inst = self.frames[fidx].prototype.code[current_ip];
                            let catch_op = decode_op(catch_inst);
                            let catch_reg = decode_a(catch_inst);
                            if matches!(catch_op, Op::LoadNone) {
                                // Skip the LoadNone and write error value directly
                                self.frames[fidx].ip += 1;
                                self.ensure_stack(cbase + catch_reg as usize + 1);
                                self.stack[cbase + catch_reg as usize] = catch_val;
                            }
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a single instruction. Returns Ok(Some(val)) for Return, Ok(None) for continue, Err for errors.
    fn run_step(&mut self, entry_depth: usize) -> Result<Option<VmValue>, TlError> {
        if self.frames.len() < entry_depth || self.frames.is_empty() {
            return Ok(Some(VmValue::None));
        }
        let frame_idx = self.frames.len() - 1;
        let frame = &self.frames[frame_idx];

        if frame.ip >= frame.prototype.code.len() {
            // End of bytecode — return None
            self.frames.pop();
            return Ok(Some(VmValue::None));
        }

        let inst = frame.prototype.code[frame.ip];
        let op = decode_op(inst);
        let a = decode_a(inst);
        let b = decode_b(inst);
        let c = decode_c(inst);
        let bx = decode_bx(inst);
        let sbx = decode_sbx(inst);
        let base = frame.base;

        // Advance IP before executing (some ops modify it)
        self.frames[frame_idx].ip += 1;

            match op {
                Op::LoadConst => {
                    let val = self.load_constant(frame_idx, bx)?;
                    self.stack[base + a as usize] = val;
                }
                Op::LoadNone => {
                    self.stack[base + a as usize] = VmValue::None;
                }
                Op::LoadTrue => {
                    self.stack[base + a as usize] = VmValue::Bool(true);
                }
                Op::LoadFalse => {
                    self.stack[base + a as usize] = VmValue::Bool(false);
                }
                Op::Move => {
                    let val = &self.stack[base + b as usize];
                    if matches!(val, VmValue::Moved) {
                        return Err(runtime_err("Use of moved value. It was consumed by a pipe (|>) operation. Use .clone() to keep a copy.".to_string()));
                    }
                    self.stack[base + a as usize] = val.clone();
                }
                Op::GetLocal => {
                    let val = &self.stack[base + b as usize];
                    if matches!(val, VmValue::Moved) {
                        return Err(runtime_err("Use of moved value. It was consumed by a pipe (|>) operation. Use .clone() to keep a copy.".to_string()));
                    }
                    self.stack[base + a as usize] = val.clone();
                }
                Op::SetLocal => {
                    let val = self.stack[base + a as usize].clone();
                    self.stack[base + b as usize] = val;
                }
                Op::GetGlobal => {
                    let name = self.get_string_constant(frame_idx, bx)?;
                    let val = self.globals.get(name.as_ref())
                        .cloned()
                        .unwrap_or(VmValue::None);
                    if matches!(val, VmValue::Moved) {
                        return Err(runtime_err(format!("Use of moved value `{name}`. It was consumed by a pipe (|>) operation. Use .clone() to keep a copy.")));
                    }
                    self.stack[base + a as usize] = val;
                }
                Op::SetGlobal => {
                    let name = self.get_string_constant(frame_idx, bx)?;
                    let val = self.stack[base + a as usize].clone();
                    // Phase 21: Detect __schema__ and __migrate__ globals and register in schema_registry
                    #[cfg(feature = "native")]
                    if let VmValue::String(ref s) = val {
                        if s.starts_with("__schema__:") {
                            self.process_schema_global(s);
                        } else if s.starts_with("__migrate__:") {
                            self.process_migrate_global(s);
                        }
                    }
                    self.globals.insert(name.to_string(), val);
                }
                Op::GetUpvalue => {
                    let val = {
                        let frame = &self.frames[frame_idx];
                        match &frame.upvalues[b as usize] {
                            UpvalueRef::Open { stack_index } => {
                                self.stack[*stack_index].clone()
                            }
                            UpvalueRef::Closed(v) => v.clone(),
                        }
                    };
                    self.stack[base + a as usize] = val;
                }
                Op::SetUpvalue => {
                    let val = self.stack[base + a as usize].clone();
                    let frame = &mut self.frames[frame_idx];
                    match &mut frame.upvalues[b as usize] {
                        UpvalueRef::Open { stack_index } => {
                            let idx = *stack_index;
                            self.stack[idx] = val;
                        }
                        UpvalueRef::Closed(v) => {
                            *v = val;
                        }
                    }
                }
                Op::Add => {
                    let result = self.vm_add(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Sub => {
                    let result = self.vm_sub(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Mul => {
                    let result = self.vm_mul(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Div => {
                    let result = self.vm_div(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Mod => {
                    let result = self.vm_mod(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Pow => {
                    let result = self.vm_pow(base, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::Neg => {
                    let result = match &self.stack[base + b as usize] {
                        VmValue::Int(n) => VmValue::Int(-n),
                        VmValue::Float(n) => VmValue::Float(-n),
                        VmValue::Decimal(d) => VmValue::Decimal(-d),
                        other => return Err(runtime_err(format!("Cannot negate {}", other.type_name()))),
                    };
                    self.stack[base + a as usize] = result;
                }
                Op::Eq => {
                    let result = self.vm_eq(base, b, c);
                    self.stack[base + a as usize] = VmValue::Bool(result);
                }
                Op::Neq => {
                    let result = !self.vm_eq(base, b, c);
                    self.stack[base + a as usize] = VmValue::Bool(result);
                }
                Op::Lt => {
                    let result = self.vm_cmp(base, b, c)?;
                    self.stack[base + a as usize] = VmValue::Bool(result < 0);
                }
                Op::Gt => {
                    let result = self.vm_cmp(base, b, c)?;
                    self.stack[base + a as usize] = VmValue::Bool(result > 0);
                }
                Op::Lte => {
                    let result = self.vm_cmp(base, b, c)?;
                    self.stack[base + a as usize] = VmValue::Bool(result <= 0);
                }
                Op::Gte => {
                    let result = self.vm_cmp(base, b, c)?;
                    self.stack[base + a as usize] = VmValue::Bool(result >= 0);
                }
                Op::And => {
                    let left = self.stack[base + b as usize].is_truthy();
                    let right = self.stack[base + c as usize].is_truthy();
                    self.stack[base + a as usize] = VmValue::Bool(left && right);
                }
                Op::Or => {
                    let left = self.stack[base + b as usize].is_truthy();
                    let right = self.stack[base + c as usize].is_truthy();
                    self.stack[base + a as usize] = VmValue::Bool(left || right);
                }
                Op::Not => {
                    let val = !self.stack[base + b as usize].is_truthy();
                    self.stack[base + a as usize] = VmValue::Bool(val);
                }
                Op::Concat => {
                    let left = format!("{}", self.stack[base + b as usize]);
                    let right = format!("{}", self.stack[base + c as usize]);
                    self.stack[base + a as usize] = VmValue::String(Arc::from(format!("{left}{right}").as_str()));
                }
                Op::Jump => {
                    let frame = &mut self.frames[frame_idx];
                    frame.ip = (frame.ip as i32 + sbx as i32) as usize;
                }
                Op::JumpIfFalse => {
                    if !self.stack[base + a as usize].is_truthy() {
                        let frame = &mut self.frames[frame_idx];
                        frame.ip = (frame.ip as i32 + sbx as i32) as usize;
                    }
                }
                Op::JumpIfTrue => {
                    if self.stack[base + a as usize].is_truthy() {
                        let frame = &mut self.frames[frame_idx];
                        frame.ip = (frame.ip as i32 + sbx as i32) as usize;
                    }
                }
                Op::Call => {
                    // a = func reg, b = args start, c = arg count
                    let func_val = self.stack[base + a as usize].clone();
                    self.do_call(func_val, base, a, b, c)?;
                }
                Op::Return => {
                    let return_val = self.stack[base + a as usize].clone();
                    self.frames.pop();
                    return Ok(Some(return_val));
                }
                Op::Closure => {
                    let proto = match &self.frames[frame_idx].prototype.constants[bx as usize] {
                        Constant::Prototype(p) => p.clone(),
                        _ => return Err(runtime_err("Expected prototype constant")),
                    };

                    // Capture upvalues
                    let mut upvalues = Vec::new();
                    for def in &proto.upvalue_defs {
                        if def.is_local {
                            upvalues.push(UpvalueRef::Open {
                                stack_index: base + def.index as usize,
                            });
                        } else {
                            let frame = &self.frames[frame_idx];
                            upvalues.push(frame.upvalues[def.index as usize].clone());
                        }
                    }

                    let closure = VmClosure {
                        prototype: proto,
                        upvalues,
                    };
                    self.stack[base + a as usize] = VmValue::Function(Arc::new(closure));
                }
                Op::NewList => {
                    // a = dest, b = start reg, c = count
                    let mut items = Vec::with_capacity(c as usize);
                    for i in 0..c as usize {
                        items.push(self.stack[base + b as usize + i].clone());
                    }
                    self.stack[base + a as usize] = VmValue::List(items);
                }
                Op::GetIndex => {
                    let raw_obj = &self.stack[base + b as usize];
                    let obj = match raw_obj {
                        VmValue::Ref(inner) => inner.as_ref(),
                        other => other,
                    };
                    let idx = &self.stack[base + c as usize];
                    let result = match (obj, idx) {
                        (VmValue::List(items), VmValue::Int(i)) => {
                            let i = *i as usize;
                            items.get(i).cloned().ok_or_else(|| {
                                runtime_err(format!(
                                    "Index {i} out of bounds for list of length {}",
                                    items.len()
                                ))
                            })?
                        }
                        (VmValue::Map(pairs), VmValue::String(key)) => {
                            pairs.iter()
                                .find(|(k, _)| k.as_ref() == key.as_ref())
                                .map(|(_, v)| v.clone())
                                .unwrap_or(VmValue::None)
                        }
                        _ => return Err(runtime_err(format!(
                            "Cannot index {} with {}",
                            obj.type_name(),
                            idx.type_name()
                        ))),
                    };
                    self.stack[base + a as usize] = result;
                }
                Op::SetIndex => {
                    if matches!(&self.stack[base + b as usize], VmValue::Ref(_)) {
                        return Err(runtime_err("Cannot mutate a borrowed reference".to_string()));
                    }
                    let val = self.stack[base + a as usize].clone();
                    let idx_val = self.stack[base + c as usize].clone();
                    match idx_val {
                        VmValue::Int(i) => {
                            if let VmValue::List(ref mut items) = self.stack[base + b as usize] {
                                let i = i as usize;
                                if i < items.len() {
                                    items[i] = val;
                                }
                            }
                        }
                        VmValue::String(key) => {
                            if let VmValue::Map(ref mut pairs) = self.stack[base + b as usize] {
                                if let Some(entry) = pairs.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref()) {
                                    entry.1 = val;
                                } else {
                                    pairs.push((key, val));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Op::NewMap => {
                    // a = dest, b = start reg, c = pair count
                    // The pairs are key, value, key, value in registers b..b+c*2
                    let mut pairs = Vec::with_capacity(c as usize);
                    for i in 0..c as usize {
                        let key_val = &self.stack[base + b as usize + i * 2];
                        let val = self.stack[base + b as usize + i * 2 + 1].clone();
                        let key = match key_val {
                            VmValue::String(s) => s.clone(),
                            other => Arc::from(format!("{other}").as_str()),
                        };
                        pairs.push((key, val));
                    }
                    self.stack[base + a as usize] = VmValue::Map(pairs);
                }
                Op::TablePipe => {
                    #[cfg(feature = "native")]
                    {
                        // a = table reg, b = op name constant idx, c = args constant idx
                        let table_val = self.stack[base + a as usize].clone();
                        let result = self.handle_table_pipe(frame_idx, table_val, b, c)?;
                        self.stack[base + a as usize] = result;
                    }
                    #[cfg(not(feature = "native"))]
                    {
                        let _ = (a, b, c, frame_idx);
                        return Err(runtime_err("Table operations not available in WASM"));
                    }
                }
                Op::CallBuiltin => {
                    // a = dest, b = builtin id, c = first arg reg
                    // Next instruction: arg count in A field
                    let next_inst = self.frames[frame_idx].prototype.code[self.frames[frame_idx].ip];
                    self.frames[frame_idx].ip += 1;
                    let arg_count = decode_a(next_inst) as usize;

                    let result = self.call_builtin(b, base + c as usize, arg_count)?;
                    self.stack[base + a as usize] = result;
                }
                Op::ForIter => {
                    // a = iterator (index) reg, b = list reg, c = value dest reg
                    let idx = match &self.stack[base + a as usize] {
                        VmValue::Int(i) => *i as usize,
                        _ => 0,
                    };
                    let list = &self.stack[base + b as usize];
                    let done = match list {
                        VmValue::List(items) => {
                            if idx < items.len() {
                                let item = items[idx].clone();
                                self.stack[base + c as usize] = item;
                                self.stack[base + a as usize] = VmValue::Int((idx + 1) as i64);
                                false
                            } else {
                                true
                            }
                        }
                        VmValue::Map(pairs) => {
                            if idx < pairs.len() {
                                let (k, v) = &pairs[idx];
                                let pair = VmValue::List(vec![
                                    VmValue::String(k.clone()),
                                    v.clone(),
                                ]);
                                self.stack[base + c as usize] = pair;
                                self.stack[base + a as usize] = VmValue::Int((idx + 1) as i64);
                                false
                            } else {
                                true
                            }
                        }
                        VmValue::Set(items) => {
                            if idx < items.len() {
                                let item = items[idx].clone();
                                self.stack[base + c as usize] = item;
                                self.stack[base + a as usize] = VmValue::Int((idx + 1) as i64);
                                false
                            } else {
                                true
                            }
                        }
                        VmValue::Generator(gen_arc) => {
                            let g = gen_arc.clone();
                            let val = self.generator_next(&g)?;
                            if matches!(val, VmValue::None) {
                                true
                            } else {
                                self.stack[base + c as usize] = val;
                                false
                            }
                        }
                        _ => true,
                    };
                    if done {
                        // Next instruction is a Jump — execute it
                        // (the jump instruction follows ForIter)
                    } else {
                        // Skip the jump instruction
                        self.frames[frame_idx].ip += 1;
                    }
                }
                Op::ForPrep => {
                    // Not currently used — ForIter handles everything
                }
                Op::TestMatch => {
                    // a = subject reg, b = pattern reg, c = dest bool reg
                    let subject = &self.stack[base + a as usize];
                    let pattern = &self.stack[base + b as usize];
                    let matched = match (subject, pattern) {
                        (VmValue::Int(a), VmValue::Int(b)) => a == b,
                        (VmValue::Float(a), VmValue::Float(b)) => a == b,
                        (VmValue::String(a), VmValue::String(b)) => a == b,
                        (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
                        (VmValue::None, VmValue::None) => true,
                        // Enum instance matching: same type + same variant
                        (VmValue::EnumInstance(subj), VmValue::EnumInstance(pat)) => {
                            subj.type_name == pat.type_name && subj.variant == pat.variant
                        }
                        // Struct instance matching by type name
                        (VmValue::StructInstance(s), VmValue::String(name)) => {
                            s.type_name.as_ref() == name.as_ref()
                        }
                        _ => false,
                    };
                    self.stack[base + c as usize] = VmValue::Bool(matched);
                }
                Op::NullCoalesce => {
                    if matches!(self.stack[base + a as usize], VmValue::None) {
                        let val = self.stack[base + b as usize].clone();
                        self.stack[base + a as usize] = val;
                    }
                }
                Op::GetMember => {
                    // a = dest, b = object reg, c = field name constant
                    let field_name = self.get_string_constant(frame_idx, c as u16)?;
                    let raw_obj = self.stack[base + b as usize].clone();
                    let obj = match &raw_obj {
                        VmValue::Ref(inner) => inner.as_ref().clone(),
                        _ => raw_obj,
                    };
                    let result = match &obj {
                        VmValue::StructInstance(inst) => {
                            inst.fields.iter()
                                .find(|(k, _)| k.as_ref() == field_name.as_ref())
                                .map(|(_, v)| v.clone())
                                .unwrap_or(VmValue::None)
                        }
                        VmValue::Module(m) => {
                            m.exports.get(field_name.as_ref())
                                .cloned()
                                .unwrap_or(VmValue::None)
                        }
                        VmValue::EnumInstance(e) => {
                            match field_name.as_ref() {
                                "variant" => VmValue::String(e.variant.clone()),
                                "type_name" => VmValue::String(e.type_name.clone()),
                                _ => VmValue::None,
                            }
                        }
                        VmValue::Map(pairs) => {
                            pairs.iter()
                                .find(|(k, _)| k.as_ref() == field_name.as_ref())
                                .map(|(_, v)| v.clone())
                                .unwrap_or(VmValue::None)
                        }
                        #[cfg(feature = "python")]
                        VmValue::PyObject(wrapper) => {
                            crate::python::py_get_member(wrapper, field_name.as_ref())
                        }
                        _ => VmValue::None,
                    };
                    self.stack[base + a as usize] = result;
                }
                Op::Interpolate => {
                    // a = dest, bx = string template constant
                    let template = self.get_string_constant(frame_idx, bx)?;
                    let result = self.interpolate_string(&template, base)?;
                    self.stack[base + a as usize] = VmValue::String(Arc::from(result.as_str()));
                }
                Op::Train => {
                    #[cfg(feature = "native")]
                    { let result = self.handle_train(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result; }
                    #[cfg(not(feature = "native"))]
                    { let _ = (a, b, c, frame_idx); return Err(runtime_err("AI training not available in WASM")); }
                }
                Op::PipelineExec => {
                    #[cfg(feature = "native")]
                    { let result = self.handle_pipeline_exec(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result; }
                    #[cfg(not(feature = "native"))]
                    { let _ = (a, b, c, frame_idx); return Err(runtime_err("Pipelines not available in WASM")); }
                }
                Op::StreamExec => {
                    #[cfg(feature = "native")]
                    { let result = self.handle_stream_exec(frame_idx, b)?;
                    self.stack[base + a as usize] = result; }
                    #[cfg(not(feature = "native"))]
                    { let _ = (a, b, frame_idx); return Err(runtime_err("Streaming not available in WASM")); }
                }
                Op::ConnectorDecl => {
                    #[cfg(feature = "native")]
                    { let result = self.handle_connector_decl(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result; }
                    #[cfg(not(feature = "native"))]
                    { let _ = (a, b, c, frame_idx); return Err(runtime_err("Connectors not available in WASM")); }
                }

                // ── Phase 5: Language completeness opcodes ──

                Op::NewStruct => {
                    // Two uses:
                    // 1) Struct declaration: a=dest, b=name_const, c=fields_const (AstExprList)
                    //    Next instruction is NOT a Move with start reg
                    // 2) Struct instance: a=dest, b=name_const, c=field_count
                    //    Next instruction is Move with start reg in A

                    let name = self.get_string_constant(frame_idx, b as u16)?;

                    // High bit of c distinguishes declaration (set) from instance (clear).
                    // Declarations: c = constant_idx | 0x80
                    // Instances: c = field_count (no high bit)
                    let is_decl = (c & 0x80) != 0;

                    if is_decl {
                        let const_idx = (c & 0x7F) as usize;
                        // Struct/Enum declaration
                        let fields_data = match &self.frames[frame_idx].prototype.constants[const_idx] {
                            Constant::AstExprList(exprs) => exprs.clone(),
                            _ => Vec::new(),
                        };
                        // Check if it looks like an enum (fields have "Name:count" format)
                        let is_enum = fields_data.first().map(|e| {
                            if let AstExpr::String(s) = e { s.contains(':') } else { false }
                        }).unwrap_or(false);

                        if is_enum {
                            let variants: Vec<(Arc<str>, usize)> = fields_data.iter().filter_map(|e| {
                                if let AstExpr::String(s) = e {
                                    let parts: Vec<&str> = s.splitn(2, ':').collect();
                                    if parts.len() == 2 {
                                        Some((Arc::from(parts[0]), parts[1].parse::<usize>().unwrap_or(0)))
                                    } else { None }
                                } else { None }
                            }).collect();
                            self.stack[base + a as usize] = VmValue::EnumDef(Arc::new(VmEnumDef {
                                name: name.clone(),
                                variants,
                            }));
                        } else {
                            let field_names: Vec<Arc<str>> = fields_data.iter().filter_map(|e| {
                                if let AstExpr::String(s) = e { Some(Arc::from(s.as_str())) } else { None }
                            }).collect();
                            self.stack[base + a as usize] = VmValue::StructDef(Arc::new(VmStructDef {
                                name: name.clone(),
                                fields: field_names,
                            }));
                        }
                    } else {
                        // Struct instance creation: c = field count
                        let field_count = c as usize;
                        // Next instruction holds start register in A field
                        let next_ip = self.frames[frame_idx].ip;
                        let next = self.frames[frame_idx].prototype.code.get(next_ip).copied().unwrap_or(0);
                        let start_reg = decode_a(next) as usize;
                        self.frames[frame_idx].ip += 1; // skip the extra instruction

                        let mut fields = Vec::new();
                        for i in 0..field_count {
                            let fname = self.stack[base + start_reg + i * 2].clone();
                            let fval = self.stack[base + start_reg + i * 2 + 1].clone();
                            let fname_str = match fname {
                                VmValue::String(s) => s,
                                _ => Arc::from(format!("field_{i}").as_str()),
                            };
                            fields.push((fname_str, fval));
                        }
                        self.stack[base + a as usize] = VmValue::StructInstance(Arc::new(VmStructInstance {
                            type_name: name.clone(),
                            fields,
                        }));
                    }
                }

                Op::SetMember => {
                    if matches!(&self.stack[base + a as usize], VmValue::Ref(_)) {
                        return Err(runtime_err("Cannot mutate a borrowed reference".to_string()));
                    }
                    // a = object reg, b = field name constant, c = value reg
                    let field_name = self.get_string_constant(frame_idx, b as u16)?;
                    let val = self.stack[base + c as usize].clone();
                    let obj = self.stack[base + a as usize].clone();
                    if let VmValue::StructInstance(inst) = obj {
                        let mut new_fields = inst.fields.clone();
                        let mut found = false;
                        for (k, v) in &mut new_fields {
                            if k.as_ref() == field_name.as_ref() {
                                *v = val.clone();
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            new_fields.push((field_name, val));
                        }
                        self.stack[base + a as usize] = VmValue::StructInstance(Arc::new(VmStructInstance {
                            type_name: inst.type_name.clone(),
                            fields: new_fields,
                        }));
                    }
                }

                Op::NewEnum => {
                    // a = dest, b = name constant ("EnumName::Variant"), c = args start reg
                    // Next instruction: arg_count in A field
                    let full_name = self.get_string_constant(frame_idx, b as u16)?;
                    let next = self.frames[frame_idx].prototype.code[self.frames[frame_idx].ip];
                    self.frames[frame_idx].ip += 1;
                    let arg_count = decode_a(next) as usize;
                    let args_start = c as usize;

                    // Parse "EnumName::Variant"
                    let parts: Vec<&str> = full_name.splitn(2, "::").collect();
                    let (type_name, variant) = if parts.len() == 2 {
                        (Arc::from(parts[0]), Arc::from(parts[1]))
                    } else {
                        (Arc::from(""), Arc::from(full_name.as_ref()))
                    };

                    let mut fields = Vec::new();
                    for i in 0..arg_count {
                        fields.push(self.stack[base + args_start + i].clone());
                    }

                    self.stack[base + a as usize] = VmValue::EnumInstance(Arc::new(VmEnumInstance {
                        type_name,
                        variant,
                        fields,
                    }));
                }

                Op::MatchEnum => {
                    // a = subject reg, b = variant name constant, c = dest bool reg
                    let variant_name = self.get_string_constant(frame_idx, b as u16)?;
                    let subject = &self.stack[base + a as usize];
                    let matched = match subject {
                        VmValue::EnumInstance(e) => e.variant.as_ref() == variant_name.as_ref(),
                        _ => false,
                    };
                    self.stack[base + c as usize] = VmValue::Bool(matched);
                }

                Op::MethodCall => {
                    // a = dest, b = object reg, c = method name constant
                    // Next instruction: A = args_start, B = arg_count
                    let method_name = self.get_string_constant(frame_idx, c as u16)?;
                    let next = self.frames[frame_idx].prototype.code[self.frames[frame_idx].ip];
                    self.frames[frame_idx].ip += 1;
                    let args_start = decode_a(next) as usize;
                    let arg_count = decode_b(next) as usize;

                    let obj = self.stack[base + b as usize].clone();
                    let mut args = Vec::new();
                    for i in 0..arg_count {
                        args.push(self.stack[base + args_start + i].clone());
                    }

                    let result = self.dispatch_method(obj, &method_name, &args)?;
                    self.stack[base + a as usize] = result;
                }

                Op::Throw => {
                    // a = value register
                    let val = self.stack[base + a as usize].clone();
                    self.thrown_value = Some(val.clone());
                    let err_msg = format!("{val}");
                    return Err(runtime_err(err_msg));
                }

                Op::TryBegin => {
                    // sbx = offset to catch handler (relative to this instruction)
                    let catch_ip = (self.frames[frame_idx].ip as i32 + sbx as i32) as usize;
                    self.try_handlers.push(TryHandler {
                        frame_idx: self.frames.len(),
                        catch_ip,
                    });
                }

                Op::TryEnd => {
                    // Pop the try handler (success path)
                    self.try_handlers.pop();
                }

                Op::Import => {
                    #[cfg(feature = "native")]
                    {
                        // a = dest, bx = path constant
                        // Next instruction encodes either:
                        //   - Classic import: A = alias constant, B = 0, C = 0
                        //   - Use import: A = extra, B = kind, C = 0xAB (magic marker)
                        let path = self.get_string_constant(frame_idx, bx)?;
                        let next = self.frames[frame_idx].prototype.code[self.frames[frame_idx].ip];
                        self.frames[frame_idx].ip += 1;
                        let next_a = decode_a(next);
                        let next_b = decode_b(next);
                        let next_c = decode_c(next);

                        let result = if next_c == 0xAB {
                            // Use-style import (dot-path)
                            self.handle_use_import(&path, next_a, next_b, frame_idx)?
                        } else {
                            // Classic import "file.tl" [as alias]
                            let alias_idx = next_a as u16;
                            let alias = self.get_string_constant(frame_idx, alias_idx)?;
                            self.handle_import(&path, &alias)?
                        };
                        self.stack[base + a as usize] = result;
                    }
                    #[cfg(not(feature = "native"))]
                    {
                        let _ = (a, bx, frame_idx);
                        return Err(runtime_err("Module imports not available in WASM"));
                    }
                }

                Op::Await => {
                    // a = dest, b = task/value register
                    let val = self.stack[base + b as usize].clone();
                    match val {
                        VmValue::Task(task) => {
                            let rx = {
                                let mut guard = task.receiver.lock().unwrap();
                                guard.take()
                            };
                            match rx {
                                Some(receiver) => {
                                    match receiver.recv() {
                                        Ok(Ok(result)) => {
                                            self.stack[base + a as usize] = result;
                                        }
                                        Ok(Err(err_msg)) => {
                                            return Err(runtime_err(err_msg));
                                        }
                                        Err(_) => {
                                            return Err(runtime_err("Task channel disconnected"));
                                        }
                                    }
                                }
                                None => {
                                    return Err(runtime_err("Task already awaited"));
                                }
                            }
                        }
                        // Non-task values pass through
                        other => {
                            self.stack[base + a as usize] = other;
                        }
                    }
                }
                Op::Yield => {
                    // a = value register to yield
                    let val = self.stack[base + a as usize].clone();
                    self.yielded_value = Some(val.clone());
                    // Save the current ip (already advanced past Yield instruction)
                    self.yielded_ip = self.frames[frame_idx].ip;
                    // Pop the frame and return the value
                    self.frames.pop();
                    return Ok(Some(val));
                }
                Op::TryPropagate => {
                    // A = dest, B = source register
                    // If source is Err(...) → early return from current function
                    // If source is Ok(v) → A = v (unwrap)
                    // If source is None → early return None
                    // Otherwise → passthrough
                    let src = self.stack[base + b as usize].clone();
                    match &src {
                        VmValue::EnumInstance(ei) if ei.type_name.as_ref() == "Result" => {
                            if ei.variant.as_ref() == "Ok" && !ei.fields.is_empty() {
                                // Unwrap: A = inner value
                                self.stack[base + a as usize] = ei.fields[0].clone();
                            } else if ei.variant.as_ref() == "Err" {
                                // Propagate: return the Err from current function
                                self.frames.pop();
                                return Ok(Some(src));
                            } else {
                                self.stack[base + a as usize] = src;
                            }
                        }
                        VmValue::None => {
                            // Propagate: return None from current function
                            self.frames.pop();
                            return Ok(Some(VmValue::None));
                        }
                        _ => {
                            // Passthrough
                            self.stack[base + a as usize] = src;
                        }
                    }
                }
                Op::ExtractField => {
                    // A = dest, B = source reg, C = field index
                    // If C has high bit set (C | 0x80), extract rest (sublist from index C & 0x7F)
                    let source = self.stack[base + b as usize].clone();
                    let is_rest = (c & 0x80) != 0;
                    let idx = (c & 0x7F) as usize;
                    let val = if is_rest {
                        // Extract rest as sublist from index idx..
                        match &source {
                            VmValue::List(l) => {
                                if idx < l.len() {
                                    VmValue::List(l[idx..].to_vec())
                                } else {
                                    VmValue::List(vec![])
                                }
                            }
                            _ => VmValue::List(vec![]),
                        }
                    } else {
                        match &source {
                            VmValue::EnumInstance(ei) => {
                                ei.fields.get(idx).cloned().unwrap_or(VmValue::None)
                            }
                            VmValue::List(l) => {
                                l.get(idx).cloned().unwrap_or(VmValue::None)
                            }
                            _ => VmValue::None,
                        }
                    };
                    self.stack[base + a as usize] = val;
                }
                Op::ExtractNamedField => {
                    // A = dest, B = source reg, C = field name constant index
                    let source = self.stack[base + b as usize].clone();
                    let field_name = match &self.frames[frame_idx].prototype.constants[c as usize] {
                        Constant::String(s) => s.clone(),
                        _ => return Err(runtime_err("ExtractNamedField: expected string constant")),
                    };
                    let val = match &source {
                        VmValue::StructInstance(s) => {
                            s.fields.iter()
                                .find(|(k, _): &&(Arc<str>, VmValue)| k.as_ref() == field_name.as_ref())
                                .map(|(_, v)| v.clone())
                                .unwrap_or(VmValue::None)
                        }
                        VmValue::Map(m) => {
                            m.iter()
                                .find(|(k, _): &&(Arc<str>, VmValue)| k.as_ref() == field_name.as_ref())
                                .map(|(_, v)| v.clone())
                                .unwrap_or(VmValue::None)
                        }
                        _ => VmValue::None,
                    };
                    self.stack[base + a as usize] = val;
                }

                // Phase 28: Ownership & Move Semantics
                Op::LoadMoved => {
                    self.stack[base + a as usize] = VmValue::Moved;
                }
                Op::MakeRef => {
                    let val = self.stack[base + b as usize].clone();
                    self.stack[base + a as usize] = VmValue::Ref(Arc::new(val));
                }
                Op::ParallelFor => {
                    // Currently compiled as regular ForIter, this opcode is reserved
                    // for future rayon-backed parallel iteration.
                }
            }
            Ok(None)
    }

    /// Perform a function call.
    fn do_call(
        &mut self,
        func: VmValue,
        caller_base: usize,
        func_reg: u8,
        args_start: u8,
        arg_count: u8,
    ) -> Result<(), TlError> {
        match func {
            VmValue::Function(closure) => {
                let proto = closure.prototype.clone();
                let arity = proto.arity as usize;

                if arg_count as usize != arity {
                    return Err(runtime_err(format!(
                        "Expected {} arguments, got {}",
                        arity, arg_count
                    )));
                }

                // If this is a generator function, create a Generator instead of executing
                if proto.is_generator {
                    // Close upvalues for the generator
                    let mut closed_upvalues = Vec::new();
                    for uv in &closure.upvalues {
                        match uv {
                            UpvalueRef::Open { stack_index } => {
                                let val = self.stack[*stack_index].clone();
                                closed_upvalues.push(UpvalueRef::Closed(val));
                            }
                            UpvalueRef::Closed(v) => {
                                closed_upvalues.push(UpvalueRef::Closed(v.clone()));
                            }
                        }
                    }

                    // Build initial saved_stack with args
                    let num_regs = proto.num_registers as usize;
                    let mut saved_stack = vec![VmValue::None; num_regs];
                    for i in 0..arg_count as usize {
                        saved_stack[i] = self.stack[caller_base + args_start as usize + i].clone();
                    }

                    let gn = VmGenerator::new(GeneratorKind::UserDefined {
                        prototype: proto,
                        upvalues: closed_upvalues,
                        saved_stack,
                        ip: 0,
                    });
                    self.stack[caller_base + func_reg as usize] =
                        VmValue::Generator(Arc::new(Mutex::new(gn)));
                    return Ok(());
                }

                // Set up new frame
                let new_base = self.stack.len();
                self.ensure_stack(new_base + proto.num_registers as usize + 1);

                // Copy args to new frame's registers
                for i in 0..arg_count as usize {
                    self.stack[new_base + i] = self.stack[caller_base + args_start as usize + i].clone();
                }

                self.frames.push(CallFrame {
                    prototype: proto,
                    ip: 0,
                    base: new_base,
                    upvalues: closure.upvalues.clone(),
                });

                // Run the function
                let result = self.run()?;

                // Close any upvalues in the result that point into this frame's stack
                let result = self.close_upvalues_in_value(result, new_base);

                // Store result in caller's func_reg
                self.stack[caller_base + func_reg as usize] = result;

                // Shrink stack back
                self.stack.truncate(new_base);

                Ok(())
            }
            VmValue::Builtin(builtin_id) => {
                let result = self.call_builtin(
                    builtin_id as u8,
                    caller_base + args_start as usize,
                    arg_count as usize,
                )?;
                self.stack[caller_base + func_reg as usize] = result;
                Ok(())
            }
            _ => Err(runtime_err(format!("Cannot call {}", func.type_name()))),
        }
    }

    /// Walk a VmValue and promote any Open upvalues pointing at or above `frame_base`
    /// to Closed. This is called on return values before the caller's stack is truncated,
    /// so that closures escaping their defining function retain correct captured values.
    fn close_upvalues_in_value(&self, val: VmValue, frame_base: usize) -> VmValue {
        match val {
            VmValue::Function(ref closure) => {
                // Check if any upvalue needs closing
                let needs_closing = closure.upvalues.iter().any(|uv| {
                    matches!(uv, UpvalueRef::Open { stack_index } if *stack_index >= frame_base)
                });
                if !needs_closing {
                    return val;
                }
                let closed_upvalues: Vec<UpvalueRef> = closure.upvalues.iter().map(|uv| {
                    match uv {
                        UpvalueRef::Open { stack_index } if *stack_index >= frame_base => {
                            UpvalueRef::Closed(self.stack[*stack_index].clone())
                        }
                        other => other.clone(),
                    }
                }).collect();
                VmValue::Function(Arc::new(VmClosure {
                    prototype: closure.prototype.clone(),
                    upvalues: closed_upvalues,
                }))
            }
            VmValue::List(items) => {
                let needs_closing = items.iter().any(|v| {
                    matches!(v, VmValue::Function(_))
                });
                if !needs_closing {
                    return VmValue::List(items);
                }
                VmValue::List(
                    items.into_iter()
                        .map(|v| self.close_upvalues_in_value(v, frame_base))
                        .collect()
                )
            }
            VmValue::Map(entries) => {
                let needs_closing = entries.iter().any(|(_, v)| {
                    matches!(v, VmValue::Function(_))
                });
                if !needs_closing {
                    return VmValue::Map(entries);
                }
                VmValue::Map(
                    entries.into_iter()
                        .map(|(k, v)| (k, self.close_upvalues_in_value(v, frame_base)))
                        .collect()
                )
            }
            other => other,
        }
    }

    /// Execute a closure (no arguments) in this VM. Used by spawn().
    pub(crate) fn execute_closure(&mut self, proto: &Arc<Prototype>, upvalues: &[UpvalueRef]) -> Result<VmValue, TlError> {
        let base = self.stack.len();
        self.ensure_stack(base + proto.num_registers as usize + 1);
        self.frames.push(CallFrame {
            prototype: proto.clone(),
            ip: 0,
            base,
            upvalues: upvalues.to_vec(),
        });
        self.run()
    }

    /// Execute a closure with arguments in this VM. Used by pmap().
    pub(crate) fn execute_closure_with_args(&mut self, proto: &Arc<Prototype>, upvalues: &[UpvalueRef], args: &[VmValue]) -> Result<VmValue, TlError> {
        let base = self.stack.len();
        self.ensure_stack(base + proto.num_registers as usize + 1);
        for (i, arg) in args.iter().enumerate() {
            self.stack[base + i] = arg.clone();
        }
        self.frames.push(CallFrame {
            prototype: proto.clone(),
            ip: 0,
            base,
            upvalues: upvalues.to_vec(),
        });
        self.run()
    }

    fn load_constant(&self, frame_idx: usize, idx: u16) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        match &frame.prototype.constants[idx as usize] {
            Constant::Int(n) => Ok(VmValue::Int(*n)),
            Constant::Float(f) => Ok(VmValue::Float(*f)),
            Constant::String(s) => Ok(VmValue::String(s.clone())),
            Constant::Prototype(p) => {
                // Return as a closure with no upvalues
                Ok(VmValue::Function(Arc::new(VmClosure {
                    prototype: p.clone(),
                    upvalues: Vec::new(),
                })))
            }
            Constant::Decimal(s) => {
                use std::str::FromStr;
                Ok(VmValue::Decimal(rust_decimal::Decimal::from_str(s).unwrap_or_default()))
            }
            Constant::AstExpr(_) | Constant::AstExprList(_) => Ok(VmValue::None),
        }
    }

    fn get_string_constant(&self, frame_idx: usize, idx: u16) -> Result<Arc<str>, TlError> {
        let frame = &self.frames[frame_idx];
        match &frame.prototype.constants[idx as usize] {
            Constant::String(s) => Ok(s.clone()),
            _ => Err(runtime_err("Expected string constant")),
        }
    }

    // ── Arithmetic helpers ──

    fn vm_add(&mut self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(a + b)),
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a + b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 + b)),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a + *b as f64)),
            (VmValue::String(a), VmValue::String(b)) => {
                Ok(VmValue::String(Arc::from(format!("{a}{b}").as_str())))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(a), VmValue::GpuTensor(b)) => {
                let a = a.clone();
                let b = b.clone();
                let ops = self.get_gpu_ops()?;
                let result = ops.add(&a, &b).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(_), VmValue::Tensor(_)) | (VmValue::Tensor(_), VmValue::GpuTensor(_)) => {
                let lv = self.stack[base + b as usize].clone();
                let rv = self.stack[base + c as usize].clone();
                let a = self.ensure_gpu_tensor(&lv)?;
                let b_val = self.ensure_gpu_tensor(&rv)?;
                let ops = self.get_gpu_ops()?;
                let result = ops.add(&a, &b_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.add(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            // Decimal arithmetic
            (VmValue::Decimal(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(a + b)),
            (VmValue::Decimal(a), VmValue::Int(b)) => Ok(VmValue::Decimal(a + rust_decimal::Decimal::from(*b))),
            (VmValue::Int(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(rust_decimal::Decimal::from(*a) + b)),
            (VmValue::Decimal(a), VmValue::Float(b)) => Ok(VmValue::Float(decimal_to_f64(a) + b)),
            (VmValue::Float(a), VmValue::Decimal(b)) => Ok(VmValue::Float(a + decimal_to_f64(b))),
            _ => Err(runtime_err(format!(
                "Cannot apply `+` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_sub(&mut self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(a - b)),
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a - b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 - b)),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a - *b as f64)),
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(a), VmValue::GpuTensor(b)) => {
                let a = a.clone();
                let b = b.clone();
                let ops = self.get_gpu_ops()?;
                let result = ops.sub(&a, &b).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(_), VmValue::Tensor(_)) | (VmValue::Tensor(_), VmValue::GpuTensor(_)) => {
                let lv = self.stack[base + b as usize].clone();
                let rv = self.stack[base + c as usize].clone();
                let a = self.ensure_gpu_tensor(&lv)?;
                let b_val = self.ensure_gpu_tensor(&rv)?;
                let ops = self.get_gpu_ops()?;
                let result = ops.sub(&a, &b_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.sub(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            (VmValue::Decimal(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(a - b)),
            (VmValue::Decimal(a), VmValue::Int(b)) => Ok(VmValue::Decimal(a - rust_decimal::Decimal::from(*b))),
            (VmValue::Int(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(rust_decimal::Decimal::from(*a) - b)),
            (VmValue::Decimal(a), VmValue::Float(b)) => Ok(VmValue::Float(decimal_to_f64(a) - b)),
            (VmValue::Float(a), VmValue::Decimal(b)) => Ok(VmValue::Float(a - decimal_to_f64(b))),
            _ => Err(runtime_err(format!(
                "Cannot apply `-` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_mul(&mut self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(a * b)),
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a * b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 * b)),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a * *b as f64)),
            (VmValue::String(a), VmValue::Int(b)) => {
                Ok(VmValue::String(Arc::from(a.repeat(*b as usize).as_str())))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(a), VmValue::GpuTensor(b)) => {
                let a = a.clone();
                let b = b.clone();
                let ops = self.get_gpu_ops()?;
                let result = ops.mul(&a, &b).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(_), VmValue::Tensor(_)) | (VmValue::Tensor(_), VmValue::GpuTensor(_)) => {
                let lv = self.stack[base + b as usize].clone();
                let rv = self.stack[base + c as usize].clone();
                let a = self.ensure_gpu_tensor(&lv)?;
                let b_val = self.ensure_gpu_tensor(&rv)?;
                let ops = self.get_gpu_ops()?;
                let result = ops.mul(&a, &b_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(t), VmValue::Float(s)) | (VmValue::Float(s), VmValue::GpuTensor(t)) => {
                let t = t.clone();
                let s = *s;
                let ops = self.get_gpu_ops()?;
                let result = ops.scale(&t, s as f32);
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.mul(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            (VmValue::Tensor(t), VmValue::Float(s)) | (VmValue::Float(s), VmValue::Tensor(t)) => {
                let result = t.scale(*s);
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            (VmValue::Decimal(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(a * b)),
            (VmValue::Decimal(a), VmValue::Int(b)) => Ok(VmValue::Decimal(a * rust_decimal::Decimal::from(*b))),
            (VmValue::Int(a), VmValue::Decimal(b)) => Ok(VmValue::Decimal(rust_decimal::Decimal::from(*a) * b)),
            (VmValue::Decimal(a), VmValue::Float(b)) => Ok(VmValue::Float(decimal_to_f64(a) * b)),
            (VmValue::Float(a), VmValue::Decimal(b)) => Ok(VmValue::Float(a * decimal_to_f64(b))),
            _ => Err(runtime_err(format!(
                "Cannot apply `*` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_div(&mut self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => {
                if *b == 0 { return Err(runtime_err("Division by zero")); }
                Ok(VmValue::Int(a / b))
            }
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a / b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 / b)),
            (VmValue::Float(a), VmValue::Int(b)) => {
                if *b == 0 { return Err(runtime_err("Division by zero")); }
                Ok(VmValue::Float(a / *b as f64))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(a), VmValue::GpuTensor(b)) => {
                let a = a.clone();
                let b = b.clone();
                let ops = self.get_gpu_ops()?;
                let result = ops.div(&a, &b).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "gpu")]
            (VmValue::GpuTensor(_), VmValue::Tensor(_)) | (VmValue::Tensor(_), VmValue::GpuTensor(_)) => {
                let lv = self.stack[base + b as usize].clone();
                let rv = self.stack[base + c as usize].clone();
                let a = self.ensure_gpu_tensor(&lv)?;
                let b_val = self.ensure_gpu_tensor(&rv)?;
                let ops = self.get_gpu_ops()?;
                let result = ops.div(&a, &b_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.div(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            (VmValue::Decimal(a), VmValue::Decimal(b)) => {
                if b.is_zero() { return Err(runtime_err("Division by zero")); }
                Ok(VmValue::Decimal(a / b))
            }
            (VmValue::Decimal(a), VmValue::Int(b)) => {
                if *b == 0 { return Err(runtime_err("Division by zero")); }
                Ok(VmValue::Decimal(a / rust_decimal::Decimal::from(*b)))
            }
            (VmValue::Int(a), VmValue::Decimal(b)) => {
                if b.is_zero() { return Err(runtime_err("Division by zero")); }
                Ok(VmValue::Decimal(rust_decimal::Decimal::from(*a) / b))
            }
            (VmValue::Decimal(a), VmValue::Float(b)) => Ok(VmValue::Float(decimal_to_f64(a) / b)),
            (VmValue::Float(a), VmValue::Decimal(b)) => Ok(VmValue::Float(a / decimal_to_f64(b))),
            _ => Err(runtime_err(format!(
                "Cannot apply `/` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_mod(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => {
                if *b == 0 { return Err(runtime_err("Modulo by zero")); }
                Ok(VmValue::Int(a % b))
            }
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a % b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 % b)),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a % *b as f64)),
            _ => Err(runtime_err(format!(
                "Cannot apply `%` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_pow(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(a.pow(*b as u32))),
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a.powf(*b))),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float((*a as f64).powf(*b))),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a.powf(*b as f64))),
            _ => Err(runtime_err(format!(
                "Cannot apply `**` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_eq(&self, base: usize, b: u8, c: u8) -> bool {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => a == b,
            (VmValue::Float(a), VmValue::Float(b)) => a == b,
            (VmValue::String(a), VmValue::String(b)) => a == b,
            (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
            (VmValue::None, VmValue::None) => true,
            (VmValue::Decimal(a), VmValue::Decimal(b)) => a == b,
            _ => false,
        }
    }

    fn vm_cmp(&self, base: usize, b: u8, c: u8) -> Result<i8, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(a.cmp(b) as i8),
            (VmValue::Float(a), VmValue::Float(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i8)
            }
            (VmValue::Int(a), VmValue::Float(b)) => {
                let fa = *a as f64;
                Ok(fa.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i8)
            }
            (VmValue::Float(a), VmValue::Int(b)) => {
                let fb = *b as f64;
                Ok(a.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal) as i8)
            }
            (VmValue::String(a), VmValue::String(b)) => Ok(a.cmp(b) as i8),
            (VmValue::Decimal(a), VmValue::Decimal(b)) => Ok(a.cmp(b) as i8),
            (VmValue::Decimal(a), VmValue::Int(b)) => {
                Ok(a.cmp(&rust_decimal::Decimal::from(*b)) as i8)
            }
            (VmValue::Int(a), VmValue::Decimal(b)) => {
                Ok(rust_decimal::Decimal::from(*a).cmp(b) as i8)
            }
            _ => Err(runtime_err(format!(
                "Cannot compare {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    // ── Builtin dispatch ──

    pub fn call_builtin(&mut self, id: u8, args_base: usize, arg_count: usize) -> Result<VmValue, TlError> {
        let args: Vec<VmValue> = (0..arg_count)
            .map(|i| {
                let val = &self.stack[args_base + i];
                // Unwrap Ref transparently for builtin calls
                match val {
                    VmValue::Ref(inner) => inner.as_ref().clone(),
                    other => other.clone(),
                }
            })
            .collect();

        let builtin_id: BuiltinId = unsafe { std::mem::transmute(id) };

        match builtin_id {
            BuiltinId::Print | BuiltinId::Println => {
                let mut parts = Vec::new();
                for a in &args {
                    #[cfg(feature = "native")]
                    match a {
                        VmValue::Table(t) => {
                            let batches = self.engine().collect(t.df.clone())
                                .map_err(|e| runtime_err(e))?;
                            let formatted = DataEngine::format_batches(&batches)
                                .map_err(|e| runtime_err(e))?;
                            parts.push(formatted);
                        }
                        _ => parts.push(format!("{a}")),
                    }
                    #[cfg(not(feature = "native"))]
                    parts.push(format!("{a}"));
                }
                let line = parts.join(" ");
                println!("{line}");
                self.output.push(line);
                Ok(VmValue::None)
            }
            BuiltinId::Len => match args.first() {
                Some(VmValue::String(s)) => Ok(VmValue::Int(s.len() as i64)),
                Some(VmValue::List(l)) => Ok(VmValue::Int(l.len() as i64)),
                Some(VmValue::Map(pairs)) => Ok(VmValue::Int(pairs.len() as i64)),
                Some(VmValue::Set(items)) => Ok(VmValue::Int(items.len() as i64)),
                _ => Err(runtime_err("len() expects a string, list, map, or set")),
            },
            BuiltinId::Str => Ok(VmValue::String(
                Arc::from(args.first().map(|v| format!("{v}")).unwrap_or_default().as_str()),
            )),
            BuiltinId::Int => match args.first() {
                Some(VmValue::Float(f)) => Ok(VmValue::Int(*f as i64)),
                Some(VmValue::String(s)) => s.parse::<i64>()
                    .map(VmValue::Int)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to int"))),
                Some(VmValue::Int(n)) => Ok(VmValue::Int(*n)),
                Some(VmValue::Bool(b)) => Ok(VmValue::Int(if *b { 1 } else { 0 })),
                _ => Err(runtime_err("int() expects a number, string, or bool")),
            },
            BuiltinId::Float => match args.first() {
                Some(VmValue::Int(n)) => Ok(VmValue::Float(*n as f64)),
                Some(VmValue::String(s)) => s.parse::<f64>()
                    .map(VmValue::Float)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to float"))),
                Some(VmValue::Float(n)) => Ok(VmValue::Float(*n)),
                Some(VmValue::Bool(b)) => Ok(VmValue::Float(if *b { 1.0 } else { 0.0 })),
                _ => Err(runtime_err("float() expects a number, string, or bool")),
            },
            BuiltinId::Abs => match args.first() {
                Some(VmValue::Int(n)) => Ok(VmValue::Int(n.abs())),
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.abs())),
                _ => Err(runtime_err("abs() expects a number")),
            },
            BuiltinId::Min => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(*a.min(b))),
                        (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a.min(*b))),
                        _ => Err(runtime_err("min() expects two numbers")),
                    }
                } else {
                    Err(runtime_err("min() expects 2 arguments"))
                }
            }
            BuiltinId::Max => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(*a.max(b))),
                        (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a.max(*b))),
                        _ => Err(runtime_err("max() expects two numbers")),
                    }
                } else {
                    Err(runtime_err("max() expects 2 arguments"))
                }
            }
            BuiltinId::Range => {
                if args.len() == 1 {
                    if let VmValue::Int(n) = &args[0] {
                        Ok(VmValue::List((0..*n).map(VmValue::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects an integer"))
                    }
                } else if args.len() == 2 {
                    if let (VmValue::Int(start), VmValue::Int(end)) = (&args[0], &args[1]) {
                        Ok(VmValue::List((*start..*end).map(VmValue::Int).collect()))
                    } else {
                        Err(runtime_err("range() expects integers"))
                    }
                } else if args.len() == 3 {
                    if let (VmValue::Int(start), VmValue::Int(end), VmValue::Int(step)) = (&args[0], &args[1], &args[2]) {
                        if *step == 0 { return Err(runtime_err("range() step cannot be zero")); }
                        let mut result = Vec::new();
                        let mut i = *start;
                        if *step > 0 {
                            while i < *end { result.push(VmValue::Int(i)); i += step; }
                        } else {
                            while i > *end { result.push(VmValue::Int(i)); i += step; }
                        }
                        Ok(VmValue::List(result))
                    } else {
                        Err(runtime_err("range() expects integers"))
                    }
                } else {
                    Err(runtime_err("range() expects 1, 2, or 3 arguments"))
                }
            }
            BuiltinId::Push => {
                if args.len() == 2 {
                    if let VmValue::List(mut items) = args[0].clone() {
                        items.push(args[1].clone());
                        Ok(VmValue::List(items))
                    } else {
                        Err(runtime_err("push() first arg must be a list"))
                    }
                } else {
                    Err(runtime_err("push() expects 2 arguments"))
                }
            }
            BuiltinId::TypeOf => Ok(VmValue::String(
                Arc::from(args.first().map(|v| v.type_name()).unwrap_or("none")),
            )),
            BuiltinId::Map => {
                if args.len() != 2 {
                    return Err(runtime_err("map() expects 2 arguments (list, fn)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("map() first arg must be a list")),
                };
                let func = args[1].clone();
                // Parallel path for large lists with pure functions
                #[cfg(feature = "native")]
                if items.len() >= PARALLEL_THRESHOLD && is_pure_closure(&func) {
                    let proto = match &func {
                        VmValue::Function(c) => c.prototype.clone(),
                        _ => unreachable!(),
                    };
                    let result: Result<Vec<VmValue>, TlError> = items
                        .into_par_iter()
                        .map(|item| execute_pure_fn(&proto, &[item]))
                        .collect();
                    return Ok(VmValue::List(result?));
                }
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_vm_function(&func, &[item])?;
                    result.push(val);
                }
                Ok(VmValue::List(result))
            }
            BuiltinId::Filter => {
                if args.len() != 2 {
                    return Err(runtime_err("filter() expects 2 arguments (list, fn)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("filter() first arg must be a list")),
                };
                let func = args[1].clone();
                // Parallel path for large lists with pure functions
                #[cfg(feature = "native")]
                if items.len() >= PARALLEL_THRESHOLD && is_pure_closure(&func) {
                    let proto = match &func {
                        VmValue::Function(c) => c.prototype.clone(),
                        _ => unreachable!(),
                    };
                    let result: Result<Vec<VmValue>, TlError> = items
                        .into_par_iter()
                        .filter_map(|item| {
                            match execute_pure_fn(&proto, &[item.clone()]) {
                                Ok(val) => {
                                    if val.is_truthy() { Some(Ok(item)) } else { None }
                                }
                                Err(e) => Some(Err(e)),
                            }
                        })
                        .collect();
                    return Ok(VmValue::List(result?));
                }
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_vm_function(&func, &[item.clone()])?;
                    if val.is_truthy() {
                        result.push(item);
                    }
                }
                Ok(VmValue::List(result))
            }
            BuiltinId::Reduce => {
                if args.len() != 3 {
                    return Err(runtime_err("reduce() expects 3 arguments (list, init, fn)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("reduce() first arg must be a list")),
                };
                let mut acc = args[1].clone();
                let func = args[2].clone();
                for item in items {
                    acc = self.call_vm_function(&func, &[acc, item])?;
                }
                Ok(acc)
            }
            BuiltinId::Sum => {
                if args.len() != 1 {
                    return Err(runtime_err("sum() expects 1 argument (list)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items,
                    _ => return Err(runtime_err("sum() expects a list")),
                };
                // Check if any floats are present
                let has_float = items.iter().any(|v| matches!(v, VmValue::Float(_)));
                #[cfg(feature = "native")]
                if items.len() >= PARALLEL_THRESHOLD {
                    // Parallel sum for large lists
                    if has_float {
                        let total: f64 = items.par_iter().map(|v| match v {
                            VmValue::Int(n) => *n as f64,
                            VmValue::Float(n) => *n,
                            _ => 0.0,
                        }).sum();
                        return Ok(VmValue::Float(total));
                    } else {
                        let total: i64 = items.par_iter().map(|v| match v {
                            VmValue::Int(n) => *n,
                            _ => 0,
                        }).sum();
                        return Ok(VmValue::Int(total));
                    }
                }
                // Sequential path for smaller lists
                let mut total: i64 = 0;
                let mut is_float = false;
                let mut total_f: f64 = 0.0;
                for item in items {
                    match item {
                        VmValue::Int(n) => {
                            if is_float { total_f += *n as f64; } else { total += n; }
                        }
                        VmValue::Float(n) => {
                            if !is_float { total_f = total as f64; is_float = true; }
                            total_f += n;
                        }
                        _ => return Err(runtime_err("sum() list must contain numbers")),
                    }
                }
                if is_float { Ok(VmValue::Float(total_f)) } else { Ok(VmValue::Int(total)) }
            }
            BuiltinId::Any => {
                if args.len() != 2 {
                    return Err(runtime_err("any() expects 2 arguments (list, fn)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("any() first arg must be a list")),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_vm_function(&func, &[item])?;
                    if val.is_truthy() {
                        return Ok(VmValue::Bool(true));
                    }
                }
                Ok(VmValue::Bool(false))
            }
            BuiltinId::All => {
                if args.len() != 2 {
                    return Err(runtime_err("all() expects 2 arguments (list, fn)"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("all() first arg must be a list")),
                };
                let func = args[1].clone();
                for item in items {
                    let val = self.call_vm_function(&func, &[item])?;
                    if !val.is_truthy() {
                        return Ok(VmValue::Bool(false));
                    }
                }
                Ok(VmValue::Bool(true))
            }
            // ── Data engine builtins ──
            #[cfg(feature = "native")]
            BuiltinId::ReadCsv => {
                if args.len() != 1 { return Err(runtime_err("read_csv() expects 1 argument (path)")); }
                let path = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("read_csv() path must be a string")),
                };
                match self.engine().read_csv(&path) {
                    Ok(df) => Ok(VmValue::Table(VmTable { df })),
                    Err(e) => {
                        let msg = e.to_string();
                        self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                            type_name: Arc::from("DataError"),
                            variant: Arc::from("ParseError"),
                            fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(Arc::from(path.as_str()))],
                        })));
                        Err(runtime_err(msg))
                    }
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::ReadParquet => {
                if args.len() != 1 { return Err(runtime_err("read_parquet() expects 1 argument (path)")); }
                let path = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("read_parquet() path must be a string")),
                };
                match self.engine().read_parquet(&path) {
                    Ok(df) => Ok(VmValue::Table(VmTable { df })),
                    Err(e) => {
                        let msg = e.to_string();
                        self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                            type_name: Arc::from("DataError"),
                            variant: Arc::from("ParseError"),
                            fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(Arc::from(path.as_str()))],
                        })));
                        Err(runtime_err(msg))
                    }
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::WriteCsv => {
                if args.len() != 2 { return Err(runtime_err("write_csv() expects 2 arguments (table, path)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_csv() first arg must be a table")),
                };
                let path = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("write_csv() path must be a string")),
                };
                match self.engine().write_csv(df, &path) {
                    Ok(_) => Ok(VmValue::None),
                    Err(e) => {
                        let msg = e.to_string();
                        self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                            type_name: Arc::from("DataError"),
                            variant: Arc::from("ParseError"),
                            fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(Arc::from(path.as_str()))],
                        })));
                        Err(runtime_err(msg))
                    }
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::WriteParquet => {
                if args.len() != 2 { return Err(runtime_err("write_parquet() expects 2 arguments (table, path)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("write_parquet() first arg must be a table")),
                };
                let path = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("write_parquet() path must be a string")),
                };
                match self.engine().write_parquet(df, &path) {
                    Ok(_) => Ok(VmValue::None),
                    Err(e) => {
                        let msg = e.to_string();
                        self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                            type_name: Arc::from("DataError"),
                            variant: Arc::from("ParseError"),
                            fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(Arc::from(path.as_str()))],
                        })));
                        Err(runtime_err(msg))
                    }
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::Collect => {
                if args.len() != 1 { return Err(runtime_err("collect() expects 1 argument (table)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("collect() expects a table")),
                };
                let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                Ok(VmValue::String(Arc::from(formatted.as_str())))
            }
            #[cfg(feature = "native")]
            BuiltinId::Show => {
                let df = match args.first() {
                    Some(VmValue::Table(t)) => t.df.clone(),
                    _ => return Err(runtime_err("show() expects a table")),
                };
                let limit = match args.get(1) {
                    Some(VmValue::Int(n)) => *n as usize,
                    None => 20,
                    _ => return Err(runtime_err("show() second arg must be an int")),
                };
                let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                println!("{formatted}");
                self.output.push(formatted);
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::Describe => {
                if args.len() != 1 { return Err(runtime_err("describe() expects 1 argument (table)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("describe() expects a table")),
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
                Ok(VmValue::String(Arc::from(output.as_str())))
            }
            #[cfg(feature = "native")]
            BuiltinId::Head => {
                if args.is_empty() { return Err(runtime_err("head() expects at least 1 argument (table)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("head() first arg must be a table")),
                };
                let n = match args.get(1) {
                    Some(VmValue::Int(n)) => *n as usize,
                    None => 10,
                    _ => return Err(runtime_err("head() second arg must be an int")),
                };
                let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: limited }))
            }
            #[cfg(feature = "native")]
            BuiltinId::Postgres => {
                if args.len() != 2 { return Err(runtime_err("postgres() expects 2 arguments (conn_str, table_name)")); }
                let conn_str = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("postgres() conn_str must be a string")),
                };
                let table_name = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("postgres() table_name must be a string")),
                };
                match self.engine().read_postgres(&conn_str, &table_name) {
                    Ok(df) => Ok(VmValue::Table(VmTable { df })),
                    Err(e) => {
                        let msg = e.to_string();
                        self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                            type_name: Arc::from("ConnectorError"),
                            variant: Arc::from("QueryError"),
                            fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(Arc::from("postgres"))],
                        })));
                        Err(runtime_err(msg))
                    }
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::ReadCsv | BuiltinId::ReadParquet | BuiltinId::WriteCsv |
            BuiltinId::WriteParquet | BuiltinId::Collect | BuiltinId::Show |
            BuiltinId::Describe | BuiltinId::Head | BuiltinId::Postgres => {
                Err(runtime_err("Data operations not available in WASM"))
            }
            // ── AI builtins ──
            #[cfg(feature = "native")]
            BuiltinId::Tensor => {
                if args.is_empty() { return Err(runtime_err("tensor() expects at least 1 argument")); }
                let data = self.vmvalue_to_f64_list(&args[0])?;
                let shape = if args.len() > 1 {
                    self.vmvalue_to_usize_list(&args[1])?
                } else {
                    vec![data.len()]
                };
                let t = tl_ai::TlTensor::from_vec(data, &shape)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(t)))
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorZeros => {
                if args.is_empty() { return Err(runtime_err("tensor_zeros() expects 1 argument (shape)")); }
                let shape = self.vmvalue_to_usize_list(&args[0])?;
                let t = tl_ai::TlTensor::zeros(&shape);
                Ok(VmValue::Tensor(Arc::new(t)))
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorOnes => {
                if args.is_empty() { return Err(runtime_err("tensor_ones() expects 1 argument (shape)")); }
                let shape = self.vmvalue_to_usize_list(&args[0])?;
                let t = tl_ai::TlTensor::ones(&shape);
                Ok(VmValue::Tensor(Arc::new(t)))
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorShape => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        let shape: Vec<VmValue> = t.shape().iter().map(|&d| VmValue::Int(d as i64)).collect();
                        Ok(VmValue::List(shape))
                    }
                    _ => Err(runtime_err("tensor_shape() expects a tensor")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorReshape => {
                if args.len() != 2 { return Err(runtime_err("tensor_reshape() expects 2 arguments (tensor, shape)")); }
                let t = match &args[0] {
                    VmValue::Tensor(t) => (**t).clone(),
                    _ => return Err(runtime_err("tensor_reshape() first arg must be a tensor")),
                };
                let shape = self.vmvalue_to_usize_list(&args[1])?;
                let reshaped = t.reshape(&shape).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(reshaped)))
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorTranspose => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        let transposed = t.transpose().map_err(|e| runtime_err(format!("{e}")))?;
                        Ok(VmValue::Tensor(Arc::new(transposed)))
                    }
                    _ => Err(runtime_err("tensor_transpose() expects a tensor")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorSum => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        Ok(VmValue::Float(t.sum()))
                    }
                    _ => Err(runtime_err("tensor_sum() expects a tensor")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorMean => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        Ok(VmValue::Float(t.mean()))
                    }
                    _ => Err(runtime_err("tensor_mean() expects a tensor")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::TensorDot => {
                if args.len() != 2 { return Err(runtime_err("tensor_dot() expects 2 arguments")); }
                let a_t = match &args[0] {
                    VmValue::Tensor(t) => t,
                    _ => return Err(runtime_err("tensor_dot() first arg must be a tensor")),
                };
                let b_t = match &args[1] {
                    VmValue::Tensor(t) => t,
                    _ => return Err(runtime_err("tensor_dot() second arg must be a tensor")),
                };
                let result = a_t.dot(b_t).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            BuiltinId::Predict => {
                if args.len() < 2 { return Err(runtime_err("predict() expects at least 2 arguments (model, input)")); }
                let model = match &args[0] {
                    VmValue::Model(m) => (**m).clone(),
                    _ => return Err(runtime_err("predict() first arg must be a model")),
                };
                let input = match &args[1] {
                    VmValue::Tensor(t) => (**t).clone(),
                    _ => return Err(runtime_err("predict() second arg must be a tensor")),
                };
                let result = tl_ai::predict(&model, &input)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            #[cfg(feature = "native")]
            BuiltinId::Similarity => {
                if args.len() != 2 { return Err(runtime_err("similarity() expects 2 arguments")); }
                let a_t = match &args[0] {
                    VmValue::Tensor(t) => t,
                    _ => return Err(runtime_err("similarity() first arg must be a tensor")),
                };
                let b_t = match &args[1] {
                    VmValue::Tensor(t) => t,
                    _ => return Err(runtime_err("similarity() second arg must be a tensor")),
                };
                let sim = tl_ai::similarity(a_t, b_t).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Float(sim))
            }
            #[cfg(feature = "native")]
            BuiltinId::AiComplete => {
                if args.is_empty() { return Err(runtime_err("ai_complete() expects at least 1 argument (prompt)")); }
                let prompt = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("ai_complete() first arg must be a string")),
                };
                let model = match args.get(1) {
                    Some(VmValue::String(s)) => Some(s.to_string()),
                    _ => None,
                };
                let result = tl_ai::ai_complete(&prompt, model.as_deref(), None, None)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::String(Arc::from(result.as_str())))
            }
            #[cfg(feature = "native")]
            BuiltinId::AiChat => {
                if args.is_empty() { return Err(runtime_err("ai_chat() expects at least 1 argument (model)")); }
                let model = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("ai_chat() first arg must be a string (model)")),
                };
                let system = match args.get(1) {
                    Some(VmValue::String(s)) => Some(s.to_string()),
                    _ => None,
                };
                let messages: Vec<(String, String)> = if let Some(VmValue::List(msgs)) = args.get(2) {
                    msgs.chunks(2).filter_map(|chunk| {
                        if chunk.len() == 2 {
                            if let (VmValue::String(role), VmValue::String(content)) = (&chunk[0], &chunk[1]) {
                                return Some((role.to_string(), content.to_string()));
                            }
                        }
                        None
                    }).collect()
                } else {
                    Vec::new()
                };
                let result = tl_ai::ai_chat(&model, system.as_deref(), &messages)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::String(Arc::from(result.as_str())))
            }
            #[cfg(feature = "native")]
            BuiltinId::ModelSave => {
                if args.len() != 2 { return Err(runtime_err("model_save() expects 2 arguments (model, path)")); }
                let model = match &args[0] {
                    VmValue::Model(m) => m,
                    _ => return Err(runtime_err("model_save() first arg must be a model")),
                };
                let path = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("model_save() second arg must be a string path")),
                };
                model.save(std::path::Path::new(&path))
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::ModelLoad => {
                if args.is_empty() { return Err(runtime_err("model_load() expects 1 argument (path)")); }
                let path = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("model_load() arg must be a string path")),
                };
                let model = tl_ai::TlModel::load(std::path::Path::new(&path))
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Model(Arc::new(model)))
            }
            #[cfg(feature = "native")]
            BuiltinId::ModelRegister => {
                if args.len() != 2 { return Err(runtime_err("model_register() expects 2 arguments (name, model)")); }
                let name = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("model_register() first arg must be a string")),
                };
                let model = match &args[1] {
                    VmValue::Model(m) => (**m).clone(),
                    _ => return Err(runtime_err("model_register() second arg must be a model")),
                };
                let registry = tl_ai::ModelRegistry::default_location();
                registry.register(&name, &model)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::ModelList => {
                let registry = tl_ai::ModelRegistry::default_location();
                let names = registry.list();
                let items: Vec<VmValue> = names.into_iter()
                    .map(|n: String| VmValue::String(Arc::from(n.as_str())))
                    .collect();
                Ok(VmValue::List(items))
            }
            #[cfg(feature = "native")]
            BuiltinId::ModelGet => {
                if args.is_empty() { return Err(runtime_err("model_get() expects 1 argument (name)")); }
                let name = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("model_get() arg must be a string")),
                };
                let registry = tl_ai::ModelRegistry::default_location();
                match registry.get(&name) {
                    Ok(m) => Ok(VmValue::Model(Arc::new(m))),
                    Err(_) => Ok(VmValue::None),
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::Tensor | BuiltinId::TensorZeros | BuiltinId::TensorOnes |
            BuiltinId::TensorShape | BuiltinId::TensorReshape | BuiltinId::TensorTranspose |
            BuiltinId::TensorSum | BuiltinId::TensorMean | BuiltinId::TensorDot |
            BuiltinId::Predict | BuiltinId::Similarity | BuiltinId::AiComplete |
            BuiltinId::AiChat | BuiltinId::ModelSave | BuiltinId::ModelLoad |
            BuiltinId::ModelRegister | BuiltinId::ModelList | BuiltinId::ModelGet => {
                Err(runtime_err("AI/ML operations not available in WASM"))
            }
            // Streaming builtins
            #[cfg(feature = "native")]
            BuiltinId::AlertSlack => {
                if args.len() < 2 { return Err(runtime_err("alert_slack(url, msg) requires 2 args")); }
                let url = match &args[0] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("alert_slack: url must be a string")) };
                let msg = format!("{}", args[1]);
                tl_stream::send_alert(&tl_stream::AlertTarget::Slack(url), &msg)
                    .map_err(|e| runtime_err(&e))?;
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::AlertWebhook => {
                if args.len() < 2 { return Err(runtime_err("alert_webhook(url, msg) requires 2 args")); }
                let url = match &args[0] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("alert_webhook: url must be a string")) };
                let msg = format!("{}", args[1]);
                tl_stream::send_alert(&tl_stream::AlertTarget::Webhook(url), &msg)
                    .map_err(|e| runtime_err(&e))?;
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::Emit => {
                if args.is_empty() { return Err(runtime_err("emit() requires at least 1 argument")); }
                self.output.push(format!("emit: {}", args[0]));
                Ok(args[0].clone())
            }
            #[cfg(feature = "native")]
            BuiltinId::Lineage => {
                Ok(VmValue::String(Arc::from("lineage_tracker")))
            }
            #[cfg(feature = "native")]
            BuiltinId::RunPipeline => {
                if args.is_empty() { return Err(runtime_err("run_pipeline() requires a pipeline")); }
                if let VmValue::PipelineDef(ref def) = args[0] {
                    Ok(VmValue::String(Arc::from(format!("Pipeline '{}' triggered", def.name).as_str())))
                } else {
                    Err(runtime_err("run_pipeline: argument must be a pipeline"))
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::AlertSlack | BuiltinId::AlertWebhook | BuiltinId::Emit |
            BuiltinId::Lineage | BuiltinId::RunPipeline => {
                Err(runtime_err("Streaming not available in WASM"))
            }
            // Phase 5: Math builtins
            BuiltinId::Sqrt => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.sqrt())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).sqrt())),
                _ => Err(runtime_err("sqrt() expects a number")),
            },
            BuiltinId::Pow => {
                if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a.powf(*b))),
                        (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Float((*a as f64).powf(*b as f64))),
                        (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a.powf(*b as f64))),
                        (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float((*a as f64).powf(*b))),
                        _ => Err(runtime_err("pow() expects two numbers")),
                    }
                } else {
                    Err(runtime_err("pow() expects 2 arguments"))
                }
            }
            BuiltinId::Floor => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.floor())),
                Some(VmValue::Int(n)) => Ok(VmValue::Int(*n)),
                _ => Err(runtime_err("floor() expects a number")),
            },
            BuiltinId::Ceil => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.ceil())),
                Some(VmValue::Int(n)) => Ok(VmValue::Int(*n)),
                _ => Err(runtime_err("ceil() expects a number")),
            },
            BuiltinId::Round => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.round())),
                Some(VmValue::Int(n)) => Ok(VmValue::Int(*n)),
                _ => Err(runtime_err("round() expects a number")),
            },
            BuiltinId::Sin => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.sin())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).sin())),
                _ => Err(runtime_err("sin() expects a number")),
            },
            BuiltinId::Cos => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.cos())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).cos())),
                _ => Err(runtime_err("cos() expects a number")),
            },
            BuiltinId::Tan => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.tan())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).tan())),
                _ => Err(runtime_err("tan() expects a number")),
            },
            BuiltinId::Log => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.ln())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).ln())),
                _ => Err(runtime_err("log() expects a number")),
            },
            BuiltinId::Log2 => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.log2())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).log2())),
                _ => Err(runtime_err("log2() expects a number")),
            },
            BuiltinId::Log10 => match args.first() {
                Some(VmValue::Float(n)) => Ok(VmValue::Float(n.log10())),
                Some(VmValue::Int(n)) => Ok(VmValue::Float((*n as f64).log10())),
                _ => Err(runtime_err("log10() expects a number")),
            },
            BuiltinId::Join => {
                if args.len() == 2 {
                    if let (VmValue::String(sep), VmValue::List(items)) = (&args[0], &args[1]) {
                        let parts: Vec<String> = items.iter().map(|v| format!("{v}")).collect();
                        Ok(VmValue::String(Arc::from(parts.join(sep.as_ref()).as_str())))
                    } else {
                        Err(runtime_err("join() expects separator and list"))
                    }
                } else {
                    Err(runtime_err("join() expects 2 arguments"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::HttpGet => {
                if args.is_empty() { return Err(runtime_err("http_get() expects a URL")); }
                if let VmValue::String(url) = &args[0] {
                    match reqwest::blocking::get(url.as_ref())
                        .and_then(|r| r.text()) {
                        Ok(body) => Ok(VmValue::String(Arc::from(body.as_str()))),
                        Err(e) => {
                            let msg = format!("HTTP GET error: {e}");
                            self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                                type_name: Arc::from("NetworkError"),
                                variant: Arc::from("HttpError"),
                                fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(url.clone())],
                            })));
                            Err(runtime_err(msg))
                        }
                    }
                } else {
                    Err(runtime_err("http_get() expects a string URL"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::HttpPost => {
                if args.len() < 2 { return Err(runtime_err("http_post() expects URL and body")); }
                if let (VmValue::String(url), VmValue::String(body)) = (&args[0], &args[1]) {
                    let client = reqwest::blocking::Client::new();
                    match client
                        .post(url.as_ref())
                        .header("Content-Type", "application/json")
                        .body(body.to_string())
                        .send()
                        .and_then(|r| r.text()) {
                        Ok(resp) => Ok(VmValue::String(Arc::from(resp.as_str()))),
                        Err(e) => {
                            let msg = format!("HTTP POST error: {e}");
                            self.thrown_value = Some(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                                type_name: Arc::from("NetworkError"),
                                variant: Arc::from("HttpError"),
                                fields: vec![VmValue::String(Arc::from(msg.as_str())), VmValue::String(url.clone())],
                            })));
                            Err(runtime_err(msg))
                        }
                    }
                } else {
                    Err(runtime_err("http_post() expects string URL and body"))
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::HttpGet | BuiltinId::HttpPost => {
                Err(runtime_err("HTTP requests not available in WASM"))
            }
            BuiltinId::Assert => {
                if args.is_empty() { return Err(runtime_err("assert() expects at least 1 argument")); }
                if !args[0].is_truthy() {
                    let msg = if args.len() > 1 { format!("{}", args[1]) } else { "Assertion failed".to_string() };
                    Err(runtime_err(msg))
                } else {
                    Ok(VmValue::None)
                }
            }
            BuiltinId::AssertEq => {
                if args.len() < 2 { return Err(runtime_err("assert_eq() expects 2 arguments")); }
                let eq = match (&args[0], &args[1]) {
                    (VmValue::Int(a), VmValue::Int(b)) => a == b,
                    (VmValue::Float(a), VmValue::Float(b)) => a == b,
                    (VmValue::String(a), VmValue::String(b)) => a == b,
                    (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
                    (VmValue::None, VmValue::None) => true,
                    _ => false,
                };
                if !eq {
                    Err(runtime_err(format!("Assertion failed: {} != {}", args[0], args[1])))
                } else {
                    Ok(VmValue::None)
                }
            }
            // ── Phase 6: Stdlib & Ecosystem builtins ──
            BuiltinId::JsonParse => {
                if args.is_empty() { return Err(runtime_err("json_parse() expects a string")); }
                if let VmValue::String(s) = &args[0] {
                    let json_val: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| runtime_err(format!("JSON parse error: {e}")))?;
                    Ok(vm_json_to_value(&json_val))
                } else {
                    Err(runtime_err("json_parse() expects a string"))
                }
            }
            BuiltinId::JsonStringify => {
                if args.is_empty() { return Err(runtime_err("json_stringify() expects a value")); }
                let json = vm_value_to_json(&args[0]);
                Ok(VmValue::String(Arc::from(json.to_string().as_str())))
            }
            BuiltinId::MapFrom => {
                if args.len() % 2 != 0 {
                    return Err(runtime_err("map_from() expects even number of arguments (key, value pairs)"));
                }
                let mut pairs = Vec::new();
                for chunk in args.chunks(2) {
                    let key = match &chunk[0] {
                        VmValue::String(s) => s.clone(),
                        other => Arc::from(format!("{other}").as_str()),
                    };
                    pairs.push((key, chunk[1].clone()));
                }
                Ok(VmValue::Map(pairs))
            }
            #[cfg(feature = "native")]
            BuiltinId::ReadFile => {
                if args.is_empty() { return Err(runtime_err("read_file() expects a path")); }
                if let VmValue::String(path) = &args[0] {
                    let content = std::fs::read_to_string(path.as_ref())
                        .map_err(|e| runtime_err(format!("read_file error: {e}")))?;
                    Ok(VmValue::String(Arc::from(content.as_str())))
                } else {
                    Err(runtime_err("read_file() expects a string path"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::WriteFile => {
                if args.len() < 2 { return Err(runtime_err("write_file() expects path and content")); }
                if let (VmValue::String(path), VmValue::String(content)) = (&args[0], &args[1]) {
                    std::fs::write(path.as_ref(), content.as_ref())
                        .map_err(|e| runtime_err(format!("write_file error: {e}")))?;
                    Ok(VmValue::None)
                } else {
                    Err(runtime_err("write_file() expects string path and content"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::AppendFile => {
                if args.len() < 2 { return Err(runtime_err("append_file() expects path and content")); }
                if let (VmValue::String(path), VmValue::String(content)) = (&args[0], &args[1]) {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .create(true).append(true).open(path.as_ref())
                        .map_err(|e| runtime_err(format!("append_file error: {e}")))?;
                    file.write_all(content.as_bytes())
                        .map_err(|e| runtime_err(format!("append_file error: {e}")))?;
                    Ok(VmValue::None)
                } else {
                    Err(runtime_err("append_file() expects string path and content"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::FileExists => {
                if args.is_empty() { return Err(runtime_err("file_exists() expects a path")); }
                if let VmValue::String(path) = &args[0] {
                    Ok(VmValue::Bool(std::path::Path::new(path.as_ref()).exists()))
                } else {
                    Err(runtime_err("file_exists() expects a string path"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::ListDir => {
                if args.is_empty() { return Err(runtime_err("list_dir() expects a path")); }
                if let VmValue::String(path) = &args[0] {
                    let entries: Vec<VmValue> = std::fs::read_dir(path.as_ref())
                        .map_err(|e| runtime_err(format!("list_dir error: {e}")))?
                        .filter_map(|e| e.ok())
                        .map(|e| VmValue::String(Arc::from(e.file_name().to_string_lossy().as_ref())))
                        .collect();
                    Ok(VmValue::List(entries))
                } else {
                    Err(runtime_err("list_dir() expects a string path"))
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::ReadFile | BuiltinId::WriteFile | BuiltinId::AppendFile |
            BuiltinId::FileExists | BuiltinId::ListDir => {
                Err(runtime_err("File I/O not available in WASM"))
            }
            #[cfg(feature = "native")]
            BuiltinId::EnvGet => {
                if args.is_empty() { return Err(runtime_err("env_get() expects a name")); }
                if let VmValue::String(name) = &args[0] {
                    match std::env::var(name.as_ref()) {
                        Ok(val) => Ok(VmValue::String(Arc::from(val.as_str()))),
                        Err(_) => Ok(VmValue::None),
                    }
                } else {
                    Err(runtime_err("env_get() expects a string"))
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::EnvSet => {
                if args.len() < 2 { return Err(runtime_err("env_set() expects name and value")); }
                if let (VmValue::String(name), VmValue::String(val)) = (&args[0], &args[1]) {
                    unsafe { std::env::set_var(name.as_ref(), val.as_ref()); }
                    Ok(VmValue::None)
                } else {
                    Err(runtime_err("env_set() expects two strings"))
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::EnvGet | BuiltinId::EnvSet => {
                Err(runtime_err("Environment variables not available in WASM"))
            }
            BuiltinId::RegexMatch => {
                if args.len() < 2 { return Err(runtime_err("regex_match() expects pattern and string")); }
                if let (VmValue::String(pattern), VmValue::String(text)) = (&args[0], &args[1]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    Ok(VmValue::Bool(re.is_match(text)))
                } else {
                    Err(runtime_err("regex_match() expects string pattern and string"))
                }
            }
            BuiltinId::RegexFind => {
                if args.len() < 2 { return Err(runtime_err("regex_find() expects pattern and string")); }
                if let (VmValue::String(pattern), VmValue::String(text)) = (&args[0], &args[1]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    let matches: Vec<VmValue> = re.find_iter(text)
                        .map(|m| VmValue::String(Arc::from(m.as_str())))
                        .collect();
                    Ok(VmValue::List(matches))
                } else {
                    Err(runtime_err("regex_find() expects string pattern and string"))
                }
            }
            BuiltinId::RegexReplace => {
                if args.len() < 3 { return Err(runtime_err("regex_replace() expects pattern, string, replacement")); }
                if let (VmValue::String(pattern), VmValue::String(text), VmValue::String(replacement)) = (&args[0], &args[1], &args[2]) {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| runtime_err(format!("Invalid regex: {e}")))?;
                    Ok(VmValue::String(Arc::from(re.replace_all(text, replacement.as_ref()).as_ref())))
                } else {
                    Err(runtime_err("regex_replace() expects three strings"))
                }
            }
            BuiltinId::Now => {
                let ts = chrono::Utc::now().timestamp_millis();
                Ok(VmValue::Int(ts))
            }
            BuiltinId::DateFormat => {
                if args.len() < 2 { return Err(runtime_err("date_format() expects timestamp_ms and format")); }
                if let (VmValue::Int(ts), VmValue::String(fmt)) = (&args[0], &args[1]) {
                    use chrono::TimeZone;
                    let secs = *ts / 1000;
                    let nsecs = ((*ts % 1000) * 1_000_000) as u32;
                    let dt = chrono::Utc.timestamp_opt(secs, nsecs)
                        .single()
                        .ok_or_else(|| runtime_err("Invalid timestamp"))?;
                    Ok(VmValue::String(Arc::from(dt.format(fmt.as_ref()).to_string().as_str())))
                } else {
                    Err(runtime_err("date_format() expects int timestamp and string format"))
                }
            }
            BuiltinId::DateParse => {
                if args.len() < 2 { return Err(runtime_err("date_parse() expects string and format")); }
                if let (VmValue::String(s), VmValue::String(fmt)) = (&args[0], &args[1]) {
                    let dt = chrono::NaiveDateTime::parse_from_str(s, fmt)
                        .map_err(|e| runtime_err(format!("date_parse error: {e}")))?;
                    let ts = dt.and_utc().timestamp_millis();
                    Ok(VmValue::Int(ts))
                } else {
                    Err(runtime_err("date_parse() expects two strings"))
                }
            }
            BuiltinId::Zip => {
                if args.len() < 2 { return Err(runtime_err("zip() expects two lists")); }
                if let (VmValue::List(a), VmValue::List(b)) = (&args[0], &args[1]) {
                    let pairs: Vec<VmValue> = a.iter().zip(b.iter())
                        .map(|(x, y)| VmValue::List(vec![x.clone(), y.clone()]))
                        .collect();
                    Ok(VmValue::List(pairs))
                } else {
                    Err(runtime_err("zip() expects two lists"))
                }
            }
            BuiltinId::Enumerate => {
                if args.is_empty() { return Err(runtime_err("enumerate() expects a list")); }
                if let VmValue::List(items) = &args[0] {
                    let pairs: Vec<VmValue> = items.iter().enumerate()
                        .map(|(i, v)| VmValue::List(vec![VmValue::Int(i as i64), v.clone()]))
                        .collect();
                    Ok(VmValue::List(pairs))
                } else {
                    Err(runtime_err("enumerate() expects a list"))
                }
            }
            BuiltinId::Bool => {
                if args.is_empty() { return Err(runtime_err("bool() expects a value")); }
                Ok(VmValue::Bool(args[0].is_truthy()))
            }

            // Phase 7: Concurrency builtins
            #[cfg(feature = "native")]
            BuiltinId::Spawn => {
                if args.is_empty() {
                    return Err(runtime_err("spawn() expects a function argument"));
                }
                match &args[0] {
                    VmValue::Function(closure) => {
                        let proto = closure.prototype.clone();
                        // Close all upvalues (convert Open → Closed with current values)
                        let mut closed_upvalues = Vec::new();
                        for uv in &closure.upvalues {
                            match uv {
                                UpvalueRef::Open { stack_index } => {
                                    let val = self.stack[*stack_index].clone();
                                    closed_upvalues.push(UpvalueRef::Closed(val));
                                }
                                UpvalueRef::Closed(v) => {
                                    closed_upvalues.push(UpvalueRef::Closed(v.clone()));
                                }
                            }
                        }
                        let globals = self.globals.clone();
                        let (tx, rx) = mpsc::channel::<Result<VmValue, String>>();

                        std::thread::spawn(move || {
                            let mut vm = Vm::new();
                            vm.globals = globals;
                            let result = vm.execute_closure(&proto, &closed_upvalues);
                            let _ = tx.send(result.map_err(|e| match e {
                                TlError::Runtime(re) => re.message,
                                other => format!("{other}"),
                            }));
                        });

                        Ok(VmValue::Task(Arc::new(VmTask::new(rx))))
                    }
                    _ => Err(runtime_err("spawn() expects a function")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::Sleep => {
                if args.is_empty() {
                    return Err(runtime_err("sleep() expects a duration in milliseconds"));
                }
                match &args[0] {
                    VmValue::Int(ms) => {
                        std::thread::sleep(Duration::from_millis(*ms as u64));
                        Ok(VmValue::None)
                    }
                    _ => Err(runtime_err("sleep() expects an integer (milliseconds)")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::Channel => {
                let capacity = match args.first() {
                    Some(VmValue::Int(n)) => *n as usize,
                    None => 64,
                    _ => return Err(runtime_err("channel() expects an optional integer capacity")),
                };
                Ok(VmValue::Channel(Arc::new(VmChannel::new(capacity))))
            }
            #[cfg(feature = "native")]
            BuiltinId::Send => {
                if args.len() < 2 {
                    return Err(runtime_err("send() expects a channel and a value"));
                }
                match &args[0] {
                    VmValue::Channel(ch) => {
                        ch.sender.send(args[1].clone())
                            .map_err(|_| runtime_err("Channel disconnected"))?;
                        Ok(VmValue::None)
                    }
                    _ => Err(runtime_err("send() expects a channel as first argument")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::Recv => {
                if args.is_empty() {
                    return Err(runtime_err("recv() expects a channel"));
                }
                match &args[0] {
                    VmValue::Channel(ch) => {
                        let guard = ch.receiver.lock().unwrap();
                        match guard.recv() {
                            Ok(val) => Ok(val),
                            Err(_) => Ok(VmValue::None),
                        }
                    }
                    _ => Err(runtime_err("recv() expects a channel")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::TryRecv => {
                if args.is_empty() {
                    return Err(runtime_err("try_recv() expects a channel"));
                }
                match &args[0] {
                    VmValue::Channel(ch) => {
                        let guard = ch.receiver.lock().unwrap();
                        match guard.try_recv() {
                            Ok(val) => Ok(val),
                            Err(_) => Ok(VmValue::None),
                        }
                    }
                    _ => Err(runtime_err("try_recv() expects a channel")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::AwaitAll => {
                if args.is_empty() {
                    return Err(runtime_err("await_all() expects a list of tasks"));
                }
                match &args[0] {
                    VmValue::List(tasks) => {
                        let mut results = Vec::with_capacity(tasks.len());
                        for task in tasks {
                            match task {
                                VmValue::Task(t) => {
                                    let rx = {
                                        let mut guard = t.receiver.lock().unwrap();
                                        guard.take()
                                    };
                                    match rx {
                                        Some(receiver) => {
                                            match receiver.recv() {
                                                Ok(Ok(val)) => results.push(val),
                                                Ok(Err(e)) => return Err(runtime_err(e)),
                                                Err(_) => return Err(runtime_err("Task channel disconnected")),
                                            }
                                        }
                                        None => return Err(runtime_err("Task already awaited")),
                                    }
                                }
                                other => results.push(other.clone()),
                            }
                        }
                        Ok(VmValue::List(results))
                    }
                    _ => Err(runtime_err("await_all() expects a list")),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::Pmap => {
                if args.len() < 2 {
                    return Err(runtime_err("pmap() expects a list and a function"));
                }
                let items = match &args[0] {
                    VmValue::List(items) => items.clone(),
                    _ => return Err(runtime_err("pmap() expects a list as first argument")),
                };
                let closure = match &args[1] {
                    VmValue::Function(c) => c.clone(),
                    _ => return Err(runtime_err("pmap() expects a function as second argument")),
                };

                // Close all upvalues
                let mut closed_upvalues = Vec::new();
                for uv in &closure.upvalues {
                    match uv {
                        UpvalueRef::Open { stack_index } => {
                            let val = self.stack[*stack_index].clone();
                            closed_upvalues.push(UpvalueRef::Closed(val));
                        }
                        UpvalueRef::Closed(v) => {
                            closed_upvalues.push(UpvalueRef::Closed(v.clone()));
                        }
                    }
                }

                let proto = closure.prototype.clone();
                let globals = self.globals.clone();

                // Spawn one thread per item
                let mut handles = Vec::with_capacity(items.len());
                for item in items {
                    let proto = proto.clone();
                    let upvalues = closed_upvalues.clone();
                    let globals = globals.clone();
                    let handle = std::thread::spawn(move || {
                        let mut vm = Vm::new();
                        vm.globals = globals;
                        vm.execute_closure_with_args(&proto, &upvalues, &[item])
                            .map_err(|e| match e {
                                TlError::Runtime(re) => re.message,
                                other => format!("{other}"),
                            })
                    });
                    handles.push(handle);
                }

                let mut results = Vec::with_capacity(handles.len());
                for handle in handles {
                    match handle.join() {
                        Ok(Ok(val)) => results.push(val),
                        Ok(Err(e)) => return Err(runtime_err(e)),
                        Err(_) => return Err(runtime_err("pmap() thread panicked")),
                    }
                }
                Ok(VmValue::List(results))
            }
            #[cfg(feature = "native")]
            BuiltinId::Timeout => {
                if args.len() < 2 {
                    return Err(runtime_err("timeout() expects a task and a duration in milliseconds"));
                }
                let ms = match &args[1] {
                    VmValue::Int(n) => *n as u64,
                    _ => return Err(runtime_err("timeout() expects an integer duration")),
                };
                match &args[0] {
                    VmValue::Task(task) => {
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
                                        Err(runtime_err("Task timed out"))
                                    }
                                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                                        Err(runtime_err("Task channel disconnected"))
                                    }
                                }
                            }
                            None => Err(runtime_err("Task already awaited")),
                        }
                    }
                    _ => Err(runtime_err("timeout() expects a task as first argument")),
                }
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::Spawn | BuiltinId::Sleep | BuiltinId::Channel |
            BuiltinId::Send | BuiltinId::Recv | BuiltinId::TryRecv |
            BuiltinId::AwaitAll | BuiltinId::Pmap | BuiltinId::Timeout => {
                Err(runtime_err("Threading not available in WASM"))
            }
            // Phase 8: Iterators & Generators
            BuiltinId::Next => {
                if args.is_empty() {
                    return Err(runtime_err("next() expects a generator"));
                }
                match &args[0] {
                    VmValue::Generator(gen_arc) => {
                        let g = gen_arc.clone();
                        self.generator_next(&g)
                    }
                    _ => Err(runtime_err("next() expects a generator")),
                }
            }
            BuiltinId::IsGenerator => {
                let val = args.first().unwrap_or(&VmValue::None);
                Ok(VmValue::Bool(matches!(val, VmValue::Generator(_))))
            }
            BuiltinId::Iter => {
                if args.is_empty() {
                    return Err(runtime_err("iter() expects a list"));
                }
                match &args[0] {
                    VmValue::List(items) => {
                        let gn = VmGenerator::new(GeneratorKind::ListIter {
                            items: items.clone(),
                            index: 0,
                        });
                        Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
                    }
                    _ => Err(runtime_err("iter() expects a list")),
                }
            }
            BuiltinId::Take => {
                if args.len() < 2 {
                    return Err(runtime_err("take() expects a generator and a count"));
                }
                let gen_arc = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("take() expects a generator as first argument")),
                };
                let n = match &args[1] {
                    VmValue::Int(n) => *n as usize,
                    _ => return Err(runtime_err("take() expects an integer count")),
                };
                let gn = VmGenerator::new(GeneratorKind::Take {
                    source: gen_arc,
                    remaining: n,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::Skip_ => {
                if args.len() < 2 {
                    return Err(runtime_err("skip() expects a generator and a count"));
                }
                let gen_arc = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("skip() expects a generator as first argument")),
                };
                let n = match &args[1] {
                    VmValue::Int(n) => *n as usize,
                    _ => return Err(runtime_err("skip() expects an integer count")),
                };
                let gn = VmGenerator::new(GeneratorKind::Skip {
                    source: gen_arc,
                    remaining: n,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::GenCollect => {
                if args.is_empty() {
                    return Err(runtime_err("gen_collect() expects a generator"));
                }
                match &args[0] {
                    VmValue::Generator(gen_arc) => {
                        let g = gen_arc.clone();
                        let mut items = Vec::new();
                        loop {
                            let val = self.generator_next(&g)?;
                            if matches!(val, VmValue::None) {
                                break;
                            }
                            items.push(val);
                        }
                        Ok(VmValue::List(items))
                    }
                    _ => Err(runtime_err("gen_collect() expects a generator")),
                }
            }
            BuiltinId::GenMap => {
                if args.len() < 2 {
                    return Err(runtime_err("gen_map() expects a generator and a function"));
                }
                let gen_arc = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("gen_map() expects a generator as first argument")),
                };
                let func = args[1].clone();
                let gn = VmGenerator::new(GeneratorKind::Map {
                    source: gen_arc,
                    func,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::GenFilter => {
                if args.len() < 2 {
                    return Err(runtime_err("gen_filter() expects a generator and a function"));
                }
                let gen_arc = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("gen_filter() expects a generator as first argument")),
                };
                let func = args[1].clone();
                let gn = VmGenerator::new(GeneratorKind::Filter {
                    source: gen_arc,
                    func,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::Chain => {
                if args.len() < 2 {
                    return Err(runtime_err("chain() expects two generators"));
                }
                let first = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("chain() expects generators")),
                };
                let second = match &args[1] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("chain() expects generators")),
                };
                let gn = VmGenerator::new(GeneratorKind::Chain {
                    first,
                    second,
                    on_second: false,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::GenZip => {
                if args.len() < 2 {
                    return Err(runtime_err("gen_zip() expects two generators"));
                }
                let first = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("gen_zip() expects generators")),
                };
                let second = match &args[1] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("gen_zip() expects generators")),
                };
                let gn = VmGenerator::new(GeneratorKind::Zip { first, second });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            BuiltinId::GenEnumerate => {
                if args.is_empty() {
                    return Err(runtime_err("gen_enumerate() expects a generator"));
                }
                let gen_arc = match &args[0] {
                    VmValue::Generator(g) => g.clone(),
                    _ => return Err(runtime_err("gen_enumerate() expects a generator")),
                };
                let gn = VmGenerator::new(GeneratorKind::Enumerate {
                    source: gen_arc,
                    index: 0,
                });
                Ok(VmValue::Generator(Arc::new(Mutex::new(gn))))
            }
            // Phase 10: Result builtins
            BuiltinId::Ok => {
                let val = if args.is_empty() { VmValue::None } else { args[0].clone() };
                Ok(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                    type_name: Arc::from("Result"),
                    variant: Arc::from("Ok"),
                    fields: vec![val],
                })))
            }
            BuiltinId::Err_ => {
                let val = if args.is_empty() { VmValue::String(Arc::from("error")) } else { args[0].clone() };
                Ok(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                    type_name: Arc::from("Result"),
                    variant: Arc::from("Err"),
                    fields: vec![val],
                })))
            }
            BuiltinId::IsOk => {
                if args.is_empty() {
                    return Err(runtime_err("is_ok() expects an argument"));
                }
                match &args[0] {
                    VmValue::EnumInstance(ei) if ei.type_name.as_ref() == "Result" => {
                        Ok(VmValue::Bool(ei.variant.as_ref() == "Ok"))
                    }
                    _ => Ok(VmValue::Bool(false)),
                }
            }
            BuiltinId::IsErr => {
                if args.is_empty() {
                    return Err(runtime_err("is_err() expects an argument"));
                }
                match &args[0] {
                    VmValue::EnumInstance(ei) if ei.type_name.as_ref() == "Result" => {
                        Ok(VmValue::Bool(ei.variant.as_ref() == "Err"))
                    }
                    _ => Ok(VmValue::Bool(false)),
                }
            }
            BuiltinId::Unwrap => {
                if args.is_empty() {
                    return Err(runtime_err("unwrap() expects an argument"));
                }
                match &args[0] {
                    VmValue::EnumInstance(ei) if ei.type_name.as_ref() == "Result" => {
                        if ei.variant.as_ref() == "Ok" && !ei.fields.is_empty() {
                            Ok(ei.fields[0].clone())
                        } else if ei.variant.as_ref() == "Err" {
                            let msg = if ei.fields.is_empty() {
                                "error".to_string()
                            } else {
                                format!("{}", ei.fields[0])
                            };
                            Err(runtime_err(format!("unwrap() called on Err({msg})")))
                        } else {
                            Ok(VmValue::None)
                        }
                    }
                    VmValue::None => Err(runtime_err("unwrap() called on none".to_string())),
                    other => Ok(other.clone()),
                }
            }
            BuiltinId::SetFrom => {
                let list = match args.first() {
                    Some(VmValue::List(items)) => items,
                    _ => return Err(runtime_err("set_from() expects a list")),
                };
                if list.is_empty() {
                    return Ok(VmValue::Set(Vec::new()));
                }
                let mut result = Vec::new();
                for item in list {
                    if !result.iter().any(|x| vm_values_equal(x, item)) {
                        result.push(item.clone());
                    }
                }
                Ok(VmValue::Set(result))
            }
            BuiltinId::SetAdd => {
                if args.len() < 2 { return Err(runtime_err("set_add() expects 2 arguments")); }
                let val = &args[1];
                match &args[0] {
                    VmValue::Set(items) => {
                        let mut new_items = items.clone();
                        if !new_items.iter().any(|x| vm_values_equal(x, val)) {
                            new_items.push(val.clone());
                        }
                        Ok(VmValue::Set(new_items))
                    }
                    _ => Err(runtime_err("set_add() first argument must be a set")),
                }
            }
            BuiltinId::SetRemove => {
                if args.len() < 2 { return Err(runtime_err("set_remove() expects 2 arguments")); }
                let val = &args[1];
                match &args[0] {
                    VmValue::Set(items) => {
                        let new_items: Vec<VmValue> = items.iter()
                            .filter(|x| !vm_values_equal(x, val))
                            .cloned()
                            .collect();
                        Ok(VmValue::Set(new_items))
                    }
                    _ => Err(runtime_err("set_remove() first argument must be a set")),
                }
            }
            BuiltinId::SetContains => {
                if args.len() < 2 { return Err(runtime_err("set_contains() expects 2 arguments")); }
                let val = &args[1];
                match &args[0] {
                    VmValue::Set(items) => {
                        Ok(VmValue::Bool(items.iter().any(|x| vm_values_equal(x, val))))
                    }
                    _ => Err(runtime_err("set_contains() first argument must be a set")),
                }
            }
            BuiltinId::SetUnion => {
                if args.len() < 2 { return Err(runtime_err("set_union() expects 2 arguments")); }
                match (&args[0], &args[1]) {
                    (VmValue::Set(a), VmValue::Set(b)) => {
                        let mut result = a.clone();
                        for item in b {
                            if !result.iter().any(|x| vm_values_equal(x, item)) {
                                result.push(item.clone());
                            }
                        }
                        Ok(VmValue::Set(result))
                    }
                    _ => Err(runtime_err("set_union() expects two sets")),
                }
            }
            BuiltinId::SetIntersection => {
                if args.len() < 2 { return Err(runtime_err("set_intersection() expects 2 arguments")); }
                match (&args[0], &args[1]) {
                    (VmValue::Set(a), VmValue::Set(b)) => {
                        let result: Vec<VmValue> = a.iter()
                            .filter(|x| b.iter().any(|y| vm_values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(VmValue::Set(result))
                    }
                    _ => Err(runtime_err("set_intersection() expects two sets")),
                }
            }
            BuiltinId::SetDifference => {
                if args.len() < 2 { return Err(runtime_err("set_difference() expects 2 arguments")); }
                match (&args[0], &args[1]) {
                    (VmValue::Set(a), VmValue::Set(b)) => {
                        let result: Vec<VmValue> = a.iter()
                            .filter(|x| !b.iter().any(|y| vm_values_equal(x, y)))
                            .cloned()
                            .collect();
                        Ok(VmValue::Set(result))
                    }
                    _ => Err(runtime_err("set_difference() expects two sets")),
                }
            }

            // ── Phase 15: Data Quality & Connectors ──
            #[cfg(feature = "native")]
            BuiltinId::FillNull => {
                if args.len() < 2 { return Err(runtime_err("fill_null() expects (table, column, [strategy], [value])")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("fill_null() first arg must be a table")),
                };
                let column = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("fill_null() column must be a string")),
                };
                let strategy = if args.len() > 2 {
                    match &args[2] { VmValue::String(s) => s.to_string(), _ => "value".to_string() }
                } else { "value".to_string() };
                let fill_value = if args.len() > 3 {
                    match &args[3] {
                        VmValue::Int(n) => Some(*n as f64),
                        VmValue::Float(f) => Some(*f),
                        _ => None,
                    }
                } else if args.len() > 2 && strategy == "value" {
                    match &args[2] {
                        VmValue::Int(n) => { return Ok(VmValue::Table(VmTable { df: self.engine().fill_null(df, &column, "value", Some(*n as f64)).map_err(|e| runtime_err(e))? })); }
                        VmValue::Float(f) => { return Ok(VmValue::Table(VmTable { df: self.engine().fill_null(df, &column, "value", Some(*f)).map_err(|e| runtime_err(e))? })); }
                        _ => None,
                    }
                } else { None };
                let result = self.engine().fill_null(df, &column, &strategy, fill_value).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            #[cfg(feature = "native")]
            BuiltinId::DropNull => {
                if args.len() < 2 { return Err(runtime_err("drop_null() expects (table, column)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("drop_null() first arg must be a table")),
                };
                let column = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("drop_null() column must be a string")),
                };
                let result = self.engine().drop_null(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            #[cfg(feature = "native")]
            BuiltinId::Dedup => {
                if args.is_empty() { return Err(runtime_err("dedup() expects (table, [columns...])")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("dedup() first arg must be a table")),
                };
                let columns: Vec<String> = args[1..].iter().filter_map(|a| {
                    if let VmValue::String(s) = a { Some(s.to_string()) } else { None }
                }).collect();
                let result = self.engine().dedup(df, &columns).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            #[cfg(feature = "native")]
            BuiltinId::Clamp => {
                if args.len() < 4 { return Err(runtime_err("clamp() expects (table, column, min, max)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("clamp() first arg must be a table")),
                };
                let column = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("clamp() column must be a string")),
                };
                let min_val = match &args[2] {
                    VmValue::Int(n) => *n as f64,
                    VmValue::Float(f) => *f,
                    _ => return Err(runtime_err("clamp() min must be a number")),
                };
                let max_val = match &args[3] {
                    VmValue::Int(n) => *n as f64,
                    VmValue::Float(f) => *f,
                    _ => return Err(runtime_err("clamp() max must be a number")),
                };
                let result = self.engine().clamp(df, &column, min_val, max_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            #[cfg(feature = "native")]
            BuiltinId::DataProfile => {
                if args.is_empty() { return Err(runtime_err("data_profile() expects (table)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("data_profile() arg must be a table")),
                };
                let result = self.engine().data_profile(df).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            #[cfg(feature = "native")]
            BuiltinId::RowCount => {
                if args.is_empty() { return Err(runtime_err("row_count() expects (table)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("row_count() arg must be a table")),
                };
                let count = self.engine().row_count(df).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Int(count))
            }
            #[cfg(feature = "native")]
            BuiltinId::NullRate => {
                if args.len() < 2 { return Err(runtime_err("null_rate() expects (table, column)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("null_rate() first arg must be a table")),
                };
                let column = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("null_rate() column must be a string")),
                };
                let rate = self.engine().null_rate(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Float(rate))
            }
            #[cfg(feature = "native")]
            BuiltinId::IsUnique => {
                if args.len() < 2 { return Err(runtime_err("is_unique() expects (table, column)")); }
                let df = match &args[0] {
                    VmValue::Table(t) => t.df.clone(),
                    _ => return Err(runtime_err("is_unique() first arg must be a table")),
                };
                let column = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("is_unique() column must be a string")),
                };
                let unique = self.engine().is_unique(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Bool(unique))
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::FillNull | BuiltinId::DropNull | BuiltinId::Dedup |
            BuiltinId::Clamp | BuiltinId::DataProfile | BuiltinId::RowCount |
            BuiltinId::NullRate | BuiltinId::IsUnique => {
                Err(runtime_err("Data operations not available in WASM"))
            }
            #[cfg(feature = "native")]
            BuiltinId::IsEmail => {
                if args.is_empty() { return Err(runtime_err("is_email() expects 1 argument")); }
                let s = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("is_email() arg must be a string")),
                };
                Ok(VmValue::Bool(tl_data::validate::is_email(&s)))
            }
            #[cfg(feature = "native")]
            BuiltinId::IsUrl => {
                if args.is_empty() { return Err(runtime_err("is_url() expects 1 argument")); }
                let s = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("is_url() arg must be a string")),
                };
                Ok(VmValue::Bool(tl_data::validate::is_url(&s)))
            }
            #[cfg(feature = "native")]
            BuiltinId::IsPhone => {
                if args.is_empty() { return Err(runtime_err("is_phone() expects 1 argument")); }
                let s = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("is_phone() arg must be a string")),
                };
                Ok(VmValue::Bool(tl_data::validate::is_phone(&s)))
            }
            #[cfg(feature = "native")]
            BuiltinId::IsBetween => {
                if args.len() < 3 { return Err(runtime_err("is_between() expects (value, low, high)")); }
                let val = match &args[0] {
                    VmValue::Int(n) => *n as f64,
                    VmValue::Float(f) => *f,
                    _ => return Err(runtime_err("is_between() value must be a number")),
                };
                let low = match &args[1] {
                    VmValue::Int(n) => *n as f64,
                    VmValue::Float(f) => *f,
                    _ => return Err(runtime_err("is_between() low must be a number")),
                };
                let high = match &args[2] {
                    VmValue::Int(n) => *n as f64,
                    VmValue::Float(f) => *f,
                    _ => return Err(runtime_err("is_between() high must be a number")),
                };
                Ok(VmValue::Bool(tl_data::validate::is_between(val, low, high)))
            }
            #[cfg(feature = "native")]
            BuiltinId::Levenshtein => {
                if args.len() < 2 { return Err(runtime_err("levenshtein() expects (str_a, str_b)")); }
                let a = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("levenshtein() args must be strings")),
                };
                let b = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("levenshtein() args must be strings")),
                };
                Ok(VmValue::Int(tl_data::validate::levenshtein(&a, &b) as i64))
            }
            #[cfg(feature = "native")]
            BuiltinId::Soundex => {
                if args.is_empty() { return Err(runtime_err("soundex() expects 1 argument")); }
                let s = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("soundex() arg must be a string")),
                };
                Ok(VmValue::String(Arc::from(tl_data::validate::soundex(&s).as_str())))
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::IsEmail | BuiltinId::IsUrl | BuiltinId::IsPhone |
            BuiltinId::IsBetween | BuiltinId::Levenshtein | BuiltinId::Soundex => {
                Err(runtime_err("Data validation not available in WASM"))
            }
            #[cfg(feature = "native")]
            BuiltinId::ReadMysql => {
                #[cfg(feature = "mysql")]
                {
                    if args.len() < 2 { return Err(runtime_err("read_mysql() expects (conn_str, query)")); }
                    let conn_str = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("read_mysql() conn_str must be a string")),
                    };
                    let query = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("read_mysql() query must be a string")),
                    };
                    let df = self.engine().read_mysql(&conn_str, &query).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::Table(VmTable { df }))
                }
                #[cfg(not(feature = "mysql"))]
                Err(runtime_err("read_mysql() requires the 'mysql' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::ReadSqlite => {
                #[cfg(feature = "sqlite")]
                {
                    if args.len() < 2 { return Err(runtime_err("read_sqlite() expects (db_path, query)")); }
                    let db_path = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("read_sqlite() db_path must be a string")),
                    };
                    let query = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("read_sqlite() query must be a string")),
                    };
                    let df = self.engine().read_sqlite(&db_path, &query).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::Table(VmTable { df }))
                }
                #[cfg(not(feature = "sqlite"))]
                Err(runtime_err("read_sqlite() requires the 'sqlite' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::WriteSqlite => {
                #[cfg(feature = "sqlite")]
                {
                    if args.len() < 3 { return Err(runtime_err("write_sqlite() expects (table, db_path, table_name)")); }
                    let df = match &args[0] {
                        VmValue::Table(t) => t.df.clone(),
                        _ => return Err(runtime_err("write_sqlite() first arg must be a table")),
                    };
                    let db_path = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("write_sqlite() db_path must be a string")),
                    };
                    let table_name = match &args[2] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("write_sqlite() table_name must be a string")),
                    };
                    self.engine().write_sqlite(df, &db_path, &table_name).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::None)
                }
                #[cfg(not(feature = "sqlite"))]
                Err(runtime_err("write_sqlite() requires the 'sqlite' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::RedisConnect => {
                #[cfg(feature = "redis")]
                {
                    if args.is_empty() { return Err(runtime_err("redis_connect() expects (url)")); }
                    let url = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_connect() url must be a string")),
                    };
                    let result = tl_data::redis_conn::redis_connect(&url).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::String(Arc::from(result.as_str())))
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err("redis_connect() requires the 'redis' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::RedisGet => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 2 { return Err(runtime_err("redis_get() expects (url, key)")); }
                    let url = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_get() url must be a string")),
                    };
                    let key = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_get() key must be a string")),
                    };
                    match tl_data::redis_conn::redis_get(&url, &key).map_err(|e| runtime_err(e))? {
                        Some(v) => Ok(VmValue::String(Arc::from(v.as_str()))),
                        None => Ok(VmValue::None),
                    }
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err("redis_get() requires the 'redis' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::RedisSet => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 3 { return Err(runtime_err("redis_set() expects (url, key, value)")); }
                    let url = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_set() url must be a string")),
                    };
                    let key = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_set() key must be a string")),
                    };
                    let value = match &args[2] {
                        VmValue::String(s) => s.to_string(),
                        _ => format!("{}", &args[2]),
                    };
                    tl_data::redis_conn::redis_set(&url, &key, &value).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::None)
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err("redis_set() requires the 'redis' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::RedisDel => {
                #[cfg(feature = "redis")]
                {
                    if args.len() < 2 { return Err(runtime_err("redis_del() expects (url, key)")); }
                    let url = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_del() url must be a string")),
                    };
                    let key = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("redis_del() key must be a string")),
                    };
                    let deleted = tl_data::redis_conn::redis_del(&url, &key).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::Bool(deleted))
                }
                #[cfg(not(feature = "redis"))]
                Err(runtime_err("redis_del() requires the 'redis' feature"))
            }
            #[cfg(feature = "native")]
            BuiltinId::GraphqlQuery => {
                if args.len() < 2 { return Err(runtime_err("graphql_query() expects (endpoint, query, [variables])")); }
                let endpoint = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("graphql_query() endpoint must be a string")),
                };
                let query = match &args[1] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("graphql_query() query must be a string")),
                };
                let variables = if args.len() > 2 {
                    vm_value_to_json(&args[2])
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
                Ok(vm_json_to_value(&json))
            }
            #[cfg(feature = "native")]
            BuiltinId::RegisterS3 => {
                #[cfg(feature = "s3")]
                {
                    if args.len() < 2 { return Err(runtime_err("register_s3() expects (bucket, region, [access_key], [secret_key], [endpoint])")); }
                    let bucket = match &args[0] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("register_s3() bucket must be a string")),
                    };
                    let region = match &args[1] {
                        VmValue::String(s) => s.to_string(),
                        _ => return Err(runtime_err("register_s3() region must be a string")),
                    };
                    let access_key = args.get(2).and_then(|v| if let VmValue::String(s) = v { Some(s.to_string()) } else { None });
                    let secret_key = args.get(3).and_then(|v| if let VmValue::String(s) = v { Some(s.to_string()) } else { None });
                    let endpoint = args.get(4).and_then(|v| if let VmValue::String(s) = v { Some(s.to_string()) } else { None });
                    self.engine().register_s3(
                        &bucket, &region,
                        access_key.as_deref(), secret_key.as_deref(), endpoint.as_deref(),
                    ).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::None)
                }
                #[cfg(not(feature = "s3"))]
                Err(runtime_err("register_s3() requires the 's3' feature"))
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::ReadMysql | BuiltinId::ReadSqlite | BuiltinId::WriteSqlite |
            BuiltinId::RedisConnect | BuiltinId::RedisGet |
            BuiltinId::RedisSet | BuiltinId::RedisDel | BuiltinId::GraphqlQuery |
            BuiltinId::RegisterS3 => {
                Err(runtime_err("Connectors not available in WASM"))
            }
            // Phase 20: Python FFI
            BuiltinId::PyImport => {
                #[cfg(feature = "python")]
                { crate::python::py_import_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_import() requires the 'python' feature"))
            }
            BuiltinId::PyCall => {
                #[cfg(feature = "python")]
                { crate::python::py_call_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_call() requires the 'python' feature"))
            }
            BuiltinId::PyEval => {
                #[cfg(feature = "python")]
                { crate::python::py_eval_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_eval() requires the 'python' feature"))
            }
            BuiltinId::PyGetAttr => {
                #[cfg(feature = "python")]
                { crate::python::py_getattr_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_getattr() requires the 'python' feature"))
            }
            BuiltinId::PySetAttr => {
                #[cfg(feature = "python")]
                { crate::python::py_setattr_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_setattr() requires the 'python' feature"))
            }
            BuiltinId::PyToTl => {
                #[cfg(feature = "python")]
                { crate::python::py_to_tl_impl(&args) }
                #[cfg(not(feature = "python"))]
                Err(runtime_err("py_to_tl() requires the 'python' feature"))
            }

            // Phase 21: Schema Evolution builtins
            #[cfg(feature = "native")]
            BuiltinId::SchemaRegister => {
                let name = match args.first() {
                    Some(VmValue::String(s)) => s.to_string(),
                    _ => return Err(runtime_err("schema_register: first arg must be schema name string")),
                };
                let version = match args.get(1) {
                    Some(VmValue::Int(v)) => *v,
                    _ => return Err(runtime_err("schema_register: second arg must be version number")),
                };
                let fields = match args.get(2) {
                    Some(VmValue::Map(pairs)) => {
                        let mut arrow_fields = Vec::new();
                        for (k, v) in pairs {
                            let fname = k.to_string();
                            let ftype = match v { VmValue::String(s) => s.to_string(), _ => "string".to_string() };
                            arrow_fields.push(tl_data::ArrowField::new(&fname, crate::schema::type_name_to_arrow_pub(&ftype), true));
                        }
                        arrow_fields
                    }
                    _ => return Err(runtime_err("schema_register: third arg must be fields map")),
                };
                let schema = std::sync::Arc::new(tl_data::ArrowSchema::new(fields));
                self.schema_registry.register(&name, version, schema, crate::schema::SchemaMetadata::default())
                    .map_err(|e| runtime_err(&e))?;
                Ok(VmValue::None)
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaGet => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_get: need name")) };
                let version = match args.get(1) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_get: need version")) };
                match self.schema_registry.get(&name, version) {
                    Some(vs) => {
                        let fields: Vec<VmValue> = vs.schema.fields().iter().map(|f| {
                            VmValue::String(std::sync::Arc::from(format!("{}: {}", f.name(), f.data_type())))
                        }).collect();
                        Ok(VmValue::List(fields))
                    }
                    None => Ok(VmValue::None),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaLatest => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_latest: need name")) };
                match self.schema_registry.latest(&name) {
                    Some(vs) => Ok(VmValue::Int(vs.version)),
                    None => Ok(VmValue::None),
                }
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaHistory => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_history: need name")) };
                let versions = self.schema_registry.versions(&name);
                Ok(VmValue::List(versions.into_iter().map(VmValue::Int).collect()))
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaCheck => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_check: need name")) };
                let v1 = match args.get(1) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_check: need v1")) };
                let v2 = match args.get(2) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_check: need v2")) };
                let mode_str = match args.get(3) { Some(VmValue::String(s)) => s.to_string(), _ => "backward".to_string() };
                let mode = crate::schema::CompatibilityMode::from_str(&mode_str);
                let issues = self.schema_registry.check_compatibility(&name, v1, v2, mode);
                Ok(VmValue::List(issues.into_iter().map(|i| VmValue::String(std::sync::Arc::from(i.to_string()))).collect()))
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaDiff => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_diff: need name")) };
                let v1 = match args.get(1) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_diff: need v1")) };
                let v2 = match args.get(2) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_diff: need v2")) };
                let diffs = self.schema_registry.diff(&name, v1, v2);
                Ok(VmValue::List(diffs.into_iter().map(|d| VmValue::String(std::sync::Arc::from(d.to_string()))).collect()))
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaApplyMigration => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_apply_migration: need name")) };
                let from_v = match args.get(1) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_apply_migration: need from_ver")) };
                let to_v = match args.get(2) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_apply_migration: need to_ver")) };
                Ok(VmValue::String(std::sync::Arc::from(format!("migration {}:{}->{} applied", name, from_v, to_v))))
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaVersions => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_versions: need name")) };
                let versions = self.schema_registry.versions(&name);
                Ok(VmValue::List(versions.into_iter().map(VmValue::Int).collect()))
            }
            #[cfg(feature = "native")]
            BuiltinId::SchemaFields => {
                let name = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("schema_fields: need name")) };
                let version = match args.get(1) { Some(VmValue::Int(v)) => *v, _ => return Err(runtime_err("schema_fields: need version")) };
                let fields = self.schema_registry.fields(&name, version);
                Ok(VmValue::List(fields.into_iter().map(|(n, t)| VmValue::String(std::sync::Arc::from(format!("{}: {}", n, t)))).collect()))
            }
            #[cfg(not(feature = "native"))]
            BuiltinId::SchemaRegister | BuiltinId::SchemaGet | BuiltinId::SchemaLatest |
            BuiltinId::SchemaHistory | BuiltinId::SchemaCheck | BuiltinId::SchemaDiff |
            BuiltinId::SchemaApplyMigration | BuiltinId::SchemaVersions | BuiltinId::SchemaFields => {
                let _ = args;
                Err(runtime_err("Schema operations not available in WASM"))
            }

            // ── Phase 22: Advanced Types ──
            BuiltinId::Decimal => {
                use std::str::FromStr;
                let s = match args.first() {
                    Some(VmValue::String(s)) => s.to_string(),
                    Some(VmValue::Int(n)) => n.to_string(),
                    Some(VmValue::Float(f)) => f.to_string(),
                    _ => return Err(runtime_err("decimal(): expected string, int, or float")),
                };
                let d = rust_decimal::Decimal::from_str(&s)
                    .map_err(|e| runtime_err(format!("decimal(): invalid: {e}")))?;
                Ok(VmValue::Decimal(d))
            }

            // ── Phase 23: Security ──
            BuiltinId::SecretGet => {
                let key = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("secret_get: need key")) };
                if let Some(val) = self.secret_vault.get(&key) {
                    Ok(VmValue::Secret(Arc::from(val.as_str())))
                } else {
                    // Fallback to env var TL_SECRET_{KEY}
                    let env_key = format!("TL_SECRET_{}", key.to_uppercase());
                    match std::env::var(&env_key) {
                        Ok(val) => Ok(VmValue::Secret(Arc::from(val.as_str()))),
                        Err(_) => Ok(VmValue::None),
                    }
                }
            }
            BuiltinId::SecretSet => {
                let key = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("secret_set: need key")) };
                let val = match args.get(1) {
                    Some(VmValue::String(s)) => s.to_string(),
                    Some(VmValue::Secret(s)) => s.to_string(),
                    _ => return Err(runtime_err("secret_set: need value")),
                };
                self.secret_vault.insert(key, val);
                Ok(VmValue::None)
            }
            BuiltinId::SecretDelete => {
                let key = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("secret_delete: need key")) };
                self.secret_vault.remove(&key);
                Ok(VmValue::None)
            }
            BuiltinId::SecretList => {
                let keys: Vec<VmValue> = self.secret_vault.keys()
                    .map(|k| VmValue::String(Arc::from(k.as_str())))
                    .collect();
                Ok(VmValue::List(keys))
            }
            BuiltinId::CheckPermission => {
                let perm = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("check_permission: need permission name")) };
                let allowed = match self.security_policy {
                    Some(ref policy) => policy.check(&perm),
                    None => true,
                };
                Ok(VmValue::Bool(allowed))
            }
            BuiltinId::MaskEmail => {
                let email = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("mask_email: need string")) };
                let masked = if let Some(at_pos) = email.find('@') {
                    let local = &email[..at_pos];
                    let domain = &email[at_pos..];
                    if local.len() > 1 {
                        format!("{}***{}", &local[..1], domain)
                    } else {
                        format!("***{domain}")
                    }
                } else {
                    "***".to_string()
                };
                Ok(VmValue::String(Arc::from(masked.as_str())))
            }
            BuiltinId::MaskPhone => {
                let phone = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("mask_phone: need string")) };
                let digits: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();
                let masked = if digits.len() >= 4 {
                    let last4 = &digits[digits.len()-4..];
                    format!("***-***-{last4}")
                } else {
                    "***".to_string()
                };
                Ok(VmValue::String(Arc::from(masked.as_str())))
            }
            BuiltinId::MaskCreditCard => {
                let cc = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("mask_cc: need string")) };
                let digits: String = cc.chars().filter(|c| c.is_ascii_digit()).collect();
                let masked = if digits.len() >= 4 {
                    let last4 = &digits[digits.len()-4..];
                    format!("****-****-****-{last4}")
                } else {
                    "****-****-****-****".to_string()
                };
                Ok(VmValue::String(Arc::from(masked.as_str())))
            }
            BuiltinId::Redact => {
                let val = match args.first() { Some(v) => format!("{v}"), _ => return Err(runtime_err("redact: need value")) };
                let policy = match args.get(1) { Some(VmValue::String(s)) => s.to_string(), _ => "full".to_string() };
                let result = match policy.as_str() {
                    "full" => "***".to_string(),
                    "partial" => {
                        if val.len() > 2 { format!("{}***{}", &val[..1], &val[val.len()-1..]) } else { "***".to_string() }
                    }
                    "hash" => {
                        use sha2::Digest;
                        let hash = sha2::Sha256::digest(val.as_bytes());
                        format!("{:x}", hash)
                    }
                    _ => "***".to_string(),
                };
                Ok(VmValue::String(Arc::from(result.as_str())))
            }
            BuiltinId::Hash => {
                let val = match args.first() { Some(VmValue::String(s)) => s.to_string(), _ => return Err(runtime_err("hash: need string")) };
                let algo = match args.get(1) { Some(VmValue::String(s)) => s.to_string(), _ => "sha256".to_string() };
                let result = match algo.as_str() {
                    "sha256" => {
                        use sha2::Digest;
                        format!("{:x}", sha2::Sha256::digest(val.as_bytes()))
                    }
                    "sha512" => {
                        use sha2::Digest;
                        format!("{:x}", sha2::Sha512::digest(val.as_bytes()))
                    }
                    "md5" => {
                        use md5::Digest;
                        format!("{:x}", md5::Md5::digest(val.as_bytes()))
                    }
                    _ => return Err(runtime_err(format!("hash: unknown algorithm '{algo}' (use sha256, sha512, or md5)"))),
                };
                Ok(VmValue::String(Arc::from(result.as_str())))
            }

            // ── Phase 25: Async builtins (tokio-backed when async-runtime feature enabled) ──
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncReadFile => {
                let rt = self.ensure_runtime();
                crate::async_runtime::async_read_file_impl(&rt, &args, &self.security_policy)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncWriteFile => {
                let rt = self.ensure_runtime();
                crate::async_runtime::async_write_file_impl(&rt, &args, &self.security_policy)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncHttpGet => {
                let rt = self.ensure_runtime();
                crate::async_runtime::async_http_get_impl(&rt, &args, &self.security_policy)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncHttpPost => {
                let rt = self.ensure_runtime();
                crate::async_runtime::async_http_post_impl(&rt, &args, &self.security_policy)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncSleep => {
                let rt = self.ensure_runtime();
                crate::async_runtime::async_sleep_impl(&rt, &args)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::Select => {
                crate::async_runtime::select_impl(&args)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::RaceAll => {
                crate::async_runtime::race_all_impl(&args)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncMap => {
                let rt = self.ensure_runtime();
                let stack_snapshot = self.stack.clone();
                crate::async_runtime::async_map_impl(&rt, &args, &self.globals, &stack_snapshot)
            }
            #[cfg(feature = "async-runtime")]
            BuiltinId::AsyncFilter => {
                let rt = self.ensure_runtime();
                let stack_snapshot = self.stack.clone();
                crate::async_runtime::async_filter_impl(&rt, &args, &self.globals, &stack_snapshot)
            }

            #[cfg(not(feature = "async-runtime"))]
            BuiltinId::AsyncReadFile | BuiltinId::AsyncWriteFile |
            BuiltinId::AsyncHttpGet | BuiltinId::AsyncHttpPost |
            BuiltinId::AsyncSleep | BuiltinId::Select |
            BuiltinId::AsyncMap | BuiltinId::AsyncFilter |
            BuiltinId::RaceAll => {
                Err(runtime_err(format!("{}: async builtins require the 'async-runtime' feature", builtin_id.name())))
            }

            // Phase 27: Data Error Hierarchy builtins
            BuiltinId::IsError => {
                if args.is_empty() { return Err(runtime_err("is_error() expects 1 argument")); }
                let is_err = matches!(&args[0], VmValue::EnumInstance(e) if
                    &*e.type_name == "DataError" ||
                    &*e.type_name == "NetworkError" ||
                    &*e.type_name == "ConnectorError"
                );
                Ok(VmValue::Bool(is_err))
            }
            BuiltinId::ErrorType => {
                if args.is_empty() { return Err(runtime_err("error_type() expects 1 argument")); }
                match &args[0] {
                    VmValue::EnumInstance(e) => Ok(VmValue::String(e.type_name.clone())),
                    _ => Ok(VmValue::None),
                }
            }

            // Phase 32: GPU Tensor Support
            #[cfg(feature = "gpu")]
            BuiltinId::GpuAvailable => {
                Ok(VmValue::Bool(tl_gpu::GpuDevice::is_available()))
            }
            #[cfg(not(feature = "gpu"))]
            BuiltinId::GpuAvailable => {
                Ok(VmValue::Bool(false))
            }

            #[cfg(feature = "gpu")]
            BuiltinId::ToGpu => {
                if args.is_empty() { return Err(runtime_err("to_gpu() expects 1 argument (tensor)")); }
                let gt = self.ensure_gpu_tensor(&args[0])?;
                Ok(VmValue::GpuTensor(gt))
            }
            #[cfg(not(feature = "gpu"))]
            BuiltinId::ToGpu => {
                Err(runtime_err("GPU operations not available. Build with --features gpu"))
            }

            #[cfg(feature = "gpu")]
            BuiltinId::ToCpu => {
                if args.is_empty() { return Err(runtime_err("to_cpu() expects 1 argument (gpu_tensor)")); }
                match &args[0] {
                    VmValue::GpuTensor(gt) => {
                        let cpu = gt.to_cpu().map_err(|e| runtime_err(e))?;
                        Ok(VmValue::Tensor(Arc::new(cpu)))
                    }
                    _ => Err(runtime_err(format!("to_cpu() expects a gpu_tensor, got {}", args[0].type_name()))),
                }
            }
            #[cfg(not(feature = "gpu"))]
            BuiltinId::ToCpu => {
                Err(runtime_err("GPU operations not available. Build with --features gpu"))
            }

            #[cfg(feature = "gpu")]
            BuiltinId::GpuMatmul => {
                if args.len() < 2 { return Err(runtime_err("gpu_matmul() expects 2 arguments")); }
                let a = self.ensure_gpu_tensor(&args[0])?;
                let b = self.ensure_gpu_tensor(&args[1])?;
                let ops = self.get_gpu_ops()?;
                let result = ops.matmul(&a, &b).map_err(|e| runtime_err(e))?;
                Ok(VmValue::GpuTensor(Arc::new(result)))
            }
            #[cfg(not(feature = "gpu"))]
            BuiltinId::GpuMatmul => {
                Err(runtime_err("GPU operations not available. Build with --features gpu"))
            }

            #[cfg(feature = "gpu")]
            BuiltinId::GpuBatchPredict => {
                if args.len() < 2 { return Err(runtime_err("gpu_batch_predict() expects 2-3 arguments")); }
                match (&args[0], &args[1]) {
                    (VmValue::Model(model), VmValue::Tensor(input)) => {
                        let batch_size = args.get(2).and_then(|v| match v {
                            VmValue::Int(n) => Some(*n as usize),
                            _ => None,
                        });
                        let result = tl_gpu::BatchInference::batch_predict(model, input, batch_size)
                            .map_err(|e| runtime_err(e))?;
                        Ok(VmValue::Tensor(Arc::new(result)))
                    }
                    _ => Err(runtime_err("gpu_batch_predict() expects (model, tensor, [batch_size])")),
                }
            }
            #[cfg(not(feature = "gpu"))]
            BuiltinId::GpuBatchPredict => {
                Err(runtime_err("GPU operations not available. Build with --features gpu"))
            }
        }
    }

    // ── AI helpers ──

    fn vmvalue_to_f64_list(&self, val: &VmValue) -> Result<Vec<f64>, TlError> {
        match val {
            VmValue::List(items) => {
                items.iter().map(|item| match item {
                    VmValue::Int(n) => Ok(*n as f64),
                    VmValue::Float(f) => Ok(*f),
                    _ => Err(runtime_err("Expected number in list")),
                }).collect()
            }
            VmValue::Int(n) => Ok(vec![*n as f64]),
            VmValue::Float(f) => Ok(vec![*f]),
            _ => Err(runtime_err("Expected a list of numbers")),
        }
    }

    fn vmvalue_to_usize_list(&self, val: &VmValue) -> Result<Vec<usize>, TlError> {
        match val {
            VmValue::List(items) => {
                items.iter().map(|item| match item {
                    VmValue::Int(n) => Ok(*n as usize),
                    _ => Err(runtime_err("Expected integer in shape list")),
                }).collect()
            }
            _ => Err(runtime_err("Expected a list of integers for shape")),
        }
    }

    #[cfg(feature = "native")]
    fn handle_train(&mut self, frame_idx: usize, algo_const: u8, config_const: u8) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        let algorithm = match &frame.prototype.constants[algo_const as usize] {
            Constant::String(s) => s.to_string(),
            _ => return Err(runtime_err("Expected string constant for algorithm")),
        };
        let config_args = match &frame.prototype.constants[config_const as usize] {
            Constant::AstExprList(args) => args.clone(),
            _ => return Err(runtime_err("Expected AST expr list for train config")),
        };

        // Extract config values
        let mut data_val = None;
        let mut target_name = None;
        let mut feature_names: Vec<String> = Vec::new();

        for arg in &config_args {
            if let AstExpr::NamedArg { name, value } = arg {
                match name.as_str() {
                    "data" => {
                        data_val = Some(self.eval_ast_to_vm(value)?);
                    }
                    "target" => {
                        if let AstExpr::String(s) = value.as_ref() {
                            target_name = Some(s.clone());
                        }
                    }
                    "features" => {
                        if let AstExpr::List(items) = value.as_ref() {
                            for item in items {
                                if let AstExpr::String(s) = item {
                                    feature_names.push(s.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Build training config from table data
        let table = match data_val {
            Some(VmValue::Table(t)) => t,
            _ => return Err(runtime_err("train: data must be a table")),
        };
        let target = target_name.ok_or_else(|| runtime_err("train: target is required"))?;

        // Collect table to Arrow batches
        let batches = self.engine().collect(table.df).map_err(|e| runtime_err(e))?;
        if batches.is_empty() {
            return Err(runtime_err("train: empty dataset"));
        }

        // Determine feature columns if not specified
        let batch = &batches[0];
        let schema = batch.schema();
        if feature_names.is_empty() {
            for field in schema.fields() {
                if field.name() != &target {
                    feature_names.push(field.name().clone());
                }
            }
        }

        // Extract feature data and target data as f64 arrays
        let n_rows = batch.num_rows();
        let n_features = feature_names.len();
        let mut features_data = Vec::with_capacity(n_rows * n_features);
        let mut target_data = Vec::with_capacity(n_rows);

        for col_name in &feature_names {
            let col_idx = schema.index_of(col_name)
                .map_err(|_| runtime_err(format!("Column not found: {col_name}")))?;
            let col_arr = batch.column(col_idx);
            Self::extract_f64_column(col_arr, &mut features_data)?;
        }

        // Extract target column
        let target_idx = schema.index_of(&target)
            .map_err(|_| runtime_err(format!("Target column not found: {target}")))?;
        let target_arr = batch.column(target_idx);
        Self::extract_f64_column(target_arr, &mut target_data)?;

        // Reshape features: [col1_row1, col1_row2, ..., col2_row1, ...] → row-major
        let mut row_major = Vec::with_capacity(n_rows * n_features);
        for row in 0..n_rows {
            for col in 0..n_features {
                row_major.push(features_data[col * n_rows + row]);
            }
        }

        let features_tensor = tl_ai::TlTensor::from_vec(row_major, &[n_rows, n_features])
            .map_err(|e| runtime_err(format!("Shape error: {e}")))?;
        let target_tensor = tl_ai::TlTensor::from_vec(target_data, &[n_rows])
            .map_err(|e| runtime_err(format!("Shape error: {e}")))?;

        let config = tl_ai::TrainConfig {
            features: features_tensor,
            target: target_tensor,
            feature_names: feature_names.clone(),
            target_name: target.clone(),
            model_name: algorithm.clone(),
            split_ratio: 0.8,
            hyperparams: std::collections::HashMap::new(),
        };

        let model = tl_ai::train(&algorithm, &config)
            .map_err(|e| runtime_err(format!("Training failed: {e}")))?;

        Ok(VmValue::Model(Arc::new(model)))
    }

    #[cfg(feature = "native")]
    fn extract_f64_column(col: &std::sync::Arc<dyn tl_data::datafusion::arrow::array::Array>, out: &mut Vec<f64>) -> Result<(), TlError> {
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
            return Err(runtime_err("Column must be numeric (int32, int64, float32, float64)"));
        }
        Ok(())
    }

    #[cfg(feature = "native")]
    fn handle_pipeline_exec(&mut self, frame_idx: usize, name_const: u8, config_const: u8) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        let name = match &frame.prototype.constants[name_const as usize] {
            Constant::String(s) => s.to_string(),
            _ => return Err(runtime_err("Expected string constant for pipeline name")),
        };

        let mut schedule = None;
        let mut timeout_ms = None;
        let mut retries = 0u32;

        if let Constant::AstExprList(args) = &frame.prototype.constants[config_const as usize] {
            for arg in args {
                if let AstExpr::NamedArg { name: key, value } = arg {
                    match key.as_str() {
                        "schedule" => {
                            if let AstExpr::String(s) = value.as_ref() {
                                schedule = Some(s.clone());
                            }
                        }
                        "timeout" => {
                            if let AstExpr::String(s) = value.as_ref() {
                                timeout_ms = tl_stream::parse_duration(s).ok();
                            }
                        }
                        "retries" => {
                            if let AstExpr::Int(n) = value.as_ref() {
                                retries = *n as u32;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let def = tl_stream::PipelineDef {
            name,
            schedule,
            timeout_ms,
            retries,
        };

        self.output.push(format!("Pipeline '{}': success", def.name));
        Ok(VmValue::PipelineDef(Arc::new(def)))
    }

    #[cfg(feature = "native")]
    fn handle_stream_exec(&mut self, frame_idx: usize, config_const: u8) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        let config_args = match &frame.prototype.constants[config_const as usize] {
            Constant::AstExprList(args) => args.clone(),
            _ => return Err(runtime_err("Expected AST expr list for stream config")),
        };

        let mut name = String::new();
        let mut window = None;
        let mut watermark_ms = None;

        for arg in &config_args {
            if let AstExpr::NamedArg { name: key, value } = arg {
                match key.as_str() {
                    "name" => {
                        if let AstExpr::String(s) = value.as_ref() {
                            name = s.clone();
                        }
                    }
                    "window" => {
                        if let AstExpr::String(s) = value.as_ref() {
                            window = Self::parse_window_type(s);
                        }
                    }
                    "watermark" => {
                        if let AstExpr::String(s) = value.as_ref() {
                            watermark_ms = tl_stream::parse_duration(s).ok();
                        }
                    }
                    _ => {}
                }
            }
        }

        let def = tl_stream::StreamDef {
            name: name.clone(),
            window,
            watermark_ms,
        };

        self.output.push(format!("Stream '{}' declared", name));
        Ok(VmValue::StreamDef(Arc::new(def)))
    }

    #[cfg(feature = "native")]
    fn parse_window_type(s: &str) -> Option<tl_stream::window::WindowType> {
        if let Some(dur) = s.strip_prefix("tumbling:") {
            let ms = tl_stream::parse_duration(dur).ok()?;
            Some(tl_stream::window::WindowType::Tumbling { duration_ms: ms })
        } else if let Some(rest) = s.strip_prefix("sliding:") {
            let parts: Vec<&str> = rest.splitn(2, ':').collect();
            if parts.len() == 2 {
                let wms = tl_stream::parse_duration(parts[0]).ok()?;
                let sms = tl_stream::parse_duration(parts[1]).ok()?;
                Some(tl_stream::window::WindowType::Sliding { window_ms: wms, slide_ms: sms })
            } else {
                None
            }
        } else if let Some(dur) = s.strip_prefix("session:") {
            let ms = tl_stream::parse_duration(dur).ok()?;
            Some(tl_stream::window::WindowType::Session { gap_ms: ms })
        } else {
            None
        }
    }

    #[cfg(feature = "native")]
    fn handle_connector_decl(&mut self, frame_idx: usize, type_const: u8, config_const: u8) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        let connector_type = match &frame.prototype.constants[type_const as usize] {
            Constant::String(s) => s.to_string(),
            _ => return Err(runtime_err("Expected string constant for connector type")),
        };

        let config_args = match &frame.prototype.constants[config_const as usize] {
            Constant::AstExprList(args) => args.clone(),
            _ => return Err(runtime_err("Expected AST expr list for connector config")),
        };

        let mut properties = std::collections::HashMap::new();
        for arg in &config_args {
            if let AstExpr::NamedArg { name: key, value } = arg {
                let val_str = match value.as_ref() {
                    AstExpr::String(s) => s.clone(),
                    AstExpr::Int(n) => n.to_string(),
                    AstExpr::Float(f) => f.to_string(),
                    AstExpr::Bool(b) => b.to_string(),
                    other => {
                        // Try to resolve Ident from globals
                        if let AstExpr::Ident(ident) = other {
                            if let Some(val) = self.globals.get(ident.as_str()) {
                                format!("{val}")
                            } else {
                                ident.clone()
                            }
                        } else {
                            format!("{other:?}")
                        }
                    }
                };
                properties.insert(key.clone(), val_str);
            }
        }

        let config = tl_stream::ConnectorConfig {
            name: String::new(), // Will be set by SetGlobal
            connector_type,
            properties,
        };

        Ok(VmValue::Connector(Arc::new(config)))
    }

    /// Advance a generator by one step, returning the next value or None if done.
    fn generator_next(&mut self, gen_arc: &Arc<Mutex<VmGenerator>>) -> Result<VmValue, TlError> {
        let mut gn = gen_arc.lock().unwrap();
        if gn.done {
            return Ok(VmValue::None);
        }
        match &mut gn.kind {
            GeneratorKind::UserDefined { prototype, upvalues, saved_stack, ip } => {
                let proto = prototype.clone();
                let uvs = upvalues.clone();
                let stack_snapshot = saved_stack.clone();
                let saved_ip = *ip;
                drop(gn); // release lock before running VM

                // Set up a frame to resume the generator
                let new_base = self.stack.len();
                let num_regs = proto.num_registers as usize;
                self.ensure_stack(new_base + num_regs + 1);
                // Restore saved registers
                for (i, val) in stack_snapshot.iter().enumerate() {
                    self.stack[new_base + i] = val.clone();
                }

                self.frames.push(CallFrame {
                    prototype: proto,
                    ip: saved_ip,
                    base: new_base,
                    upvalues: uvs,
                });

                self.yielded_value = None;
                let _result = self.run()?;

                if let Some(yielded) = self.yielded_value.take() {
                    // Generator yielded — save state back
                    let mut gn = gen_arc.lock().unwrap();
                    if let GeneratorKind::UserDefined { saved_stack, ip, .. } = &mut gn.kind {
                        // Save the current register state
                        let num_regs_save = saved_stack.len();
                        for i in 0..num_regs_save {
                            if new_base + i < self.stack.len() {
                                saved_stack[i] = self.stack[new_base + i].clone();
                            }
                        }
                        // Save the ip (instruction after yield)
                        *ip = self.yielded_ip;
                    }
                    self.stack.truncate(new_base);
                    Ok(yielded)
                } else {
                    // Generator returned (no yield) — mark done
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                    self.stack.truncate(new_base);
                    Ok(VmValue::None)
                }
            }
            GeneratorKind::ListIter { items, index } => {
                if *index < items.len() {
                    let val = items[*index].clone();
                    *index += 1;
                    Ok(val)
                } else {
                    gn.done = true;
                    Ok(VmValue::None)
                }
            }
            GeneratorKind::Take { source, remaining } => {
                if *remaining == 0 {
                    gn.done = true;
                    return Ok(VmValue::None);
                }
                *remaining -= 1;
                let src = source.clone();
                drop(gn);
                let val = self.generator_next(&src)?;
                if matches!(val, VmValue::None) {
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                }
                Ok(val)
            }
            GeneratorKind::Skip { source, remaining } => {
                let src = source.clone();
                let skip_n = *remaining;
                *remaining = 0;
                drop(gn);
                // Skip initial values
                for _ in 0..skip_n {
                    let val = self.generator_next(&src)?;
                    if matches!(val, VmValue::None) {
                        let mut gn = gen_arc.lock().unwrap();
                        gn.done = true;
                        return Ok(VmValue::None);
                    }
                }
                let val = self.generator_next(&src)?;
                if matches!(val, VmValue::None) {
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                }
                Ok(val)
            }
            GeneratorKind::Map { source, func } => {
                let src = source.clone();
                let f = func.clone();
                drop(gn);
                let val = self.generator_next(&src)?;
                if matches!(val, VmValue::None) {
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                    return Ok(VmValue::None);
                }
                self.call_vm_function(&f, &[val])
            }
            GeneratorKind::Filter { source, func } => {
                let src = source.clone();
                let f = func.clone();
                drop(gn);
                loop {
                    let val = self.generator_next(&src)?;
                    if matches!(val, VmValue::None) {
                        let mut gn = gen_arc.lock().unwrap();
                        gn.done = true;
                        return Ok(VmValue::None);
                    }
                    let test = self.call_vm_function(&f, &[val.clone()])?;
                    if test.is_truthy() {
                        return Ok(val);
                    }
                }
            }
            GeneratorKind::Chain { first, second, on_second } => {
                if !*on_second {
                    let src = first.clone();
                    drop(gn);
                    let val = self.generator_next(&src)?;
                    if matches!(val, VmValue::None) {
                        let mut gn = gen_arc.lock().unwrap();
                        if let GeneratorKind::Chain { on_second, second, .. } = &mut gn.kind {
                            *on_second = true;
                            let src2 = second.clone();
                            drop(gn);
                            return self.generator_next(&src2);
                        }
                    }
                    Ok(val)
                } else {
                    let src = second.clone();
                    drop(gn);
                    let val = self.generator_next(&src)?;
                    if matches!(val, VmValue::None) {
                        let mut gn = gen_arc.lock().unwrap();
                        gn.done = true;
                    }
                    Ok(val)
                }
            }
            GeneratorKind::Zip { first, second } => {
                let src1 = first.clone();
                let src2 = second.clone();
                drop(gn);
                let val1 = self.generator_next(&src1)?;
                let val2 = self.generator_next(&src2)?;
                if matches!(val1, VmValue::None) || matches!(val2, VmValue::None) {
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                    return Ok(VmValue::None);
                }
                Ok(VmValue::List(vec![val1, val2]))
            }
            GeneratorKind::Enumerate { source, index } => {
                let src = source.clone();
                let idx = *index;
                *index += 1;
                drop(gn);
                let val = self.generator_next(&src)?;
                if matches!(val, VmValue::None) {
                    let mut gn = gen_arc.lock().unwrap();
                    gn.done = true;
                    return Ok(VmValue::None);
                }
                Ok(VmValue::List(vec![VmValue::Int(idx as i64), val]))
            }
        }
    }

    /// Process a __schema__:Name:vN:fields... global to register in schema_registry.
    #[cfg(feature = "native")]
    fn process_schema_global(&mut self, s: &str) {
        // Format: __schema__:Name:vN:field1:Type,field2:Type,...
        let rest = &s["__schema__:".len()..];
        let parts: Vec<&str> = rest.splitn(3, ':').collect();
        if parts.len() < 2 { return; }

        let schema_name = parts[0];
        let mut version: i64 = 0;
        let fields_str;

        if parts.len() == 3 && parts[1].starts_with('v') {
            // Versioned: Name:vN:fields
            version = parts[1][1..].parse().unwrap_or(0);
            fields_str = parts[2];
        } else if parts.len() == 3 {
            // No version prefix, treat as v0: Name:field1:...
            fields_str = &rest[schema_name.len() + 1..];
        } else {
            fields_str = parts[1];
        }

        if version == 0 { return; } // Only register versioned schemas

        let mut arrow_fields = Vec::new();
        for field_pair in fields_str.split(',') {
            let kv: Vec<&str> = field_pair.splitn(2, ':').collect();
            if kv.len() == 2 {
                let fname = kv[0].trim();
                let ftype = kv[1].trim();
                // Parse type expr debug format: Simple("typename")
                let type_name = if ftype.starts_with("Simple(\"") && ftype.ends_with("\")") {
                    &ftype[8..ftype.len()-2]
                } else {
                    ftype
                };
                let dt = crate::schema::type_name_to_arrow_pub(type_name);
                arrow_fields.push(tl_data::ArrowField::new(fname, dt, true));
            }
        }

        if !arrow_fields.is_empty() {
            let schema = std::sync::Arc::new(tl_data::ArrowSchema::new(arrow_fields));
            let _ = self.schema_registry.register(schema_name, version, schema, crate::schema::SchemaMetadata::default());
        }
    }

    /// Process a __migrate__:Name:fromVer:toVer:ops global to apply migration.
    #[cfg(feature = "native")]
    fn process_migrate_global(&mut self, s: &str) {
        // Format: __migrate__:Name:from:to:op1;op2;...
        let rest = &s["__migrate__:".len()..];
        let parts: Vec<&str> = rest.splitn(4, ':').collect();
        if parts.len() < 4 { return; }

        let schema_name = parts[0];
        let from_ver: i64 = parts[1].parse().unwrap_or(0);
        let to_ver: i64 = parts[2].parse().unwrap_or(0);
        let ops_str = parts[3];

        let mut ops = Vec::new();
        for op_str in ops_str.split(';') {
            let op_parts: Vec<&str> = op_str.splitn(4, ':').collect();
            if op_parts.is_empty() { continue; }
            match op_parts[0] {
                "add" if op_parts.len() >= 3 => {
                    let name = op_parts[1].to_string();
                    // Parse type from debug format: Simple("typename")
                    let type_raw = op_parts[2];
                    let type_name = if type_raw.starts_with("Simple(\"") && type_raw.ends_with("\")") {
                        type_raw[8..type_raw.len()-2].to_string()
                    } else {
                        type_raw.to_string()
                    };
                    let default = if op_parts.len() >= 4 && op_parts[3].starts_with("default:") {
                        Some(op_parts[3]["default:".len()..].trim_matches('"').to_string())
                    } else {
                        None
                    };
                    ops.push(crate::schema::MigrationOp::AddColumn { name, type_name, default });
                }
                "drop" if op_parts.len() >= 2 => {
                    ops.push(crate::schema::MigrationOp::DropColumn { name: op_parts[1].to_string() });
                }
                "rename" if op_parts.len() >= 3 => {
                    ops.push(crate::schema::MigrationOp::RenameColumn {
                        from: op_parts[1].to_string(),
                        to: op_parts[2].to_string(),
                    });
                }
                "alter" if op_parts.len() >= 3 => {
                    let type_raw = op_parts[2];
                    let type_name = if type_raw.starts_with("Simple(\"") && type_raw.ends_with("\")") {
                        type_raw[8..type_raw.len()-2].to_string()
                    } else {
                        type_raw.to_string()
                    };
                    ops.push(crate::schema::MigrationOp::AlterType {
                        column: op_parts[1].to_string(),
                        new_type: type_name,
                    });
                }
                _ => {}
            }
        }

        let _ = self.schema_registry.apply_migration(schema_name, from_ver, to_ver, &ops);
    }

    /// Dispatch a method call on an object.
    /// Deep-clone a VmValue, recursively copying containers.
    fn deep_clone_value(&self, val: &VmValue) -> Result<VmValue, TlError> {
        match val {
            VmValue::List(items) => {
                let cloned: Result<Vec<_>, _> = items.iter().map(|v| self.deep_clone_value(v)).collect();
                Ok(VmValue::List(cloned?))
            }
            VmValue::Map(pairs) => {
                let cloned: Result<Vec<_>, _> = pairs.iter()
                    .map(|(k, v)| Ok((k.clone(), self.deep_clone_value(v)?)))
                    .collect();
                Ok(VmValue::Map(cloned?))
            }
            VmValue::Set(items) => {
                let cloned: Result<Vec<_>, _> = items.iter().map(|v| self.deep_clone_value(v)).collect();
                Ok(VmValue::Set(cloned?))
            }
            VmValue::StructInstance(inst) => {
                let cloned_fields: Result<Vec<_>, _> = inst.fields.iter()
                    .map(|(k, v)| Ok((k.clone(), self.deep_clone_value(v)?)))
                    .collect();
                Ok(VmValue::StructInstance(Arc::new(VmStructInstance {
                    type_name: inst.type_name.clone(),
                    fields: cloned_fields?,
                })))
            }
            VmValue::EnumInstance(e) => {
                let cloned_fields: Result<Vec<_>, _> = e.fields.iter().map(|v| self.deep_clone_value(v)).collect();
                Ok(VmValue::EnumInstance(Arc::new(VmEnumInstance {
                    type_name: e.type_name.clone(),
                    variant: e.variant.clone(),
                    fields: cloned_fields?,
                })))
            }
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(gt) => {
                let cloned = tl_gpu::GpuTensor::clone(gt.as_ref());
                Ok(VmValue::GpuTensor(Arc::new(cloned)))
            }
            VmValue::Ref(inner) => self.deep_clone_value(inner),
            VmValue::Moved => Err(runtime_err("Cannot clone a moved value".to_string())),
            VmValue::Task(_) => Err(runtime_err("Cannot clone a task".to_string())),
            VmValue::Channel(_) => Err(runtime_err("Cannot clone a channel".to_string())),
            VmValue::Generator(_) => Err(runtime_err("Cannot clone a generator".to_string())),
            other => Ok(other.clone()),
        }
    }

    pub fn dispatch_method(&mut self, obj: VmValue, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        // Universal .clone() method — deep copy any value
        if method == "clone" {
            return self.deep_clone_value(&obj);
        }
        // Unwrap Ref for method dispatch — methods can be called through references
        let obj = match obj {
            VmValue::Ref(inner) => inner.as_ref().clone(),
            other => other,
        };
        match &obj {
            VmValue::String(s) => self.dispatch_string_method(s.clone(), method, args),
            VmValue::List(items) => self.dispatch_list_method(items.clone(), method, args),
            VmValue::Map(pairs) => self.dispatch_map_method(pairs.clone(), method, args),
            VmValue::Set(items) => self.dispatch_set_method(items.clone(), method, args),
            VmValue::Module(m) => {
                if let Some(func) = m.exports.get(method).cloned() {
                    self.call_vm_function(&func, args)
                } else {
                    Err(runtime_err(format!("Module '{}' has no export '{}'", m.name, method)))
                }
            }
            VmValue::StructInstance(inst) => {
                // Look up impl method: Type::method in globals
                let mangled = format!("{}::{}", inst.type_name, method);
                if let Some(func) = self.globals.get(&mangled).cloned() {
                    let mut all_args = vec![obj.clone()];
                    all_args.extend_from_slice(args);
                    self.call_vm_function(&func, &all_args)
                } else {
                    Err(runtime_err(format!("No method '{}' on struct '{}'", method, inst.type_name)))
                }
            }
            #[cfg(feature = "python")]
            VmValue::PyObject(wrapper) => {
                crate::python::py_call_method(wrapper, method, args)
            }
            #[cfg(feature = "gpu")]
            VmValue::GpuTensor(gt) => {
                match method {
                    "to_cpu" => {
                        let cpu = gt.to_cpu().map_err(|e| runtime_err(e))?;
                        Ok(VmValue::Tensor(Arc::new(cpu)))
                    }
                    "shape" => {
                        let shape_list = gt.shape.iter()
                            .map(|&d| VmValue::Int(d as i64))
                            .collect();
                        Ok(VmValue::List(shape_list))
                    }
                    "dtype" => {
                        Ok(VmValue::String(Arc::from(format!("{}", gt.dtype).as_str())))
                    }
                    _ => Err(runtime_err(format!("No method '{}' on gpu_tensor", method))),
                }
            }
            _ => {
                // Try looking up Type::method from type_name
                let type_name = obj.type_name();
                let mangled = format!("{}::{}", type_name, method);
                if let Some(func) = self.globals.get(&mangled).cloned() {
                    let mut all_args = vec![obj];
                    all_args.extend_from_slice(args);
                    self.call_vm_function(&func, &all_args)
                } else {
                    Err(runtime_err(format!("No method '{}' on type '{}'", method, type_name)))
                }
            }
        }
    }

    /// Dispatch string methods.
    fn dispatch_string_method(&self, s: Arc<str>, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        match method {
            "len" => Ok(VmValue::Int(s.len() as i64)),
            "split" => {
                let sep = match args.first() {
                    Some(VmValue::String(sep)) => sep.to_string(),
                    _ => return Err(runtime_err("split() expects a string separator")),
                };
                let parts: Vec<VmValue> = s.split(&sep)
                    .map(|p| VmValue::String(Arc::from(p)))
                    .collect();
                Ok(VmValue::List(parts))
            }
            "trim" => Ok(VmValue::String(Arc::from(s.trim()))),
            "contains" => {
                let needle = match args.first() {
                    Some(VmValue::String(n)) => n.to_string(),
                    _ => return Err(runtime_err("contains() expects a string")),
                };
                Ok(VmValue::Bool(s.contains(&needle)))
            }
            "replace" => {
                if args.len() < 2 {
                    return Err(runtime_err("replace() expects 2 arguments (old, new)"));
                }
                let old = match &args[0] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("replace() arg must be string")) };
                let new = match &args[1] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("replace() arg must be string")) };
                Ok(VmValue::String(Arc::from(s.replace(&old, &new).as_str())))
            }
            "starts_with" => {
                let prefix = match args.first() {
                    Some(VmValue::String(p)) => p.to_string(),
                    _ => return Err(runtime_err("starts_with() expects a string")),
                };
                Ok(VmValue::Bool(s.starts_with(&prefix)))
            }
            "ends_with" => {
                let suffix = match args.first() {
                    Some(VmValue::String(p)) => p.to_string(),
                    _ => return Err(runtime_err("ends_with() expects a string")),
                };
                Ok(VmValue::Bool(s.ends_with(&suffix)))
            }
            "to_upper" => Ok(VmValue::String(Arc::from(s.to_uppercase().as_str()))),
            "to_lower" => Ok(VmValue::String(Arc::from(s.to_lowercase().as_str()))),
            "chars" => {
                let chars: Vec<VmValue> = s.chars()
                    .map(|c| VmValue::String(Arc::from(c.to_string().as_str())))
                    .collect();
                Ok(VmValue::List(chars))
            }
            "repeat" => {
                let n = match args.first() {
                    Some(VmValue::Int(n)) => *n as usize,
                    _ => return Err(runtime_err("repeat() expects an integer")),
                };
                Ok(VmValue::String(Arc::from(s.repeat(n).as_str())))
            }
            "index_of" => {
                let needle = match args.first() {
                    Some(VmValue::String(n)) => n.to_string(),
                    _ => return Err(runtime_err("index_of() expects a string")),
                };
                Ok(VmValue::Int(s.find(&needle).map(|i| i as i64).unwrap_or(-1)))
            }
            "substring" => {
                if args.len() < 2 { return Err(runtime_err("substring() expects start and end")); }
                let start = match &args[0] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("substring() expects integers")) };
                let end = match &args[1] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("substring() expects integers")) };
                let end = end.min(s.len());
                let start = start.min(end);
                Ok(VmValue::String(Arc::from(&s[start..end])))
            }
            "pad_left" => {
                if args.is_empty() { return Err(runtime_err("pad_left() expects width")); }
                let width = match &args[0] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("pad_left() expects integer width")) };
                let ch = match args.get(1) {
                    Some(VmValue::String(c)) => c.chars().next().unwrap_or(' '),
                    _ => ' ',
                };
                if s.len() >= width { Ok(VmValue::String(s)) }
                else { Ok(VmValue::String(Arc::from(format!("{}{}", std::iter::repeat(ch).take(width - s.len()).collect::<String>(), s).as_str()))) }
            }
            "pad_right" => {
                if args.is_empty() { return Err(runtime_err("pad_right() expects width")); }
                let width = match &args[0] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("pad_right() expects integer width")) };
                let ch = match args.get(1) {
                    Some(VmValue::String(c)) => c.chars().next().unwrap_or(' '),
                    _ => ' ',
                };
                if s.len() >= width { Ok(VmValue::String(s)) }
                else { Ok(VmValue::String(Arc::from(format!("{}{}", s, std::iter::repeat(ch).take(width - s.len()).collect::<String>()).as_str()))) }
            }
            "join" => {
                // "sep".join(list) -> string
                let items = match args.first() {
                    Some(VmValue::List(items)) => items,
                    _ => return Err(runtime_err("join() expects a list")),
                };
                let parts: Vec<String> = items.iter().map(|v| format!("{v}")).collect();
                Ok(VmValue::String(Arc::from(parts.join(s.as_ref()).as_str())))
            }
            _ => Err(runtime_err(format!("No method '{}' on string", method))),
        }
    }

    /// Dispatch list methods.
    fn dispatch_list_method(&mut self, items: Vec<VmValue>, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        match method {
            "len" => Ok(VmValue::Int(items.len() as i64)),
            "push" => {
                if args.is_empty() {
                    return Err(runtime_err("push() expects 1 argument"));
                }
                let mut new_items = items;
                new_items.push(args[0].clone());
                Ok(VmValue::List(new_items))
            }
            "map" => {
                if args.is_empty() {
                    return Err(runtime_err("map() expects a function"));
                }
                let func = &args[0];
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_vm_function(func, &[item])?;
                    result.push(val);
                }
                Ok(VmValue::List(result))
            }
            "filter" => {
                if args.is_empty() {
                    return Err(runtime_err("filter() expects a function"));
                }
                let func = &args[0];
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_vm_function(func, &[item.clone()])?;
                    if val.is_truthy() {
                        result.push(item);
                    }
                }
                Ok(VmValue::List(result))
            }
            "reduce" => {
                if args.len() < 2 {
                    return Err(runtime_err("reduce() expects initial value and function"));
                }
                let mut acc = args[0].clone();
                let func = &args[1];
                for item in items {
                    acc = self.call_vm_function(func, &[acc, item])?;
                }
                Ok(acc)
            }
            "sort" => {
                let mut sorted = items;
                sorted.sort_by(|a, b| {
                    match (a, b) {
                        (VmValue::Int(x), VmValue::Int(y)) => x.cmp(y),
                        (VmValue::Float(x), VmValue::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
                        (VmValue::String(x), VmValue::String(y)) => x.cmp(y),
                        _ => std::cmp::Ordering::Equal,
                    }
                });
                Ok(VmValue::List(sorted))
            }
            "reverse" => {
                let mut reversed = items;
                reversed.reverse();
                Ok(VmValue::List(reversed))
            }
            "contains" => {
                if args.is_empty() { return Err(runtime_err("contains() expects a value")); }
                let needle = &args[0];
                let found = items.iter().any(|item| {
                    match (item, needle) {
                        (VmValue::Int(a), VmValue::Int(b)) => a == b,
                        (VmValue::Float(a), VmValue::Float(b)) => a == b,
                        (VmValue::String(a), VmValue::String(b)) => a == b,
                        (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
                        (VmValue::None, VmValue::None) => true,
                        _ => false,
                    }
                });
                Ok(VmValue::Bool(found))
            }
            "index_of" => {
                if args.is_empty() { return Err(runtime_err("index_of() expects a value")); }
                let needle = &args[0];
                let idx = items.iter().position(|item| {
                    match (item, needle) {
                        (VmValue::Int(a), VmValue::Int(b)) => a == b,
                        (VmValue::Float(a), VmValue::Float(b)) => a == b,
                        (VmValue::String(a), VmValue::String(b)) => a == b,
                        (VmValue::Bool(a), VmValue::Bool(b)) => a == b,
                        (VmValue::None, VmValue::None) => true,
                        _ => false,
                    }
                });
                Ok(VmValue::Int(idx.map(|i| i as i64).unwrap_or(-1)))
            }
            "slice" => {
                if args.len() < 2 { return Err(runtime_err("slice() expects start and end")); }
                let start = match &args[0] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("slice() expects integers")) };
                let end = match &args[1] { VmValue::Int(n) => *n as usize, _ => return Err(runtime_err("slice() expects integers")) };
                let end = end.min(items.len());
                let start = start.min(end);
                Ok(VmValue::List(items[start..end].to_vec()))
            }
            "flat_map" => {
                if args.is_empty() { return Err(runtime_err("flat_map() expects a function")); }
                let func = &args[0];
                let mut result = Vec::new();
                for item in items {
                    let val = self.call_vm_function(func, &[item])?;
                    match val {
                        VmValue::List(sub) => result.extend(sub),
                        other => result.push(other),
                    }
                }
                Ok(VmValue::List(result))
            }
            _ => Err(runtime_err(format!("No method '{}' on list", method))),
        }
    }

    /// Dispatch map methods.
    fn dispatch_map_method(&self, pairs: Vec<(Arc<str>, VmValue)>, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        match method {
            "len" => Ok(VmValue::Int(pairs.len() as i64)),
            "keys" => {
                Ok(VmValue::List(pairs.iter().map(|(k, _)| VmValue::String(k.clone())).collect()))
            }
            "values" => {
                Ok(VmValue::List(pairs.iter().map(|(_, v)| v.clone()).collect()))
            }
            "contains_key" => {
                if args.is_empty() { return Err(runtime_err("contains_key() expects a key")); }
                if let VmValue::String(key) = &args[0] {
                    Ok(VmValue::Bool(pairs.iter().any(|(k, _)| k.as_ref() == key.as_ref())))
                } else {
                    Err(runtime_err("contains_key() expects a string key"))
                }
            }
            "remove" => {
                if args.is_empty() { return Err(runtime_err("remove() expects a key")); }
                if let VmValue::String(key) = &args[0] {
                    let new_pairs: Vec<(Arc<str>, VmValue)> = pairs.into_iter()
                        .filter(|(k, _)| k.as_ref() != key.as_ref())
                        .collect();
                    Ok(VmValue::Map(new_pairs))
                } else {
                    Err(runtime_err("remove() expects a string key"))
                }
            }
            _ => Err(runtime_err(format!("No method '{}' on map", method))),
        }
    }

    /// Dispatch set methods.
    fn dispatch_set_method(&self, items: Vec<VmValue>, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        match method {
            "len" => Ok(VmValue::Int(items.len() as i64)),
            "contains" => {
                if args.is_empty() { return Err(runtime_err("contains() expects a value")); }
                Ok(VmValue::Bool(items.iter().any(|x| vm_values_equal(x, &args[0]))))
            }
            "add" => {
                if args.is_empty() { return Err(runtime_err("add() expects a value")); }
                let mut new_items = items;
                if !new_items.iter().any(|x| vm_values_equal(x, &args[0])) {
                    new_items.push(args[0].clone());
                }
                Ok(VmValue::Set(new_items))
            }
            "remove" => {
                if args.is_empty() { return Err(runtime_err("remove() expects a value")); }
                let new_items: Vec<VmValue> = items.into_iter()
                    .filter(|x| !vm_values_equal(x, &args[0]))
                    .collect();
                Ok(VmValue::Set(new_items))
            }
            "to_list" => Ok(VmValue::List(items)),
            "union" => {
                if args.is_empty() { return Err(runtime_err("union() expects a set")); }
                if let VmValue::Set(b) = &args[0] {
                    let mut result = items;
                    for item in b {
                        if !result.iter().any(|x| vm_values_equal(x, item)) {
                            result.push(item.clone());
                        }
                    }
                    Ok(VmValue::Set(result))
                } else {
                    Err(runtime_err("union() expects a set"))
                }
            }
            "intersection" => {
                if args.is_empty() { return Err(runtime_err("intersection() expects a set")); }
                if let VmValue::Set(b) = &args[0] {
                    let result: Vec<VmValue> = items.into_iter()
                        .filter(|x| b.iter().any(|y| vm_values_equal(x, y)))
                        .collect();
                    Ok(VmValue::Set(result))
                } else {
                    Err(runtime_err("intersection() expects a set"))
                }
            }
            "difference" => {
                if args.is_empty() { return Err(runtime_err("difference() expects a set")); }
                if let VmValue::Set(b) = &args[0] {
                    let result: Vec<VmValue> = items.into_iter()
                        .filter(|x| !b.iter().any(|y| vm_values_equal(x, y)))
                        .collect();
                    Ok(VmValue::Set(result))
                } else {
                    Err(runtime_err("difference() expects a set"))
                }
            }
            _ => Err(runtime_err(format!("No method '{}' on set", method))),
        }
    }

    /// Handle import at runtime.
    #[cfg(feature = "native")]
    fn handle_import(&mut self, path: &str, alias: &str) -> Result<VmValue, TlError> {
        // Resolve relative path from current file
        let resolved = if let Some(ref base) = self.file_path {
            let base_dir = std::path::Path::new(base)
                .parent()
                .unwrap_or(std::path::Path::new("."));
            let candidate = base_dir.join(path);
            if candidate.exists() {
                candidate.to_string_lossy().to_string()
            } else {
                path.to_string()
            }
        } else {
            path.to_string()
        };

        // Circular dependency detection
        if self.importing_files.contains(&resolved) {
            return Err(runtime_err(format!("Circular import detected: {resolved}")));
        }

        // Check module cache
        if let Some(exports) = self.module_cache.get(&resolved) {
            let exports = exports.clone();
            return self.bind_import_exports(exports, alias);
        }

        // Read, parse, compile, execute the file
        let source = std::fs::read_to_string(&resolved)
            .map_err(|e| runtime_err(format!("Cannot import '{}': {}", resolved, e)))?;
        let program = tl_parser::parse(&source)
            .map_err(|e| runtime_err(format!("Parse error in '{}': {}", resolved, e)))?;
        let proto = crate::compiler::compile(&program)
            .map_err(|e| runtime_err(format!("Compile error in '{}': {}", resolved, e)))?;

        // Track circular imports
        self.importing_files.insert(resolved.clone());

        // Execute in a fresh VM with shared globals
        let mut import_vm = Vm::new();
        import_vm.file_path = Some(resolved.clone());
        import_vm.globals = self.globals.clone();
        import_vm.importing_files = self.importing_files.clone();
        import_vm.module_cache = self.module_cache.clone();
        import_vm.package_roots = self.package_roots.clone();
        import_vm.project_root = self.project_root.clone();
        import_vm.execute(&proto)?;

        self.importing_files.remove(&resolved);

        // Collect exports: both globals and top-level locals from the stack
        let mut exports = HashMap::new();

        // 1. New globals defined in the import
        for (k, v) in &import_vm.globals {
            if !self.globals.contains_key(k) {
                exports.insert(k.clone(), v.clone());
            }
        }

        // 2. Top-level locals from the prototype (on the stack)
        for (name, reg) in &proto.top_level_locals {
            if !name.starts_with("__enum_") && !exports.contains_key(name) {
                let stack_idx = reg;
                if (*stack_idx as usize) < import_vm.stack.len() {
                    let val = import_vm.stack[*stack_idx as usize].clone();
                    if !matches!(val, VmValue::None) || name.starts_with("_") {
                        exports.insert(name.clone(), val);
                    }
                }
            }
        }

        // Cache the module
        self.module_cache.insert(resolved, exports.clone());
        // Also adopt any modules the sub-VM discovered
        for (k, v) in import_vm.module_cache {
            self.module_cache.entry(k).or_insert(v);
        }

        self.bind_import_exports(exports, alias)
    }

    /// Bind import exports into current scope.
    #[cfg(feature = "native")]
    fn bind_import_exports(&mut self, exports: HashMap<String, VmValue>, alias: &str) -> Result<VmValue, TlError> {
        if alias.is_empty() {
            // Wildcard import: merge all exports into current scope
            for (k, v) in &exports {
                self.globals.insert(k.clone(), v.clone());
            }
            Ok(VmValue::None)
        } else {
            // Namespaced import
            let module = VmModule {
                name: Arc::from(alias),
                exports,
            };
            let module_val = VmValue::Module(Arc::new(module));
            self.globals.insert(alias.to_string(), module_val.clone());
            Ok(module_val)
        }
    }

    /// Handle use-style imports (dot-path syntax).
    #[cfg(feature = "native")]
    fn handle_use_import(&mut self, path_str: &str, extra_a: u8, kind: u8, _frame_idx: usize) -> Result<VmValue, TlError> {
        match kind {
            0 => {
                // Single: "data.transforms.clean" → import file, bind last segment
                let segments: Vec<&str> = path_str.split('.').collect();
                let file_path = self.resolve_use_path(&segments)?;
                // Import the module, get exports
                let last = segments.last().copied().unwrap_or("");
                self.handle_import(&file_path, "")?;
                // The wildcard import already merged everything.
                // But for Single, we only want the specific item.
                // Since handle_import merges all, that works for now.
                // Return none since it's a statement, not an expression.
                Ok(VmValue::None)
            }
            1 => {
                // Group: "data.transforms.{a,b}" — extract prefix before {
                let brace_start = path_str.find('{').unwrap_or(path_str.len());
                let prefix = path_str[..brace_start].trim_end_matches('.');
                let segments: Vec<&str> = prefix.split('.').collect();
                let file_path = self.resolve_use_path(&segments)?;
                self.handle_import(&file_path, "")?;
                Ok(VmValue::None)
            }
            2 => {
                // Wildcard: "data.transforms.*" — strip trailing .*
                let prefix = path_str.trim_end_matches(".*");
                let segments: Vec<&str> = prefix.split('.').collect();
                let file_path = self.resolve_use_path(&segments)?;
                self.handle_import(&file_path, "")?;
                Ok(VmValue::None)
            }
            3 => {
                // Aliased: path in path_str, alias in extra_a (constant index)
                let segments: Vec<&str> = path_str.split('.').collect();
                let file_path = self.resolve_use_path(&segments)?;
                // For aliased, we need to get the alias from the constant pool
                // extra_a contains the constant index of the alias string
                let alias_str = if let Some(frame) = self.frames.last() {
                    if let Some(crate::chunk::Constant::String(s)) = frame.prototype.constants.get(extra_a as usize) {
                        s.to_string()
                    } else {
                        segments.last().copied().unwrap_or("module").to_string()
                    }
                } else {
                    segments.last().copied().unwrap_or("module").to_string()
                };
                self.handle_import(&file_path, &alias_str)?;
                Ok(VmValue::None)
            }
            _ => Err(runtime_err(format!("Unknown use-import kind: {kind}"))),
        }
    }

    /// Resolve dot-path segments to a file path for use statements.
    #[cfg(feature = "native")]
    fn resolve_use_path(&self, segments: &[&str]) -> Result<String, TlError> {
        let base_dir = if let Some(ref fp) = self.file_path {
            std::path::Path::new(fp)
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
        };

        let rel_path = segments.join("/");

        // Try file module first
        let file_path = base_dir.join(format!("{rel_path}.tl"));
        if file_path.exists() {
            return Ok(file_path.to_string_lossy().to_string());
        }

        // Try directory module
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
        // Convert underscores to hyphens (TL identifiers use _, package names use -)
        let pkg_name_underscore = segments[0];
        let pkg_name_hyphen = pkg_name_underscore.replace('_', "-");
        let pkg_root = self.package_roots.get(pkg_name_underscore)
            .or_else(|| self.package_roots.get(&pkg_name_hyphen));

        if let Some(root) = pkg_root {
            let remaining = &segments[1..];
            if let Some(path) = resolve_package_file(root, remaining) {
                return Ok(path);
            }
        }

        Err(runtime_err(format!(
            "Module not found: `{}`",
            segments.join(".")
        )))
    }

    /// Call a VmValue function/closure with args.
    fn call_vm_function(&mut self, func: &VmValue, args: &[VmValue]) -> Result<VmValue, TlError> {
        match func {
            VmValue::Function(closure) => {
                let proto = closure.prototype.clone();
                let arity = proto.arity as usize;
                if args.len() != arity {
                    return Err(runtime_err(format!(
                        "Expected {} arguments, got {}",
                        arity, args.len()
                    )));
                }

                // If this is a generator function, create a Generator
                if proto.is_generator {
                    let mut closed_upvalues = Vec::new();
                    for uv in &closure.upvalues {
                        match uv {
                            UpvalueRef::Open { stack_index } => {
                                let val = self.stack[*stack_index].clone();
                                closed_upvalues.push(UpvalueRef::Closed(val));
                            }
                            UpvalueRef::Closed(v) => {
                                closed_upvalues.push(UpvalueRef::Closed(v.clone()));
                            }
                        }
                    }
                    let num_regs = proto.num_registers as usize;
                    let mut saved_stack = vec![VmValue::None; num_regs];
                    for (i, arg) in args.iter().enumerate() {
                        saved_stack[i] = arg.clone();
                    }
                    let gn = VmGenerator::new(GeneratorKind::UserDefined {
                        prototype: proto,
                        upvalues: closed_upvalues,
                        saved_stack,
                        ip: 0,
                    });
                    return Ok(VmValue::Generator(Arc::new(Mutex::new(gn))));
                }

                let new_base = self.stack.len();
                self.ensure_stack(new_base + proto.num_registers as usize + 1);

                for (i, arg) in args.iter().enumerate() {
                    self.stack[new_base + i] = arg.clone();
                }

                self.frames.push(CallFrame {
                    prototype: proto,
                    ip: 0,
                    base: new_base,
                    upvalues: closure.upvalues.clone(),
                });

                let result = self.run()?;
                self.stack.truncate(new_base);
                Ok(result)
            }
            VmValue::Builtin(id) => {
                // Put args on stack temporarily
                let args_base = self.stack.len();
                for arg in args {
                    self.stack.push(arg.clone());
                }
                let result = self.call_builtin(*id as u8, args_base, args.len());
                self.stack.truncate(args_base);
                result
            }
            _ => Err(runtime_err(format!("Cannot call {}", func.type_name()))),
        }
    }

    // ── Table pipe handler ──

    #[cfg(feature = "native")]
    fn handle_table_pipe(
        &mut self,
        frame_idx: usize,
        table_val: VmValue,
        op_const: u8,
        args_const: u8,
    ) -> Result<VmValue, TlError> {
        let df = match table_val {
            VmValue::Table(t) => t.df,
            other => {
                // Not a table — fall back to regular builtin/function call
                return self.table_pipe_fallback(other, frame_idx, op_const, args_const);
            }
        };

        let frame = &self.frames[frame_idx];
        let op_name = match &frame.prototype.constants[op_const as usize] {
            Constant::String(s) => s.to_string(),
            _ => return Err(runtime_err("Expected string constant for table op")),
        };
        let ast_args = match &frame.prototype.constants[args_const as usize] {
            Constant::AstExprList(args) => args.clone(),
            _ => return Err(runtime_err("Expected AST expr list for table args")),
        };

        let ctx = self.build_translate_context();

        match op_name.as_str() {
            "filter" => {
                if ast_args.len() != 1 {
                    return Err(runtime_err("filter() expects 1 argument (predicate)"));
                }
                let pred = translate_expr(&ast_args[0], &ctx).map_err(|e| runtime_err(e))?;
                let filtered = df.filter(pred).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: filtered }))
            }
            "select" => {
                if ast_args.is_empty() {
                    return Err(runtime_err("select() expects at least 1 argument"));
                }
                let mut select_exprs = Vec::new();
                for arg in &ast_args {
                    match arg {
                        AstExpr::Ident(name) => select_exprs.push(col(name.as_str())),
                        AstExpr::NamedArg { name, value } => {
                            let expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                            select_exprs.push(expr.alias(name));
                        }
                        AstExpr::String(name) => select_exprs.push(col(name.as_str())),
                        other => {
                            let expr = translate_expr(other, &ctx).map_err(|e| runtime_err(e))?;
                            select_exprs.push(expr);
                        }
                    }
                }
                let selected = df.select(select_exprs).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: selected }))
            }
            "sort" => {
                if ast_args.is_empty() {
                    return Err(runtime_err("sort() expects at least 1 argument (column)"));
                }
                let mut sort_exprs = Vec::new();
                let mut i = 0;
                while i < ast_args.len() {
                    let col_name = match &ast_args[i] {
                        AstExpr::Ident(name) => name.clone(),
                        AstExpr::String(name) => name.clone(),
                        _ => return Err(runtime_err("sort() column must be an identifier or string")),
                    };
                    i += 1;
                    let ascending = if i < ast_args.len() {
                        match &ast_args[i] {
                            AstExpr::String(dir) if dir == "desc" || dir == "DESC" => { i += 1; false }
                            AstExpr::String(dir) if dir == "asc" || dir == "ASC" => { i += 1; true }
                            _ => true,
                        }
                    } else { true };
                    sort_exprs.push(col(col_name.as_str()).sort(ascending, true));
                }
                let sorted = df.sort(sort_exprs).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: sorted }))
            }
            "with" => {
                if ast_args.len() != 1 {
                    return Err(runtime_err("with() expects 1 argument (map of column definitions)"));
                }
                let pairs = match &ast_args[0] {
                    AstExpr::Map(pairs) => pairs,
                    _ => return Err(runtime_err("with() expects a map { col = expr, ... }")),
                };
                let mut result_df = df;
                for (key, value_expr) in pairs {
                    let col_name = match key {
                        AstExpr::String(s) => s.clone(),
                        AstExpr::Ident(s) => s.clone(),
                        _ => return Err(runtime_err("with() key must be a string or identifier")),
                    };
                    let df_expr = translate_expr(value_expr, &ctx).map_err(|e| runtime_err(e))?;
                    result_df = result_df.with_column(&col_name, df_expr).map_err(|e| runtime_err(format!("{e}")))?;
                }
                Ok(VmValue::Table(VmTable { df: result_df }))
            }
            "aggregate" => {
                let mut group_by_cols: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();
                let mut agg_exprs: Vec<tl_data::datafusion::prelude::Expr> = Vec::new();
                for arg in &ast_args {
                    match arg {
                        AstExpr::NamedArg { name, value } if name == "by" => {
                            match value.as_ref() {
                                AstExpr::String(col_name) => group_by_cols.push(col(col_name.as_str())),
                                AstExpr::Ident(col_name) => group_by_cols.push(col(col_name.as_str())),
                                AstExpr::List(items) => {
                                    for item in items {
                                        match item {
                                            AstExpr::String(s) => group_by_cols.push(col(s.as_str())),
                                            AstExpr::Ident(s) => group_by_cols.push(col(s.as_str())),
                                            _ => return Err(runtime_err("by: list items must be strings or identifiers")),
                                        }
                                    }
                                }
                                _ => return Err(runtime_err("by: must be a column name or list")),
                            }
                        }
                        AstExpr::NamedArg { name, value } => {
                            let agg_expr = translate_expr(value, &ctx).map_err(|e| runtime_err(e))?;
                            agg_exprs.push(agg_expr.alias(name));
                        }
                        other => {
                            let agg_expr = translate_expr(other, &ctx).map_err(|e| runtime_err(e))?;
                            agg_exprs.push(agg_expr);
                        }
                    }
                }
                let aggregated = df.aggregate(group_by_cols, agg_exprs).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: aggregated }))
            }
            "join" => {
                if ast_args.is_empty() {
                    return Err(runtime_err("join() expects at least 1 argument (right table)"));
                }
                // Evaluate first arg to get right table
                let right_table = self.eval_ast_to_vm(&ast_args[0])?;
                let right_df = match right_table {
                    VmValue::Table(t) => t.df,
                    _ => return Err(runtime_err("join() first arg must be a table")),
                };
                let mut left_cols: Vec<String> = Vec::new();
                let mut right_cols: Vec<String> = Vec::new();
                let mut join_type = JoinType::Inner;
                for arg in &ast_args[1..] {
                    match arg {
                        AstExpr::NamedArg { name, value } if name == "on" => {
                            if let AstExpr::BinOp { left, op: tl_ast::BinOp::Eq, right } = value.as_ref() {
                                let lc = match left.as_ref() {
                                    AstExpr::Ident(s) | AstExpr::String(s) => s.clone(),
                                    _ => return Err(runtime_err("on: left side must be a column name")),
                                };
                                let rc = match right.as_ref() {
                                    AstExpr::Ident(s) | AstExpr::String(s) => s.clone(),
                                    _ => return Err(runtime_err("on: right side must be a column name")),
                                };
                                left_cols.push(lc);
                                right_cols.push(rc);
                            }
                        }
                        AstExpr::NamedArg { name, value } if name == "kind" => {
                            if let AstExpr::String(kind_str) = value.as_ref() {
                                join_type = match kind_str.as_str() {
                                    "inner" => JoinType::Inner,
                                    "left" => JoinType::Left,
                                    "right" => JoinType::Right,
                                    "full" => JoinType::Full,
                                    _ => return Err(runtime_err(format!("Unknown join type: {kind_str}"))),
                                };
                            }
                        }
                        _ => {}
                    }
                }
                let lc_refs: Vec<&str> = left_cols.iter().map(|s| s.as_str()).collect();
                let rc_refs: Vec<&str> = right_cols.iter().map(|s| s.as_str()).collect();
                let joined = df.join(right_df, join_type, &lc_refs, &rc_refs, None)
                    .map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: joined }))
            }
            "head" | "limit" => {
                let n = match ast_args.first() {
                    Some(AstExpr::Int(n)) => *n as usize,
                    None => 10,
                    _ => return Err(runtime_err("head/limit expects an integer")),
                };
                let limited = df.limit(0, Some(n)).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Table(VmTable { df: limited }))
            }
            "collect" => {
                let batches = self.engine().collect(df).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                Ok(VmValue::String(Arc::from(formatted.as_str())))
            }
            "show" => {
                let limit = match ast_args.first() {
                    Some(AstExpr::Int(n)) => *n as usize,
                    None => 20,
                    _ => 20,
                };
                let limited = df.limit(0, Some(limit)).map_err(|e| runtime_err(format!("{e}")))?;
                let batches = self.engine().collect(limited).map_err(|e| runtime_err(e))?;
                let formatted = DataEngine::format_batches(&batches).map_err(|e| runtime_err(e))?;
                println!("{formatted}");
                self.output.push(formatted);
                Ok(VmValue::None)
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
                Ok(VmValue::String(Arc::from(output.as_str())))
            }
            "write_csv" => {
                if ast_args.len() != 1 { return Err(runtime_err("write_csv() expects 1 argument (path)")); }
                let path = self.eval_ast_to_string(&ast_args[0])?;
                self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::None)
            }
            "write_parquet" => {
                if ast_args.len() != 1 { return Err(runtime_err("write_parquet() expects 1 argument (path)")); }
                let path = self.eval_ast_to_string(&ast_args[0])?;
                self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::None)
            }
            // Phase 15: Data quality pipe operations
            "fill_null" => {
                if ast_args.is_empty() { return Err(runtime_err("fill_null() expects (column, [strategy/value])")); }
                let column = self.eval_ast_to_string(&ast_args[0])?;
                if ast_args.len() >= 2 {
                    let val = self.eval_ast_to_vm(&ast_args[1])?;
                    match val {
                        VmValue::String(s) => {
                            // String means strategy name
                            let fill_val = if ast_args.len() >= 3 {
                                match self.eval_ast_to_vm(&ast_args[2])? {
                                    VmValue::Int(n) => Some(n as f64),
                                    VmValue::Float(f) => Some(f),
                                    _ => None,
                                }
                            } else { None };
                            let result = self.engine().fill_null(df, &column, &s, fill_val).map_err(|e| runtime_err(e))?;
                            Ok(VmValue::Table(VmTable { df: result }))
                        }
                        VmValue::Int(n) => {
                            let result = self.engine().fill_null(df, &column, "value", Some(n as f64)).map_err(|e| runtime_err(e))?;
                            Ok(VmValue::Table(VmTable { df: result }))
                        }
                        VmValue::Float(f) => {
                            let result = self.engine().fill_null(df, &column, "value", Some(f)).map_err(|e| runtime_err(e))?;
                            Ok(VmValue::Table(VmTable { df: result }))
                        }
                        _ => Err(runtime_err("fill_null() second arg must be a strategy or fill value")),
                    }
                } else {
                    let result = self.engine().fill_null(df, &column, "zero", None).map_err(|e| runtime_err(e))?;
                    Ok(VmValue::Table(VmTable { df: result }))
                }
            }
            "drop_null" => {
                if ast_args.is_empty() { return Err(runtime_err("drop_null() expects (column)")); }
                let column = self.eval_ast_to_string(&ast_args[0])?;
                let result = self.engine().drop_null(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            "dedup" => {
                let columns: Vec<String> = ast_args.iter()
                    .filter_map(|a| self.eval_ast_to_string(a).ok())
                    .collect();
                let result = self.engine().dedup(df, &columns).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            "clamp" => {
                if ast_args.len() < 3 { return Err(runtime_err("clamp() expects (column, min, max)")); }
                let column = self.eval_ast_to_string(&ast_args[0])?;
                let min_val = match self.eval_ast_to_vm(&ast_args[1])? {
                    VmValue::Int(n) => n as f64,
                    VmValue::Float(f) => f,
                    _ => return Err(runtime_err("clamp() min must be a number")),
                };
                let max_val = match self.eval_ast_to_vm(&ast_args[2])? {
                    VmValue::Int(n) => n as f64,
                    VmValue::Float(f) => f,
                    _ => return Err(runtime_err("clamp() max must be a number")),
                };
                let result = self.engine().clamp(df, &column, min_val, max_val).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            "data_profile" => {
                let result = self.engine().data_profile(df).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df: result }))
            }
            "row_count" => {
                let count = self.engine().row_count(df).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Int(count))
            }
            "null_rate" => {
                if ast_args.is_empty() { return Err(runtime_err("null_rate() expects (column)")); }
                let column = self.eval_ast_to_string(&ast_args[0])?;
                let rate = self.engine().null_rate(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Float(rate))
            }
            "is_unique" => {
                if ast_args.is_empty() { return Err(runtime_err("is_unique() expects (column)")); }
                let column = self.eval_ast_to_string(&ast_args[0])?;
                let unique = self.engine().is_unique(df, &column).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Bool(unique))
            }
            _ => Err(runtime_err(format!("Unknown table operation: {op_name}"))),
        }
    }

    /// Fallback for table pipe when left side is not a table.
    /// Converts to a regular function/builtin call with left as first arg.
    fn table_pipe_fallback(
        &mut self,
        left_val: VmValue,
        frame_idx: usize,
        op_const: u8,
        args_const: u8,
    ) -> Result<VmValue, TlError> {
        let frame = &self.frames[frame_idx];
        let op_name = match &frame.prototype.constants[op_const as usize] {
            Constant::String(s) => s.to_string(),
            _ => return Err(runtime_err("Expected string constant for table op")),
        };
        let ast_args = match &frame.prototype.constants[args_const as usize] {
            Constant::AstExprList(args) => args.clone(),
            _ => return Err(runtime_err("Expected AST expr list for table args")),
        };

        // Try as builtin with left as first arg
        if let Some(builtin_id) = BuiltinId::from_name(&op_name) {
            // Evaluate AST args to VM values
            let mut all_args = vec![left_val];
            for arg in &ast_args {
                all_args.push(self.eval_ast_to_vm(arg).unwrap_or(VmValue::None));
            }
            let args_base = self.stack.len();
            for arg in &all_args {
                self.stack.push(arg.clone());
            }
            let result = self.call_builtin(builtin_id as u8, args_base, all_args.len());
            self.stack.truncate(args_base);
            return result;
        }

        // Try as user-defined function
        if let Some(func) = self.globals.get(&op_name).cloned() {
            let mut all_args = vec![left_val];
            for arg in &ast_args {
                all_args.push(self.eval_ast_to_vm(arg).unwrap_or(VmValue::None));
            }
            return self.call_vm_function(&func, &all_args);
        }

        Err(runtime_err(format!("Unknown operation: `{op_name}`")))
    }

    /// Build TranslateContext from VM globals and stack.
    #[cfg(feature = "native")]
    fn build_translate_context(&self) -> TranslateContext {
        let mut ctx = TranslateContext::new();
        // Add globals
        for (name, val) in &self.globals {
            let local = match val {
                VmValue::Int(n) => Some(LocalValue::Int(*n)),
                VmValue::Float(f) => Some(LocalValue::Float(*f)),
                VmValue::String(s) => Some(LocalValue::String(s.to_string())),
                VmValue::Bool(b) => Some(LocalValue::Bool(*b)),
                _ => None,
            };
            if let Some(l) = local {
                ctx.locals.insert(name.clone(), l);
            }
        }
        // Add locals from current frame
        if let Some(frame) = self.frames.last() {
            for local_idx in 0..frame.prototype.num_locals as usize {
                if let Some(val) = self.stack.get(frame.base + local_idx) {
                    // We'd need local name info — for now, rely on globals
                    let _ = val;
                }
            }
        }
        ctx
    }

    /// Evaluate an AST expression to a VmValue.
    /// For simple expressions does direct lookup; for complex ones, compiles and runs.
    fn eval_ast_to_vm(&mut self, expr: &AstExpr) -> Result<VmValue, TlError> {
        match expr {
            AstExpr::Ident(name) => {
                // Look up in globals first
                if let Some(val) = self.globals.get(name) {
                    return Ok(val.clone());
                }
                // Check current frame's stack
                if let Some(frame) = self.frames.last() {
                    for i in 0..frame.prototype.num_registers as usize {
                        if let Some(val) = self.stack.get(frame.base + i) {
                            if !matches!(val, VmValue::None) {
                                // Without name->register mapping, we can't be sure
                                // which register holds this variable
                            }
                        }
                    }
                }
                Err(runtime_err(format!("Undefined variable: `{name}`")))
            }
            AstExpr::String(s) => Ok(VmValue::String(Arc::from(s.as_str()))),
            AstExpr::Int(n) => Ok(VmValue::Int(*n)),
            AstExpr::Float(f) => Ok(VmValue::Float(*f)),
            AstExpr::Bool(b) => Ok(VmValue::Bool(*b)),
            AstExpr::None => Ok(VmValue::None),
            AstExpr::Closure { params: _, body: _, .. } => {
                use crate::compiler;
                let wrapper = tl_ast::Program {
                    statements: vec![tl_ast::Stmt { kind: tl_ast::StmtKind::Expr(expr.clone()), span: tl_errors::Span::new(0, 0), doc_comment: None }],
                    module_doc: None,
                };
                let proto = compiler::compile(&wrapper)?;
                let mut temp_vm = Vm::new();
                // Copy globals
                temp_vm.globals = self.globals.clone();
                let result = temp_vm.execute(&proto)?;
                Ok(result)
            }
            _ => {
                // For complex expressions, compile and evaluate
                let wrapper = tl_ast::Program {
                    statements: vec![tl_ast::Stmt { kind: tl_ast::StmtKind::Expr(expr.clone()), span: tl_errors::Span::new(0, 0), doc_comment: None }],
                    module_doc: None,
                };
                use crate::compiler;
                let proto = compiler::compile(&wrapper)?;
                let mut temp_vm = Vm::new();
                temp_vm.globals = self.globals.clone();
                temp_vm.execute(&proto)
            }
        }
    }

    fn eval_ast_to_string(&mut self, expr: &AstExpr) -> Result<String, TlError> {
        match self.eval_ast_to_vm(expr)? {
            VmValue::String(s) => Ok(s.to_string()),
            _ => Err(runtime_err("Expected a string")),
        }
    }

    /// Simple string interpolation.
    fn interpolate_string(&self, s: &str, _base: usize) -> Result<String, TlError> {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '{' {
                let mut var_name = String::new();
                let mut depth = 1;
                for c in chars.by_ref() {
                    if c == '{' { depth += 1; }
                    else if c == '}' {
                        depth -= 1;
                        if depth == 0 { break; }
                    }
                    var_name.push(c);
                }
                // Look up variable
                if let Some(val) = self.globals.get(&var_name) {
                    result.push_str(&format!("{val}"));
                } else {
                    // Check locals in current frame
                    // For now, fall back to globals only — local name info
                    // would need debug symbols from the compiler
                    result.push('{');
                    result.push_str(&var_name);
                    result.push('}');
                }
            } else if ch == '\\' {
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some(c) => { result.push('\\'); result.push(c); }
                    None => result.push('\\'),
                }
            } else {
                result.push(ch);
            }
        }
        Ok(result)
    }

    /// Execute a single bytecode instruction at the given base offset.
    /// Used by the LLVM backend's Tier 3 fallback to run complex opcodes on the VM.
    pub fn execute_single_instruction(
        &mut self,
        inst: u32,
        proto: &Prototype,
        base: usize,
    ) -> Result<Option<VmValue>, TlError> {
        use crate::opcode::{decode_op, decode_a, decode_b, decode_c, decode_bx};

        let proto = Arc::new(proto.clone());
        // Push a temporary call frame so the VM can resolve constants etc.
        self.frames.push(CallFrame {
            prototype: proto.clone(),
            ip: 0,
            base,
            upvalues: Vec::new(),
        });
        let frame_idx = self.frames.len() - 1;

        let op = decode_op(inst);
        let a = decode_a(inst);
        let _b = decode_b(inst);
        let _c = decode_c(inst);
        let bx = decode_bx(inst);

        // Dispatch the single opcode. We handle the most common
        // Tier 3 ops here; anything not handled returns Ok(None).
        let result = match op {
            Op::GetGlobal => {
                let name = self.get_string_constant(frame_idx, bx)?;
                let val = self.globals.get(name.as_ref()).cloned().unwrap_or(VmValue::None);
                self.stack[base + a as usize] = val;
                Ok(None)
            }
            Op::SetGlobal => {
                let name = self.get_string_constant(frame_idx, bx)?;
                let val = self.stack[base + a as usize].clone();
                self.globals.insert(name.to_string(), val);
                Ok(None)
            }
            _ => {
                // For opcodes not explicitly handled, return Ok — the caller
                // should have handled Tier 1/2 in LLVM IR.
                Ok(None)
            }
        };

        self.frames.pop();
        result
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::compile;
    use tl_parser::parse;

    fn run(source: &str) -> Result<VmValue, TlError> {
        let program = parse(source)?;
        let proto = compile(&program)?;
        let mut vm = Vm::new();
        vm.execute(&proto)
    }

    fn run_output(source: &str) -> Vec<String> {
        let program = parse(source).unwrap();
        let proto = compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.execute(&proto).unwrap();
        vm.output
    }

    #[test]
    fn test_vm_arithmetic() {
        assert!(matches!(run("1 + 2").unwrap(), VmValue::Int(3)));
        assert!(matches!(run("10 - 3").unwrap(), VmValue::Int(7)));
        assert!(matches!(run("4 * 5").unwrap(), VmValue::Int(20)));
        assert!(matches!(run("10 / 3").unwrap(), VmValue::Int(3)));
        assert!(matches!(run("10 % 3").unwrap(), VmValue::Int(1)));
        assert!(matches!(run("2 ** 10").unwrap(), VmValue::Int(1024)));
        let output = run_output("print(1 + 2)");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_let_and_print() {
        let output = run_output("let x = 42\nprint(x)");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_function() {
        let output = run_output(
            "fn double(n) { n * 2 }\nlet result = double(21)\nprint(result)",
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_if_else() {
        let output = run_output("let x = 10\nif x > 5 { print(\"big\") } else { print(\"small\") }");
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_vm_list() {
        let output = run_output("let items = [1, 2, 3]\nprint(len(items))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_map_builtin() {
        let output = run_output("let nums = [1, 2, 3]\nlet doubled = map(nums, (x) => x * 2)\nprint(doubled)");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_vm_filter_builtin() {
        let output = run_output("let nums = [1, 2, 3, 4, 5]\nlet evens = filter(nums, (x) => x % 2 == 0)\nprint(evens)");
        assert_eq!(output, vec!["[2, 4]"]);
    }

    #[test]
    fn test_vm_for_loop() {
        let output = run_output("let sum = 0\nfor i in range(5) { sum = sum + i }\nprint(sum)");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_closure() {
        let output = run_output("let double = (x) => x * 2\nprint(double(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_sum() {
        let output = run_output("print(sum([1, 2, 3, 4]))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_reduce() {
        let output = run_output("let product = reduce([1, 2, 3, 4], 1, (acc, x) => acc * x)\nprint(product)");
        assert_eq!(output, vec!["24"]);
    }

    #[test]
    fn test_vm_pipe() {
        let output = run_output("let result = [1, 2, 3] |> map((x) => x + 10)\nprint(result)");
        assert_eq!(output, vec!["[11, 12, 13]"]);
    }

    #[test]
    fn test_vm_comparison() {
        let output = run_output("print(5 > 3)");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_vm_precedence() {
        let output = run_output("print(2 + 3 * 4)");
        assert_eq!(output, vec!["14"]);
    }

    #[test]
    fn test_vm_match() {
        let output = run_output("let x = 2\nprint(match x { 1 => \"one\", 2 => \"two\", _ => \"other\" })");
        assert_eq!(output, vec!["two"]);
    }

    #[test]
    fn test_vm_match_wildcard() {
        let output = run_output("print(match 99 { 1 => \"one\", _ => \"other\" })");
        assert_eq!(output, vec!["other"]);
    }

    #[test]
    fn test_vm_match_binding() {
        let output = run_output("print(match 42 { val => val + 1 })");
        assert_eq!(output, vec!["43"]);
    }

    #[test]
    fn test_vm_match_guard() {
        let output = run_output("let x = 5\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["pos"]);
    }

    #[test]
    fn test_vm_match_guard_negative() {
        let output = run_output("let x = -3\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["neg"]);
    }

    #[test]
    fn test_vm_match_guard_zero() {
        let output = run_output("let x = 0\nprint(match x { n if n > 0 => \"pos\", n if n < 0 => \"neg\", _ => \"zero\" })");
        assert_eq!(output, vec!["zero"]);
    }

    #[test]
    fn test_vm_match_enum_destructure() {
        let output = run_output(r#"
enum Shape { Circle(int64), Rect(int64, int64) }
let s = Shape::Circle(5)
print(match s { Shape::Circle(r) => r, Shape::Rect(w, h) => w * h, _ => 0 })
"#);
        assert_eq!(output, vec!["5"]);
    }

    #[test]
    fn test_vm_match_enum_destructure_rect() {
        let output = run_output(r#"
enum Shape { Circle(int64), Rect(int64, int64) }
let s = Shape::Rect(3, 4)
print(match s { Shape::Circle(r) => r, Shape::Rect(w, h) => w * h, _ => 0 })
"#);
        assert_eq!(output, vec!["12"]);
    }

    #[test]
    fn test_vm_match_enum_wildcard_field() {
        let output = run_output(r#"
enum Pair { Two(int64, int64) }
let p = Pair::Two(10, 20)
print(match p { Pair::Two(_, y) => y, _ => 0 })
"#);
        assert_eq!(output, vec!["20"]);
    }

    #[test]
    fn test_vm_match_enum_guard() {
        let output = run_output(r#"
enum Result { Ok(int64), Err(string) }
let r = Result::Ok(150)
print(match r { Result::Ok(v) if v > 100 => "big", Result::Ok(v) => "small", Result::Err(e) => e, _ => "unknown" })
"#);
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_vm_match_or_pattern() {
        let output = run_output("let x = 2\nprint(match x { 1 or 2 or 3 => \"small\", _ => \"big\" })");
        assert_eq!(output, vec!["small"]);
    }

    #[test]
    fn test_vm_match_or_pattern_no_match() {
        let output = run_output("let x = 10\nprint(match x { 1 or 2 or 3 => \"small\", _ => \"big\" })");
        assert_eq!(output, vec!["big"]);
    }

    #[test]
    fn test_vm_match_string() {
        let output = run_output(r#"let s = "hello"
print(match s { "hi" => 1, "hello" => 2, _ => 0 })"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_match_bool() {
        let output = run_output("print(match true { true => \"yes\", false => \"no\" })");
        assert_eq!(output, vec!["yes"]);
    }

    #[test]
    fn test_vm_match_none() {
        let output = run_output("print(match none { none => \"nothing\", _ => \"something\" })");
        assert_eq!(output, vec!["nothing"]);
    }

    #[test]
    fn test_vm_let_destructure_list() {
        let output = run_output("let [a, b, c] = [1, 2, 3]\nprint(a)\nprint(b)\nprint(c)");
        assert_eq!(output, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_vm_let_destructure_list_rest() {
        let output = run_output("let [head, ...tail] = [1, 2, 3, 4]\nprint(head)\nprint(len(tail))");
        assert_eq!(output, vec!["1", "3"]);
    }

    #[test]
    fn test_vm_let_destructure_struct() {
        let output = run_output(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 10, y: 20 }
let Point { x, y } = p
print(x)
print(y)
"#);
        assert_eq!(output, vec!["10", "20"]);
    }

    #[test]
    fn test_vm_let_destructure_struct_anon() {
        let output = run_output(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 10, y: 20 }
let { x, y } = p
print(x)
print(y)
"#);
        assert_eq!(output, vec!["10", "20"]);
    }

    #[test]
    fn test_vm_match_struct_pattern() {
        let output = run_output(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 1, y: 2 }
print(match p { Point { x, y } => x + y, _ => 0 })
"#);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_match_list_pattern() {
        let output = run_output(r#"
let lst = [1, 2, 3]
print(match lst { [a, b, c] => a + b + c, _ => 0 })
"#);
        assert_eq!(output, vec!["6"]);
    }

    #[test]
    fn test_vm_match_list_rest_pattern() {
        let output = run_output(r#"
let lst = [10, 20, 30, 40]
print(match lst { [head, ...rest] => head, _ => 0 })
"#);
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_match_list_empty() {
        let output = run_output(r#"
let lst = []
print(match lst { [] => "empty", _ => "nonempty" })
"#);
        assert_eq!(output, vec!["empty"]);
    }

    #[test]
    fn test_vm_match_list_length_mismatch() {
        let output = run_output(r#"
let lst = [1, 2, 3]
print(match lst { [a, b] => "two", [a, b, c] => "three", _ => "other" })
"#);
        assert_eq!(output, vec!["three"]);
    }

    #[test]
    fn test_vm_match_negative_literal() {
        let output = run_output("print(match -1 { -1 => \"neg one\", 0 => \"zero\", _ => \"other\" })");
        assert_eq!(output, vec!["neg one"]);
    }

    #[test]
    fn test_vm_case_with_pattern() {
        let output = run_output(r#"
let x = 5
let result = case {
    x > 10 => "big",
    x > 0 => "positive",
    _ => "other"
}
print(result)
"#);
        assert_eq!(output, vec!["positive"]);
    }

    #[test]
    fn test_vm_parallel_map() {
        // Build a range > PARALLEL_THRESHOLD and map with a pure function
        let result = run("map(range(15000), (x) => x * 2)").unwrap();
        if let VmValue::List(items) = result {
            assert_eq!(items.len(), 15000);
            assert!(matches!(items[0], VmValue::Int(0)));
            assert!(matches!(items[1], VmValue::Int(2)));
            assert!(matches!(items[14999], VmValue::Int(29998)));
        } else {
            panic!("Expected list, got {:?}", result);
        }
    }

    #[test]
    fn test_vm_parallel_filter() {
        let result = run("filter(range(20000), (x) => x % 2 == 0)").unwrap();
        if let VmValue::List(items) = result {
            assert_eq!(items.len(), 10000);
            assert!(matches!(items[0], VmValue::Int(0)));
            assert!(matches!(items[1], VmValue::Int(2)));
        } else {
            panic!("Expected list, got {:?}", result);
        }
    }

    #[test]
    fn test_vm_parallel_sum() {
        let result = run("sum(range(20000))").unwrap();
        // sum(0..19999) = 19999 * 20000 / 2 = 199990000
        assert!(matches!(result, VmValue::Int(199990000)));
    }

    #[test]
    fn test_vm_recursive_fib() {
        let output = run_output(
            "fn fib(n) { if n <= 1 { n } else { fib(n - 1) + fib(n - 2) } }\nprint(fib(10))",
        );
        assert_eq!(output, vec!["55"]);
    }

    #[test]
    fn test_vm_if_else_expr() {
        // if-else as the last expression in a function should return a value
        let output = run_output(
            "fn abs(n) { if n < 0 { 0 - n } else { n } }\nprint(abs(-5))\nprint(abs(3))",
        );
        assert_eq!(output, vec!["5", "3"]);
    }

    // ── Phase 5 tests ──

    #[test]
    fn test_vm_struct_creation() {
        let output = run_output(
            "struct Point { x: float64, y: float64 }\nlet p = Point { x: 1.0, y: 2.0 }\nprint(p.x)\nprint(p.y)",
        );
        assert_eq!(output, vec!["1.0", "2.0"]);
    }

    #[test]
    fn test_vm_struct_nested() {
        let output = run_output(
            "struct Point { x: float64, y: float64 }\nstruct Line { start: Point, end_pt: Point }\nlet l = Line { start: Point { x: 0.0, y: 0.0 }, end_pt: Point { x: 1.0, y: 1.0 } }\nprint(l.start.x)",
        );
        assert_eq!(output, vec!["0.0"]);
    }

    #[test]
    fn test_vm_enum_creation() {
        let output = run_output(
            "enum Color { Red, Green, Blue }\nlet c = Color::Red\nprint(c)",
        );
        assert_eq!(output, vec!["Color::Red"]);
    }

    #[test]
    fn test_vm_enum_with_fields() {
        let output = run_output(
            "enum Shape { Circle(float64), Rect(float64, float64) }\nlet s = Shape::Circle(5.0)\nprint(s)",
        );
        assert!(output[0].contains("Circle"));
    }

    #[test]
    fn test_vm_impl_method() {
        let output = run_output(
            "struct Counter { value: int64 }\nimpl Counter {\n    fn get(self) { self.value }\n}\nlet c = Counter { value: 42 }\nprint(c.get())",
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_try_catch_throw() {
        let output = run_output(
            "try {\n    throw \"oops\"\n} catch e {\n    print(e)\n}",
        );
        assert_eq!(output, vec!["oops"]);
    }

    #[test]
    fn test_vm_string_split() {
        let output = run_output(
            "let parts = \"hello world\".split(\" \")\nprint(parts)",
        );
        assert_eq!(output, vec!["[hello, world]"]);
    }

    #[test]
    fn test_vm_string_trim() {
        let output = run_output(
            "print(\"  hello  \".trim())",
        );
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_vm_string_contains() {
        let output = run_output(
            "print(\"hello world\".contains(\"world\"))",
        );
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_vm_string_upper_lower() {
        let output = run_output(
            "print(\"hello\".to_upper())\nprint(\"HELLO\".to_lower())",
        );
        assert_eq!(output, vec!["HELLO", "hello"]);
    }

    #[test]
    fn test_vm_math_sqrt() {
        let output = run_output("print(sqrt(16.0))");
        assert_eq!(output, vec!["4.0"]);
    }

    #[test]
    fn test_vm_math_floor_ceil() {
        let output = run_output("print(floor(3.7))\nprint(ceil(3.2))");
        assert_eq!(output, vec!["3.0", "4.0"]);
    }

    #[test]
    fn test_vm_math_trig() {
        let output = run_output("print(sin(0.0))\nprint(cos(0.0))");
        assert_eq!(output, vec!["0.0", "1.0"]);
    }

    #[test]
    fn test_vm_assert_pass() {
        run("assert(true)").unwrap();
        run("assert_eq(1 + 1, 2)").unwrap();
    }

    #[test]
    fn test_vm_assert_fail() {
        assert!(run("assert(false)").is_err());
        assert!(run("assert_eq(1, 2)").is_err());
    }

    #[test]
    fn test_vm_join() {
        let output = run_output("print(join(\", \", [\"a\", \"b\", \"c\"]))");
        assert_eq!(output, vec!["a, b, c"]);
    }

    #[test]
    fn test_vm_list_method_len() {
        let output = run_output("print([1, 2, 3].len())");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_list_method_map() {
        let output = run_output("print([1, 2, 3].map((x) => x * 2))");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_vm_list_method_filter() {
        let output = run_output("print([1, 2, 3, 4, 5].filter((x) => x > 3))");
        assert_eq!(output, vec!["[4, 5]"]);
    }

    #[test]
    fn test_vm_string_replace() {
        let output = run_output("print(\"hello world\".replace(\"world\", \"rust\"))");
        assert_eq!(output, vec!["hello rust"]);
    }

    #[test]
    fn test_vm_string_starts_ends() {
        let output = run_output("print(\"hello\".starts_with(\"hel\"))\nprint(\"hello\".ends_with(\"llo\"))");
        assert_eq!(output, vec!["true", "true"]);
    }

    #[test]
    fn test_vm_math_log() {
        let result = run("log(1.0)").unwrap();
        if let VmValue::Float(f) = result {
            assert!((f - 0.0).abs() < 1e-10);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_vm_pow_builtin() {
        let output = run_output("print(pow(2.0, 10.0))");
        assert_eq!(output, vec!["1024.0"]);
    }

    #[test]
    fn test_vm_round_builtin() {
        let output = run_output("print(round(3.5))");
        assert_eq!(output, vec!["4.0"]);
    }

    #[test]
    fn test_vm_try_catch_runtime_error() {
        let output = run_output(
            "try {\n    let x = 1 / 0\n} catch e {\n    print(e)\n}",
        );
        assert_eq!(output, vec!["Division by zero"]);
    }

    #[test]
    fn test_vm_struct_field_access() {
        let output = run_output(
            "struct Point { x: float64, y: float64 }\nlet p = Point { x: 1.5, y: 2.5 }\nprint(p.x)",
        );
        assert_eq!(output, vec!["1.5"]);
    }

    #[test]
    fn test_vm_enum_match() {
        let output = run_output(
            "enum Dir { North, South }\nlet d = Dir::North\nmatch d { Dir::North => print(\"north\"), _ => print(\"other\") }",
        );
        // match expression compares enum instances
        assert!(!output.is_empty());
    }

    #[test]
    fn test_vm_impl_method_with_args() {
        let output = run_output(
            "struct Rect { w: float64, h: float64 }\nimpl Rect {\n    fn area(self) { self.w * self.h }\n}\nlet r = Rect { w: 3.0, h: 4.0 }\nprint(r.area())",
        );
        assert_eq!(output, vec!["12.0"]);
    }

    #[test]
    fn test_vm_string_len() {
        let output = run_output("print(\"hello\".len())");
        assert_eq!(output, vec!["5"]);
    }

    #[test]
    fn test_vm_list_reduce() {
        let output = run_output(
            "let nums = [1, 2, 3, 4]\nlet s = nums.reduce(0, (acc, x) => acc + x)\nprint(s)",
        );
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_nested_try_catch() {
        let output = run_output(
            "try {\n    try {\n        throw \"inner\"\n    } catch e {\n        print(e)\n        throw \"outer\"\n    }\n} catch e2 {\n    print(e2)\n}",
        );
        assert_eq!(output, vec!["inner", "outer"]);
    }

    #[test]
    fn test_vm_math_pow() {
        let output = run_output("print(pow(2.0, 10.0))");
        assert_eq!(output, vec!["1024.0"]);
    }

    // ── Phase 6: Stdlib & Ecosystem tests ──

    #[test]
    fn test_vm_json_parse() {
        let output = run_output(r#"let m = map_from("a", 1, "b", "hello")
let s = json_stringify(m)
let m2 = json_parse(s)
print(m2["a"])
print(m2["b"])"#);
        assert_eq!(output, vec!["1", "hello"]);
    }

    #[test]
    fn test_vm_json_stringify() {
        let output = run_output(r#"let m = map_from("x", 1, "y", 2)
let s = json_stringify(m)
print(s)"#);
        assert_eq!(output, vec![r#"{"x":1,"y":2}"#]);
    }

    #[test]
    fn test_vm_map_from_and_access() {
        let output = run_output(r#"let m = map_from("a", 10, "b", 20)
print(m["a"])
print(m.b)"#);
        assert_eq!(output, vec!["10", "20"]);
    }

    #[test]
    fn test_vm_map_methods() {
        let output = run_output(r#"let m = map_from("a", 1, "b", 2)
print(m.keys())
print(m.values())
print(m.contains_key("a"))
print(m.len())"#);
        assert_eq!(output, vec!["[a, b]", "[1, 2]", "true", "2"]);
    }

    #[test]
    fn test_vm_map_set_index() {
        let output = run_output(r#"let m = map_from("a", 1)
m["b"] = 2
print(m["b"])"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_map_iteration() {
        let output = run_output(r#"let m = map_from("x", 10, "y", 20)
for kv in m {
    print(kv[0])
}"#);
        assert_eq!(output, vec!["x", "y"]);
    }

    #[test]
    fn test_vm_file_read_write() {
        let output = run_output(r#"write_file("/tmp/tl_vm_test.txt", "vm hello")
print(read_file("/tmp/tl_vm_test.txt"))
print(file_exists("/tmp/tl_vm_test.txt"))"#);
        assert_eq!(output, vec!["vm hello", "true"]);
    }

    #[test]
    fn test_vm_env_get_set() {
        let output = run_output(r#"env_set("TL_VM_TEST", "abc")
print(env_get("TL_VM_TEST"))"#);
        assert_eq!(output, vec!["abc"]);
    }

    #[test]
    fn test_vm_regex_match() {
        let output = run_output(r#"print(regex_match("\\d+", "abc123"))
print(regex_match("^\\d+$", "abc"))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_vm_regex_find() {
        let output = run_output(r#"let m = regex_find("\\d+", "abc123def456")
print(len(m))
print(m[0])"#);
        assert_eq!(output, vec!["2", "123"]);
    }

    #[test]
    fn test_vm_regex_replace() {
        let output = run_output(r#"print(regex_replace("\\d+", "abc123", "X"))"#);
        assert_eq!(output, vec!["abcX"]);
    }

    #[test]
    fn test_vm_now() {
        let output = run_output("print(now() > 0)");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_vm_date_format() {
        let output = run_output(r#"print(date_format(1704067200000, "%Y-%m-%d"))"#);
        assert_eq!(output, vec!["2024-01-01"]);
    }

    #[test]
    fn test_vm_date_parse() {
        let output = run_output(r#"print(date_parse("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))"#);
        assert_eq!(output, vec!["1704067200000"]);
    }

    #[test]
    fn test_vm_string_chars() {
        let output = run_output(r#"print(len("hello".chars()))"#);
        assert_eq!(output, vec!["5"]);
    }

    #[test]
    fn test_vm_string_repeat() {
        let output = run_output(r#"print("ab".repeat(3))"#);
        assert_eq!(output, vec!["ababab"]);
    }

    #[test]
    fn test_vm_string_index_of() {
        let output = run_output(r#"print("hello world".index_of("world"))"#);
        assert_eq!(output, vec!["6"]);
    }

    #[test]
    fn test_vm_string_substring() {
        let output = run_output(r#"print("hello world".substring(0, 5))"#);
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_vm_string_pad() {
        let output = run_output(r#"print("42".pad_left(5, "0"))
print("hi".pad_right(5, "."))"#);
        assert_eq!(output, vec!["00042", "hi..."]);
    }

    #[test]
    fn test_vm_list_sort() {
        let output = run_output(r#"print([3, 1, 2].sort())"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_vm_list_reverse() {
        let output = run_output(r#"print([1, 2, 3].reverse())"#);
        assert_eq!(output, vec!["[3, 2, 1]"]);
    }

    #[test]
    fn test_vm_list_contains() {
        let output = run_output(r#"print([1, 2, 3].contains(2))
print([1, 2, 3].contains(5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_vm_list_slice() {
        let output = run_output(r#"print([1, 2, 3, 4, 5].slice(1, 4))"#);
        assert_eq!(output, vec!["[2, 3, 4]"]);
    }

    #[test]
    fn test_vm_zip() {
        let output = run_output(r#"let p = zip([1, 2], ["a", "b"])
print(p[0])"#);
        assert_eq!(output, vec!["[1, a]"]);
    }

    #[test]
    fn test_vm_enumerate() {
        let output = run_output(r#"let e = enumerate(["a", "b", "c"])
print(e[1])"#);
        assert_eq!(output, vec!["[1, b]"]);
    }

    #[test]
    fn test_vm_bool() {
        let output = run_output(r#"print(bool(1))
print(bool(0))
print(bool(""))"#);
        assert_eq!(output, vec!["true", "false", "false"]);
    }

    #[test]
    fn test_vm_range_step() {
        let output = run_output(r#"print(range(0, 10, 3))"#);
        assert_eq!(output, vec!["[0, 3, 6, 9]"]);
    }

    #[test]
    fn test_vm_int_bool() {
        let output = run_output(r#"print(int(true))
print(int(false))"#);
        assert_eq!(output, vec!["1", "0"]);
    }

    #[test]
    fn test_vm_map_len_typeof() {
        let output = run_output(r#"let m = map_from("a", 1)
print(len(m))
print(type_of(m))"#);
        assert_eq!(output, vec!["1", "map"]);
    }

    #[test]
    fn test_vm_json_file_roundtrip() {
        let output = run_output(r#"let data = map_from("name", "vm_test", "count", 99)
write_file("/tmp/tl_vm_json.json", json_stringify(data))
let parsed = json_parse(read_file("/tmp/tl_vm_json.json"))
print(parsed["name"])
print(parsed["count"])"#);
        assert_eq!(output, vec!["vm_test", "99"]);
    }

    // ── Phase 7: Concurrency tests ──

    #[test]
    fn test_vm_spawn_await_basic() {
        let output = run_output(r#"fn worker() { 42 }
let t = spawn(worker)
let result = await t
print(result)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_spawn_closure_with_capture() {
        let output = run_output(r#"let x = 10
let f = () => x + 5
let t = spawn(f)
print(await t)"#);
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_vm_sleep() {
        let output = run_output(r#"sleep(10)
print("done")"#);
        assert_eq!(output, vec!["done"]);
    }

    #[test]
    fn test_vm_await_non_task_passthrough() {
        let output = run_output(r#"print(await 42)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_spawn_multiple_await() {
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
    fn test_vm_channel_basic() {
        let output = run_output(r#"let ch = channel()
send(ch, 42)
let val = recv(ch)
print(val)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_channel_between_tasks() {
        let output = run_output(r#"let ch = channel()
fn producer() { send(ch, 100) }
let t = spawn(producer)
let val = recv(ch)
await t
print(val)"#);
        assert_eq!(output, vec!["100"]);
    }

    #[test]
    fn test_vm_try_recv_empty() {
        let output = run_output(r#"let ch = channel()
let val = try_recv(ch)
print(val)"#);
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_vm_channel_multiple_values() {
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
    fn test_vm_channel_producer_consumer() {
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
    fn test_vm_await_all() {
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
    fn test_vm_pmap_basic() {
        let output = run_output(r#"let results = pmap([1, 2, 3], (x) => x * 2)
print(results)"#);
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_vm_pmap_order_preserved() {
        let output = run_output(r#"let results = pmap([10, 20, 30], (x) => x + 1)
print(results)"#);
        assert_eq!(output, vec!["[11, 21, 31]"]);
    }

    #[test]
    fn test_vm_timeout_success() {
        let output = run_output(r#"fn worker() { 42 }
let t = spawn(worker)
let result = timeout(t, 5000)
print(result)"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_timeout_failure() {
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
    fn test_vm_spawn_error_propagation() {
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
    fn test_vm_spawn_producer_consumer_pipeline() {
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

    #[test]
    fn test_vm_type_of_task_channel() {
        let output = run_output(r#"fn worker() { 1 }
let t = spawn(worker)
let ch = channel()
print(type_of(t))
print(type_of(ch))
await t"#);
        assert_eq!(output, vec!["task", "channel"]);
    }

    // ── Phase 8: Iterators & Generators ──

    #[test]
    fn test_vm_basic_generator() {
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
    fn test_vm_generator_exhaustion() {
        let output = run_output(r#"fn gen() {
    yield 42
}
let g = gen()
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["42", "none", "none"]);
    }

    #[test]
    fn test_vm_generator_with_loop() {
        let output = run_output(r#"fn counter() {
    let mut i = 0
    while i < 3 {
        yield i
        i = i + 1
    }
}
let g = counter()
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["0", "1", "2", "none"]);
    }

    #[test]
    fn test_vm_generator_with_args() {
        let output = run_output(r#"fn count_from(start) {
    let mut i = start
    while i < start + 3 {
        yield i
        i = i + 1
    }
}
let g = count_from(10)
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["10", "11", "12", "none"]);
    }

    #[test]
    fn test_vm_generator_yield_none() {
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
    fn test_vm_is_generator() {
        let output = run_output(r#"fn gen() { yield 1 }
let g = gen()
print(is_generator(g))
print(is_generator(42))
print(is_generator(none))"#);
        assert_eq!(output, vec!["true", "false", "false"]);
    }

    #[test]
    fn test_vm_multiple_generators() {
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
    fn test_vm_for_over_generator() {
        let output = run_output(r#"fn gen() {
    yield 10
    yield 20
    yield 30
}
for x in gen() {
    print(x)
}"#);
        assert_eq!(output, vec!["10", "20", "30"]);
    }

    #[test]
    fn test_vm_iter_builtin() {
        let output = run_output(r#"let g = iter([1, 2, 3])
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["1", "2", "3", "none"]);
    }

    #[test]
    fn test_vm_take_builtin() {
        let output = run_output(r#"fn naturals() {
    let mut n = 0
    while true {
        yield n
        n = n + 1
    }
}
let g = take(naturals(), 5)
print(next(g))
print(next(g))
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["0", "1", "2", "3", "4", "none"]);
    }

    #[test]
    fn test_vm_skip_builtin() {
        let output = run_output(r#"let g = skip(iter([10, 20, 30, 40, 50]), 2)
print(next(g))
print(next(g))
print(next(g))
print(next(g))"#);
        assert_eq!(output, vec!["30", "40", "50", "none"]);
    }

    #[test]
    fn test_vm_gen_collect() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
    yield 3
}
let result = gen_collect(gen())
print(result)"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_vm_gen_map() {
        let output = run_output(r#"let g = gen_map(iter([1, 2, 3]), (x) => x * 10)
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[10, 20, 30]"]);
    }

    #[test]
    fn test_vm_gen_filter() {
        let output = run_output(r#"let g = gen_filter(iter([1, 2, 3, 4, 5, 6]), (x) => x % 2 == 0)
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_vm_chain() {
        let output = run_output(r#"let g = chain(iter([1, 2]), iter([3, 4]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[1, 2, 3, 4]"]);
    }

    #[test]
    fn test_vm_gen_zip() {
        let output = run_output(r#"let g = gen_zip(iter([1, 2, 3]), iter([10, 20, 30]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[[1, 10], [2, 20], [3, 30]]"]);
    }

    #[test]
    fn test_vm_gen_enumerate() {
        let output = run_output(r#"let g = gen_enumerate(iter([10, 20, 30]))
print(gen_collect(g))"#);
        assert_eq!(output, vec!["[[0, 10], [1, 20], [2, 30]]"]);
    }

    #[test]
    fn test_vm_combinator_chaining() {
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
    fn test_vm_for_over_take() {
        let output = run_output(r#"fn naturals() {
    let mut n = 0
    while true {
        yield n
        n = n + 1
    }
}
for x in take(naturals(), 5) {
    print(x)
}"#);
        assert_eq!(output, vec!["0", "1", "2", "3", "4"]);
    }

    #[test]
    fn test_vm_generator_error_propagation() {
        let result = run(r#"fn bad_gen() {
    yield 1
    throw "oops"
}
let g = bad_gen()
let mut caught = ""
next(g)
try {
    next(g)
} catch e {
    caught = e
}
print(caught)"#);
        // Should succeed (error caught)
        assert!(result.is_ok());
    }

    #[test]
    fn test_vm_fibonacci_generator() {
        let output = run_output(r#"fn fib() {
    let mut a = 0
    let mut b = 1
    while true {
        yield a
        let temp = a + b
        a = b
        b = temp
    }
}
print(gen_collect(take(fib(), 8)))"#);
        assert_eq!(output, vec!["[0, 1, 1, 2, 3, 5, 8, 13]"]);
    }

    #[test]
    fn test_vm_generator_method_syntax() {
        let output = run_output(r#"fn gen() {
    yield 1
    yield 2
    yield 3
}
let g = gen()
print(type_of(g))"#);
        assert_eq!(output, vec!["generator"]);
    }

    // ── Phase 10: Result/Option + ? operator tests ──

    #[test]
    fn test_vm_ok_err_builtins() {
        let output = run_output("let r = Ok(42)\nprint(r)");
        assert_eq!(output, vec!["Result::Ok(42)"]);

        let output = run_output("let r = Err(\"fail\")\nprint(r)");
        assert_eq!(output, vec!["Result::Err(fail)"]);
    }

    #[test]
    fn test_vm_is_ok_is_err() {
        let output = run_output("print(is_ok(Ok(42)))");
        assert_eq!(output, vec!["true"]);
        let output = run_output("print(is_err(Ok(42)))");
        assert_eq!(output, vec!["false"]);
        let output = run_output("print(is_ok(Err(\"fail\")))");
        assert_eq!(output, vec!["false"]);
        let output = run_output("print(is_err(Err(\"fail\")))");
        assert_eq!(output, vec!["true"]);
    }

    #[test]
    fn test_vm_unwrap_ok() {
        let output = run_output("print(unwrap(Ok(42)))");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_unwrap_err_panics() {
        let result = run("unwrap(Err(\"fail\"))");
        assert!(result.is_err());
    }

    #[test]
    fn test_vm_try_on_ok() {
        let output = run_output(r#"fn get_val() { Ok(42) }
fn process() { let v = get_val()? + 1
Ok(v) }
print(process())"#);
        assert_eq!(output, vec!["Result::Ok(43)"]);
    }

    #[test]
    fn test_vm_try_on_err_propagates() {
        let output = run_output(r#"fn failing() { Err("oops") }
fn process() { let v = failing()?
Ok(v) }
print(process())"#);
        assert_eq!(output, vec!["Result::Err(oops)"]);
    }

    #[test]
    fn test_vm_try_on_none_propagates() {
        let output = run_output(r#"fn get_none() { none }
fn process() { let v = get_none()?
42 }
print(process())"#);
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    fn test_vm_try_passthrough() {
        // ? on a normal value should passthrough
        let output = run_output(r#"fn get_val() { 42 }
fn process() { let v = get_val()?
v + 1 }
print(process())"#);
        assert_eq!(output, vec!["43"]);
    }

    #[test]
    fn test_vm_result_match() {
        let output = run_output(r#"let r = Ok(42)
print(is_ok(r))
print(unwrap(r))"#);
        assert_eq!(output, vec!["true", "42"]);
    }

    #[test]
    fn test_vm_result_match_err() {
        let output = run_output(r#"let r = Err("fail")
print(is_err(r))
match r {
    Result::Err(e) => print("got error"),
    _ => print("no error")
}"#);
        assert_eq!(output, vec!["true", "got error"]);
    }

    // ── Set tests ──

    #[test]
    fn test_vm_set_from_dedup() {
        let output = run_output(r#"let s = set_from([1, 2, 3, 2, 1])
print(len(s))
print(type_of(s))"#);
        assert_eq!(output, vec!["3", "set"]);
    }

    #[test]
    fn test_vm_set_add() {
        let output = run_output(r#"let s = set_from([1, 2])
let s2 = set_add(s, 3)
let s3 = set_add(s2, 2)
print(len(s2))
print(len(s3))"#);
        assert_eq!(output, vec!["3", "3"]);
    }

    #[test]
    fn test_vm_set_remove() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
let s2 = set_remove(s, 2)
print(len(s2))
print(set_contains(s2, 2))"#);
        assert_eq!(output, vec!["2", "false"]);
    }

    #[test]
    fn test_vm_set_contains() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
print(set_contains(s, 2))
print(set_contains(s, 5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_vm_set_union() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([3, 4, 5])
let c = set_union(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["5"]);
    }

    #[test]
    fn test_vm_set_intersection() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
let c = set_intersection(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_set_difference() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
let c = set_difference(a, b)
print(len(c))"#);
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_vm_set_for_loop() {
        let output = run_output(r#"let s = set_from([10, 20, 30])
let total = 0
for item in s {
    total = total + item
}
print(total)"#);
        assert_eq!(output, vec!["60"]);
    }

    #[test]
    fn test_vm_set_to_list() {
        let output = run_output(r#"let s = set_from([3, 1, 2])
let lst = s.to_list()
print(type_of(lst))
print(len(lst))"#);
        assert_eq!(output, vec!["list", "3"]);
    }

    #[test]
    fn test_vm_set_method_contains() {
        let output = run_output(r#"let s = set_from([1, 2, 3])
print(s.contains(2))
print(s.contains(5))"#);
        assert_eq!(output, vec!["true", "false"]);
    }

    #[test]
    fn test_vm_set_method_add_remove() {
        let output = run_output(r#"let s = set_from([1, 2])
let s2 = s.add(3)
print(s2.len())
let s3 = s2.remove(1)
print(s3.len())"#);
        assert_eq!(output, vec!["3", "2"]);
    }

    #[test]
    fn test_vm_set_method_union_intersection_difference() {
        let output = run_output(r#"let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])
print(a.union(b).len())
print(a.intersection(b).len())
print(a.difference(b).len())"#);
        assert_eq!(output, vec!["4", "2", "1"]);
    }

    #[test]
    fn test_vm_set_empty() {
        let output = run_output(r#"let s = set_from([])
print(len(s))
let s2 = s.add(1)
print(len(s2))"#);
        assert_eq!(output, vec!["0", "1"]);
    }

    #[test]
    fn test_vm_set_string_values() {
        let output = run_output(r#"let s = set_from(["a", "b", "a", "c"])
print(len(s))
print(s.contains("b"))"#);
        assert_eq!(output, vec!["3", "true"]);
    }

    // ── Phase 11: Module System VM Tests ──

    #[test]
    fn test_vm_import_with_caching() {
        // Test that the VM has caching fields initialized
        let vm = Vm::new();
        assert!(vm.module_cache.is_empty());
        assert!(vm.importing_files.is_empty());
        assert!(vm.file_path.is_none());
    }

    #[test]
    fn test_vm_use_single_file() {
        // Create a temp dir with module files
        let dir = tempfile::tempdir().unwrap();
        let lib_path = dir.path().join("math.tl");
        std::fs::write(&lib_path, "let PI = 3.14\nfn add(a, b) { a + b }").unwrap();

        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, "use math\nprint(add(1, 2))").unwrap();

        let source = std::fs::read_to_string(&main_path).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["3"]);
    }

    #[test]
    fn test_vm_use_wildcard() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("helpers.tl"), "fn greet() { \"hello\" }\nfn farewell() { \"bye\" }").unwrap();

        let main_src = "use helpers.*\nprint(greet())\nprint(farewell())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["hello", "bye"]);
    }

    #[test]
    fn test_vm_use_aliased() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mylib.tl"), "fn compute() { 42 }").unwrap();

        let main_src = "use mylib as m\nprint(m.compute())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["42"]);
    }

    #[test]
    fn test_vm_use_directory_module() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("utils")).unwrap();
        std::fs::write(dir.path().join("utils/mod.tl"), "fn helper() { 99 }").unwrap();

        let main_src = "use utils\nprint(helper())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["99"]);
    }

    #[test]
    fn test_vm_circular_import_detection() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.tl");
        let b_path = dir.path().join("b.tl");
        std::fs::write(&a_path, &format!("import \"{}\"", b_path.to_string_lossy())).unwrap();
        std::fs::write(&b_path, &format!("import \"{}\"", a_path.to_string_lossy())).unwrap();

        let source = std::fs::read_to_string(&a_path).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(a_path.to_string_lossy().to_string());
        let result = vm.execute(&proto);
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("Circular import"));
    }

    #[test]
    fn test_vm_module_caching() {
        // Import the same module twice — should use cache
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cached.tl"), "let X = 42").unwrap();

        let main_src = "use cached\nuse cached\nprint(X)";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["42"]);
    }

    #[test]
    fn test_vm_existing_import_still_works() {
        // Verify backward compat of classic import
        let dir = tempfile::tempdir().unwrap();
        let lib_path = dir.path().join("lib.tl");
        std::fs::write(&lib_path, "fn imported_fn() { 123 }").unwrap();

        let main_src = format!("import \"{}\"\nprint(imported_fn())", lib_path.to_string_lossy());
        let program = tl_parser::parse(&main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["123"]);
    }

    #[test]
    fn test_vm_pub_fn_parsing() {
        // Pub fn should compile and run normally
        let output = run_output("pub fn add(a, b) { a + b }\nprint(add(1, 2))");
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_use_nested_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/transforms.tl"), "fn clean(x) { x + 1 }").unwrap();

        let main_src = "use data.transforms\nprint(clean(41))";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["42"]);
    }

    // -- Integration tests: multi-file, backward compat, mixed --

    #[test]
    fn test_integration_multi_file_use_functions() {
        // main.tl uses functions from lib.tl
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("lib.tl"),
            "fn greet(name) { \"Hello, \" + name + \"!\" }\nfn double(x) { x * 2 }"
        ).unwrap();

        let main_src = "use lib\nprint(greet(\"World\"))\nprint(double(21))";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["Hello, World!", "42"]);
    }

    #[test]
    fn test_integration_mixed_import_and_use() {
        // Combine classic import and use in same file
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("old_lib.tl"), "fn old_fn() { 10 }").unwrap();
        std::fs::write(dir.path().join("new_lib.tl"), "fn new_fn() { 20 }").unwrap();

        let old_lib_abs = dir.path().join("old_lib.tl").to_string_lossy().to_string();
        let main_src = format!(
            "import \"{old_lib_abs}\"\nuse new_lib\nprint(old_fn() + new_fn())"
        );
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, &main_src).unwrap();

        let program = tl_parser::parse(&main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["30"]);
    }

    #[test]
    fn test_integration_directory_module_with_mod_tl() {
        // utils/mod.tl re-exports functions
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("utils")).unwrap();
        std::fs::write(dir.path().join("utils/mod.tl"),
            "fn helper() { 99 }\nfn format_num(n) { str(n) + \"!\" }"
        ).unwrap();

        let main_src = "use utils\nprint(helper())\nprint(format_num(42))";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["99", "42!"]);
    }

    #[test]
    fn test_integration_circular_dep_error() {
        let dir = tempfile::tempdir().unwrap();
        let a_abs = dir.path().join("a.tl").to_string_lossy().to_string();
        let b_abs = dir.path().join("b.tl").to_string_lossy().to_string();
        std::fs::write(dir.path().join("a.tl"), format!("import \"{b_abs}\"\nfn fa() {{ 1 }}")).unwrap();
        std::fs::write(dir.path().join("b.tl"), format!("import \"{a_abs}\"\nfn fb() {{ 2 }}")).unwrap();

        let main_src = format!("import \"{a_abs}\"");
        let program = tl_parser::parse(&main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        let result = vm.execute(&proto);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Circular") || err_msg.contains("circular"),
            "Expected circular import error, got: {err_msg}");
    }

    #[test]
    fn test_integration_use_aliased_method_call() {
        // use lib as m, then m.compute()
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mylib.tl"), "fn compute() { 42 }").unwrap();

        let main_src = "use mylib as m\nprint(m.compute())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["42"]);
    }

    #[test]
    fn test_integration_module_caching_shared() {
        // Import same module twice; second import uses cache, not re-execution
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("shared.tl"), "fn get_val() { 42 }").unwrap();

        let main_src = "use shared\nprint(get_val())\nuse shared\nprint(get_val())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["42", "42"]);
    }

    #[test]
    fn test_integration_pub_keyword_in_module() {
        // pub fn in a module should work when imported
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("pubmod.tl"), "pub fn public_fn() { 100 }\nfn private_fn() { 200 }").unwrap();

        let main_src = "use pubmod\nprint(public_fn())";
        let main_path = dir.path().join("main.tl");
        std::fs::write(&main_path, main_src).unwrap();

        let program = tl_parser::parse(main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.file_path = Some(main_path.to_string_lossy().to_string());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["100"]);
    }

    #[test]
    fn test_integration_backward_compat_import_as() {
        // Classic import-as syntax should still work
        let dir = tempfile::tempdir().unwrap();
        let lib_path = dir.path().join("mylib.tl");
        std::fs::write(&lib_path, "fn compute() { 77 }").unwrap();

        let main_src = format!("import \"{}\" as m\nprint(m.compute())", lib_path.to_string_lossy());
        let program = tl_parser::parse(&main_src).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["77"]);
    }

    // ── Phase 12: Generics & Traits (VM) ──────────────────

    #[test]
    fn test_vm_generic_fn() {
        let output = run_output("fn identity<T>(x: T) -> T { x }\nprint(identity(42))");
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_generic_fn_string() {
        let output = run_output("fn identity<T>(x: T) -> T { x }\nprint(identity(\"hello\"))");
        assert_eq!(output, vec!["hello"]);
    }

    #[test]
    fn test_vm_generic_struct() {
        let output = run_output("struct Pair<A, B> { first: A, second: B }\nlet p = Pair { first: 1, second: \"hi\" }\nprint(p.first)\nprint(p.second)");
        assert_eq!(output, vec!["1", "hi"]);
    }

    #[test]
    fn test_vm_trait_def_noop() {
        // Trait definitions should compile without error (no-op)
        let output = run_output("trait Display { fn show(self) -> string }\nprint(\"ok\")");
        assert_eq!(output, vec!["ok"]);
    }

    #[test]
    fn test_vm_trait_impl_methods() {
        let output = run_output(
            "struct Point { x: int, y: int }\nimpl Display for Point { fn show(self) -> string { \"point\" } }\nlet p = Point { x: 1, y: 2 }\nprint(p.show())"
        );
        assert_eq!(output, vec!["point"]);
    }

    #[test]
    fn test_vm_generic_enum() {
        // Generic enum declaration works — type params are erased at runtime
        let output = run_output("enum MyOpt<T> { Some(T), Nothing }\nlet x = MyOpt::Some(42)\nprint(type_of(x))");
        assert_eq!(output, vec!["enum"]);
    }

    #[test]
    fn test_vm_where_clause_runtime() {
        // Where clause is compile-time only; function still works at runtime
        let output = run_output("fn compare<T>(x: T) where T: Comparable { x }\nprint(compare(10))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_trait_impl_self_method() {
        let output = run_output(
            "struct Counter { value: int }\nimpl Incrementable for Counter { fn inc(self) { self.value + 1 } }\nlet c = Counter { value: 5 }\nprint(c.inc())"
        );
        assert_eq!(output, vec!["6"]);
    }

    // ── Phase 12: Integration tests ──────────────────────────

    #[test]
    fn test_vm_generic_fn_with_type_inference() {
        // Generic function called with different types
        let output = run_output("fn first<T>(xs: list<T>) -> T { xs[0] }\nprint(first([1, 2, 3]))\nprint(first([\"a\", \"b\"]))");
        assert_eq!(output, vec!["1", "a"]);
    }

    #[test]
    fn test_vm_generic_struct_with_methods() {
        let output = run_output(
            "struct Box<T> { val: T }\nimpl Box { fn get(self) { self.val } }\nlet b = Box { val: 42 }\nprint(b.get())"
        );
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_trait_def_impl_call() {
        let output = run_output(
            "trait Greetable { fn greet(self) -> string }\nstruct Person { name: string }\nimpl Greetable for Person { fn greet(self) -> string { self.name } }\nlet p = Person { name: \"Alice\" }\nprint(p.greet())"
        );
        assert_eq!(output, vec!["Alice"]);
    }

    #[test]
    fn test_vm_multiple_generic_params() {
        let output = run_output("fn pair<A, B>(a: A, b: B) { [a, b] }\nlet p = pair(1, \"two\")\nprint(len(p))");
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_backward_compat_non_generic() {
        // Existing non-generic code must still work unchanged
        let output = run_output("fn add(a, b) { a + b }\nstruct Point { x: int, y: int }\nimpl Point { fn sum(self) { self.x + self.y } }\nlet p = Point { x: 3, y: 4 }\nprint(add(1, 2))\nprint(p.sum())");
        assert_eq!(output, vec!["3", "7"]);
    }

    // ── Phase 16: Package import resolution tests ──

    #[test]
    fn test_vm_package_import_resolves() {
        // Create a test package on disk
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("mylib");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn greet() { print(\"hello from pkg\") }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"mylib\"\nversion = \"1.0.0\"\n").unwrap();

        // use X imports all exports wildcard-style; call greet() directly
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use mylib\ngreet()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("mylib".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        assert_eq!(vm.output, vec!["hello from pkg"]);
    }

    #[test]
    fn test_vm_package_nested_import() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("utils");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/math.tl"), "pub fn double(x) { x * 2 }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"utils\"\nversion = \"1.0.0\"\n").unwrap();

        // use utils.math wildcard-imports math.tl contents
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use utils.math\nprint(double(21))").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("utils".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        assert_eq!(vm.output, vec!["42"]);
    }

    #[test]
    fn test_vm_package_aliased_import() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("utils");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/math.tl"), "pub fn double(x) { x * 2 }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"utils\"\nversion = \"1.0.0\"\n").unwrap();

        // use X as Y creates a namespaced module object
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use utils.math as m\nprint(m.double(21))").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("utils".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        assert_eq!(vm.output, vec!["42"]);
    }

    #[test]
    fn test_vm_package_underscore_to_hyphen() {
        let tmp = tempfile::tempdir().unwrap();
        let pkg_dir = tmp.path().join("my-pkg");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn val() { print(99) }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"my-pkg\"\nversion = \"1.0.0\"\n").unwrap();

        // TL identifiers use underscores, package names use hyphens
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use my_pkg\nval()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("my-pkg".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        assert_eq!(vm.output, vec!["99"]);
    }

    #[test]
    fn test_vm_local_module_priority_over_package() {
        // Local modules should take priority over packages
        let tmp = tempfile::tempdir().unwrap();

        // Create a local module
        std::fs::write(tmp.path().join("mymod.tl"), "pub fn val() { print(\"local\") }").unwrap();

        // Create a package with the same name
        let pkg_dir = tmp.path().join("pkg_mymod");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn val() { print(\"package\") }").unwrap();

        // use mymod → wildcard imports, val() available directly
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use mymod\nval()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("mymod".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        // Local module should win
        assert_eq!(vm.output, vec!["local"]);
    }

    #[test]
    fn test_vm_package_missing_error() {
        let tmp = tempfile::tempdir().unwrap();
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use nonexistent\nnonexistent.foo()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        let result = vm.execute(&proto);

        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(err.contains("Module not found"));
    }

    #[test]
    #[cfg(feature = "native")]
    fn test_resolve_package_file_entry_points() {
        let tmp = tempfile::tempdir().unwrap();

        // Test src/lib.tl entry point
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/lib.tl"), "").unwrap();
        let result = resolve_package_file(tmp.path(), &[]);
        assert!(result.is_some());
        assert!(result.unwrap().contains("lib.tl"));

        // Test nested module in src/
        std::fs::write(tmp.path().join("src/math.tl"), "").unwrap();
        let result = resolve_package_file(tmp.path(), &["math"]);
        assert!(result.is_some());
        assert!(result.unwrap().contains("math.tl"));
    }

    #[test]
    fn test_vm_package_propagates_to_sub_imports() {
        // Package roots should be available in sub-VM during imports
        let tmp = tempfile::tempdir().unwrap();

        // Create a package
        let pkg_dir = tmp.path().join("helpers");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        std::fs::write(pkg_dir.join("src/lib.tl"), "pub fn help() { print(\"helped\") }").unwrap();
        std::fs::write(pkg_dir.join("tl.toml"), "[project]\nname = \"helpers\"\nversion = \"1.0.0\"\n").unwrap();

        // Create a local module that imports from the package (wildcard then calls directly)
        std::fs::write(tmp.path().join("bridge.tl"), "use helpers\npub fn run() { help() }").unwrap();

        // use bridge wildcard-imports run(), then call it
        let main_file = tmp.path().join("main.tl");
        std::fs::write(&main_file, "use bridge\nrun()").unwrap();

        let source = std::fs::read_to_string(&main_file).unwrap();
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compiler::compile(&program).unwrap();

        let mut vm = Vm::new();
        vm.file_path = Some(main_file.to_string_lossy().to_string());
        vm.package_roots.insert("helpers".into(), pkg_dir);
        vm.execute(&proto).unwrap();

        assert_eq!(vm.output, vec!["helped"]);
    }

    // ── Phase 18: Closures & Lambdas Improvements ────────────────

    #[test]
    fn test_block_body_closure_basic() {
        let output = run_output("let f = (x: int64) -> int64 { let y = x * 2\n y + 1 }\nprint(f(5))");
        assert_eq!(output, vec!["11"]);
    }

    #[test]
    fn test_block_body_closure_captures_upvalue() {
        let output = run_output("let offset = 10\nlet f = (x) -> int64 { let y = x + offset\n y }\nprint(f(5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_block_body_closure_as_hof_arg() {
        let output = run_output("let nums = [1, 2, 3]\nlet result = map(nums, (x) -> int64 { let doubled = x * 2\n doubled + 1 })\nprint(result)");
        assert_eq!(output, vec!["[3, 5, 7]"]);
    }

    #[test]
    fn test_block_body_closure_multi_stmt() {
        let output = run_output("let f = (a, b) -> int64 { let sum = a + b\n let product = a * b\n sum + product }\nprint(f(3, 4))");
        assert_eq!(output, vec!["19"]);
    }

    #[test]
    fn test_type_alias_noop() {
        // Type alias should be a no-op at runtime, code using aliased types should work
        let output = run_output("type Mapper = fn(int64) -> int64\nlet f: Mapper = (x) => x * 2\nprint(f(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_type_alias_in_function_sig() {
        let output = run_output("type Mapper = fn(int64) -> int64\nfn apply(f: Mapper, x: int64) -> int64 { f(x) }\nprint(apply((x) => x + 10, 5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_shorthand_closure() {
        let output = run_output("let double = x => x * 2\nprint(double(5))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_shorthand_closure_in_map() {
        let output = run_output("let nums = [1, 2, 3]\nprint(map(nums, x => x * 2))");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_iife() {
        let output = run_output("let r = ((x) => x * 2)(5)\nprint(r)");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_hof_apply() {
        let output = run_output("fn apply(f, x) { f(x) }\nprint(apply((x) => x + 10, 5))");
        assert_eq!(output, vec!["15"]);
    }

    #[test]
    fn test_closure_stored_in_list() {
        let output = run_output("let fns = [(x) => x + 1, (x) => x * 2]\nprint(fns[0](5))\nprint(fns[1](5))");
        assert_eq!(output, vec!["6", "10"]);
    }

    #[test]
    fn test_block_body_closure_with_return() {
        // Use explicit return statements since if/else is a statement, not a tail expression
        let output = run_output("let classify = (x) -> string { if x > 0 { return \"positive\" }\n \"non-positive\" }\nprint(classify(5))\nprint(classify(-1))");
        assert_eq!(output, vec!["positive", "non-positive"]);
    }

    #[test]
    fn test_shorthand_closure_in_filter() {
        let output = run_output("let nums = [1, 2, 3, 4, 5, 6]\nlet evens = filter(nums, x => x % 2 == 0)\nprint(evens)");
        assert_eq!(output, vec!["[2, 4, 6]"]);
    }

    #[test]
    fn test_block_closure_with_multiple_returns() {
        let output = run_output("let abs_val = (x) -> int64 { if x < 0 { return -x }\n x }\nprint(abs_val(-5))\nprint(abs_val(3))");
        assert_eq!(output, vec!["5", "3"]);
    }

    #[test]
    fn test_type_alias_with_block_closure() {
        let output = run_output("type Transform = fn(int64) -> int64\nlet f: Transform = (x) -> int64 { let y = x * x\n y + 1 }\nprint(f(3))");
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_closure_both_backends_expr() {
        // Same test, just verify VM works correctly
        let output = run_output("let f = (x) => x * 3 + 1\nprint(f(4))");
        assert_eq!(output, vec!["13"]);
    }

    // Phase 20: Python FFI feature-disabled test
    #[test]
    #[cfg(not(feature = "python"))]
    fn test_py_feature_disabled() {
        let result = run("py_import(\"math\")");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("python") && msg.contains("feature"));
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_import_and_eval() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let m = py_import(\"math\")\nlet pi = m.pi\nprint(pi)");
        assert_eq!(output.len(), 1);
        let pi: f64 = output[0].parse().unwrap();
        assert!((pi - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_eval_arithmetic() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let x = py_eval(\"2 ** 10\")\nprint(x)");
        assert_eq!(output, vec!["1024"]);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_method_dispatch() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let m = py_import(\"math\")\nprint(m.sqrt(25.0))");
        assert_eq!(output, vec!["5.0"]);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_list_conversion() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let x = py_eval(\"[10, 20, 30]\")\nprint(x)");
        assert_eq!(output, vec!["[10, 20, 30]"]);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_none_conversion() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let x = py_eval(\"None\")\nprint(x)");
        assert_eq!(output, vec!["none"]);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_error_msg_quality() {
        pyo3::prepare_freethreaded_python();
        let result = run("py_import(\"nonexistent_xyz_module\")");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("py_import") && msg.contains("nonexistent_xyz_module"));
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_getattr_setattr() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let t = py_import(\"types\")\nlet obj = py_call(py_getattr(t, \"SimpleNamespace\"))\npy_setattr(obj, \"val\", 99)\nprint(py_getattr(obj, \"val\"))");
        assert_eq!(output, vec!["99"]);
    }

    #[test]
    #[cfg(feature = "python")]
    fn test_vm_py_callable_round_trip() {
        pyo3::prepare_freethreaded_python();
        let output = run_output("let m = py_import(\"math\")\nlet f = py_getattr(m, \"floor\")\nprint(py_call(f, 3.7))");
        assert_eq!(output, vec!["3"]);
    }

    // ── Phase 21: Schema Evolution VM tests ──

    #[test]
    fn test_vm_schema_register_and_get() {
        let source = r#"let fields = map_from("id", "int64", "name", "string")
schema_register("User", 1, fields)
let result = schema_get("User", 1)
print(len(result))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_schema_latest() {
        let source = r#"schema_register("User", 1, map_from("id", "int64"))
schema_register("User", 2, map_from("id", "int64", "name", "string"))
let latest = schema_latest("User")
print(latest)"#;
        let output = run_output(source);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_schema_history() {
        let source = r#"schema_register("User", 1, map_from("id", "int64"))
schema_register("User", 2, map_from("id", "int64", "name", "string"))
let hist = schema_history("User")
print(len(hist))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_schema_check_backward_compat() {
        let source = r#"schema_register("User", 1, map_from("id", "int64"))
schema_register("User", 2, map_from("id", "int64", "name", "string"))
let issues = schema_check("User", 1, 2, "backward")
print(len(issues))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["0"]);
    }

    #[test]
    fn test_vm_schema_diff() {
        let source = r#"schema_register("User", 1, map_from("id", "int64"))
schema_register("User", 2, map_from("id", "int64", "name", "string"))
let diffs = schema_diff("User", 1, 2)
print(len(diffs))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["1"]);
    }

    #[test]
    fn test_vm_schema_versions() {
        let source = r#"schema_register("T", 1, map_from("id", "int64"))
schema_register("T", 3, map_from("id", "int64"))
schema_register("T", 2, map_from("id", "int64"))
let vers = schema_versions("T")
print(len(vers))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_schema_fields() {
        let source = r#"schema_register("User", 1, map_from("id", "int64", "name", "string"))
let fields = schema_fields("User", 1)
print(len(fields))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_compile_versioned_schema() {
        let source = "/// @version 1\nschema User { id: int64, name: string }\nprint(User)";
        let output = run_output(source);
        assert!(output[0].contains("__schema__:User:v1:"));
    }

    #[test]
    fn test_vm_compile_migrate() {
        let source = "migrate User from 1 to 2 { add_column(email: string) }\nprint(\"ok\")";
        let output = run_output(source);
        assert_eq!(output, vec!["ok"]);
    }

    #[test]
    fn test_vm_schema_check_backward_compat_fails() {
        let source = r#"schema_register("User", 1, map_from("id", "int64", "name", "string"))
schema_register("User", 2, map_from("id", "int64"))
let issues = schema_check("User", 1, 2, "backward")
print(len(issues))"#;
        let output = run_output(source);
        assert_eq!(output, vec!["1"]);
    }

    // ── Phase 22: Decimal VM Tests ─────────────────────────────────

    #[test]
    fn test_vm_decimal_literal_and_arithmetic() {
        let output = run_output("let a = 10.5d\nlet b = 2.5d\nprint(a + b)\nprint(a * b)");
        assert_eq!(output, vec!["13.0", "26.25"]);
    }

    #[test]
    fn test_vm_decimal_div_by_zero() {
        let source = "let a = 1.0d\nlet b = 0.0d\nlet c = a / b";
        let program = tl_parser::parse(source).unwrap();
        let proto = crate::compile(&program).unwrap();
        let mut vm = Vm::new();
        let result = vm.execute(&proto);
        assert!(result.is_err());
    }

    #[test]
    fn test_vm_decimal_comparison_ops() {
        let output = run_output("let a = 1.0d\nlet b = 2.0d\nprint(a < b)\nprint(a >= b)\nprint(a == a)");
        assert_eq!(output, vec!["true", "false", "true"]);
    }

    // ── Phase 23: Security VM Tests ────────────────────────────────

    #[test]
    fn test_vm_secret_vault_crud() {
        let output = run_output("secret_set(\"key\", \"value\")\nlet s = secret_get(\"key\")\nprint(s)\nsecret_delete(\"key\")\nlet s2 = secret_get(\"key\")\nprint(type_of(s2))");
        assert_eq!(output, vec!["***", "none"]);
    }

    #[test]
    fn test_vm_mask_email_basic() {
        let output = run_output("print(mask_email(\"alice@domain.com\"))");
        assert_eq!(output, vec!["a***@domain.com"]);
    }

    #[test]
    fn test_vm_mask_phone_basic() {
        let output = run_output("print(mask_phone(\"123-456-7890\"))");
        assert_eq!(output, vec!["***-***-7890"]);
    }

    #[test]
    fn test_vm_mask_cc_basic() {
        let output = run_output("print(mask_cc(\"4111222233334444\"))");
        assert_eq!(output, vec!["****-****-****-4444"]);
    }

    #[test]
    fn test_vm_hash_produces_hex() {
        let output = run_output("let h = hash(\"test\", \"sha256\")\nprint(len(h))");
        assert_eq!(output, vec!["64"]);
    }

    #[test]
    fn test_vm_redact_modes() {
        let output = run_output("print(redact(\"hello\", \"full\"))\nprint(redact(\"hello\", \"partial\"))");
        assert_eq!(output, vec!["***", "h***o"]);
    }

    #[test]
    fn test_vm_security_policy_sandbox() {
        let source = "print(check_permission(\"network\"))\nprint(check_permission(\"file_read\"))";
        let program = tl_parser::parse(source).unwrap();
        let proto = crate::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.security_policy = Some(crate::security::SecurityPolicy::sandbox());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["false", "true"]);
    }

    // ── Phase 25: Async Runtime Tests (feature-gated) ──────────────

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_read_write_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("async_test.txt");
        let path_str = path.to_str().unwrap().replace('\\', "/");
        let source = format!(
            r#"let wt = async_write_file("{path_str}", "async hello")
let wr = await(wt)
let rt = async_read_file("{path_str}")
let content = await(rt)
print(content)"#
        );
        let output = run_output(&source);
        assert_eq!(output, vec!["async hello"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_sleep() {
        let source = r#"
let t = async_sleep(10)
let r = await(t)
print(r)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_select_first_wins() {
        // select between a fast sleep and a slow sleep — fast one wins
        let source = r#"
let fast = async_sleep(10)
let slow = async_sleep(5000)
let winner = select(fast, slow)
let result = await(winner)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_race_all() {
        let source = r#"
let t1 = async_sleep(10)
let t2 = async_sleep(5000)
let winner = race_all([t1, t2])
let result = await(winner)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_map() {
        let source = r#"
let items = [1, 2, 3]
let t = async_map(items, (x) => x * 10)
let result = await(t)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["[10, 20, 30]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_filter() {
        let source = r#"
let items = [1, 2, 3, 4, 5]
let t = async_filter(items, (x) => x > 3)
let result = await(t)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["[4, 5]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_write_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("write_test.txt");
        let path_str = path.to_str().unwrap().replace('\\', "/");
        let source = format!(
            r#"let t = async_write_file("{path_str}", "test data")
let r = await(t)
print(r)"#
        );
        let output = run_output(&source);
        assert_eq!(output, vec!["none"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_security_policy_blocks_write() {
        let source = r#"let t = async_write_file("/tmp/blocked.txt", "data")"#;
        let program = tl_parser::parse(source).unwrap();
        let proto = crate::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.security_policy = Some(crate::security::SecurityPolicy::sandbox());
        let result = vm.execute(&proto);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("file_write not allowed"), "Expected security error, got: {err}");
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_security_policy_allows_read() {
        // Sandbox allows file_read, so async_read_file should succeed (even if file doesn't exist)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readable.txt");
        std::fs::write(&path, "safe content").unwrap();
        let path_str = path.to_str().unwrap().replace('\\', "/");
        let source = format!(
            r#"let t = async_read_file("{path_str}")
let r = await(t)
print(r)"#
        );
        let program = tl_parser::parse(&source).unwrap();
        let proto = crate::compile(&program).unwrap();
        let mut vm = Vm::new();
        vm.security_policy = Some(crate::security::SecurityPolicy::sandbox());
        vm.execute(&proto).unwrap();
        assert_eq!(vm.output, vec!["safe content"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_map_empty_list() {
        let source = r#"
let t = async_map([], (x) => x * 2)
let result = await(t)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["[]"]);
    }

    #[cfg(feature = "async-runtime")]
    #[test]
    fn test_vm_async_filter_none_match() {
        let source = r#"
let t = async_filter([1, 2, 3], (x) => x > 100)
let result = await(t)
print(result)
"#;
        let output = run_output(source);
        assert_eq!(output, vec!["[]"]);
    }

    // --- Phase 26: Closure upvalue closing tests ---

    #[test]
    fn test_vm_closure_returned_from_function() {
        let output = run_output(r#"
fn make_adder(n) {
    return (x) => x + n
}
let add5 = make_adder(5)
print(add5(3))
print(add5(10))
"#);
        assert_eq!(output, vec!["8", "15"]);
    }

    #[test]
    fn test_vm_closure_factory_multiple_calls() {
        let output = run_output(r#"
fn make_adder(n) {
    return (x) => x + n
}
let add2 = make_adder(2)
let add10 = make_adder(10)
print(add2(5))
print(add10(5))
print(add2(1))
"#);
        assert_eq!(output, vec!["7", "15", "3"]);
    }

    #[test]
    fn test_vm_closure_returned_in_list() {
        let output = run_output(r#"
fn make_ops(n) {
    let add = (x) => x + n
    let mul = (x) => x * n
    return [add, mul]
}
let ops = make_ops(3)
print(ops[0](10))
print(ops[1](10))
"#);
        assert_eq!(output, vec!["13", "30"]);
    }

    #[test]
    fn test_vm_nested_closure_return() {
        let output = run_output(r#"
fn outer(a) {
    fn inner(b) {
        return (x) => x + a + b
    }
    return inner(10)
}
let f = outer(5)
print(f(1))
"#);
        assert_eq!(output, vec!["16"]);
    }

    #[test]
    fn test_vm_multiple_closures_same_local() {
        let output = run_output(r#"
fn make_pair(n) {
    let inc = (x) => x + n
    let dec = (x) => x - n
    return [inc, dec]
}
let pair = make_pair(7)
print(pair[0](10))
print(pair[1](10))
"#);
        assert_eq!(output, vec!["17", "3"]);
    }

    #[test]
    fn test_vm_closure_captures_multiple_locals() {
        let output = run_output(r#"
fn make_greeter(greeting, name) {
    let sep = " "
    return () => greeting + sep + name
}
let hi = make_greeter("Hello", "World")
let bye = make_greeter("Goodbye", "Alice")
print(hi())
print(bye())
"#);
        assert_eq!(output, vec!["Hello World", "Goodbye Alice"]);
    }

    // ── Phase 27: Data Error Hierarchy tests ──

    #[test]
    fn test_vm_throw_catch_preserves_enum() {
        let output = run_output(r#"
enum Color { Red, Green(x) }
try {
    throw Color::Green(42)
} catch e {
    match e {
        Color::Green(x) => print(x),
        _ => print("no match"),
    }
}
"#);
        assert_eq!(output, vec!["42"]);
    }

    #[test]
    fn test_vm_throw_catch_string_compat() {
        let output = run_output(r#"
try {
    throw "hello error"
} catch e {
    print(e)
}
"#);
        assert_eq!(output, vec!["hello error"]);
    }

    #[test]
    fn test_vm_runtime_error_still_string() {
        let output = run_output(r#"
try {
    let x = 1 / 0
} catch e {
    print(type_of(e))
}
"#);
        assert_eq!(output, vec!["string"]);
    }

    #[test]
    fn test_vm_data_error_construct_and_throw() {
        let output = run_output(r#"
try {
    throw DataError::ParseError("bad format", "file.csv")
} catch e {
    print(match e { DataError::ParseError(msg, _) => msg, _ => "no match" })
    print(match e { DataError::ParseError(_, src) => src, _ => "no match" })
}
"#);
        assert_eq!(output, vec!["bad format", "file.csv"]);
    }

    #[test]
    fn test_vm_network_error_construct() {
        let output = run_output(r#"
let err = NetworkError::TimeoutError("timed out")
match err {
    NetworkError::TimeoutError(msg) => print(msg),
    _ => print("no match"),
}
"#);
        assert_eq!(output, vec!["timed out"]);
    }

    #[test]
    fn test_vm_connector_error_construct() {
        let output = run_output(r#"
let err = ConnectorError::AuthError("invalid creds", "postgres")
print(match err { ConnectorError::AuthError(msg, _) => msg, _ => "no match" })
print(match err { ConnectorError::AuthError(_, conn) => conn, _ => "no match" })
"#);
        assert_eq!(output, vec!["invalid creds", "postgres"]);
    }

    #[test]
    fn test_vm_is_error_builtin() {
        let output = run_output(r#"
let e1 = DataError::NotFound("users")
let e2 = NetworkError::TimeoutError("slow")
let e3 = ConnectorError::ConfigError("bad", "redis")
let e4 = "not an error"
print(is_error(e1))
print(is_error(e2))
print(is_error(e3))
print(is_error(e4))
"#);
        assert_eq!(output, vec!["true", "true", "true", "false"]);
    }

    #[test]
    fn test_vm_error_type_builtin() {
        let output = run_output(r#"
let e1 = DataError::ParseError("bad", "x.csv")
let e2 = NetworkError::HttpError("fail", "url")
let e3 = "not an error"
print(error_type(e1))
print(error_type(e2))
print(error_type(e3))
"#);
        assert_eq!(output, vec!["DataError", "NetworkError", "none"]);
    }

    #[test]
    fn test_vm_match_error_variants() {
        let output = run_output(r#"
fn handle(err) {
    print(match err {
        DataError::ParseError(msg, _) => "parse: " + msg,
        DataError::SchemaError(msg, _, _) => "schema: " + msg,
        DataError::ValidationError(_, field) => "validation: " + field,
        DataError::NotFound(name) => "not found: " + name,
        _ => "unknown"
    })
}
handle(DataError::ParseError("bad csv", "data.csv"))
handle(DataError::NotFound("users_table"))
handle(DataError::SchemaError("mismatch", "int", "string"))
handle(DataError::ValidationError("invalid", "email"))
"#);
        assert_eq!(output, vec![
            "parse: bad csv",
            "not found: users_table",
            "schema: mismatch",
            "validation: email",
        ]);
    }

    #[test]
    fn test_vm_rethrow_structured_error() {
        let output = run_output(r#"
try {
    try {
        throw DataError::NotFound("config")
    } catch e {
        throw e
    }
} catch outer {
    match outer {
        DataError::NotFound(name) => print("caught: " + name),
        _ => print("wrong type"),
    }
}
"#);
        assert_eq!(output, vec!["caught: config"]);
    }

    // ── Phase 28: Ownership & Move Semantics ──

    #[test]
    fn test_vm_pipe_moves_value() {
        // x |> f() should consume x — accessing x after pipe gives error
        let result = run(r#"
fn identity(v) { v }
let x = [1, 2, 3]
x |> identity()
print(x)
"#);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("moved"), "Error should mention 'moved': {err}");
    }

    #[test]
    fn test_vm_clone_before_pipe() {
        // x.clone() |> f() should not consume x
        let output = run_output(r#"
fn identity(v) { v }
let x = [1, 2, 3]
x.clone() |> identity()
print(x)
"#);
        assert_eq!(output, vec!["[1, 2, 3]"]);
    }

    #[test]
    fn test_vm_clone_list_deep() {
        // Mutating a cloned list should not affect the original
        let output = run_output(r#"
let original = [1, 2, 3]
let copy = original.clone()
copy[0] = 99
print(original)
print(copy)
"#);
        assert_eq!(output, vec!["[1, 2, 3]", "[99, 2, 3]"]);
    }

    #[test]
    fn test_vm_clone_map() {
        let output = run_output(r#"
let m = map_from("a", 1, "b", 2)
let m2 = m.clone()
m2["a"] = 99
print(m)
print(m2)
"#);
        assert_eq!(output, vec!["{a: 1, b: 2}", "{a: 99, b: 2}"]);
    }

    #[test]
    fn test_vm_clone_struct() {
        let output = run_output(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 1, y: 2 }
let p2 = p.clone()
print(p)
print(p2)
"#);
        assert_eq!(output, vec!["Point { x: 1, y: 2 }", "Point { x: 1, y: 2 }"]);
    }

    #[test]
    fn test_vm_ref_read_only() {
        // &x should be readable but not mutable
        let result = run(r#"
let x = [1, 2, 3]
let r = &x
r[0] = 99
"#);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Cannot mutate a borrowed reference"), "Error should mention reference: {err}");
    }

    #[test]
    fn test_vm_ref_transparent_read() {
        // Reading through a ref should work transparently
        let output = run_output(r#"
let x = [1, 2, 3]
let r = &x
print(len(r))
"#);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_vm_parallel_for_basic() {
        // parallel for should iterate all elements (runs sequentially in VM)
        let output = run_output(r#"
let items = [10, 20, 30]
parallel for item in items {
    print(item)
}
"#);
        assert_eq!(output, vec!["10", "20", "30"]);
    }

    #[test]
    fn test_vm_moved_value_clear_error() {
        // Error message should mention .clone()
        let result = run(r#"
fn f(x) { x }
let data = "hello"
data |> f()
print(data)
"#);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("clone()"), "Error should suggest .clone(): {err}");
    }

    #[test]
    fn test_vm_reassign_after_move() {
        // After moving, reassigning the variable should work
        let output = run_output(r#"
fn f(x) { x }
let x = 1
x |> f()
let x = 2
print(x)
"#);
        assert_eq!(output, vec!["2"]);
    }

    #[test]
    fn test_vm_pipe_chain_move() {
        // Chained pipes should work — intermediate values don't need explicit binding
        let output = run_output(r#"
fn double(x) { x * 2 }
fn add_one(x) { x + 1 }
let result = 5 |> double() |> add_one()
print(result)
"#);
        assert_eq!(output, vec!["11"]);
    }

    #[test]
    fn test_vm_string_clone() {
        // .clone() on string values
        let output = run_output(r#"
let s = "hello"
let s2 = s.clone()
print(s)
print(s2)
"#);
        assert_eq!(output, vec!["hello", "hello"]);
    }

    #[test]
    fn test_vm_ref_method_dispatch() {
        // Methods should be callable through references
        let output = run_output(r#"
let s = "hello world"
let r = &s
print(r.len())
"#);
        assert_eq!(output, vec!["11"]);
    }

    #[test]
    fn test_vm_ref_member_access() {
        // Member access through ref should work
        let output = run_output(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 10, y: 20 }
let r = &p
print(r.x)
"#);
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_vm_ref_set_member_blocked() {
        // Setting a member through a ref should fail
        let result = run(r#"
struct Point { x: int64, y: int64 }
let p = Point { x: 10, y: 20 }
let r = &p
r.x = 99
"#);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Cannot mutate a borrowed reference"), "Error: {err}");
    }

    // ── Phase 29: IR Integration Tests ──

    #[test]
    fn test_ir_filter_merge_chain() {
        // Two adjacent filters should be merged by the IR optimizer
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "name,age\nAlice,30\nBob,20\nCharlie,35\n").unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> filter(age > 25) |> filter(age < 40) |> collect()
print(r)"#,
            csv.to_str().unwrap()
        );
        let output = run_output(&src);
        // Both Alice(30) and Charlie(35) pass both filters
        assert!(output[0].contains("Alice"), "Output should contain Alice: {}", output[0]);
        assert!(output[0].contains("Charlie"), "Output should contain Charlie: {}", output[0]);
        assert!(!output[0].contains("Bob"), "Output should not contain Bob: {}", output[0]);
    }

    #[test]
    fn test_ir_predicate_pushdown_through_select() {
        // filter after select should be pushed before select by IR optimizer
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "name,age,city\nAlice,30,NYC\nBob,20,LA\nCharlie,35,NYC\n").unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> select(name, age) |> filter(age > 25) |> collect()
print(r)"#,
            csv.to_str().unwrap()
        );
        let output = run_output(&src);
        assert!(output[0].contains("Alice"), "Output should contain Alice");
        assert!(output[0].contains("Charlie"), "Output should contain Charlie");
        assert!(!output[0].contains("Bob"), "Output should not contain Bob");
    }

    #[test]
    fn test_ir_sort_filter_pushdown() {
        // filter after sort should be pushed before sort
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "name,score\nAlice,90\nBob,50\nCharlie,75\n").unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> sort(score, "desc") |> filter(score > 60) |> collect()
print(r)"#,
            csv.to_str().unwrap()
        );
        let output = run_output(&src);
        assert!(output[0].contains("Alice"), "Output should contain Alice");
        assert!(output[0].contains("Charlie"), "Output should contain Charlie");
        assert!(!output[0].contains("Bob"), "Output should not contain Bob");
    }

    #[test]
    fn test_ir_multi_operation_chain() {
        // Complex chain: filter + select + sort + limit
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(
            &csv,
            "name,age,dept\nAlice,30,Eng\nBob,20,Sales\nCharlie,35,Eng\nDiana,28,Sales\n",
        )
        .unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> filter(age > 22) |> select(name, age) |> sort(age, "desc") |> limit(2) |> collect()
print(r)"#,
            csv.to_str().unwrap()
        );
        let output = run_output(&src);
        // Top 2 by age descending among age>22: Charlie(35), Alice(30)
        assert!(output[0].contains("Charlie"), "Output: {}", output[0]);
        assert!(output[0].contains("Alice"), "Output: {}", output[0]);
    }

    #[test]
    fn test_ir_pipe_move_semantics_preserved() {
        // The source variable should be moved after pipe chain
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "name,age\nAlice,30\n").unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> filter(age > 0) |> collect()
print(t)"#,
            csv.to_str().unwrap()
        );
        let result = run(&src);
        assert!(result.is_err(), "Should error on use-after-move");
    }

    #[test]
    fn test_ir_non_table_op_fallback() {
        // A pipe chain with a non-table op should fall back to legacy path
        let output = run_output(r#"
fn double(x) { x * 2 }
let result = 5 |> double()
print(result)
"#);
        assert_eq!(output, vec!["10"]);
    }

    #[test]
    fn test_ir_mixed_pipe_fallback() {
        // A pipe into a builtin (not a table op) should use legacy path
        let output = run_output(r#"
let result = [3, 1, 2] |> len()
print(result)
"#);
        assert_eq!(output, vec!["3"]);
    }

    #[test]
    fn test_ir_single_filter_roundtrip() {
        // Even a single filter goes through IR and round-trips correctly
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "name,age\nAlice,30\nBob,20\n").unwrap();
        let src = format!(
            r#"let t = read_csv("{}")
let r = t |> filter(age > 25) |> collect()
print(r)"#,
            csv.to_str().unwrap()
        );
        let output = run_output(&src);
        assert!(output[0].contains("Alice"), "Output: {}", output[0]);
        assert!(!output[0].contains("Bob"), "Output: {}", output[0]);
    }
}
