// ThinkingLanguage — Bytecode Virtual Machine
// Register-based VM that executes compiled bytecode.

use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use rayon::prelude::*;
use tl_ast::Expr as AstExpr;
use tl_errors::{RuntimeError, TlError};
use tl_data::{DataEngine, JoinType, col};
use tl_data::translate::{translate_expr, LocalValue, TranslateContext};

use crate::chunk::*;
use crate::opcode::*;
use crate::value::*;

fn runtime_err(msg: impl Into<String>) -> TlError {
    TlError::Runtime(RuntimeError {
        message: msg.into(),
        span: None,
    })
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
const PARALLEL_THRESHOLD: usize = 10_000;

/// Check if a closure is pure (no captured upvalues) and thus safe to run in parallel.
fn is_pure_closure(func: &VmValue) -> bool {
    match func {
        VmValue::Function(closure) => closure.upvalues.is_empty(),
        _ => false,
    }
}

/// Execute a pure function (no upvalues) in an isolated mini-VM.
/// Used by rayon parallel operations — each thread gets its own stack.
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
    stack: Vec<VmValue>,
    /// Call frame stack
    frames: Vec<CallFrame>,
    /// Global variables
    globals: HashMap<String, VmValue>,
    /// Data engine (lazily initialized)
    data_engine: Option<DataEngine>,
    /// Captured output (for testing)
    pub output: Vec<String>,
    /// Try-catch handler stack
    try_handlers: Vec<TryHandler>,
}

impl Vm {
    pub fn new() -> Self {
        Vm {
            stack: Vec::with_capacity(256),
            frames: Vec::new(),
            globals: HashMap::new(),
            data_engine: None,
            output: Vec::new(),
            try_handlers: Vec::new(),
        }
    }

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

        self.run()
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
                                self.stack[cbase + catch_reg as usize] = VmValue::String(Arc::from(err_msg.as_str()));
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
                    let val = self.stack[base + b as usize].clone();
                    self.stack[base + a as usize] = val;
                }
                Op::GetLocal => {
                    let val = self.stack[base + b as usize].clone();
                    self.stack[base + a as usize] = val;
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
                    self.stack[base + a as usize] = val;
                }
                Op::SetGlobal => {
                    let name = self.get_string_constant(frame_idx, bx)?;
                    let val = self.stack[base + a as usize].clone();
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
                    let obj = &self.stack[base + b as usize];
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
                    // a = table reg, b = op name constant idx, c = args constant idx
                    let table_val = self.stack[base + a as usize].clone();
                    let result = self.handle_table_pipe(frame_idx, table_val, b, c)?;
                    self.stack[base + a as usize] = result;
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
                    let obj = self.stack[base + b as usize].clone();
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
                    // a = dest, b = algorithm constant, c = config AstExprList constant
                    let result = self.handle_train(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::PipelineExec => {
                    let result = self.handle_pipeline_exec(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result;
                }
                Op::StreamExec => {
                    let result = self.handle_stream_exec(frame_idx, b)?;
                    self.stack[base + a as usize] = result;
                }
                Op::ConnectorDecl => {
                    let result = self.handle_connector_decl(frame_idx, b, c)?;
                    self.stack[base + a as usize] = result;
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
                    // a = dest, bx = path constant
                    // Next instruction: A = alias constant index
                    let path = self.get_string_constant(frame_idx, bx)?;
                    let next = self.frames[frame_idx].prototype.code[self.frames[frame_idx].ip];
                    self.frames[frame_idx].ip += 1;
                    let alias_idx = decode_a(next) as u16;
                    let alias = self.get_string_constant(frame_idx, alias_idx)?;

                    let result = self.handle_import(&path, &alias)?;
                    self.stack[base + a as usize] = result;
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

    /// Execute a closure (no arguments) in this VM. Used by spawn().
    fn execute_closure(&mut self, proto: &Arc<Prototype>, upvalues: &[UpvalueRef]) -> Result<VmValue, TlError> {
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
    fn execute_closure_with_args(&mut self, proto: &Arc<Prototype>, upvalues: &[UpvalueRef], args: &[VmValue]) -> Result<VmValue, TlError> {
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

    fn vm_add(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
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
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.add(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            _ => Err(runtime_err(format!(
                "Cannot apply `+` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_sub(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
        let left = &self.stack[base + b as usize];
        let right = &self.stack[base + c as usize];
        match (left, right) {
            (VmValue::Int(a), VmValue::Int(b)) => Ok(VmValue::Int(a - b)),
            (VmValue::Float(a), VmValue::Float(b)) => Ok(VmValue::Float(a - b)),
            (VmValue::Int(a), VmValue::Float(b)) => Ok(VmValue::Float(*a as f64 - b)),
            (VmValue::Float(a), VmValue::Int(b)) => Ok(VmValue::Float(a - *b as f64)),
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.sub(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            _ => Err(runtime_err(format!(
                "Cannot apply `-` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_mul(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
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
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.mul(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            (VmValue::Tensor(t), VmValue::Float(s)) | (VmValue::Float(s), VmValue::Tensor(t)) => {
                let result = t.scale(*s);
                Ok(VmValue::Tensor(Arc::new(result)))
            }
            _ => Err(runtime_err(format!(
                "Cannot apply `*` to {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn vm_div(&self, base: usize, b: u8, c: u8) -> Result<VmValue, TlError> {
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
            (VmValue::Tensor(a), VmValue::Tensor(b)) => {
                let result = a.div(b).map_err(|e| runtime_err(format!("{e}")))?;
                Ok(VmValue::Tensor(Arc::new(result)))
            }
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
            _ => Err(runtime_err(format!(
                "Cannot compare {} and {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    // ── Builtin dispatch ──

    fn call_builtin(&mut self, id: u8, args_base: usize, arg_count: usize) -> Result<VmValue, TlError> {
        let args: Vec<VmValue> = (0..arg_count)
            .map(|i| self.stack[args_base + i].clone())
            .collect();

        let builtin_id: BuiltinId = unsafe { std::mem::transmute(id) };

        match builtin_id {
            BuiltinId::Print | BuiltinId::Println => {
                let mut parts = Vec::new();
                for a in &args {
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
                _ => Err(runtime_err("len() expects a string, list, or map")),
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
            BuiltinId::ReadCsv => {
                if args.len() != 1 { return Err(runtime_err("read_csv() expects 1 argument (path)")); }
                let path = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("read_csv() path must be a string")),
                };
                let df = self.engine().read_csv(&path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df }))
            }
            BuiltinId::ReadParquet => {
                if args.len() != 1 { return Err(runtime_err("read_parquet() expects 1 argument (path)")); }
                let path = match &args[0] {
                    VmValue::String(s) => s.to_string(),
                    _ => return Err(runtime_err("read_parquet() path must be a string")),
                };
                let df = self.engine().read_parquet(&path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df }))
            }
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
                self.engine().write_csv(df, &path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::None)
            }
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
                self.engine().write_parquet(df, &path).map_err(|e| runtime_err(e))?;
                Ok(VmValue::None)
            }
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
                let df = self.engine().read_postgres(&conn_str, &table_name)
                    .map_err(|e| runtime_err(e))?;
                Ok(VmValue::Table(VmTable { df }))
            }
            // ── AI builtins ──
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
            BuiltinId::TensorZeros => {
                if args.is_empty() { return Err(runtime_err("tensor_zeros() expects 1 argument (shape)")); }
                let shape = self.vmvalue_to_usize_list(&args[0])?;
                let t = tl_ai::TlTensor::zeros(&shape);
                Ok(VmValue::Tensor(Arc::new(t)))
            }
            BuiltinId::TensorOnes => {
                if args.is_empty() { return Err(runtime_err("tensor_ones() expects 1 argument (shape)")); }
                let shape = self.vmvalue_to_usize_list(&args[0])?;
                let t = tl_ai::TlTensor::ones(&shape);
                Ok(VmValue::Tensor(Arc::new(t)))
            }
            BuiltinId::TensorShape => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        let shape: Vec<VmValue> = t.shape().iter().map(|&d| VmValue::Int(d as i64)).collect();
                        Ok(VmValue::List(shape))
                    }
                    _ => Err(runtime_err("tensor_shape() expects a tensor")),
                }
            }
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
            BuiltinId::TensorTranspose => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        let transposed = t.transpose().map_err(|e| runtime_err(format!("{e}")))?;
                        Ok(VmValue::Tensor(Arc::new(transposed)))
                    }
                    _ => Err(runtime_err("tensor_transpose() expects a tensor")),
                }
            }
            BuiltinId::TensorSum => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        Ok(VmValue::Float(t.sum()))
                    }
                    _ => Err(runtime_err("tensor_sum() expects a tensor")),
                }
            }
            BuiltinId::TensorMean => {
                match args.first() {
                    Some(VmValue::Tensor(t)) => {
                        Ok(VmValue::Float(t.mean()))
                    }
                    _ => Err(runtime_err("tensor_mean() expects a tensor")),
                }
            }
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
            BuiltinId::ModelList => {
                let registry = tl_ai::ModelRegistry::default_location();
                let names = registry.list();
                let items: Vec<VmValue> = names.into_iter()
                    .map(|n: String| VmValue::String(Arc::from(n.as_str())))
                    .collect();
                Ok(VmValue::List(items))
            }
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
            // Streaming builtins
            BuiltinId::AlertSlack => {
                if args.len() < 2 { return Err(runtime_err("alert_slack(url, msg) requires 2 args")); }
                let url = match &args[0] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("alert_slack: url must be a string")) };
                let msg = format!("{}", args[1]);
                tl_stream::send_alert(&tl_stream::AlertTarget::Slack(url), &msg)
                    .map_err(|e| runtime_err(&e))?;
                Ok(VmValue::None)
            }
            BuiltinId::AlertWebhook => {
                if args.len() < 2 { return Err(runtime_err("alert_webhook(url, msg) requires 2 args")); }
                let url = match &args[0] { VmValue::String(s) => s.to_string(), _ => return Err(runtime_err("alert_webhook: url must be a string")) };
                let msg = format!("{}", args[1]);
                tl_stream::send_alert(&tl_stream::AlertTarget::Webhook(url), &msg)
                    .map_err(|e| runtime_err(&e))?;
                Ok(VmValue::None)
            }
            BuiltinId::Emit => {
                if args.is_empty() { return Err(runtime_err("emit() requires at least 1 argument")); }
                self.output.push(format!("emit: {}", args[0]));
                Ok(args[0].clone())
            }
            BuiltinId::Lineage => {
                Ok(VmValue::String(Arc::from("lineage_tracker")))
            }
            BuiltinId::RunPipeline => {
                if args.is_empty() { return Err(runtime_err("run_pipeline() requires a pipeline")); }
                if let VmValue::PipelineDef(ref def) = args[0] {
                    Ok(VmValue::String(Arc::from(format!("Pipeline '{}' triggered", def.name).as_str())))
                } else {
                    Err(runtime_err("run_pipeline: argument must be a pipeline"))
                }
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
            BuiltinId::HttpGet => {
                if args.is_empty() { return Err(runtime_err("http_get() expects a URL")); }
                if let VmValue::String(url) = &args[0] {
                    let body = reqwest::blocking::get(url.as_ref())
                        .map_err(|e| runtime_err(format!("HTTP GET error: {e}")))?
                        .text()
                        .map_err(|e| runtime_err(format!("HTTP response error: {e}")))?;
                    Ok(VmValue::String(Arc::from(body.as_str())))
                } else {
                    Err(runtime_err("http_get() expects a string URL"))
                }
            }
            BuiltinId::HttpPost => {
                if args.len() < 2 { return Err(runtime_err("http_post() expects URL and body")); }
                if let (VmValue::String(url), VmValue::String(body)) = (&args[0], &args[1]) {
                    let client = reqwest::blocking::Client::new();
                    let resp = client
                        .post(url.as_ref())
                        .header("Content-Type", "application/json")
                        .body(body.to_string())
                        .send()
                        .map_err(|e| runtime_err(format!("HTTP POST error: {e}")))?
                        .text()
                        .map_err(|e| runtime_err(format!("HTTP response error: {e}")))?;
                    Ok(VmValue::String(Arc::from(resp.as_str())))
                } else {
                    Err(runtime_err("http_post() expects string URL and body"))
                }
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
            BuiltinId::FileExists => {
                if args.is_empty() { return Err(runtime_err("file_exists() expects a path")); }
                if let VmValue::String(path) = &args[0] {
                    Ok(VmValue::Bool(std::path::Path::new(path.as_ref()).exists()))
                } else {
                    Err(runtime_err("file_exists() expects a string path"))
                }
            }
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
            BuiltinId::EnvSet => {
                if args.len() < 2 { return Err(runtime_err("env_set() expects name and value")); }
                if let (VmValue::String(name), VmValue::String(val)) = (&args[0], &args[1]) {
                    unsafe { std::env::set_var(name.as_ref(), val.as_ref()); }
                    Ok(VmValue::None)
                } else {
                    Err(runtime_err("env_set() expects two strings"))
                }
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
            BuiltinId::Channel => {
                let capacity = match args.first() {
                    Some(VmValue::Int(n)) => *n as usize,
                    None => 64,
                    _ => return Err(runtime_err("channel() expects an optional integer capacity")),
                };
                Ok(VmValue::Channel(Arc::new(VmChannel::new(capacity))))
            }
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

    /// Dispatch a method call on an object.
    fn dispatch_method(&mut self, obj: VmValue, method: &str, args: &[VmValue]) -> Result<VmValue, TlError> {
        match &obj {
            VmValue::String(s) => self.dispatch_string_method(s.clone(), method, args),
            VmValue::List(items) => self.dispatch_list_method(items.clone(), method, args),
            VmValue::Map(pairs) => self.dispatch_map_method(pairs.clone(), method, args),
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

    /// Handle import at runtime.
    fn handle_import(&mut self, path: &str, alias: &str) -> Result<VmValue, TlError> {
        // Read, parse, compile, execute the file
        let source = std::fs::read_to_string(path)
            .map_err(|e| runtime_err(format!("Cannot import '{}': {}", path, e)))?;
        let program = tl_parser::parse(&source)
            .map_err(|e| runtime_err(format!("Parse error in '{}': {}", path, e)))?;
        let proto = crate::compiler::compile(&program)
            .map_err(|e| runtime_err(format!("Compile error in '{}': {}", path, e)))?;

        // Execute in a fresh VM with shared globals
        let mut import_vm = Vm::new();
        import_vm.globals = self.globals.clone();
        import_vm.execute(&proto)?;

        // Collect new globals that were defined in the import
        let mut exports = HashMap::new();
        for (k, v) in &import_vm.globals {
            if !self.globals.contains_key(k) {
                exports.insert(k.clone(), v.clone());
            }
        }

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
            AstExpr::Closure { params: _, body: _ } => {
                use crate::compiler;
                let wrapper = tl_ast::Program {
                    statements: vec![tl_ast::Stmt::Expr(expr.clone())],
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
                    statements: vec![tl_ast::Stmt::Expr(expr.clone())],
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
}
