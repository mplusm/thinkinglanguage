// ThinkingLanguage — Bytecode Virtual Machine
// Register-based VM that executes compiled bytecode.

use std::collections::HashMap;
use std::sync::Arc;

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
}

impl Vm {
    pub fn new() -> Self {
        Vm {
            stack: Vec::with_capacity(256),
            frames: Vec::new(),
            globals: HashMap::new(),
            data_engine: None,
            output: Vec::new(),
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
            if self.frames.len() < entry_depth || self.frames.is_empty() {
                return Ok(VmValue::None);
            }
            let frame_idx = self.frames.len() - 1;
            let frame = &self.frames[frame_idx];

            if frame.ip >= frame.prototype.code.len() {
                // End of bytecode — return None
                self.frames.pop();
                return Ok(VmValue::None);
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
                    return Ok(return_val);
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
                    if let VmValue::Int(i) = idx_val {
                        if let VmValue::List(ref mut items) = self.stack[base + b as usize] {
                            let i = i as usize;
                            if i < items.len() {
                                items[i] = val;
                            }
                        }
                    }
                }
                Op::NewMap => {
                    // For now, maps are stored as lists of pairs (not used much in TL)
                    // a = dest, b = start reg, c = pair count
                    // The pairs are key, value, key, value in registers b..b+c*2
                    let items: Vec<VmValue> = (0..c as usize * 2)
                        .map(|i| self.stack[base + b as usize + i].clone())
                        .collect();
                    self.stack[base + a as usize] = VmValue::List(items);
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
                    // Currently only used for schema access or table member
                    // a = dest, b = object reg, c = field constant
                    self.stack[base + a as usize] = VmValue::None;
                }
                Op::Interpolate => {
                    // a = dest, bx = string template constant
                    let template = self.get_string_constant(frame_idx, bx)?;
                    let result = self.interpolate_string(&template, base)?;
                    self.stack[base + a as usize] = VmValue::String(Arc::from(result.as_str()));
                }
            }
        }
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
                _ => Err(runtime_err("len() expects a string or list")),
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
                _ => Err(runtime_err("int() expects a number or string")),
            },
            BuiltinId::Float => match args.first() {
                Some(VmValue::Int(n)) => Ok(VmValue::Float(*n as f64)),
                Some(VmValue::String(s)) => s.parse::<f64>()
                    .map(VmValue::Float)
                    .map_err(|_| runtime_err(format!("Cannot convert '{s}' to float"))),
                Some(VmValue::Float(n)) => Ok(VmValue::Float(*n)),
                _ => Err(runtime_err("float() expects a number or string")),
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
                } else {
                    Err(runtime_err("range() expects 1 or 2 arguments"))
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
}
