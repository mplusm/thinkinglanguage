// ThinkingLanguage — LLVM Runtime Helpers
// extern "C" functions callable from LLVM-compiled native code.
// These bridge compiled code back into the VM runtime for dynamic dispatch.

use std::sync::Arc;
use tl_compiler::Vm;
use tl_compiler::chunk::{Constant, Prototype};
use tl_compiler::value::VmValue;

/// Bridge struct passed to compiled functions so they can access VM state.
#[repr(C)]
pub struct VmContext {
    pub vm: *mut Vm,
    pub prototype: *const Prototype,
}

// ── Arithmetic helpers ──

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_add(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x + y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x + y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 + y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x + *y as f64),
        (VmValue::String(x), VmValue::String(y)) => {
            VmValue::String(Arc::from(format!("{x}{y}").as_str()))
        }
        (VmValue::Decimal(x), VmValue::Decimal(y)) => VmValue::Decimal(x + y),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_sub(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x - y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x - y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 - y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x - *y as f64),
        (VmValue::Decimal(x), VmValue::Decimal(y)) => VmValue::Decimal(x - y),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_mul(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x * y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x * y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 * y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x + *y as f64),
        (VmValue::Decimal(x), VmValue::Decimal(y)) => VmValue::Decimal(x * y),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_div(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) if *y != 0 => VmValue::Int(x / y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x / y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 / y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x / *y as f64),
        (VmValue::Decimal(x), VmValue::Decimal(y)) if !y.is_zero() => VmValue::Decimal(x / y),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_mod(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) if *y != 0 => VmValue::Int(x % y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x % y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 % y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x % *y as f64),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_pow(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int((*x as f64).powi(*y as i32) as i64),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x.powf(*y)),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float((*x as f64).powf(*y)),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x.powi(*y as i32)),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

// ── Unary ops ──

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_neg(a: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let result = match a {
        VmValue::Int(x) => VmValue::Int(-x),
        VmValue::Float(x) => VmValue::Float(-x),
        VmValue::Decimal(x) => VmValue::Decimal(-x),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_not(a: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let result = VmValue::Bool(!a.is_truthy());
    unsafe {
        out.write(result);
    }
}

// ── Comparison helpers (produce Bool VmValue) ──

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_eq(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = VmValue::Bool(vmvalue_eq(a, b));
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_neq(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = VmValue::Bool(!vmvalue_eq(a, b));
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_lt(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let result = VmValue::Bool(tl_rt_cmp(a, b) < 0);
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_gt(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let result = VmValue::Bool(tl_rt_cmp(a, b) > 0);
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_lte(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let result = VmValue::Bool(tl_rt_cmp(a, b) <= 0);
    unsafe {
        out.write(result);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_gte(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let result = VmValue::Bool(tl_rt_cmp(a, b) >= 0);
    unsafe {
        out.write(result);
    }
}

/// Comparison returning -1, 0, 1
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_cmp(a: *const VmValue, b: *const VmValue) -> i64 {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => {
            if x < y {
                -1
            } else if x > y {
                1
            } else {
                0
            }
        }
        (VmValue::Float(x), VmValue::Float(y)) => {
            if x < y {
                -1
            } else if x > y {
                1
            } else {
                0
            }
        }
        (VmValue::Int(x), VmValue::Float(y)) => {
            let xf = *x as f64;
            if xf < *y {
                -1
            } else if xf > *y {
                1
            } else {
                0
            }
        }
        (VmValue::Float(x), VmValue::Int(y)) => {
            let yf = *y as f64;
            if *x < yf {
                -1
            } else if *x > yf {
                1
            } else {
                0
            }
        }
        (VmValue::String(x), VmValue::String(y)) => match x.as_ref().cmp(y.as_ref()) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        },
        _ => 0,
    }
}

/// Truthiness check
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_is_truthy(val: *const VmValue) -> i64 {
    let val = unsafe { &*val };
    if val.is_truthy() { 1 } else { 0 }
}

// ── String concatenation ──

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_concat(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = VmValue::String(Arc::from(
        format!("{}{}", vmvalue_display(a), vmvalue_display(b)).as_str(),
    ));
    unsafe {
        out.write(result);
    }
}

// ── Constant loading ──

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_load_none(out: *mut VmValue) {
    unsafe {
        out.write(VmValue::None);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_load_true(out: *mut VmValue) {
    unsafe {
        out.write(VmValue::Bool(true));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_load_false(out: *mut VmValue) {
    unsafe {
        out.write(VmValue::Bool(false));
    }
}

/// Load constant from prototype's constant pool into out.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_get_const(proto: *const Prototype, idx: i64, out: *mut VmValue) {
    let proto = unsafe { &*proto };
    let val = match &proto.constants[idx as usize] {
        Constant::Int(n) => VmValue::Int(*n),
        Constant::Float(n) => VmValue::Float(*n),
        Constant::String(s) => VmValue::String(s.clone()),
        Constant::Decimal(s) => {
            use std::str::FromStr;
            VmValue::Decimal(rust_decimal::Decimal::from_str(s).unwrap_or_default())
        }
        Constant::Prototype(p) => VmValue::Function(Arc::new(tl_compiler::value::VmClosure {
            prototype: p.clone(),
            upvalues: Vec::new(),
        })),
        Constant::AstExpr(_) | Constant::AstExprList(_) => VmValue::None,
    };
    unsafe {
        out.write(val);
    }
}

// ── Global access ──

/// Get a global variable by name. Returns 0 on success, 1 if moved.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_get_global(
    ctx: *mut VmContext,
    name_ptr: *const u8,
    name_len: i64,
    out: *mut VmValue,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name_ptr, name_len as usize))
    };
    let val = vm.globals.get(name).cloned().unwrap_or(VmValue::None);
    if matches!(val, VmValue::Moved) {
        return 1; // error: use after move
    }
    unsafe {
        out.write(val);
    }
    0
}

/// Set a global variable by name.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_set_global(
    ctx: *mut VmContext,
    name_ptr: *const u8,
    name_len: i64,
    val: *const VmValue,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name_ptr, name_len as usize))
    };
    let val = unsafe { (*val).clone() };
    vm.globals.insert(name.to_string(), val);
    0
}

// ── Function calls ──

/// Call a TL function (VmValue::Function or VmValue::Builtin).
/// Returns 0 on success, 1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_call(
    ctx: *mut VmContext,
    func: *const VmValue,
    args: *const VmValue,
    nargs: i64,
    out: *mut VmValue,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let func = unsafe { &*func };
    let args_slice = if nargs > 0 {
        unsafe { std::slice::from_raw_parts(args, nargs as usize) }
    } else {
        &[]
    };

    match func {
        VmValue::Function(closure) => {
            // Execute via VM: set up stack and call execute
            let proto = &closure.prototype;
            let base = vm.stack.len();
            let num_regs = proto.num_registers as usize;
            vm.stack.resize(base + num_regs + 1, VmValue::None);
            for (i, arg) in args_slice.iter().enumerate() {
                vm.stack[base + i] = arg.clone();
            }
            match vm.execute(proto) {
                Ok(val) => {
                    unsafe {
                        out.write(val);
                    }
                    0
                }
                Err(_e) => {
                    unsafe {
                        out.write(VmValue::None);
                    }
                    1
                }
            }
        }
        VmValue::Builtin(id) => {
            tl_rt_call_builtin(ctx as *mut VmContext, *id as i64, args, nargs, out)
        }
        _ => {
            unsafe {
                out.write(VmValue::None);
            }
            1
        }
    }
}

/// Call a builtin function by ID.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_call_builtin(
    ctx: *mut VmContext,
    builtin_id: i64,
    args: *const VmValue,
    nargs: i64,
    out: *mut VmValue,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let args_slice = if nargs > 0 {
        unsafe { std::slice::from_raw_parts(args, nargs as usize) }
    } else {
        &[]
    };

    // Push args onto VM stack temporarily, then call_builtin
    let base = vm.stack.len();
    for arg in args_slice {
        vm.stack.push(arg.clone());
    }
    match vm.call_builtin(builtin_id as u8, base, nargs as usize) {
        Ok(val) => {
            vm.stack.truncate(base);
            unsafe {
                out.write(val);
            }
            0
        }
        Err(_e) => {
            vm.stack.truncate(base);
            unsafe {
                out.write(VmValue::None);
            }
            1
        }
    }
}

// ── Data structure construction ──

/// Construct a list from an array of VmValues.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_make_list(vals: *const VmValue, count: i64, out: *mut VmValue) {
    let items = if count > 0 {
        unsafe { std::slice::from_raw_parts(vals, count as usize) }.to_vec()
    } else {
        Vec::new()
    };
    unsafe {
        out.write(VmValue::List(items));
    }
}

/// Construct a map from parallel key/value arrays.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_make_map(
    keys: *const VmValue,
    vals: *const VmValue,
    count: i64,
    out: *mut VmValue,
) {
    let mut pairs = Vec::new();
    for i in 0..count as usize {
        let key = unsafe { &*keys.add(i) };
        let val = unsafe { &*vals.add(i) };
        if let VmValue::String(s) = key {
            pairs.push((s.clone(), val.clone()));
        }
    }
    unsafe {
        out.write(VmValue::Map(pairs));
    }
}

// ── Index access ──

/// Get element by index: val[idx] → out. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_get_index(
    val: *const VmValue,
    idx: *const VmValue,
    out: *mut VmValue,
) -> i64 {
    let val = unsafe { &*val };
    let idx = unsafe { &*idx };
    let result = match (val, idx) {
        (VmValue::List(items), VmValue::Int(i)) => {
            let index = if *i < 0 { items.len() as i64 + i } else { *i } as usize;
            items.get(index).cloned().unwrap_or(VmValue::None)
        }
        (VmValue::Map(pairs), VmValue::String(key)) => pairs
            .iter()
            .find(|(k, _)| k.as_ref() == key.as_ref())
            .map(|(_, v)| v.clone())
            .unwrap_or(VmValue::None),
        (VmValue::String(s), VmValue::Int(i)) => {
            let index = if *i < 0 { s.len() as i64 + i } else { *i } as usize;
            s.chars()
                .nth(index)
                .map(|c| VmValue::String(Arc::from(c.to_string().as_str())))
                .unwrap_or(VmValue::None)
        }
        (VmValue::Ref(inner), _) => {
            return tl_rt_get_index(inner.as_ref() as *const VmValue, idx, out);
        }
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
    0
}

/// Set element by index: val[idx] = new_val. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_set_index(
    val: *mut VmValue,
    idx: *const VmValue,
    new_val: *const VmValue,
) -> i64 {
    let val = unsafe { &mut *val };
    let idx = unsafe { &*idx };
    let new_val = unsafe { (*new_val).clone() };
    match (val, idx) {
        (VmValue::List(items), VmValue::Int(i)) => {
            let index = if *i < 0 { items.len() as i64 + i } else { *i } as usize;
            if index < items.len() {
                items[index] = new_val;
            }
        }
        (VmValue::Map(pairs), VmValue::String(key)) => {
            if let Some(entry) = pairs.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref()) {
                entry.1 = new_val;
            } else {
                pairs.push((key.clone(), new_val));
            }
        }
        (VmValue::Ref(_), _) => return 1, // refs are read-only
        _ => {}
    }
    0
}

// ── Member access ──

/// Get member by name: val.name → out.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_get_member(
    val: *const VmValue,
    name_ptr: *const u8,
    name_len: i64,
    out: *mut VmValue,
) -> i64 {
    let val = unsafe { &*val };
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name_ptr, name_len as usize))
    };
    let result = match val {
        VmValue::StructInstance(inst) => inst
            .fields
            .iter()
            .find(|(k, _)| k.as_ref() == name)
            .map(|(_, v)| v.clone())
            .unwrap_or(VmValue::None),
        VmValue::Map(pairs) => pairs
            .iter()
            .find(|(k, _)| k.as_ref() == name)
            .map(|(_, v)| v.clone())
            .unwrap_or(VmValue::None),
        VmValue::Module(m) => m.exports.get(name).cloned().unwrap_or(VmValue::None),
        VmValue::EnumDef(def) => {
            // Return a function-like value that creates an enum variant
            if def.variants.iter().any(|(n, _)| n.as_ref() == name) {
                VmValue::String(Arc::from(format!("{}::{}", def.name, name).as_str()))
            } else {
                VmValue::None
            }
        }
        VmValue::Ref(inner) => {
            return tl_rt_get_member(inner.as_ref() as *const VmValue, name_ptr, name_len, out);
        }
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
    0
}

/// Set member by name: val.name = new_val.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_set_member(
    val: *mut VmValue,
    name_ptr: *const u8,
    name_len: i64,
    new_val: *const VmValue,
) -> i64 {
    let val = unsafe { &mut *val };
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name_ptr, name_len as usize))
    };
    let new_val = unsafe { (*new_val).clone() };
    match val {
        VmValue::StructInstance(inst) => {
            let inst = Arc::make_mut(inst);
            if let Some(field) = inst.fields.iter_mut().find(|(k, _)| k.as_ref() == name) {
                field.1 = new_val;
            }
        }
        VmValue::Map(pairs) => {
            if let Some(entry) = pairs.iter_mut().find(|(k, _)| k.as_ref() == name) {
                entry.1 = new_val;
            } else {
                pairs.push((Arc::from(name), new_val));
            }
        }
        VmValue::Ref(_) => return 1, // refs are read-only
        _ => {}
    }
    0
}

// ── Method call ──

/// Call a method on an object: obj.name(args...) → out.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_method_call(
    ctx: *mut VmContext,
    obj: *const VmValue,
    name_ptr: *const u8,
    name_len: i64,
    args: *const VmValue,
    nargs: i64,
    out: *mut VmValue,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let obj = unsafe { (*obj).clone() };
    let name = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(name_ptr, name_len as usize))
    };
    let args_slice = if nargs > 0 {
        unsafe { std::slice::from_raw_parts(args, nargs as usize) }.to_vec()
    } else {
        Vec::new()
    };

    match vm.dispatch_method(obj, name, &args_slice) {
        Ok(val) => {
            unsafe {
                out.write(val);
            }
            0
        }
        Err(_e) => {
            unsafe {
                out.write(VmValue::None);
            }
            1
        }
    }
}

// ── Closure construction ──

/// Create a closure from a prototype constant and upvalue data.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_make_closure(
    proto: *const Prototype,
    const_idx: *const VmValue,
    _num_upvalues: i64,
    _regs_base: *const VmValue,
    out: *mut VmValue,
) {
    let _proto = unsafe { &*proto };
    // For now, create a simple closure without upvalues
    // (upvalue capture requires more complex logic)
    let const_val = unsafe { &*const_idx };
    if let VmValue::Function(closure) = const_val {
        unsafe {
            out.write(VmValue::Function(closure.clone()));
        }
    } else {
        unsafe {
            out.write(VmValue::None);
        }
    }
}

// ── VM fallback: execute a single opcode via the VM ──

/// Execute a single opcode on the VM (Tier 3 fallback).
/// Used for complex/domain-specific opcodes that are too complex to emit as LLVM IR.
///
/// regs_base points to the start of the register array for the current function.
/// num_regs is the count of register slots.
/// Returns 0 on success, 1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_vm_exec_op(
    ctx: *mut VmContext,
    opcode: i64,
    a: i64,
    b: i64,
    c: i64,
    regs_base: *mut VmValue,
    num_regs: i64,
) -> i64 {
    let ctx = unsafe { &mut *ctx };
    let vm = unsafe { &mut *ctx.vm };
    let proto = unsafe { &*ctx.prototype };

    // Reconstruct the instruction word
    let inst = ((opcode as u32) << 24)
        | ((a as u32 & 0xFF) << 16)
        | ((b as u32 & 0xFF) << 8)
        | (c as u32 & 0xFF);

    // Copy register values into VM stack for execution
    let base = vm.stack.len();
    let nr = num_regs as usize;
    let regs = unsafe { std::slice::from_raw_parts(regs_base, nr) };
    for reg in regs {
        vm.stack.push(reg.clone());
    }

    // Execute single instruction via VM
    let result = vm.execute_single_instruction(inst, proto, base);

    // Copy modified registers back
    for i in 0..nr {
        let val = vm.stack[base + i].clone();
        unsafe {
            regs_base.add(i).write(val);
        }
    }
    vm.stack.truncate(base);

    match result {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

// ── Register copy (memcpy-like) ──

/// Copy a VmValue from src to dst (register move).
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_move_value(src: *const VmValue, dst: *mut VmValue) {
    let val = unsafe { (*src).clone() };
    unsafe {
        dst.write(val);
    }
}

// ── Internal helpers ──

fn vmvalue_eq(a: &VmValue, b: &VmValue) -> bool {
    match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => x == y,
        (VmValue::Float(x), VmValue::Float(y)) => x == y,
        (VmValue::Int(x), VmValue::Float(y)) => (*x as f64) == *y,
        (VmValue::Float(x), VmValue::Int(y)) => *x == (*y as f64),
        (VmValue::String(x), VmValue::String(y)) => x == y,
        (VmValue::Bool(x), VmValue::Bool(y)) => x == y,
        (VmValue::None, VmValue::None) => true,
        (VmValue::Decimal(x), VmValue::Decimal(y)) => x == y,
        _ => false,
    }
}

fn vmvalue_display(val: &VmValue) -> String {
    match val {
        VmValue::Int(n) => n.to_string(),
        VmValue::Float(n) => format!("{n}"),
        VmValue::String(s) => s.to_string(),
        VmValue::Bool(b) => b.to_string(),
        VmValue::None => "none".to_string(),
        VmValue::Decimal(d) => d.to_string(),
        _ => format!("{val}"),
    }
}
