// ThinkingLanguage — JIT Runtime Helpers
// extern "C" functions callable from Cranelift-generated native code.
// These handle dynamic type dispatch that the JIT can't do statically.

use crate::value::VmValue;
use std::sync::Arc;

/// Runtime addition: handles int+int, float+float, int+float, string+string
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
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

/// Runtime subtraction
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_sub(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x - y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x - y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 - y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x - *y as f64),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

/// Runtime multiplication
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_mul(a: *const VmValue, b: *const VmValue, out: *mut VmValue) {
    let a = unsafe { &*a };
    let b = unsafe { &*b };
    let result = match (a, b) {
        (VmValue::Int(x), VmValue::Int(y)) => VmValue::Int(x * y),
        (VmValue::Float(x), VmValue::Float(y)) => VmValue::Float(x * y),
        (VmValue::Int(x), VmValue::Float(y)) => VmValue::Float(*x as f64 * y),
        (VmValue::Float(x), VmValue::Int(y)) => VmValue::Float(x * *y as f64),
        _ => VmValue::None,
    };
    unsafe {
        out.write(result);
    }
}

/// Runtime comparison: returns -1, 0, or 1
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
                -1.0 as i64
            } else if x > y {
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Runtime truthiness check
#[unsafe(no_mangle)]
pub extern "C" fn tl_rt_is_truthy(val: *const VmValue) -> i64 {
    let val = unsafe { &*val };
    if val.is_truthy() { 1 } else { 0 }
}
