// Phase 20: Python FFI — VM Integration Tests
#![cfg(feature = "python")]

use tl_compiler::{compile, Vm, VmValue};
use tl_parser::parse;

fn run(src: &str) -> Result<VmValue, String> {
    let program = parse(src).map_err(|e| format!("Parse error: {e}"))?;
    let proto = compile(&program).map_err(|e| format!("Compile error: {e}"))?;
    let mut vm = Vm::new();
    vm.execute(&proto).map_err(|e| format!("Runtime error: {e}"))
}

fn run_output(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

// --- E2E Tests: Full parse → compile → VM pipeline ---

#[test]
fn test_e2e_math_sqrt() {
    let output = run_output(r#"
        let m = py_import("math")
        let result = m.sqrt(16.0)
        print(result)
    "#);
    assert_eq!(output, vec!["4.0"]);
}

#[test]
fn test_e2e_math_pi() {
    let output = run_output(r#"
        let m = py_import("math")
        print(m.pi)
    "#);
    assert_eq!(output.len(), 1);
    let pi: f64 = output[0].parse().unwrap();
    assert!((pi - std::f64::consts::PI).abs() < 1e-10);
}

#[test]
fn test_e2e_json_module() {
    let output = run_output(r#"
        let json = py_import("json")
        let data = json.dumps([1, 2, 3])
        print(data)
    "#);
    assert_eq!(output, vec!["[1, 2, 3]"]);
}

#[test]
fn test_e2e_os_path() {
    let output = run_output(r#"
        let os = py_import("os")
        let sep = py_getattr(os, "sep")
        print(sep)
    "#);
    assert_eq!(output, vec!["/"]);
}

#[test]
fn test_e2e_list_roundtrip() {
    let output = run_output(r#"
        let result = py_eval("[1, 2, 3]")
        print(result)
    "#);
    assert_eq!(output, vec!["[1, 2, 3]"]);
}

#[test]
fn test_e2e_dict_roundtrip() {
    // Note: can't use {} in TL strings (triggers interpolation), so build dict via Python
    let output = run_output(r#"
        let result = py_eval("dict(x=42)")
        print(result)
    "#);
    // Map display: {x: 42}
    assert_eq!(output.len(), 1);
    assert!(output[0].contains("x") && output[0].contains("42"));
}

#[test]
fn test_e2e_nested_attr() {
    let output = run_output(r#"
        let os = py_import("os")
        let path = py_getattr(os, "path")
        let sep = py_getattr(path, "sep")
        print(sep)
    "#);
    assert_eq!(output, vec!["/"]);
}

#[test]
fn test_e2e_py_exception_msg() {
    let result = run(r#"
        py_eval("1/0")
    "#);
    assert!(result.is_err());
    let msg = result.unwrap_err();
    assert!(msg.contains("py_eval()") || msg.contains("ZeroDivision"));
}

#[test]
fn test_e2e_py_object_pass() {
    let output = run_output(r#"
        let m = py_import("math")
        fn get_pi(module) {
            return py_getattr(module, "pi")
        }
        let pi = get_pi(m)
        print(pi)
    "#);
    assert_eq!(output.len(), 1);
    let pi: f64 = output[0].parse().unwrap();
    assert!((pi - std::f64::consts::PI).abs() < 1e-10);
}

#[test]
fn test_e2e_py_callback_args() {
    let output = run_output(r#"
        let m = py_import("math")
        let pow_fn = py_getattr(m, "pow")
        let result = py_call(pow_fn, 2.0, 10.0)
        print(result)
    "#);
    assert_eq!(output, vec!["1024.0"]);
}

#[test]
fn test_e2e_py_import_caching() {
    // Importing same module twice should work
    let output = run_output(r#"
        let m1 = py_import("math")
        let m2 = py_import("math")
        let p1 = py_getattr(m1, "pi")
        let p2 = py_getattr(m2, "pi")
        print(p1 == p2)
    "#);
    assert_eq!(output, vec!["true"]);
}

#[test]
fn test_e2e_multiline_eval() {
    let output = run_output(r#"
        let result = py_eval("sum([i**2 for i in range(5)])")
        print(result)
    "#);
    // 0^2 + 1^2 + 2^2 + 3^2 + 4^2 = 0 + 1 + 4 + 9 + 16 = 30
    assert_eq!(output, vec!["30"]);
}

// --- VM-level tests ---

#[test]
fn test_vm_py_import_math() {
    let result = run(r#"let m = py_import("math")"#).unwrap();
    // Should succeed (result is the last expression value or None)
    let _ = result; // Just check no error
}

#[test]
fn test_vm_py_eval_basic() {
    let output = run_output(r#"
        let x = py_eval("42")
        print(x)
    "#);
    assert_eq!(output, vec!["42"]);
}

#[test]
fn test_vm_py_getattr_pi() {
    let output = run_output(r#"
        let m = py_import("math")
        let pi = py_getattr(m, "pi")
        print(pi)
    "#);
    assert_eq!(output.len(), 1);
    let pi: f64 = output[0].parse().unwrap();
    assert!((pi - std::f64::consts::PI).abs() < 1e-10);
}

#[test]
fn test_vm_py_call_function() {
    let output = run_output(r#"
        let m = py_import("math")
        let sqrt_fn = py_getattr(m, "sqrt")
        let result = py_call(sqrt_fn, 16.0)
        print(result)
    "#);
    assert_eq!(output, vec!["4.0"]);
}

#[test]
fn test_vm_py_to_tl() {
    let output = run_output(r#"
        let m = py_import("math")
        let pi_obj = py_getattr(m, "pi")
        let pi = py_to_tl(pi_obj)
        print(pi)
    "#);
    assert_eq!(output.len(), 1);
    let pi: f64 = output[0].parse().unwrap();
    assert!((pi - std::f64::consts::PI).abs() < 1e-10);
}

#[test]
fn test_vm_py_member_access() {
    // Test GetMember opcode for PyObject
    let output = run_output(r#"
        let m = py_import("math")
        let pi = m.pi
        print(pi)
    "#);
    assert_eq!(output.len(), 1);
    let pi: f64 = output[0].parse().unwrap();
    assert!((pi - std::f64::consts::PI).abs() < 1e-10);
}

#[test]
fn test_vm_py_method_call() {
    // Test MethodCall opcode for PyObject
    let output = run_output(r#"
        let m = py_import("math")
        let result = m.sqrt(16.0)
        print(result)
    "#);
    assert_eq!(output, vec!["4.0"]);
}

#[test]
fn test_vm_py_chained() {
    // Test chained attribute access via py_getattr
    let output = run_output(r#"
        let os = py_import("os")
        let path = py_getattr(os, "path")
        let result = path.join("a", "b")
        print(result)
    "#);
    assert_eq!(output, vec!["a/b"]);
}

#[test]
fn test_vm_py_error_propagation() {
    let result = run(r#"
        let m = py_import("nonexistent_module_xyz")
    "#);
    assert!(result.is_err());
}

#[test]
fn test_vm_py_none_result() {
    let output = run_output(r#"
        let result = py_eval("None")
        print(result)
    "#);
    assert_eq!(output, vec!["none"]);
}

#[test]
fn test_vm_py_print() {
    // Printing a PyObject should show repr
    let output = run_output(r#"
        let m = py_import("math")
        print(type_of(m))
    "#);
    assert_eq!(output, vec!["pyobject"]);
}

#[test]
fn test_vm_py_setattr() {
    // Create a simple Python object and set an attribute on it
    let output = run_output(r#"
        let types = py_import("types")
        let obj = py_call(py_getattr(types, "SimpleNamespace"))
        py_setattr(obj, "x", 42)
        let x = py_getattr(obj, "x")
        print(x)
    "#);
    assert_eq!(output, vec!["42"]);
}

#[test]
fn test_vm_py_bool_conversion() {
    let output = run_output(r#"
        let result = py_eval("True")
        print(result)
    "#);
    assert_eq!(output, vec!["true"]);
}

#[test]
fn test_vm_py_set_conversion() {
    // Note: can't use {} in TL strings (triggers interpolation), so use set()
    let output = run_output(r#"
        let result = py_eval("len(set([1, 2, 3]))")
        print(result)
    "#);
    assert_eq!(output, vec!["3"]);
}
