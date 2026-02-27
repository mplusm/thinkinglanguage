// Phase 13: Semantic Analysis & Optimization — Integration Tests
// Tests constant folding, dead code elimination, and runtime behavior

use tl_compiler::{compile, compile_with_source, Vm, VmValue};
use tl_parser::parse;

/// Helper: parse + compile + run, return result
fn run(src: &str) -> Result<VmValue, String> {
    let program = parse(src).map_err(|e| format!("Parse error: {e}"))?;
    let proto = compile(&program).map_err(|e| format!("Compile error: {e}"))?;
    let mut vm = Vm::new();
    vm.execute(&proto).map_err(|e| format!("Runtime error: {e}"))
}

/// Helper: parse + compile with source, return disassembled bytecode
fn disasm(src: &str) -> String {
    let program = parse(src).unwrap();
    let proto = compile_with_source(&program, src).unwrap();
    proto.disassemble()
}

/// Assert a VmValue is an Int with the expected value
fn assert_int(val: &VmValue, expected: i64) {
    if let VmValue::Int(n) = val {
        assert_eq!(*n, expected, "Expected Int({expected}), got Int({n})");
    } else {
        panic!("Expected Int({expected}), got {val}");
    }
}

/// Assert a VmValue is a Bool with the expected value
fn assert_bool(val: &VmValue, expected: bool) {
    if let VmValue::Bool(b) = val {
        assert_eq!(*b, expected, "Expected Bool({expected}), got Bool({b})");
    } else {
        panic!("Expected Bool({expected}), got {val}");
    }
}

// ── Constant Folding ────────────────────────────────────────────────

#[test]
fn test_constant_folding_arithmetic() {
    // `2 + 3 * 4` should fold to `14` at compile time
    let bytecode = disasm("let x = 2 + 3 * 4\nprint(x)");
    assert!(bytecode.contains("14"), "Should fold 2 + 3 * 4 to 14");
    assert!(!bytecode.contains("Mul"), "Should not have Mul instruction");
    assert!(!bytecode.contains(" Add"), "Should not have Add instruction");

    let val = run("2 + 3 * 4").unwrap();
    assert_int(&val, 14);
}

#[test]
fn test_constant_folding_string_concat() {
    let bytecode = disasm("let x = \"hello\" + \" world\"\nprint(x)");
    assert!(bytecode.contains("hello world"), "Should fold string concatenation");
}

#[test]
fn test_constant_folding_boolean() {
    let val = run("not true").unwrap();
    assert_bool(&val, false);

    let val = run("true and false").unwrap();
    assert_bool(&val, false);
}

#[test]
fn test_constant_folding_does_not_fold_variables() {
    let bytecode = disasm("let x = 5\nlet y = x + 1\nprint(y)");
    assert!(bytecode.contains("Add"), "Should have Add when variable is involved");
}

#[test]
fn test_constant_folding_nested() {
    // Nested constant expressions should fold recursively
    let val = run("(1 + 2) * (3 + 4)").unwrap();
    assert_int(&val, 21);

    let bytecode = disasm("let x = (1 + 2) * (3 + 4)\nprint(x)");
    assert!(bytecode.contains("21"), "Should fold nested constants to 21");
}

#[test]
fn test_constant_folding_comparison() {
    let val = run("10 > 5").unwrap();
    assert_bool(&val, true);

    let val = run("3 == 3").unwrap();
    assert_bool(&val, true);
}

// ── Dead Code Elimination ────────────────────────────────────────────

#[test]
fn test_dce_after_return() {
    let bytecode = disasm(r#"
fn foo() {
    return 1
    print("unreachable")
}
print(foo())
"#);
    // The function's sub-prototype should NOT contain "unreachable"
    // Find the foo function's disassembly section
    let foo_section = bytecode.split("=== foo ===").nth(1).unwrap_or("");
    assert!(!foo_section.contains("unreachable"), "Should not compile code after return");

    let val = run(r#"
fn foo() {
    return 1
    print("unreachable")
}
foo()
"#).unwrap();
    assert_int(&val, 1);
}

#[test]
fn test_dce_after_break() {
    let val = run(r#"
let mut result = 0
for i in range(10) {
    if i == 5 {
        break
    }
    result = result + 1
}
result
"#).unwrap();
    assert_int(&val, 5);
}

#[test]
fn test_dce_if_both_branches_return() {
    // If both branches return, code after if is dead
    let val = run(r#"
fn classify(x: int) -> string {
    if x > 0 {
        return "positive"
    } else {
        return "non-positive"
    }
}
classify(42)
"#).unwrap();
    assert!(matches!(val, VmValue::String(ref s) if s.as_ref() == "positive"));
}

// ── Struct + Methods + Optimization Together ────────────────────────

#[test]
fn test_complex_struct_with_constant_folding() {
    // Struct creation + field access + constant folding together
    let val = run(r#"
struct Point { x: int, y: int }
let offset = 10 + 20 + 30
let p = Point { x: 1 + 2, y: 3 + 4 }
p.x + p.y + offset
"#).unwrap();
    // p.x = 3, p.y = 7 (constant folded), offset = 60 (constant folded), total = 70
    assert_int(&val, 70);
}

// ── Backward Compatibility ────────────────────────────────────────────

#[test]
fn test_backward_compat_arithmetic() {
    let val = run("1 + 2 * 3").unwrap();
    assert_int(&val, 7);
}

#[test]
fn test_backward_compat_strings() {
    let val = run(r#"let s = "hello"
len(s)"#).unwrap();
    assert_int(&val, 5);
}

#[test]
fn test_backward_compat_lists() {
    let val = run("let xs = [1, 2, 3]\nlen(xs)").unwrap();
    assert_int(&val, 3);
}

#[test]
fn test_backward_compat_functions() {
    let val = run("fn add(a, b) { a + b }\nadd(1, 2)").unwrap();
    assert_int(&val, 3);
}

#[test]
fn test_backward_compat_if_else() {
    let val = run("if true { 1 } else { 2 }").unwrap();
    assert_int(&val, 1);
}

#[test]
fn test_backward_compat_for_loop() {
    let val = run("let mut sum = 0\nfor i in range(5) { sum = sum + i }\nsum").unwrap();
    assert_int(&val, 10);
}

#[test]
fn test_backward_compat_match() {
    let val = run("match 42 { 42 => true, _ => false }").unwrap();
    assert_bool(&val, true);
}

#[test]
fn test_backward_compat_closures() {
    let val = run("let f = (x) => x * 2\nf(21)").unwrap();
    assert_int(&val, 42);
}

#[test]
fn test_backward_compat_while_loop() {
    let val = run(r#"
let mut i = 0
while i < 10 {
    i = i + 1
}
i
"#).unwrap();
    assert_int(&val, 10);
}

#[test]
fn test_backward_compat_try_catch() {
    let val = run(r#"
let mut result = 0
try {
    throw "error"
    result = 999
} catch e {
    result = 42
}
result
"#).unwrap();
    assert_int(&val, 42);
}
