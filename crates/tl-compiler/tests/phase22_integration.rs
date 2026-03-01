// Phase 22: Advanced Type System — Integration Tests
// Tests parse → compile → VM pipeline for Decimal, new types, inference

use tl_compiler::{Vm, VmValue, compile};
use tl_parser::parse;

/// Helper: parse + compile + run, return VM output lines
fn run_output(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

/// Helper: parse + compile + run, return result
fn run(src: &str) -> Result<VmValue, String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).map_err(|e| format!("{e}"))
}

// ── Decimal Literal ────────────────────────────────────────────────

#[test]
fn test_e2e_decimal_literal() {
    let output = run_output(
        r#"
let x = 3.14d
print(x)
"#,
    );
    assert_eq!(output, vec!["3.14"]);
}

#[test]
fn test_e2e_decimal_arithmetic() {
    let output = run_output(
        r#"
let a = 10.50d
let b = 3.25d
print(a + b)
print(a - b)
print(a * b)
"#,
    );
    assert_eq!(output, vec!["13.75", "7.25", "34.1250"]);
}

#[test]
fn test_e2e_decimal_int_promotion() {
    let output = run_output(
        r#"
let d = 5.5d
let i = 2
print(d + i)
print(i + d)
"#,
    );
    assert_eq!(output, vec!["7.5", "7.5"]);
}

#[test]
fn test_e2e_decimal_comparison() {
    let output = run_output(
        r#"
let a = 1.5d
let b = 2.5d
print(a < b)
print(a == a)
print(a != b)
"#,
    );
    assert_eq!(output, vec!["true", "true", "true"]);
}

#[test]
fn test_e2e_decimal_negation() {
    let output = run_output(
        r#"
let x = 5.0d
print(-x)
"#,
    );
    assert_eq!(output, vec!["-5.0"]);
}

#[test]
fn test_e2e_decimal_builtin() {
    let output = run_output(
        r#"
let x = decimal("123.456")
print(x)
let y = decimal(42)
print(y)
"#,
    );
    assert_eq!(output, vec!["123.456", "42"]);
}

#[test]
fn test_e2e_decimal_type_of() {
    let output = run_output(
        r#"
let x = 1.0d
print(type_of(x))
"#,
    );
    assert_eq!(output, vec!["decimal"]);
}

#[test]
fn test_e2e_decimal_division() {
    let output = run_output(
        r#"
let a = 10.0d
let b = 3.0d
let c = a / b
print(type_of(c))
"#,
    );
    assert_eq!(output, vec!["decimal"]);
}

#[test]
fn test_e2e_decimal_float_mixed() {
    let output = run_output(
        r#"
let d = 5.5d
let f = 2.0
let result = d + f
print(type_of(result))
"#,
    );
    // Decimal + Float promotes to Float
    assert_eq!(output, vec!["float64"]);
}

#[test]
fn test_e2e_decimal_in_list() {
    let output = run_output(
        r#"
let prices = [1.99d, 2.49d, 3.99d]
let total = prices[0] + prices[1] + prices[2]
print(total)
"#,
    );
    assert_eq!(output, vec!["8.47"]);
}
