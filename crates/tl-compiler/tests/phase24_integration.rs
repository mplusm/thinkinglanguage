// Phase 24: Async/Await & Runtime — Integration Tests
// Tests parse → compile → VM pipeline for async fn, async builtins

use tl_compiler::{compile, Vm};
use tl_parser::parse;

/// Helper: parse + compile + run, return VM output lines
fn run_output(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

/// Helper: parse + compile + run, expect runtime error
fn run_err(src: &str) -> String {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    match vm.execute(&proto) {
        Err(e) => format!("{e}"),
        Ok(_) => "no error".to_string(),
    }
}

// ── Async Function Declaration ─────────────────────────────────────

#[test]
fn test_e2e_async_fn_parse_compile() {
    // Async functions should parse and compile without error
    let output = run_output(r#"
async fn fetch_data() {
    return "data"
}
print("parsed ok")
"#);
    assert_eq!(output, vec!["parsed ok"]);
}

#[test]
fn test_e2e_async_fn_with_params() {
    let output = run_output(r#"
async fn greet(name) {
    return "hello " + name
}
print("defined ok")
"#);
    assert_eq!(output, vec!["defined ok"]);
}

// ── Async Builtins (Stub Tests) ────────────────────────────────────

#[test]
fn test_e2e_async_read_file_requires_feature() {
    let err = run_err(r#"
let t = async_read_file("test.txt")
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_async_write_file_requires_feature() {
    let err = run_err(r#"
let t = async_write_file("test.txt", "content")
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_async_http_get_requires_feature() {
    let err = run_err(r#"
let t = async_http_get("https://example.com")
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_async_sleep_requires_feature() {
    let err = run_err(r#"
let t = async_sleep(100)
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_select_requires_feature() {
    let err = run_err(r#"
let t = select()
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_race_all_requires_feature() {
    let err = run_err(r#"
let t = race_all([])
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_async_map_requires_feature() {
    let err = run_err(r#"
let t = async_map([], (x) => x)
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}

#[test]
fn test_e2e_async_filter_requires_feature() {
    let err = run_err(r#"
let t = async_filter([], (x) => true)
"#);
    assert!(err.contains("async"), "Expected async feature error, got: {err}");
}
