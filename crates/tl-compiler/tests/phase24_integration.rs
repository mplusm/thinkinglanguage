// Phase 24/25: Async/Await & Runtime — Integration Tests
// Tests parse → compile → VM pipeline for async fn, async builtins

use tl_compiler::{Vm, compile};
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
    let output = run_output(
        r#"
async fn fetch_data() {
    return "data"
}
print("parsed ok")
"#,
    );
    assert_eq!(output, vec!["parsed ok"]);
}

#[test]
fn test_e2e_async_fn_with_params() {
    let output = run_output(
        r#"
async fn greet(name) {
    return "hello " + name
}
print("defined ok")
"#,
    );
    assert_eq!(output, vec!["defined ok"]);
}

// ── Async Builtins (Stub Tests — only when async-runtime is NOT enabled) ──

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_read_file_requires_feature() {
    let err = run_err(
        r#"
let t = async_read_file("test.txt")
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_write_file_requires_feature() {
    let err = run_err(
        r#"
let t = async_write_file("test.txt", "content")
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_http_get_requires_feature() {
    let err = run_err(
        r#"
let t = async_http_get("https://example.com")
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_sleep_requires_feature() {
    let err = run_err(
        r#"
let t = async_sleep(100)
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_select_requires_feature() {
    let err = run_err(
        r#"
let t = select()
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_race_all_requires_feature() {
    let err = run_err(
        r#"
let t = race_all([])
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_map_requires_feature() {
    let err = run_err(
        r#"
let t = async_map([], (x) => x)
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

#[cfg(not(feature = "async-runtime"))]
#[test]
fn test_e2e_async_filter_requires_feature() {
    let err = run_err(
        r#"
let t = async_filter([], (x) => true)
"#,
    );
    assert!(
        err.contains("async"),
        "Expected async feature error, got: {err}"
    );
}

// ── Phase 25: Real Async Tests (only when async-runtime IS enabled) ──

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e2e_async.txt");
    let path_str = path.to_str().unwrap().replace('\\', "/");
    let source = format!(
        r#"let wt = async_write_file("{path_str}", "integration test")
let wr = await(wt)
let rt = async_read_file("{path_str}")
let content = await(rt)
print(content)"#
    );
    let output = run_output(&source);
    assert_eq!(output, vec!["integration test"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_sleep_completes() {
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
fn test_e2e_select_two_sleeps() {
    let source = r#"
let fast = async_sleep(10)
let slow = async_sleep(5000)
let winner = select(fast, slow)
let r = await(winner)
print(r)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["none"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_race_all_sleeps() {
    let source = r#"
let t1 = async_sleep(10)
let t2 = async_sleep(5000)
let winner = race_all([t1, t2])
let r = await(winner)
print(r)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["none"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_map_multiply() {
    let source = r#"
let t = async_map([10, 20, 30], (x) => x * 2)
let result = await(t)
print(result)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["[20, 40, 60]"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_filter_evens() {
    let source = r#"
let t = async_filter([1, 2, 3, 4, 5, 6], (x) => x % 2 == 0)
let result = await(t)
print(result)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["[2, 4, 6]"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_map_empty() {
    let source = r#"
let t = async_map([], (x) => x)
let result = await(t)
print(result)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["[]"]);
}

#[cfg(feature = "async-runtime")]
#[test]
fn test_e2e_async_filter_all_rejected() {
    let source = r#"
let t = async_filter([1, 2, 3], (x) => false)
let result = await(t)
print(result)
"#;
    let output = run_output(source);
    assert_eq!(output, vec!["[]"]);
}
