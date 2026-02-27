// Phase 15: Data Quality & Connectors — VM Integration Tests

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

fn assert_bool(val: &VmValue, expected: bool) {
    if let VmValue::Bool(b) = val {
        assert_eq!(*b, expected, "Expected Bool({expected}), got Bool({b})");
    } else {
        panic!("Expected Bool({expected}), got {val}");
    }
}

fn assert_int(val: &VmValue, expected: i64) {
    if let VmValue::Int(n) = val {
        assert_eq!(*n, expected, "Expected Int({expected}), got Int({n})");
    } else {
        panic!("Expected Int({expected}), got {val}");
    }
}

// ── Validation builtins ──

#[test]
fn test_vm_is_email() {
    assert_bool(&run(r#"is_email("user@example.com")"#).unwrap(), true);
    assert_bool(&run(r#"is_email("not-email")"#).unwrap(), false);
}

#[test]
fn test_vm_is_url() {
    assert_bool(&run(r#"is_url("https://example.com")"#).unwrap(), true);
    assert_bool(&run(r#"is_url("not a url")"#).unwrap(), false);
}

#[test]
fn test_vm_is_phone() {
    assert_bool(&run(r#"is_phone("+1-555-555-5555")"#).unwrap(), true);
    assert_bool(&run(r#"is_phone("abc")"#).unwrap(), false);
}

#[test]
fn test_vm_is_between() {
    assert_bool(&run("is_between(5, 1, 10)").unwrap(), true);
    assert_bool(&run("is_between(15, 1, 10)").unwrap(), false);
}

// ── Fuzzy matching ──

#[test]
fn test_vm_levenshtein() {
    assert_int(&run(r#"levenshtein("kitten", "sitting")"#).unwrap(), 3);
}

#[test]
fn test_vm_soundex() {
    let output = run_output(r#"print(soundex("Robert"))"#);
    assert_eq!(output, vec!["R163"]);
}

// ── Data quality via pipe ──

#[test]
fn test_vm_fill_null_pipe() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,\n3,Charlie,25\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let result = data |> fill_null("age", 0)
let count = row_count(result)
print(count)"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["3"]);
}

#[test]
fn test_vm_drop_null_pipe() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,,25\n3,Charlie,35\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let result = data |> drop_null("name")
let count = row_count(result)
print(count)"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["2"]);
}

#[test]
fn test_vm_dedup_pipe() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,val\n1,a\n2,b\n2,b\n3,c\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let result = data |> dedup()
let count = row_count(result)
print(count)"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["3"]);
}

#[test]
fn test_vm_clamp_pipe() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,score\n1,10\n2,50\n3,90\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let result = data |> clamp("score", 20, 80)
print(row_count(result))"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["3"]);
}

// ── Data profile & validation ──

#[test]
fn test_vm_data_profile() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,val\n1,10\n2,20\n3,30\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let profile = data |> data_profile()
print(row_count(profile))"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    let count: i64 = output[0].parse().unwrap();
    assert!(count >= 2);
}

#[test]
fn test_vm_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,val\n1,a\n2,b\n3,c\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
print(data |> row_count())"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["3"]);
}

#[test]
fn test_vm_null_rate() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,\n3,Charlie\n4,\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let rate = data |> null_rate("name")
print(rate)"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert!(!output.is_empty());
}

#[test]
fn test_vm_is_unique() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,val\n1,a\n2,b\n3,c\n").unwrap();
    let src = format!(
        r#"let data = read_csv("{}")
let unique = data |> is_unique("id")
print(unique)"#,
        csv_path.to_str().unwrap()
    );
    let output = run_output(&src);
    assert_eq!(output, vec!["true"]);
}

// ── GraphQL error case ──

#[test]
fn test_vm_graphql_query_error() {
    let result = run(r#"graphql_query("http://localhost:99999/graphql", "{ users { id } }")"#);
    assert!(result.is_err());
}
