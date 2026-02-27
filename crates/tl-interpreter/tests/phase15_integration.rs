// Phase 15: Data Quality & Connectors — Interpreter Integration Tests

use tl_interpreter::{Interpreter, Value};
use tl_parser::parse;

fn run(source: &str) -> Result<Value, tl_errors::TlError> {
    let program = parse(source)?;
    let mut interp = Interpreter::new();
    interp.execute(&program)
}

fn run_output(source: &str) -> Vec<String> {
    let program = parse(source).unwrap();
    let mut interp = Interpreter::new();
    interp.execute(&program).unwrap();
    interp.output
}

fn assert_bool(val: Value, expected: bool) {
    match val {
        Value::Bool(b) => assert_eq!(b, expected),
        _ => panic!("Expected Bool({expected}), got {val}"),
    }
}

fn assert_int(val: Value, expected: i64) {
    match val {
        Value::Int(n) => assert_eq!(n, expected),
        _ => panic!("Expected Int({expected}), got {val}"),
    }
}

// ── Validation builtins ──

#[test]
fn test_interp_is_email() {
    assert_bool(run(r#"is_email("user@example.com")"#).unwrap(), true);
    assert_bool(run(r#"is_email("nope")"#).unwrap(), false);
}

#[test]
fn test_interp_is_url() {
    assert_bool(run(r#"is_url("https://example.com")"#).unwrap(), true);
}

#[test]
fn test_interp_is_phone() {
    assert_bool(run(r#"is_phone("+1-555-555-5555")"#).unwrap(), true);
}

#[test]
fn test_interp_is_between() {
    assert_bool(run("is_between(5, 1, 10)").unwrap(), true);
}

// ── Fuzzy matching ──

#[test]
fn test_interp_levenshtein() {
    assert_int(run(r#"levenshtein("kitten", "sitting")"#).unwrap(), 3);
}

#[test]
fn test_interp_soundex() {
    let output = run_output(r#"print(soundex("Robert"))"#);
    assert_eq!(output, vec!["R163"]);
}

// ── Data quality via pipe ──

#[test]
fn test_interp_fill_null_pipe() {
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
fn test_interp_drop_null_pipe() {
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
fn test_interp_dedup_pipe() {
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
fn test_interp_clamp_pipe() {
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
fn test_interp_row_count() {
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
fn test_interp_is_unique() {
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
fn test_interp_graphql_query_error() {
    let result = run(r#"graphql_query("http://localhost:99999/graphql", "{ users { id } }")"#);
    assert!(result.is_err());
}
