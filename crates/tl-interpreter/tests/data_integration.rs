use std::fs;
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

#[test]
fn test_schema_definition() {
    let result = run(
        r#"
        schema User {
            id: int64,
            name: string,
            age: float64
        }
        type_of(User)
    "#,
    )
    .unwrap();
    assert!(matches!(result, Value::String(s) if s == "schema"));
}

#[test]
fn test_read_csv() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        type_of(t)"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let result = run(&source).unwrap();
    assert!(matches!(result, Value::String(s) if s == "table"));
}

#[test]
fn test_filter_table() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> filter(age > 28) |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("Alice"));
    assert!(table_output.contains("Charlie"));
    assert!(!table_output.contains("Bob"));
}

#[test]
fn test_select_columns() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> select(name, age) |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("name"));
    assert!(table_output.contains("age"));
    // id should NOT be in output since we only selected name and age
    assert!(!table_output.contains("| id"));
}

#[test]
fn test_sort_table() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> sort(age) |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    // Bob (25) should appear before Alice (30) before Charlie (35)
    let bob_pos = table_output.find("Bob").unwrap();
    let alice_pos = table_output.find("Alice").unwrap();
    let charlie_pos = table_output.find("Charlie").unwrap();
    assert!(bob_pos < alice_pos);
    assert!(alice_pos < charlie_pos);
}

#[test]
fn test_with_derived_column() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> with {{ doubled = age * 2 }} |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("doubled"));
    assert!(table_output.contains("60")); // Alice: 30 * 2
    assert!(table_output.contains("50")); // Bob: 25 * 2
}

#[test]
fn test_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(
        &csv_path,
        "id,name,dept,salary\n1,Alice,Eng,100\n2,Bob,Eng,120\n3,Charlie,Sales,90\n",
    )
    .unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> aggregate(by: dept, total: sum(salary), n: count()) |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("Eng"));
    assert!(table_output.contains("Sales"));
    assert!(table_output.contains("220")); // Eng total: 100 + 120
}

#[test]
fn test_head_limit() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(
        &csv_path,
        "id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n",
    )
    .unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> head(2) |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    // Should only have 2 rows
    assert!(table_output.contains("A"));
    assert!(table_output.contains("B"));
    // C, D, E should not appear
    assert!(!table_output.contains("| C"));
}

#[test]
fn test_join_tables() {
    let dir = tempfile::tempdir().unwrap();
    let users_path = dir.path().join("users.csv");
    let orders_path = dir.path().join("orders.csv");
    fs::write(&users_path, "id,name\n1,Alice\n2,Bob\n").unwrap();
    fs::write(&orders_path, "order_id,user_id,amount\n101,1,50\n102,1,30\n103,2,40\n").unwrap();

    let source = format!(
        r#"let users = read_csv("{}")
        let orders = read_csv("{}")
        users |> join(orders, on: id == user_id) |> show()"#,
        users_path.to_str().unwrap().replace('\\', "\\\\"),
        orders_path.to_str().unwrap().replace('\\', "\\\\"),
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("Alice"));
    assert!(table_output.contains("Bob"));
}

#[test]
fn test_describe_table() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> describe()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    assert!(output[0].contains("id"));
    assert!(output[0].contains("name"));
    assert!(output[0].contains("age"));
}

#[test]
fn test_chained_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(
        &csv_path,
        "id,name,age,dept\n1,Alice,30,Eng\n2,Bob,25,Eng\n3,Charlie,35,Sales\n4,Diana,28,Eng\n",
    )
    .unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t
            |> filter(dept == "Eng")
            |> filter(age > 26)
            |> select(name, age)
            |> sort(age, "desc")
            |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    let table_output = &output[0];
    assert!(table_output.contains("Alice"));
    assert!(table_output.contains("Diana"));
    assert!(!table_output.contains("Bob")); // age 25, filtered out
    assert!(!table_output.contains("Charlie")); // Sales dept, filtered out
}

#[test]
fn test_print_table() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        print(t)"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\")
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    assert!(output[0].contains("Alice"));
    assert!(output[0].contains("Bob"));
}

#[test]
fn test_parquet_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test.csv");
    let parquet_dir = dir.path().join("output_parquet");
    fs::write(&csv_path, "id,name,age\n1,Alice,30\n2,Bob,25\n").unwrap();

    let source = format!(
        r#"let t = read_csv("{}")
        t |> write_parquet("{}")
        let t2 = read_parquet("{}")
        t2 |> show()"#,
        csv_path.to_str().unwrap().replace('\\', "\\\\"),
        parquet_dir.to_str().unwrap().replace('\\', "\\\\"),
        parquet_dir.to_str().unwrap().replace('\\', "\\\\"),
    );
    let output = run_output(&source);
    assert!(!output.is_empty());
    assert!(output[0].contains("Alice"));
    assert!(output[0].contains("Bob"));
}

#[test]
fn test_existing_features_unchanged() {
    // Verify that existing Phase 0 features still work
    let result = run("1 + 2 * 3").unwrap();
    assert!(matches!(result, Value::Int(7)));

    let output = run_output("let x = [1, 2, 3] |> map((n) => n * 10)\nprint(x)");
    assert_eq!(output, vec!["[10, 20, 30]"]);

    let output = run_output("fn add(a, b) { a + b }\nprint(5 |> add(3))");
    assert_eq!(output, vec!["8"]);
}
