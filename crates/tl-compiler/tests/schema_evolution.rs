// Phase 21: Schema Evolution & Migration — Integration Tests
// Tests parse → compile → VM pipeline for versioned schemas and migrations

use tl_compiler::{Vm, VmValue, compile};
use tl_parser::parse;

/// Helper: parse + compile + run, return result
fn run(src: &str) -> Result<VmValue, String> {
    let program = parse(src).map_err(|e| format!("Parse error: {e}"))?;
    let proto = compile(&program).map_err(|e| format!("Compile error: {e}"))?;
    let mut vm = Vm::new();
    vm.execute(&proto)
        .map_err(|e| format!("Runtime error: {e}"))
}

/// Helper: parse + compile + run, return VM output lines
fn run_output(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

/// Helper: parse + compile + run, return VM with state
fn run_vm(src: &str) -> Vm {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm
}

// ── Schema Definition & Registration ────────────────────────────────

#[test]
fn test_e2e_define_schema_v1_verify_fields() {
    let output = run_output(
        r#"
/// @version 1
schema User {
    id: int64
    name: string
}
let f = schema_fields("User", 1)
print(len(f))
"#,
    );
    assert_eq!(output, vec!["2"]);
}

#[test]
fn test_e2e_define_schema_v1_evolve_to_v2() {
    let output = run_output(
        r#"
/// @version 1
schema User {
    id: int64
    name: string
}
/// @version 2
schema UserV2 {
    id: int64
    name: string
    email: string
}
schema_register("User", 2, map_from("id", "int64", "name", "string", "email", "string"))
print(schema_latest("User"))
"#,
    );
    assert_eq!(output, vec!["2"]);
}

// ── Migrate Statement ───────────────────────────────────────────────

#[test]
fn test_e2e_migrate_add_column() {
    let output = run_output(
        r#"
/// @version 1
schema Product {
    id: int64
    name: string
}
migrate Product from 1 to 2 {
    add_column(price: float64, default: "0.0")
}
print(schema_latest("Product"))
let f = schema_fields("Product", 2)
print(len(f))
"#,
    );
    assert_eq!(output, vec!["2", "3"]);
}

#[test]
fn test_e2e_migrate_drop_column() {
    let output = run_output(
        r#"
/// @version 1
schema Record {
    id: int64
    legacy: string
    name: string
}
migrate Record from 1 to 2 {
    drop_column(legacy)
}
let f = schema_fields("Record", 2)
print(len(f))
"#,
    );
    assert_eq!(output, vec!["2"]);
}

#[test]
fn test_e2e_migrate_rename_column() {
    let output = run_output(
        r#"
/// @version 1
schema Item {
    id: int64
    item_name: string
}
migrate Item from 1 to 2 {
    rename_column(item_name, name)
}
let f = schema_fields("Item", 2)
print(f)
"#,
    );
    let output_str = &output[0];
    assert!(
        output_str.contains("name"),
        "Expected 'name' in fields, got: {}",
        output_str
    );
    assert!(
        !output_str.contains("item_name"),
        "Unexpected 'item_name', got: {}",
        output_str
    );
}

// ── Schema Registry Builtins ────────────────────────────────────────

#[test]
fn test_e2e_schema_latest_after_multiple_versions() {
    let output = run_output(
        r#"
schema_register("Order", 1, map_from("id", "int64"))
schema_register("Order", 2, map_from("id", "int64", "total", "float64"))
schema_register("Order", 3, map_from("id", "int64", "total", "float64", "status", "string"))
print(schema_latest("Order"))
"#,
    );
    assert_eq!(output, vec!["3"]);
}

#[test]
fn test_e2e_schema_history_returns_ordered_versions() {
    let output = run_output(
        r#"
schema_register("Event", 3, map_from("id", "int64", "data", "string", "ts", "string"))
schema_register("Event", 1, map_from("id", "int64"))
schema_register("Event", 2, map_from("id", "int64", "data", "string"))
print(schema_history("Event"))
"#,
    );
    // Should be sorted regardless of insertion order
    assert_eq!(output, vec!["[1, 2, 3]"]);
}

#[test]
fn test_e2e_schema_check_backward_compat_passes() {
    let output = run_output(
        r#"
schema_register("T", 1, map_from("id", "int64"))
schema_register("T", 2, map_from("id", "int64", "name", "string"))
let issues = schema_check("T", 1, 2, "backward")
print(len(issues))
"#,
    );
    // Adding a column is backward compatible
    assert_eq!(output, vec!["0"]);
}

#[test]
fn test_e2e_schema_check_backward_compat_fails_on_drop() {
    let output = run_output(
        r#"
schema_register("T", 1, map_from("id", "int64", "name", "string"))
schema_register("T", 2, map_from("id", "int64"))
let issues = schema_check("T", 1, 2, "backward")
print(len(issues))
"#,
    );
    // Removing a column breaks backward compat
    assert_eq!(output, vec!["1"]);
}

#[test]
fn test_e2e_schema_diff_between_v1_v2() {
    let output = run_output(
        r#"
schema_register("D", 1, map_from("id", "int64", "name", "string"))
schema_register("D", 2, map_from("id", "int64", "name", "string", "email", "string"))
let d = schema_diff("D", 1, 2)
print(len(d))
print(d)
"#,
    );
    assert_eq!(output[0], "1");
    assert!(
        output[1].contains("added"),
        "Expected 'added' in diff output: {}",
        output[1]
    );
}

// ── Field Annotations ───────────────────────────────────────────────

#[test]
fn test_e2e_field_since_annotation_preserved() {
    // @since in field doc comments should propagate to schema metadata
    let output = run_output(
        r#"
/// @version 1
schema WithSince {
    id: int64
    /// @since 1
    name: string
}
let f = schema_fields("WithSince", 1)
print(len(f))
"#,
    );
    assert_eq!(output, vec!["2"]);
}

#[test]
fn test_e2e_schema_with_default_values() {
    let output = run_output(
        r#"
/// @version 1
schema WithDefault {
    id: int64
    status: string = "active"
}
let f = schema_fields("WithDefault", 1)
print(len(f))
"#,
    );
    assert_eq!(output, vec!["2"]);
}

// ── Full Workflow ───────────────────────────────────────────────────

#[test]
fn test_e2e_full_workflow() {
    // Define v1, migrate to v2, check compat, get diff
    let output = run_output(
        r#"
/// @version 1
schema User {
    id: int64
    name: string
}
migrate User from 1 to 2 {
    add_column(email: string, default: "")
}
let latest = schema_latest("User")
print(latest)
let vers = schema_versions("User")
print(vers)
let issues = schema_check("User", 1, 2, "backward")
print(len(issues))
let diff = schema_diff("User", 1, 2)
print(len(diff))
"#,
    );
    assert_eq!(output[0], "2"); // latest version
    assert_eq!(output[1], "[1, 2]"); // versions
    assert_eq!(output[2], "0"); // no backward compat issues
    assert_eq!(output[3], "1"); // one diff (field added)
}

#[test]
fn test_e2e_schema_versions() {
    let output = run_output(
        r#"
schema_register("V", 1, map_from("a", "int64"))
schema_register("V", 3, map_from("a", "int64", "b", "string"))
schema_register("V", 2, map_from("a", "int64", "c", "float64"))
print(schema_versions("V"))
"#,
    );
    assert_eq!(output, vec!["[1, 2, 3]"]);
}
