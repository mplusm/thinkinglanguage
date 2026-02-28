// Phase 23: Security & Access Control — Integration Tests
// Tests parse → compile → VM pipeline for secrets, masking, security

use tl_compiler::{compile, Vm, VmValue};
use tl_parser::parse;

/// Helper: parse + compile + run, return VM output lines
fn run_output(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

/// Helper: run with security policy
fn run_output_sandbox(src: &str) -> Vec<String> {
    let program = parse(src).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.security_policy = Some(tl_compiler::security::SecurityPolicy::sandbox());
    vm.execute(&proto).unwrap();
    vm.output.clone()
}

// ── Secret Vault ───────────────────────────────────────────────────

#[test]
fn test_e2e_secret_set_get() {
    let output = run_output(r#"
secret_set("db_pass", "s3cret!")
let s = secret_get("db_pass")
print(s)
"#);
    // Secret display is redacted
    assert_eq!(output, vec!["***"]);
}

#[test]
fn test_e2e_secret_list() {
    let output = run_output(r#"
secret_set("key1", "val1")
secret_set("key2", "val2")
let keys = secret_list()
print(len(keys))
"#);
    assert_eq!(output, vec!["2"]);
}

#[test]
fn test_e2e_secret_delete() {
    let output = run_output(r#"
secret_set("temp", "value")
secret_delete("temp")
let result = secret_get("temp")
print(type_of(result))
"#);
    // After delete, should return none
    assert_eq!(output, vec!["none"]);
}

// ── Data Masking ───────────────────────────────────────────────────

#[test]
fn test_e2e_mask_email() {
    let output = run_output(r#"
let masked = mask_email("user@example.com")
print(masked)
"#);
    assert_eq!(output, vec!["u***@example.com"]);
}

#[test]
fn test_e2e_mask_phone() {
    let output = run_output(r#"
let masked = mask_phone("555-123-4567")
print(masked)
"#);
    assert_eq!(output, vec!["***-***-4567"]);
}

#[test]
fn test_e2e_mask_credit_card() {
    let output = run_output(r#"
let masked = mask_cc("4111111111111111")
print(masked)
"#);
    assert_eq!(output, vec!["****-****-****-1111"]);
}

#[test]
fn test_e2e_redact_full() {
    let output = run_output(r#"
let r = redact("sensitive data", "full")
print(r)
"#);
    assert_eq!(output, vec!["***"]);
}

#[test]
fn test_e2e_redact_partial() {
    let output = run_output(r#"
let r = redact("secret", "partial")
print(r)
"#);
    assert_eq!(output, vec!["s***t"]);
}

#[test]
fn test_e2e_hash_sha256() {
    let output = run_output(r#"
let h = hash("hello", "sha256")
print(len(h))
"#);
    // SHA-256 hex is 64 chars
    assert_eq!(output, vec!["64"]);
}

#[test]
fn test_e2e_hash_md5() {
    let output = run_output(r#"
let h = hash("hello", "md5")
print(len(h))
"#);
    // MD5 hex is 32 chars
    assert_eq!(output, vec!["32"]);
}

// ── Security Policy ────────────────────────────────────────────────

#[test]
fn test_e2e_check_permission_sandbox() {
    let output = run_output_sandbox(r#"
let net = check_permission("network")
let read = check_permission("file_read")
let write = check_permission("file_write")
print(net)
print(read)
print(write)
"#);
    assert_eq!(output, vec!["false", "true", "false"]);
}

#[test]
fn test_e2e_check_permission_permissive() {
    let output = run_output(r#"
let net = check_permission("network")
let write = check_permission("file_write")
print(net)
print(write)
"#);
    // Without sandbox, everything is allowed
    assert_eq!(output, vec!["true", "true"]);
}
