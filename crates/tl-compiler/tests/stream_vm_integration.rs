// Stream/Pipeline Integration Tests — VM backend
// Tests pipeline execution, source/sink declarations, stream declarations, and streaming builtins.

use tl_compiler::{compile, Vm, VmValue};
use tl_parser::parse;

fn run(source: &str) -> Result<VmValue, tl_errors::TlError> {
    let program = parse(source)?;
    let proto = compile(&program)?;
    let mut vm = Vm::new();
    vm.execute(&proto)
}

fn run_output(source: &str) -> Vec<String> {
    let program = parse(source).unwrap();
    let proto = compile(&program).unwrap();
    let mut vm = Vm::new();
    vm.execute(&proto).unwrap();
    vm.output
}

// ── Pipeline ──

#[test]
fn test_vm_pipeline_basic() {
    let output = run_output(r#"
pipeline simple_etl {
    extract { let data = [1, 2, 3] }
    transform { let doubled = map(data, (x) => x * 2) }
    load { println(doubled) }
}
    "#);
    assert!(output.iter().any(|s| s.contains("[2, 4, 6]")));
}

#[test]
fn test_vm_pipeline_on_success() {
    let output = run_output(r#"
pipeline with_success {
    extract { let x = 1 }
    transform { let y = x + 1 }
    load { let z = y }
    on_success { println("ETL completed!") }
}
    "#);
    assert!(output.iter().any(|s| s == "ETL completed!"));
}

#[test]
fn test_vm_pipeline_with_retries() {
    let output = run_output(r#"
pipeline retry_test {
    retries: 2,
    extract { let data = [1, 2, 3] }
    transform { let result = sum(data) }
    load { println(result) }
}
    "#);
    assert!(output.iter().any(|s| s == "6"));
}

#[test]
fn test_vm_pipeline_result_stored() {
    let result = run(r#"
pipeline my_pipe {
    extract { let x = 1 }
    transform { let y = x }
    load { let z = y }
}
my_pipe
    "#).unwrap();
    assert!(matches!(result, VmValue::PipelineDef(_)));
}

// ── Source/Sink declarations ──

#[test]
fn test_vm_source_decl() {
    let result = run(r#"
source kafka_in = connector kafka {
    topic: "events",
    group: "my_group"
}
kafka_in
    "#).unwrap();
    assert!(matches!(result, VmValue::Connector(_)));
}

#[test]
fn test_vm_sink_decl() {
    let result = run(r#"
sink output = connector channel {
    buffer: 100
}
output
    "#).unwrap();
    assert!(matches!(result, VmValue::Connector(_)));
}

// ── Stream declarations ──

#[test]
fn test_vm_stream_decl_basic() {
    let result = run(r#"
source my_src = connector channel { buffer: 10 }
stream events {
    source: my_src,
    window: tumbling(5m),
    transform: { let x = 1 }
}
events
    "#).unwrap();
    assert!(matches!(result, VmValue::StreamDef(_)));
}

// ── Streaming builtins ──

#[test]
fn test_vm_emit_builtin() {
    let output = run_output(r#"
emit(42)
    "#);
    assert!(output.iter().any(|s| s.contains("emit: 42")));
}

#[test]
fn test_vm_lineage_builtin() {
    let result = run(r#"
lineage("my_pipeline")
    "#).unwrap();
    if let VmValue::String(s) = result {
        assert_eq!(&*s, "lineage_tracker");
    } else {
        panic!("Expected string from lineage()");
    }
}
