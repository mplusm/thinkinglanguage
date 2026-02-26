// ThinkingLanguage — Stream/Pipeline integration tests (interpreter backend)

use tl_interpreter::{Interpreter, Value};
use tl_parser::parse;

fn run(source: &str) -> (Value, Vec<String>) {
    let program = parse(source).unwrap();
    let mut interp = Interpreter::new();
    let result = interp.execute(&program).unwrap();
    (result, interp.output)
}

fn run_err(source: &str) -> String {
    let program = parse(source).unwrap();
    let mut interp = Interpreter::new();
    match interp.execute(&program) {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("Expected error"),
    }
}

#[test]
fn test_pipeline_basic() {
    let (_, output) = run(r#"
pipeline simple_etl {
    extract { let data = [1, 2, 3] }
    transform { let doubled = map(data, (x) => x * 2) }
    load { println(doubled) }
}
    "#);
    assert!(output.iter().any(|s| s.contains("[2, 4, 6]")));
    assert!(output.iter().any(|s| s.contains("simple_etl") && s.contains("success")));
}

#[test]
fn test_pipeline_on_success() {
    let (_, output) = run(r#"
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
fn test_pipeline_with_retries() {
    let (_, output) = run(r#"
pipeline retry_test {
    retries: 2,
    extract { let data = [1, 2, 3] }
    transform { let result = sum(data) }
    load { println(result) }
}
    "#);
    assert!(output.iter().any(|s| s == "6"));
    assert!(output.iter().any(|s| s.contains("retry_test") && s.contains("success")));
}

#[test]
fn test_pipeline_stored_in_env() {
    let (val, _) = run(r#"
pipeline my_pipe {
    extract { let x = 1 }
    transform { let y = x }
    load { let z = y }
}
my_pipe
    "#);
    assert!(matches!(val, Value::Pipeline(_)));
}

#[test]
fn test_source_decl() {
    let (val, _) = run(r#"
source kafka_in = connector kafka {
    topic: "events",
    group: "my_group"
}
kafka_in
    "#);
    assert!(matches!(val, Value::Connector(_)));
    if let Value::Connector(c) = val {
        assert_eq!(c.connector_type, "kafka");
        assert_eq!(c.properties.get("topic").unwrap(), "events");
    }
}

#[test]
fn test_sink_decl() {
    let (val, _) = run(r#"
sink output = connector channel {
    buffer: 100
}
output
    "#);
    assert!(matches!(val, Value::Connector(_)));
    if let Value::Connector(c) = val {
        assert_eq!(c.connector_type, "channel");
    }
}

#[test]
fn test_stream_decl_basic() {
    let (_, output) = run(r#"
source my_src = connector channel { buffer: 10 }
stream events {
    source: my_src,
    window: tumbling(5m),
    transform: { let x = 1 }
}
    "#);
    assert!(output.iter().any(|s| s.contains("events") && s.contains("declared")));
}

#[test]
fn test_emit_builtin() {
    let (_, output) = run(r#"
emit(42)
    "#);
    assert!(output.iter().any(|s| s.contains("emit: 42")));
}
