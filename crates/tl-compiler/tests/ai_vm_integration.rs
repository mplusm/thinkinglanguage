// AI Integration Tests — VM backend
// Tests tensor operations, training, model save/load through the bytecode VM.

use tl_compiler::{Vm, VmValue, compile};
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

// ── Tensor creation ──

#[test]
fn test_vm_tensor_from_list() {
    let result = run("tensor([1.0, 2.0, 3.0])").unwrap();
    assert!(matches!(result, VmValue::Tensor(_)));
}

#[test]
fn test_vm_tensor_zeros() {
    let output = run_output("let t = tensor_zeros([2, 3])\nprint(tensor_shape(t))");
    assert_eq!(output, vec!["[2, 3]"]);
}

#[test]
fn test_vm_tensor_ones() {
    let output = run_output("let t = tensor_ones([3])\nprint(tensor_sum(t))");
    // tensor_sum returns f64, so prints as 3 or 3.0
    assert!(output[0] == "3" || output[0] == "3.0");
}

// ── Tensor operations ──

#[test]
fn test_vm_tensor_reshape() {
    let output = run_output(
        "let t = tensor([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], [2, 3])\nlet r = tensor_reshape(t, [3, 2])\nprint(tensor_shape(r))",
    );
    assert_eq!(output, vec!["[3, 2]"]);
}

#[test]
fn test_vm_tensor_transpose() {
    let output = run_output(
        "let t = tensor([1.0, 2.0, 3.0, 4.0], [2, 2])\nlet tr = tensor_transpose(t)\nprint(tensor_shape(tr))",
    );
    assert_eq!(output, vec!["[2, 2]"]);
}

#[test]
fn test_vm_tensor_sum_mean() {
    // tensor_sum/mean return f64, which the VM stores as VmValue::Float
    let result = run("tensor_sum(tensor([1.0, 2.0, 3.0, 4.0]))").unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 10.0).abs() < 1e-6);
    } else {
        panic!("Expected float, got {:?}", result);
    }

    let result = run("tensor_mean(tensor([1.0, 2.0, 3.0, 4.0]))").unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 2.5).abs() < 1e-6);
    } else {
        panic!("Expected float, got {:?}", result);
    }
}

#[test]
fn test_vm_tensor_dot() {
    let result = run(
        "let a = tensor([1.0, 2.0, 3.0])\nlet b = tensor([4.0, 5.0, 6.0])\ntensor_sum(tensor_dot(a, b))",
    ).unwrap();
    // dot([1,2,3], [4,5,6]) = [4,10,18], sum = 32
    if let VmValue::Float(f) = result {
        assert!((f - 32.0).abs() < 1e-6);
    } else {
        panic!("Expected float, got {:?}", result);
    }
}

// ── Tensor arithmetic ──

#[test]
fn test_vm_tensor_add() {
    let result =
        run("let a = tensor([1.0, 2.0, 3.0])\nlet b = tensor([4.0, 5.0, 6.0])\ntensor_sum(a + b)")
            .unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 21.0).abs() < 1e-6);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_vm_tensor_sub() {
    let result = run(
        "let a = tensor([10.0, 20.0, 30.0])\nlet b = tensor([1.0, 2.0, 3.0])\ntensor_sum(a - b)",
    )
    .unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 54.0).abs() < 1e-6);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_vm_tensor_mul() {
    let result =
        run("let a = tensor([2.0, 3.0])\nlet b = tensor([4.0, 5.0])\ntensor_sum(a * b)").unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 23.0).abs() < 1e-6);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_vm_tensor_scalar_mul() {
    let result = run("let t = tensor([1.0, 2.0, 3.0])\ntensor_sum(t * 2.0)").unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 12.0).abs() < 1e-6);
    } else {
        panic!("Expected float");
    }
}

// ── Similarity ──

#[test]
fn test_vm_similarity() {
    let result =
        run("let a = tensor([1.0, 0.0, 0.0])\nlet b = tensor([1.0, 0.0, 0.0])\nsimilarity(a, b)")
            .unwrap();
    if let VmValue::Float(f) = result {
        assert!((f - 1.0).abs() < 1e-6, "Expected ~1.0, got {f}");
    } else {
        panic!("Expected float, got {:?}", result);
    }
}

// ── Model registry ──

#[test]
fn test_vm_model_list() {
    let result = run("model_list()").unwrap();
    assert!(matches!(result, VmValue::List(_)));
}

// ── Training (linear regression via VM) ──
// The VM's handle_train uses eval_ast_to_vm for config values.
// The `data` ident in train config gets looked up via globals.
// This works because `let data = read_csv(...)` sets a global.

#[test]
fn test_vm_train_linear_regression() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("train_data.csv");
    std::fs::write(
        &csv_path,
        "x,y\n1.0,2.0\n2.0,4.0\n3.0,6.0\n4.0,8.0\n5.0,10.0\n6.0,12.0\n7.0,14.0\n8.0,16.0\n9.0,18.0\n10.0,20.0\n",
    )
    .unwrap();

    // Use string path directly in read_csv within the train block config
    // to avoid variable resolution issues
    let source = format!(
        r#"let data = read_csv("{path}")
model linreg = train linear {{
    data: data,
    target: "y",
    features: ["x"]
}}
type_of(linreg)"#,
        path = csv_path.display()
    );

    let result = run(&source).unwrap();
    assert!(matches!(result, VmValue::String(ref s) if s.as_ref() == "model"));
}

// ── Model save/load round-trip ──

#[test]
fn test_vm_model_save_load() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("data.csv");
    let model_path = dir.path().join("test_model");
    std::fs::write(
        &csv_path,
        "x,y\n1.0,2.0\n2.0,4.0\n3.0,6.0\n4.0,8.0\n5.0,10.0\n6.0,12.0\n7.0,14.0\n8.0,16.0\n9.0,18.0\n10.0,20.0\n",
    )
    .unwrap();

    let source = format!(
        r#"let data = read_csv("{csv}")
model m = train linear {{
    data: data,
    target: "y",
    features: ["x"]
}}
model_save(m, "{model}")
let loaded = model_load("{model}")
type_of(loaded)"#,
        csv = csv_path.display(),
        model = model_path.display()
    );

    let result = run(&source).unwrap();
    assert!(matches!(result, VmValue::String(ref s) if s.as_ref() == "model"));
}
