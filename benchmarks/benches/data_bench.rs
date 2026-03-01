// Data benchmarks: CSV pipeline with DataFusion.

use criterion::{Criterion, criterion_group, criterion_main};
use std::io::Write;
use tempfile::NamedTempFile;
use tl_compiler::{Vm, compile};
use tl_interpreter::Interpreter;
use tl_parser::parse;

fn create_test_csv(rows: usize) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,name,value,category").unwrap();
    for i in 0..rows {
        let cat = if i % 3 == 0 {
            "A"
        } else if i % 3 == 1 {
            "B"
        } else {
            "C"
        };
        writeln!(f, "{},item_{},{},{}", i, i, (i as f64) * 1.5, cat).unwrap();
    }
    f.flush().unwrap();
    f
}

fn data_pipeline_source(csv_path: &str) -> String {
    format!(
        r#"
let data = read_csv("{csv_path}")
let result = data
    |> filter(value > 100.0)
    |> select(id, name, value)
    |> sort(value)
let batches = collect(result)
len(batches)
"#,
    )
}

fn data_aggregate_source(csv_path: &str) -> String {
    format!(
        r#"
let data = read_csv("{csv_path}")
let result = data
    |> aggregate(avg(value), count(id))
collect(result)
"#,
    )
}

fn bench_data_pipeline_interpreter(c: &mut Criterion) {
    let csv = create_test_csv(100_000);
    let path = csv.path().to_str().unwrap().replace('\\', "/");
    let source = data_pipeline_source(&path);
    let program = parse(&source).unwrap();

    c.bench_function("100k row pipeline interpreter", |b| {
        b.iter(|| {
            let mut interp = Interpreter::new();
            interp.execute(&program).unwrap();
        })
    });
}

fn bench_data_pipeline_vm(c: &mut Criterion) {
    let csv = create_test_csv(100_000);
    let path = csv.path().to_str().unwrap().replace('\\', "/");
    let source = data_pipeline_source(&path);
    let program = parse(&source).unwrap();
    let proto = compile(&program).unwrap();

    c.bench_function("100k row pipeline VM", |b| {
        b.iter(|| {
            let mut vm = Vm::new();
            vm.execute(&proto).unwrap();
        })
    });
}

fn bench_data_aggregate_interpreter(c: &mut Criterion) {
    let csv = create_test_csv(100_000);
    let path = csv.path().to_str().unwrap().replace('\\', "/");
    let source = data_aggregate_source(&path);
    let program = parse(&source).unwrap();

    c.bench_function("100k aggregate interpreter", |b| {
        b.iter(|| {
            let mut interp = Interpreter::new();
            interp.execute(&program).unwrap();
        })
    });
}

fn bench_data_aggregate_vm(c: &mut Criterion) {
    let csv = create_test_csv(100_000);
    let path = csv.path().to_str().unwrap().replace('\\', "/");
    let source = data_aggregate_source(&path);
    let program = parse(&source).unwrap();
    let proto = compile(&program).unwrap();

    c.bench_function("100k aggregate VM", |b| {
        b.iter(|| {
            let mut vm = Vm::new();
            vm.execute(&proto).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_data_pipeline_interpreter,
    bench_data_pipeline_vm,
    bench_data_aggregate_interpreter,
    bench_data_aggregate_vm,
);
criterion_main!(benches);
