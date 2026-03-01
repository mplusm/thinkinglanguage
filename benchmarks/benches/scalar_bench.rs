// Scalar benchmarks: interpreter vs VM on compute-intensive tasks.

use criterion::{Criterion, criterion_group, criterion_main};
use tl_compiler::{Vm, compile};
use tl_interpreter::Interpreter;
use tl_parser::parse;

fn fib_source() -> &'static str {
    r#"
fn fib(n) {
    if n <= 1 { n }
    else { fib(n - 1) + fib(n - 2) }
}
fib(25)
"#
}

fn sum_loop_source() -> &'static str {
    r#"
let total = 0
for i in range(1000000) {
    total = total + i
}
total
"#
}

fn map_filter_source() -> &'static str {
    r#"
let nums = range(100000)
let doubled = map(nums, (x) => x * 2)
let evens = filter(doubled, (x) => x % 4 == 0)
sum(evens)
"#
}

fn bench_fib_interpreter(c: &mut Criterion) {
    let source = fib_source();
    let program = parse(source).unwrap();
    c.bench_function("fib(25) interpreter", |b| {
        b.iter(|| {
            let mut interp = Interpreter::new();
            interp.execute(&program).unwrap();
        })
    });
}

fn bench_fib_vm(c: &mut Criterion) {
    let source = fib_source();
    let program = parse(source).unwrap();
    let proto = compile(&program).unwrap();
    c.bench_function("fib(25) VM", |b| {
        b.iter(|| {
            let mut vm = Vm::new();
            vm.execute(&proto).unwrap();
        })
    });
}

fn bench_sum_loop_interpreter(c: &mut Criterion) {
    let source = sum_loop_source();
    let program = parse(source).unwrap();
    c.bench_function("sum 1M interpreter", |b| {
        b.iter(|| {
            let mut interp = Interpreter::new();
            interp.execute(&program).unwrap();
        })
    });
}

fn bench_sum_loop_vm(c: &mut Criterion) {
    let source = sum_loop_source();
    let program = parse(source).unwrap();
    let proto = compile(&program).unwrap();
    c.bench_function("sum 1M VM", |b| {
        b.iter(|| {
            let mut vm = Vm::new();
            vm.execute(&proto).unwrap();
        })
    });
}

fn bench_map_filter_interpreter(c: &mut Criterion) {
    let source = map_filter_source();
    let program = parse(source).unwrap();
    c.bench_function("map+filter 100k interpreter", |b| {
        b.iter(|| {
            let mut interp = Interpreter::new();
            interp.execute(&program).unwrap();
        })
    });
}

fn bench_map_filter_vm(c: &mut Criterion) {
    let source = map_filter_source();
    let program = parse(source).unwrap();
    let proto = compile(&program).unwrap();
    c.bench_function("map+filter 100k VM", |b| {
        b.iter(|| {
            let mut vm = Vm::new();
            vm.execute(&proto).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_fib_interpreter,
    bench_fib_vm,
    bench_sum_loop_interpreter,
    bench_sum_loop_vm,
    bench_map_filter_interpreter,
    bench_map_filter_vm,
);
criterion_main!(benches);
