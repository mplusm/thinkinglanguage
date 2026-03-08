// t55 — Circular Import Avoidance
// Tests: single-file programs with mutual function calls work correctly

test "no circular import in single file" {
    fn a() { 1 }
    fn b() { a() + 1 }
    assert_eq(b(), 2)
}

test "sequential function calls" {
    fn step1(n) { n + 1 }
    fn step2(n) { step1(n) * 2 }
    fn step3(n) { step2(n) + 10 }
    assert_eq(step3(0), 12)
    assert_eq(step3(4), 20)
}

test "function chain with data flow" {
    fn parse_value(s) { int(s) }
    fn double_it(n) { n * 2 }
    fn format_result(n) { str(n) }

    let result = format_result(double_it(parse_value("21")))
    assert_eq(result, "42")
}

test "recursive function" {
    fn factorial(n) {
        if n <= 1 { 1 }
        else { n * factorial(n - 1) }
    }
    assert_eq(factorial(5), 120)
    assert_eq(factorial(1), 1)
    assert_eq(factorial(0), 1)
}

test "helper functions pattern" {
    fn validate(x) { x > 0 }
    fn process(x) { x * 10 }
    fn execute(x) {
        if validate(x) { process(x) }
        else { -1 }
    }
    assert_eq(execute(5), 50)
    assert_eq(execute(-1), -1)
}
