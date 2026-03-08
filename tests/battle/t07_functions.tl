// t07 — Functions
// Tests: named fn, recursion, returns, type annotations, higher-order

test "basic function" {
    fn add(a, b) {
        a + b
    }
    assert_eq(add(2, 3), 5)
}

test "function no args" {
    fn greeting() {
        "hello"
    }
    assert_eq(greeting(), "hello")
}

test "typed function" {
    fn square(n: int64) -> int64 {
        n * n
    }
    assert_eq(square(5), 25)
}

test "recursion fibonacci" {
    fn fib(n) {
        if n <= 1 { n } else { fib(n - 1) + fib(n - 2) }
    }
    assert_eq(fib(0), 0)
    assert_eq(fib(1), 1)
    assert_eq(fib(10), 55)
}

test "recursion factorial" {
    fn fact(n) {
        if n <= 1 { 1 } else { n * fact(n - 1) }
    }
    assert_eq(fact(5), 120)
    assert_eq(fact(0), 1)
}

test "explicit return" {
    fn early(x) {
        if x > 0 {
            return "pos"
        }
        return "non-pos"
    }
    assert_eq(early(5), "pos")
    assert_eq(early(-1), "non-pos")
    assert_eq(early(0), "non-pos")
}

test "function as value" {
    fn greet() {
        "hello"
    }
    let f = greet
    assert_eq(f(), "hello")
}

test "nested function" {
    fn outer() {
        fn inner() {
            42
        }
        inner()
    }
    assert_eq(outer(), 42)
}

test "function with default-like pattern" {
    fn multiply(a, b) {
        a * b
    }
    assert_eq(multiply(3, 4), 12)
    assert_eq(multiply(0, 100), 0)
    assert_eq(multiply(-2, 5), -10)
}

test "function calling function" {
    fn double(x) { x * 2 }
    fn quadruple(x) { double(double(x)) }
    assert_eq(quadruple(3), 12)
}

test "function with string operations" {
    fn shout(msg) {
        msg.to_upper() + "!"
    }
    assert_eq(shout("hello"), "HELLO!")
}

test "recursive sum" {
    fn rec_sum(n) {
        if n <= 0 { 0 } else { n + rec_sum(n - 1) }
    }
    assert_eq(rec_sum(10), 55)
}
