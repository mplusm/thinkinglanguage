test "struct with methods and match" {
    struct Shape { kind: string, size: float64 }
    impl Shape {
        fn area(self) {
            match self.kind {
                "circle" => 3.14 * self.size * self.size,
                "square" => self.size * self.size,
                _ => 0.0
            }
        }
    }
    let c = Shape { kind: "circle", size: 2.0 }
    let s = Shape { kind: "square", size: 3.0 }
    assert(c.area() > 12.0)
    assert(c.area() < 13.0)
    assert_eq(s.area(), 9.0)
}

test "enum with closures" {
    enum Op { Add, Mul }
    fn get_op(op) {
        match op {
            Op::Add => (a, b) => a + b,
            Op::Mul => (a, b) => a * b,
            _ => (a, b) => 0
        }
    }
    let add = get_op(Op::Add)
    let mul = get_op(Op::Mul)
    assert_eq(add(3, 4), 7)
    assert_eq(mul(3, 4), 12)
}

test "pipe with struct" {
    struct Score { value: int64 }
    let scores = [Score { value: 90 }, Score { value: 80 }, Score { value: 70 }]
    let values = scores.map((s) => s.value)
    let total = values |> sum()
    assert_eq(total, 240)
}

test "generator with error handling" {
    fn risky_gen() {
        yield 1
        yield 2
        yield 3
    }
    let g = risky_gen()
    let mut total = 0
    let mut val = next(g)
    while val != none {
        total = total + val
        val = next(g)
    }
    assert_eq(total, 6)
}

test "closures and channels" {
    let ch = channel()
    let transform = (x) => x * 2
    send(ch, transform(5))
    send(ch, transform(10))
    assert_eq(recv(ch), 10)
    assert_eq(recv(ch), 20)
}

test "error recovery with retry pattern" {
    let mut attempts = 0
    let mut result = nil
    while attempts < 3 {
        try {
            attempts = attempts + 1
            if attempts < 3 {
                throw "not ready"
            }
            result = "success"
        } catch e {
            // retry
        }
        if result != nil {
            break
        }
    }
    assert_eq(result, "success")
    assert_eq(attempts, 3)
}
