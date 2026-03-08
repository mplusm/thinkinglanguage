// t02 — Arithmetic
// Tests: +, -, *, /, %, **, precedence, int/float, division by zero

test "addition" {
    assert_eq(2 + 3, 5)
    assert_eq(0 + 0, 0)
    assert_eq(100 + 200, 300)
}

test "subtraction" {
    assert_eq(10 - 3, 7)
    assert_eq(5 - 5, 0)
    assert_eq(3 - 10, -7)
}

test "multiplication" {
    assert_eq(4 * 5, 20)
    assert_eq(0 * 999, 0)
    assert_eq(-3 * 4, -12)
}

test "integer division" {
    assert_eq(10 / 3, 3)
    assert_eq(7 / 2, 3)
    assert_eq(100 / 10, 10)
}

test "modulo" {
    assert_eq(10 % 3, 1)
    assert_eq(7 % 2, 1)
    assert_eq(8 % 4, 0)
}

test "exponentiation" {
    assert_eq(2 ** 10, 1024)
    assert_eq(3 ** 3, 27)
    assert_eq(5 ** 0, 1)
}

test "precedence mul before add" {
    assert_eq(2 + 3 * 4, 14)
}

test "precedence with parens" {
    assert_eq((2 + 3) * 4, 20)
}

test "nested parentheses" {
    assert_eq(((2 + 3) * (4 - 1)), 15)
}

test "float addition" {
    let x = 1.5 + 2.5
    assert_eq(x, 4.0)
}

test "float multiplication" {
    let x = 2.5 * 4.0
    assert_eq(x, 10.0)
}

test "float approximate" {
    let x = 3.0 + 0.14
    assert(x > 3.13)
    assert(x < 3.15)
}

test "negative numbers" {
    assert_eq(-5 + 3, -2)
    assert_eq(-3 * -4, 12)
    assert_eq(-10 / 2, -5)
}

test "division by zero throws" {
    let mut caught = false
    try {
        let x = 1 / 0
    } catch e {
        caught = true
    }
    assert(caught)
}

test "chained arithmetic" {
    let result = 1 + 2 + 3 + 4 + 5
    assert_eq(result, 15)
}

test "large numbers" {
    assert_eq(1000000 * 1000, 1000000000)
}
