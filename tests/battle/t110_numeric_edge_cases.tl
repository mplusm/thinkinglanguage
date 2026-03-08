// t110 — Numeric Edge Cases
// Tests: large integers, zero handling, sign, exponentiation, modulo

test "large integers" {
    let x = 1000000000
    assert(x > 999999999)
    assert(x == 1000000000)
}

test "negative large" {
    let x = -1000000000
    assert(x < 0)
    assert(x == -1000000000)
}

test "zero handling" {
    assert_eq(0 * 100, 0)
    assert_eq(0 + 0, 0)
    assert_eq(0 - 0, 0)
    assert_eq(100 * 0, 0)
}

test "sign builtin" {
    assert_eq(sign(-5), -1)
    assert_eq(sign(0), 0)
    assert_eq(sign(5), 1)
    assert_eq(sign(-100), -1)
    assert_eq(sign(999), 1)
}

test "exponentiation" {
    assert_eq(2 ** 0, 1)
    assert_eq(2 ** 1, 2)
    assert_eq(2 ** 10, 1024)
    assert_eq(0 ** 0, 1)
}

test "modulo" {
    assert_eq(10 % 3, 1)
    assert_eq(9 % 3, 0)
    assert_eq(7 % 2, 1)
}

test "modulo negative" {
    let r = -7 % 3
    assert_eq(r, -1)
}

test "integer division" {
    assert_eq(10 / 3, 3)
    assert_eq(9 / 3, 3)
    assert_eq(7 / 2, 3)
}

test "operator precedence" {
    assert_eq(2 + 3 * 4, 14)
    assert_eq((2 + 3) * 4, 20)
    assert_eq(10 - 2 * 3, 4)
}

test "chained arithmetic" {
    let x = 1 + 2 + 3 + 4 + 5
    assert_eq(x, 15)
    let y = 100 - 10 - 20 - 30
    assert_eq(y, 40)
}
