// t109 — Float Edge Cases
// Tests: is_nan, is_infinite, precision, small/negative floats, conversion

test "is_nan false for normal" {
    assert_eq(is_nan(1.0), false)
    assert_eq(is_nan(0.0), false)
    assert_eq(is_nan(-3.14), false)
}

test "is_infinite false for normal" {
    assert_eq(is_infinite(1.0), false)
    assert_eq(is_infinite(0.0), false)
    assert_eq(is_infinite(-999.99), false)
}

test "float precision" {
    let x = 0.1 + 0.2
    assert(x > 0.29)
    assert(x < 0.31)
}

test "very small float" {
    let x = 0.000001
    assert(x > 0.0)
    assert(x < 0.001)
}

test "negative float addition" {
    let result = -3.14 + 3.14
    assert_eq(result, 0.0)
}

test "float to int truncation" {
    assert_eq(int(3.99), 3)
    assert_eq(int(-2.5), -2)
    assert_eq(int(0.9), 0)
}

test "float multiplication" {
    let x = 2.5 * 4.0
    assert_eq(x, 10.0)
}

test "float division" {
    let x = 10.0 / 3.0
    assert(x > 3.33)
    assert(x < 3.34)
}

test "float comparison" {
    assert(1.5 > 1.0)
    assert(1.0 < 1.5)
    assert(2.0 >= 2.0)
    assert(2.0 <= 2.0)
}

test "exp and log builtins" {
    assert_eq(exp(0.0), 1.0)
    assert_eq(log2(8.0), 3.0)
    assert_eq(log10(100.0), 2.0)
}

test "tan of zero" {
    assert_eq(tan(0.0), 0.0)
}
