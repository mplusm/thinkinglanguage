test "tan" {
    assert_eq(tan(0.0), 0.0)
}

test "exp" {
    assert_eq(exp(0.0), 1.0)
}

test "log2" {
    assert_eq(log2(8.0), 3.0)
}

test "log10" {
    assert_eq(log10(100.0), 2.0)
}

test "sign negative" {
    assert_eq(sign(-42), -1)
}

test "sign zero" {
    assert_eq(sign(0), 0)
}

test "sign positive" {
    assert_eq(sign(7), 1)
}

test "is_nan false" {
    assert_eq(is_nan(1.0), false)
}

test "is_nan zero" {
    assert_eq(is_nan(0.0), false)
}

test "is_infinite false" {
    assert_eq(is_infinite(42.0), false)
}

test "is_infinite zero" {
    assert_eq(is_infinite(0.0), false)
}

test "exp and log inverse" {
    let x = 2.0
    let result = log(exp(x))
    assert(result > 1.99)
    assert(result < 2.01)
}
