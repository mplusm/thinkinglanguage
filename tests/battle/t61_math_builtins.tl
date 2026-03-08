test "abs" {
    assert_eq(abs(-5), 5)
    assert_eq(abs(5), 5)
    assert_eq(abs(0), 0)
}

test "floor ceil" {
    assert_eq(floor(3.7), 3.0)
    assert_eq(ceil(3.2), 4.0)
    assert_eq(floor(-1.5), -2.0)
}

test "round" {
    assert_eq(round(3.5), 4.0)
    assert_eq(round(3.4), 3.0)
}

test "sqrt" {
    assert_eq(sqrt(16.0), 4.0)
    assert_eq(sqrt(0.0), 0.0)
}

test "pow" {
    assert_eq(pow(2.0, 10.0), 1024.0)
    assert_eq(pow(3.0, 0.0), 1.0)
}

test "min max" {
    assert_eq(min(3, 7), 3)
    assert_eq(max(3, 7), 7)
}

test "sin cos" {
    assert_eq(sin(0.0), 0.0)
    assert_eq(cos(0.0), 1.0)
}

test "log" {
    let r = log(1.0)
    assert(r > -0.001)
    assert(r < 0.001)
}
