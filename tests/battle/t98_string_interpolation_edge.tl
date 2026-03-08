test "simple interpolation" {
    let x = 42
    let s = "value is {x}"
    assert_eq(s, "value is 42")
}

test "multiple interpolation" {
    let a = "hello"
    let b = "world"
    let s = "{a} {b}"
    assert_eq(s, "hello world")
}

test "interpolation with precomputed expression" {
    let x = 5
    let val = x + 1
    let s = "result: {val}"
    assert_eq(s, "result: 6")
}

test "interpolation with precomputed method" {
    let upper = "hello".to_upper()
    let s = "upper: {upper}"
    assert_eq(s, "upper: HELLO")
}

test "no interpolation in regular string" {
    // Simple strings without braces
    let s = "plain text"
    assert_eq(s, "plain text")
}
