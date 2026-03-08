test "line comment" {
    // this is a comment
    let x = 42
    assert_eq(x, 42)
}

test "code after comment line" {
    let a = 1
    // comment
    let b = 2
    assert_eq(a + b, 3)
}
