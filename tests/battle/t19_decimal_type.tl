test "decimal literal" {
    let x = 10.5d
    assert_eq(type_of(x), "decimal")
}

test "decimal add" {
    assert(10.5d + 2.5d == 13.0d)
}

test "decimal mul" {
    assert(10.5d * 2.0d == 21.0d)
}

test "decimal comparison" {
    assert(1.0d < 2.0d)
    assert(2.0d >= 2.0d)
    assert(1.0d == 1.0d)
}

test "decimal div by zero" {
    let mut caught = false
    try {
        let x = 1.0d / 0.0d
    } catch e {
        caught = true
    }
    assert(caught)
}
