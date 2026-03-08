test "nil value" {
    let x = nil
    assert_eq(x, nil)
}

test "nil type" {
    assert_eq(type_of(nil), "none")
}

test "nil equality" {
    assert(nil == nil)
    assert(nil != 0)
    assert(nil != false)
    assert(nil != "")
}

test "nil in list" {
    let xs = [1, nil, 3]
    assert_eq(xs[1], nil)
}
