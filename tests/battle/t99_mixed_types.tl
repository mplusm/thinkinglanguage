test "list of mixed types" {
    let mixed = [1, "hello", true, nil, 3.14]
    assert_eq(len(mixed), 5)
    assert_eq(mixed[0], 1)
    assert_eq(mixed[1], "hello")
    assert_eq(mixed[2], true)
    assert_eq(mixed[3], nil)
}

test "map with various value types" {
    let m = map_from("i", 1, "s", "hello", "b", true)
    assert_eq(m["i"], 1)
    assert_eq(m["s"], "hello")
    assert_eq(m["b"], true)
}

test "functions on mixed types" {
    fn identity(x) { x }
    assert_eq(identity(42), 42)
    assert_eq(identity("hi"), "hi")
    assert_eq(identity(true), true)
    assert_eq(identity(nil), nil)
}

test "type checking at runtime" {
    let values = [42, "hello", true, 3.14, nil, [1,2], set_from([1])]
    let types = values |> map((x) => type_of(x))
    assert_eq(types[0], "int64")
    assert_eq(types[1], "string")
    assert_eq(types[2], "bool")
    assert_eq(types[3], "float64")
    assert_eq(types[4], "none")
    assert_eq(types[5], "list")
    assert_eq(types[6], "set")
}
