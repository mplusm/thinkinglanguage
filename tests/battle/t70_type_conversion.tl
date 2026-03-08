test "str conversion" {
    assert_eq(str(42), "42")
    assert_eq(str(3.14), "3.14")
    assert_eq(str(true), "true")
    assert_eq(str(nil), "none")
}

test "int conversion" {
    assert_eq(int("42"), 42)
    assert_eq(int(3.9), 3)
}

test "float conversion" {
    assert_eq(float("3.14"), 3.14)
    assert_eq(float(42), 42.0)
}

test "type_of" {
    assert_eq(type_of(42), "int64")
    assert_eq(type_of(3.14), "float64")
    assert_eq(type_of("hi"), "string")
    assert_eq(type_of(true), "bool")
    assert_eq(type_of(nil), "none")
    assert_eq(type_of([1,2]), "list")
}
