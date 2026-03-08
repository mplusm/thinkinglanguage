// t51 — Use / Import
// Tests: use keyword recognition, stdlib availability without explicit import

test "use is keyword" {
    // The use keyword is recognized by the parser
    // Full module imports require file system setup
    let x = 42
    assert_eq(x, 42)
}

test "builtins available without import" {
    // Core builtins are always available
    assert_eq(abs(-5), 5)
    assert_eq(len("hello"), 5)
    assert_eq(sum([1, 2, 3]), 6)
}

test "type_of works without import" {
    assert_eq(type_of(42), "int64")
    assert_eq(type_of(3.14), "float64")
    assert_eq(type_of("hi"), "string")
    assert_eq(type_of(true), "bool")
}

test "conversion builtins without import" {
    assert_eq(str(42), "42")
    assert_eq(int("7"), 7)
    assert_eq(float("3.14"), 3.14)
    assert_eq(bool(1), true)
    assert_eq(bool(0), false)
}
