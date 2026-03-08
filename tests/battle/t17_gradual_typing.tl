test "untyped function" {
    fn add(a, b) { a + b }
    assert_eq(add(1, 2), 3)
    assert_eq(add("a", "b"), "ab")
}

test "mixed typed untyped" {
    fn process(x: int64, label) { str(x) + " " + label }
    assert_eq(process(42, "items"), "42 items")
}

test "any works with everything" {
    fn box(x: any) -> any { x }
    assert_eq(box(1), 1)
    assert_eq(box("s"), "s")
    assert_eq(box(true), true)
}
