test "int annotation" {
    fn id(x: int64) -> int64 { x }
    assert_eq(id(42), 42)
}

test "float annotation" {
    fn half(x: float64) -> float64 { x / 2.0 }
    let r = half(10.0)
    assert_eq(r, 5.0)
}

test "string annotation" {
    fn greet(name: string) -> string { "hello " + name }
    assert_eq(greet("world"), "hello world")
}

test "bool annotation" {
    fn negate(b: bool) -> bool { not b }
    assert_eq(negate(true), false)
}

test "any annotation" {
    fn identity(x: any) -> any { x }
    assert_eq(identity(42), 42)
    assert_eq(identity("hi"), "hi")
}
