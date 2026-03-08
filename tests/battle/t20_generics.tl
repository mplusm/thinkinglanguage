test "generic identity" {
    fn identity<T>(x: T) -> T { x }
    assert_eq(identity(42), 42)
    assert_eq(identity("hi"), "hi")
}

test "generic struct" {
    struct Pair<A, B> { first: A, second: B }
    let p = Pair { first: 1, second: "hi" }
    assert_eq(p.first, 1)
    assert_eq(p.second, "hi")
}

test "generic function with list" {
    fn first<T>(xs: list<T>) -> T { xs[0] }
    assert_eq(first([1, 2, 3]), 1)
    assert_eq(first(["a", "b"]), "a")
}

test "generic enum" {
    enum MyOpt<T> { Some(T), Nothing }
    let x = MyOpt::Some(42)
    assert_eq(type_of(x), "enum")
}
