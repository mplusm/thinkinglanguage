test "trait def and impl" {
    trait Describable { fn describe(self) -> string }
    struct Dog { name: string }
    impl Describable for Dog { fn describe(self) -> string { "dog: " + self.name } }
    let d = Dog { name: "Rex" }
    assert_eq(d.describe(), "dog: Rex")
}

test "multiple trait impls" {
    struct Point { x: int64, y: int64 }
    impl Display for Point { fn show(self) -> string { "point" } }
    let p = Point { x: 1, y: 2 }
    assert_eq(p.show(), "point")
}

test "where clause compiles" {
    fn compare<T>(x: T) where T: Comparable { x }
    assert_eq(compare(10), 10)
}
