test "wildcard catches all" {
    let x = 42
    let r = match x { 1 => "one", 2 => "two", _ => "other" }
    assert_eq(r, "other")
}

test "enum all variants covered" {
    enum Bool { True, False }
    let b = Bool::True
    let r = match b {
        Bool::True => "yes",
        Bool::False => "no",
        _ => "maybe"
    }
    assert_eq(r, "yes")
}

test "binding catches everything" {
    let x = 999
    let r = match x { val => val * 2 }
    assert_eq(r, 1998)
}
