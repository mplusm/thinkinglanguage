test "chars" {
    let c = "abc".chars()
    assert_eq(len(c), 3)
    assert_eq(c[0], "a")
}

test "join" {
    assert_eq(join(", ", ["a", "b", "c"]), "a, b, c")
}

test "repeat" {
    assert_eq("ab".repeat(3), "ababab")
}

test "index_of" {
    assert_eq("hello world".index_of("world"), 6)
}

test "pad_left" {
    assert_eq("42".pad_left(5, "0"), "00042")
}

test "pad_right" {
    assert_eq("hi".pad_right(5, "."), "hi...")
}

test "split rejoin" {
    let parts = "a-b-c".split("-")
    assert_eq(join("-", parts), "a-b-c")
}

test "substring" {
    assert_eq("hello".substring(1, 4), "ell")
}

test "chained methods" {
    let r = "  Hello World  ".trim().to_lower()
    assert_eq(r, "hello world")
}
