// t108 — Unicode String Handling
// Tests: accented chars, various unicode, len (byte-based), contains, split

test "accented characters" {
    let s = "café"
    assert_eq(s.len(), 5)
    assert(s.contains("caf"))
}

test "unicode equality" {
    let a = "hello"
    let b = "hello"
    assert(a == b)
}

test "unicode in list" {
    let items = ["alpha", "beta", "gamma"]
    assert_eq(len(items), 3)
    assert_eq(items[0], "alpha")
    assert_eq(items[2], "gamma")
}

test "string with special chars" {
    let s = "line1\nline2\ttab"
    assert(s.contains("line1"))
    assert(s.contains("line2"))
}

test "empty string" {
    let s = ""
    assert_eq(s.len(), 0)
    assert(not s.contains("x"))
}

test "string concat with accents" {
    let a = "caf"
    let b = "é"
    let c = a + b
    assert_eq(c, "café")
    assert_eq(c.len(), 5)
}

test "string split unicode" {
    let s = "a-b-c"
    let parts = s.split("-")
    assert_eq(len(parts), 3)
    assert_eq(parts[0], "a")
    assert_eq(parts[2], "c")
}

test "string trim preserves content" {
    let s = "  hello  "
    assert_eq(s.trim(), "hello")
}
