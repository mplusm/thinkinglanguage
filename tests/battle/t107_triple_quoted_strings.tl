// t107 — Triple-Quoted Strings
// Tests: basic triple-quoted strings, multi-line, auto-dedentation

test "basic triple quoted string" {
    let s = """
        hello world
    """
    assert_eq(s, "hello world")
    assert(s.contains("hello"))
}

test "multi-line triple quoted" {
    let s = """
        line one
        line two
        line three
    """
    let lines = s.trim().split("\n")
    assert_eq(len(lines), 3)
    assert_eq(lines[0], "line one")
    assert_eq(lines[1], "line two")
    assert_eq(lines[2], "line three")
}

test "triple quoted preserves content" {
    let s = """
        no interpolation test
    """
    assert(s.contains("no interpolation"))
}

test "triple quoted single line content" {
    let s = """
        just one line
    """
    assert_eq(s, "just one line")
}

test "triple quoted multi-line equality" {
    let s = """
        alpha
        beta
    """
    assert_eq(s, "alpha\nbeta")
}

test "triple quoted with special chars" {
    let s = """
        tab	here
    """
    assert(s.contains("tab"))
    assert(s.contains("here"))
}
