test "regex_match true" { assert(regex_match("\\d+", "abc123")) }
test "regex_match false" { assert(not regex_match("^\\d+$", "abc")) }
test "regex_match full" { assert(regex_match("^\\d+$", "12345")) }
test "regex_find" {
    let matches = regex_find("\\d+", "abc123def456")
    assert_eq(len(matches), 2)
    assert_eq(matches[0], "123")
    assert_eq(matches[1], "456")
}
test "regex_replace" {
    let r = regex_replace("\\d+", "abc123", "X")
    assert_eq(r, "abcX")
}
test "regex word boundary" {
    assert(regex_match("\\bfoo\\b", "foo bar"))
    assert(not regex_match("\\bfoo\\b", "foobar"))
}
