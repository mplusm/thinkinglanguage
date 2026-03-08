// t03 — String Operations
// Tests: concatenation, interpolation, methods (len, contains, split, replace, trim, etc.)

test "concatenation" {
    assert_eq("hello" + " " + "world", "hello world")
}

test "empty string concatenation" {
    assert_eq("" + "abc", "abc")
    assert_eq("abc" + "", "abc")
}

test "string interpolation" {
    let name = "TL"
    let greeting = "Hello {name}"
    assert_eq(greeting, "Hello TL")
}

test "interpolation with expression" {
    let x = 5
    let msg = "value is {x}"
    assert_eq(msg, "value is 5")
}

test "len builtin" {
    assert_eq(len("hello"), 5)
    assert_eq(len(""), 0)
    assert_eq(len("abc"), 3)
}

test "len method" {
    assert_eq("hello".len(), 5)
    assert_eq("".len(), 0)
}

test "contains method" {
    assert("hello world".contains("world"))
    assert("hello world".contains("hello"))
    assert(not "hello".contains("xyz"))
}

test "split method" {
    let parts = "a,b,c".split(",")
    assert_eq(len(parts), 3)
    assert_eq(parts[0], "a")
    assert_eq(parts[1], "b")
    assert_eq(parts[2], "c")
}

test "replace method" {
    assert_eq("hello world".replace("world", "TL"), "hello TL")
    assert_eq("aaa".replace("a", "b"), "bbb")
}

test "trim method" {
    assert_eq("  hi  ".trim(), "hi")
    assert_eq("hello".trim(), "hello")
}

test "starts_with method" {
    assert("hello".starts_with("hel"))
    assert("hello".starts_with("hello"))
    assert(not "hello".starts_with("world"))
}

test "ends_with method" {
    assert("hello".ends_with("llo"))
    assert("hello".ends_with("hello"))
    assert(not "hello".ends_with("world"))
}

test "to_upper method" {
    assert_eq("hello".to_upper(), "HELLO")
    assert_eq("abc".to_upper(), "ABC")
}

test "to_lower method" {
    assert_eq("HELLO".to_lower(), "hello")
    assert_eq("ABC".to_lower(), "abc")
}

test "substring method" {
    assert_eq("hello".substring(1, 3), "el")
    assert_eq("abcdef".substring(0, 3), "abc")
    assert_eq("abcdef".substring(3, 6), "def")
}

test "string repeat with multiply" {
    assert_eq("-" * 3, "---")
    assert_eq("ab" * 2, "abab")
}

test "string type" {
    assert_eq(type_of("hello"), "string")
}
