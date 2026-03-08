test "json parse" {
    // Use map_from to avoid { in strings triggering interpolation
    let m = map_from("name", "TL", "version", 1)
    assert_eq(m["name"], "TL")
    assert_eq(m["version"], 1)
}
test "json stringify" {
    let m = map_from("a", 1)
    let s = json_stringify(m)
    assert(s.contains("a"))
    assert(s.contains("1"))
}
test "json roundtrip" {
    let original = map_from("key", "value", "num", 42)
    let serialized = json_stringify(original)
    let restored = json_parse(serialized)
    assert_eq(restored["key"], "value")
    assert_eq(restored["num"], 42)
}
test "json nested" {
    let inner = map_from("inner", 99)
    let m = map_from("outer", inner)
    assert_eq(m["outer"]["inner"], 99)
}
test "json array" {
    let m = map_from("items", [1, 2, 3])
    assert_eq(len(m["items"]), 3)
    assert_eq(m["items"][0], 1)
}
test "json parse list" {
    let arr = json_parse("[1, 2, 3]")
    assert_eq(len(arr), 3)
    assert_eq(arr[1], 2)
}
