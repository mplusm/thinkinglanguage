// t10 — Maps
// Tests: creation via map_from, access, keys, values, contains_key, len, nested

test "map creation and access" {
    let m = map_from("a", 1, "b", 2)
    assert_eq(m["a"], 1)
    assert_eq(m["b"], 2)
}

test "map keys" {
    let m = map_from("x", 10, "y", 20)
    let k = m.keys()
    assert_eq(len(k), 2)
}

test "map values" {
    let m = map_from("x", 10, "y", 20)
    let v = m.values()
    assert_eq(len(v), 2)
}

test "map contains_key true" {
    let m = map_from("name", "TL")
    assert(m.contains_key("name"))
}

test "map contains_key false" {
    let m = map_from("name", "TL")
    assert(not m.contains_key("nope"))
}

test "map len" {
    let m = map_from("a", 1, "b", 2, "c", 3)
    assert_eq(m.len(), 3)
}

test "map single entry" {
    let m = map_from("key", "value")
    assert_eq(m["key"], "value")
    assert_eq(m.len(), 1)
}

test "nested map" {
    let inner = map_from("inner", 42)
    let m = map_from("outer", inner)
    assert_eq(m["outer"]["inner"], 42)
}

test "map with int values" {
    let m = map_from("num", 42)
    assert_eq(m["num"], 42)
}

test "map with string values" {
    let m = map_from("greeting", "hello")
    assert_eq(m["greeting"], "hello")
}

test "map with bool values" {
    let m = map_from("flag", true)
    assert_eq(m["flag"], true)
}

test "map type" {
    let m = map_from("k", 1)
    assert_eq(type_of(m), "map")
}

test "json stringify and parse roundtrip" {
    let m = map_from("a", 1, "b", 2)
    let s = json_stringify(m)
    let m2 = json_parse(s)
    assert_eq(m2["a"], 1)
    assert_eq(m2["b"], 2)
}

test "json parse list" {
    let items = json_parse("[1, 2, 3]")
    assert_eq(len(items), 3)
    assert_eq(items[0], 1)
    assert_eq(items[1], 2)
    assert_eq(items[2], 3)
}

test "map with multiple types" {
    let m = map_from("num", 42, "str", "hello", "flag", true)
    assert_eq(m["num"], 42)
    assert_eq(m["str"], "hello")
    assert_eq(m["flag"], true)
}

test "map with list value" {
    let m = map_from("items", [1, 2, 3])
    let items = m["items"]
    assert_eq(len(items), 3)
    assert_eq(items[0], 1)
}

test "deeply nested map" {
    let c = map_from("c", 99)
    let b = map_from("b", c)
    let a = map_from("a", b)
    assert_eq(a["a"]["b"]["c"], 99)
}
