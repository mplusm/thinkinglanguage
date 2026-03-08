// t54 — Stdlib Builtins
// Tests: math, string, list, and conversion builtins available without import

test "math abs" {
    assert_eq(abs(-5), 5)
    assert_eq(abs(5), 5)
    assert_eq(abs(0), 0)
}

test "math sqrt" {
    assert(sqrt(16.0) > 3.99)
    assert(sqrt(16.0) < 4.01)
    assert(sqrt(1.0) > 0.99)
    assert(sqrt(1.0) < 1.01)
}

test "math min max" {
    assert_eq(min(3, 7), 3)
    assert_eq(max(3, 7), 7)
    assert_eq(min(-1, 1), -1)
    assert_eq(max(-1, 1), 1)
}

test "math floor ceil" {
    assert_eq(floor(3.7), 3.0)
    assert_eq(ceil(3.2), 4.0)
    assert_eq(floor(-1.5), -2.0)
    assert_eq(ceil(-1.5), -1.0)
}

test "string len" {
    assert_eq(len("hello"), 5)
    assert_eq(len(""), 0)
    assert_eq(len("a"), 1)
}

test "string conversion" {
    assert_eq(str(42), "42")
    assert_eq(str(3.14), "3.14")
    assert_eq(str(true), "true")
    assert_eq(str(nil), "none")
}

test "list len and sum" {
    assert_eq(len([1, 2, 3]), 3)
    assert_eq(len([]), 0)
    assert_eq(sum([1, 2, 3]), 6)
    assert_eq(sum([10, 20, 30]), 60)
}

test "list push and pop" {
    let mut items = [1, 2, 3]
    items = push(items, 4)
    assert_eq(len(items), 4)
    assert_eq(items[3], 4)
}

test "range builtin" {
    let r = range(5)
    assert_eq(len(r), 5)
    assert_eq(r[0], 0)
    assert_eq(r[4], 4)
}

test "type_of all types" {
    assert_eq(type_of(42), "int64")
    assert_eq(type_of(3.14), "float64")
    assert_eq(type_of("hi"), "string")
    assert_eq(type_of(true), "bool")
    assert_eq(type_of(nil), "none")
    assert_eq(type_of([1, 2]), "list")
}
