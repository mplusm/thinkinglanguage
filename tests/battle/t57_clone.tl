// t57 — Clone / Deep Copy
// Tests: .clone() on lists, structs, nested data; clone + pipe preserves original

test "clone list" {
    let original = [1, 2, 3]
    let copy = original.clone()
    assert(copy == [1, 2, 3])
    assert(original == [1, 2, 3])
}

test "clone preserves values" {
    let src = [10, 20, 30]
    let dst = src.clone()
    assert_eq(dst[0], 10)
    assert_eq(dst[1], 20)
    assert_eq(dst[2], 30)
    assert_eq(len(dst), 3)
}

test "clone struct" {
    struct Point { x: int64, y: int64 }
    let p = Point { x: 1, y: 2 }
    let p2 = p.clone()
    assert_eq(p.x, 1)
    assert_eq(p.y, 2)
    assert_eq(p2.x, 1)
    assert_eq(p2.y, 2)
}

test "clone and pipe" {
    let data = [1, 2, 3]
    let copy = data.clone()
    let result = copy |> map((x) => x * 10)
    assert(result == [10, 20, 30])
    // original should still be accessible after clone was piped
    assert(data == [1, 2, 3])
}

test "clone nested list" {
    let original = [[1, 2], [3, 4]]
    let copy = original.clone()
    assert(copy == [[1, 2], [3, 4]])
    assert(original == [[1, 2], [3, 4]])
}

test "clone does not alias" {
    let a = [1, 2, 3]
    let b = a.clone()
    // Modifying b should not affect a (deep copy)
    let result = b |> map((x) => x + 100)
    assert(result == [101, 102, 103])
    assert(a == [1, 2, 3])
}

test "clone empty list" {
    let empty = []
    let copy = empty.clone()
    assert_eq(len(copy), 0)
}

test "clone string value" {
    let s = "hello"
    let s2 = s.clone()
    assert_eq(s2, "hello")
    assert_eq(s, "hello")
}
