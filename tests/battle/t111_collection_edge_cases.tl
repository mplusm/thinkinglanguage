// t111 — Collection Edge Cases
// Tests: empty/single/nested/large collections, flatten, map, set edge cases

test "empty list operations" {
    let x = []
    assert_eq(len(x), 0)
    assert_eq(sum(x), 0)
    assert_eq(type_of(x), "list")
}

test "single element list" {
    let x = [42]
    assert_eq(x[0], 42)
    assert_eq(len(x), 1)
}

test "large list" {
    let mut x = []
    for i in range(100) {
        x = push(x, i)
    }
    assert_eq(len(x), 100)
    assert_eq(x[0], 0)
    assert_eq(x[99], 99)
    assert_eq(x[50], 50)
}

test "nested 2 levels" {
    let x = [[1, 2], [3, 4]]
    assert_eq(x[0][0], 1)
    assert_eq(x[0][1], 2)
    assert_eq(x[1][0], 3)
    assert_eq(x[1][1], 4)
}

test "nested 3 levels" {
    let x = [[[1]]]
    assert_eq(x[0][0][0], 1)
}

test "list flatten" {
    let x = [1, [2, 3], [4, 5]]
    let f = x.flatten()
    assert_eq(len(f), 5)
    assert_eq(f[0], 1)
    assert_eq(f[1], 2)
    assert_eq(f[4], 5)
}

test "flatten one level only" {
    let x = [1, [2, [3]]]
    let f = x.flatten()
    assert_eq(len(f), 3)
    assert_eq(f[0], 1)
    assert_eq(f[1], 2)
    assert(f[2] == [3])
}

test "empty map" {
    let m = map_from()
    assert_eq(m.len(), 0)
    assert_eq(type_of(m), "map")
}

test "map first key wins" {
    let m = map_from("a", 1, "a", 2)
    assert_eq(m["a"], 1)
}

test "set from empty" {
    let s = set_from([])
    assert_eq(len(s), 0)
}

test "set deduplication" {
    let s = set_from([1, 1, 1, 1])
    assert_eq(s.len(), 1)
}

test "list negative indexing" {
    let x = [10, 20, 30, 40]
    assert_eq(x[-1], 40)
    assert_eq(x[-2], 30)
    assert_eq(x[-4], 10)
}

test "list of mixed types" {
    let x = [1, "two", true, nil]
    assert_eq(len(x), 4)
    assert_eq(x[0], 1)
    assert_eq(x[1], "two")
    assert_eq(x[2], true)
    assert_eq(x[3], nil)
}
