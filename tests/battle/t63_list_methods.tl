test "map" {
    assert([1,2,3].map((x) => x * 2) == [2, 4, 6])
}

test "filter" {
    assert([1,2,3,4,5].filter((x) => x > 3) == [4, 5])
}

test "reduce" {
    assert_eq([1,2,3,4].reduce(0, (acc, x) => acc + x), 10)
}

test "contains" {
    assert([1,2,3].contains(2))
    assert(not [1,2,3].contains(5))
}

test "reverse" {
    assert([1,2,3].reverse() == [3, 2, 1])
}

test "sort" {
    assert([3,1,2].sort() == [1, 2, 3])
}

test "find" {
    assert_eq([1,2,3,4,5].find((x) => x > 3), 4)
}

test "unique" {
    assert([1,2,2,3,3,3].unique() == [1, 2, 3])
}

test "zip" {
    let z = [1,2,3].zip(["a","b","c"])
    assert(z[0] == [1, "a"])
    assert(z[2] == [3, "c"])
}

test "flat_map" {
    let r = [1,2,3].flat_map((x) => [x, x * 10])
    assert(r == [1, 10, 2, 20, 3, 30])
}
