test "set creation" {
    let s = set_from([1, 2, 3, 2, 1])
    assert_eq(s.len(), 3)
}

test "set add" {
    let s = set_from([1, 2])
    let s2 = s.add(3)
    assert_eq(s2.len(), 3)
    let s3 = s2.add(2)
    assert_eq(s3.len(), 3)
}

test "set remove" {
    let s = set_from([1, 2, 3])
    let s2 = s.remove(2)
    assert_eq(s2.len(), 2)
    assert(not s2.contains(2))
}

test "set contains" {
    let s = set_from([1, 2, 3])
    assert(s.contains(2))
    assert(not s.contains(5))
}

test "set union" {
    let a = set_from([1, 2, 3])
    let b = set_from([3, 4, 5])
    assert_eq(a.union(b).len(), 5)
}

test "set intersection" {
    let a = set_from([1, 2, 3])
    let b = set_from([2, 3, 4])
    assert_eq(a.intersection(b).len(), 2)
}

test "set difference" {
    let a = set_from([1, 2, 3])
    let b = set_from([2, 3, 4])
    assert_eq(a.difference(b).len(), 1)
}

test "set to list" {
    let s = set_from([1, 2, 3])
    let lst = s.to_list()
    assert_eq(type_of(lst), "list")
    assert_eq(len(lst), 3)
}

test "set for loop" {
    let s = set_from([10, 20, 30])
    let mut total = 0
    for item in s {
        total = total + item
    }
    assert_eq(total, 60)
}

test "set strings" {
    let s = set_from(["a", "b", "a"])
    assert_eq(s.len(), 2)
}

test "empty set" {
    let s = set_from([])
    assert_eq(s.len(), 0)
    let s2 = s.add(1)
    assert_eq(s2.len(), 1)
}
