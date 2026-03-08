test "pmap basic" {
    let nums = [1, 2, 3, 4, 5]
    let results = pmap(nums, (x) => x * x)
    assert_eq(len(results), 5)
    assert_eq(sum(results), 55)
}

test "pmap strings" {
    let words = ["hello", "world"]
    let results = pmap(words, (w) => w.len())
    assert_eq(results[0], 5)
    assert_eq(results[1], 5)
}

test "pmap empty" {
    let results = pmap([], (x) => x)
    assert_eq(len(results), 0)
}
