test "take" {
    fn nums() {
        yield 1
        yield 2
        yield 3
        yield 4
        yield 5
    }
    let g = take(nums(), 3)
    assert_eq(next(g), 1)
    assert_eq(next(g), 2)
    assert_eq(next(g), 3)
    assert_eq(next(g), none)
}

test "skip" {
    fn nums() {
        yield 1
        yield 2
        yield 3
        yield 4
        yield 5
    }
    let g = skip(nums(), 2)
    assert_eq(next(g), 3)
    assert_eq(next(g), 4)
}

test "take from infinite-like" {
    fn count_from(n) {
        let mut i = n
        while true {
            yield i
            i = i + 1
        }
    }
    let g = take(count_from(1), 5)
    assert_eq(next(g), 1)
    assert_eq(next(g), 2)
    assert_eq(next(g), 3)
    assert_eq(next(g), 4)
    assert_eq(next(g), 5)
    assert_eq(next(g), none)
}
