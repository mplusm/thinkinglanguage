test "basic generator" {
    fn gen() {
        yield 1
        yield 2
        yield 3
    }
    let g = gen()
    assert_eq(next(g), 1)
    assert_eq(next(g), 2)
    assert_eq(next(g), 3)
    assert_eq(next(g), none)
}

test "generator exhaustion" {
    fn gen() { yield 42 }
    let g = gen()
    assert_eq(next(g), 42)
    assert_eq(next(g), none)
    assert_eq(next(g), none)
}

test "generator with computation" {
    fn squares() {
        for i in range(1, 5) {
            yield i * i
        }
    }
    let g = squares()
    assert_eq(next(g), 1)
    assert_eq(next(g), 4)
    assert_eq(next(g), 9)
    assert_eq(next(g), 16)
    assert_eq(next(g), none)
}
