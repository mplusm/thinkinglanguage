test "gen_map" {
    fn nums() {
        yield 1
        yield 2
        yield 3
    }
    let g = gen_map(nums(), (x) => x * 10)
    assert_eq(next(g), 10)
    assert_eq(next(g), 20)
    assert_eq(next(g), 30)
    assert_eq(next(g), none)
}

test "gen_filter" {
    fn nums() {
        yield 1
        yield 2
        yield 3
        yield 4
        yield 5
    }
    let g = gen_filter(nums(), (x) => x % 2 == 0)
    assert_eq(next(g), 2)
    assert_eq(next(g), 4)
    assert_eq(next(g), none)
}

test "chain" {
    fn a() {
        yield 1
        yield 2
    }
    fn b() {
        yield 3
        yield 4
    }
    let g = chain(a(), b())
    assert_eq(next(g), 1)
    assert_eq(next(g), 2)
    assert_eq(next(g), 3)
    assert_eq(next(g), 4)
    assert_eq(next(g), none)
}

test "gen_enumerate" {
    fn letters() {
        yield "a"
        yield "b"
        yield "c"
    }
    let g = gen_enumerate(letters())
    let first = next(g)
    assert_eq(first[0], 0)
    assert_eq(first[1], "a")
}
