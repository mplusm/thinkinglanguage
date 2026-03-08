test "generator in for loop" {
    fn gen() {
        yield 10
        yield 20
        yield 30
    }
    let mut total = 0
    for val in gen() {
        total = total + val
    }
    assert_eq(total, 60)
}

test "generator range-like in for" {
    fn my_range(start, end_val) {
        let mut i = start
        while i < end_val {
            yield i
            i = i + 1
        }
    }
    let mut count = 0
    for i in my_range(5, 10) {
        count = count + 1
    }
    assert_eq(count, 5)
}

test "generator filter then for" {
    fn nums() {
        yield 1
        yield 2
        yield 3
        yield 4
        yield 5
    }
    let evens = gen_filter(nums(), (x) => x % 2 == 0)
    let mut total = 0
    for x in evens {
        total = total + x
    }
    assert_eq(total, 6)
}
