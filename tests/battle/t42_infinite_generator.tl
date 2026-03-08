test "infinite counter with take" {
    fn counter() {
        let mut n = 0
        while true {
            yield n
            n = n + 1
        }
    }
    let first5 = take(counter(), 5)
    assert_eq(first5, [0, 1, 2, 3, 4])
}

test "fibonacci generator" {
    fn fibs() {
        let mut a = 0
        let mut b = 1
        while true {
            yield a
            let temp = b
            b = a + b
            a = temp
        }
    }
    let first8 = take(fibs(), 8)
    assert_eq(first8, [0, 1, 1, 2, 3, 5, 8, 13])
}

test "infinite with gen_map and take" {
    fn naturals() {
        let mut n = 1
        while true { yield n; n = n + 1 }
    }
    let squares = take(gen_map(naturals(), (x) => x * x), 5)
    assert_eq(squares, [1, 4, 9, 16, 25])
}
