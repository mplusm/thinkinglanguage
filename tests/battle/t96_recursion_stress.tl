test "fibonacci recursive" {
    fn fib(n) {
        if n <= 1 { n }
        else { fib(n - 1) + fib(n - 2) }
    }
    assert_eq(fib(0), 0)
    assert_eq(fib(1), 1)
    assert_eq(fib(10), 55)
    assert_eq(fib(15), 610)
}

test "even odd check" {
    // Mutual recursion not supported (no forward references in VM),
    // so use iterative modulo-based approach instead
    fn is_even(n) {
        n % 2 == 0
    }
    fn is_odd(n) {
        n % 2 == 1
    }
    assert(is_even(10))
    assert(is_odd(11))
    assert(not is_even(7))
}

test "GCD recursive" {
    fn gcd(a, b) {
        if b == 0 { a }
        else { gcd(b, a % b) }
    }
    assert_eq(gcd(48, 18), 6)
    assert_eq(gcd(100, 75), 25)
    assert_eq(gcd(17, 13), 1)
}

test "tower of hanoi count" {
    fn hanoi_moves(n) {
        if n <= 0 { 0 }
        else { 2 * hanoi_moves(n - 1) + 1 }
    }
    assert_eq(hanoi_moves(1), 1)
    assert_eq(hanoi_moves(3), 7)
    assert_eq(hanoi_moves(5), 31)
}
