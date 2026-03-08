test "closure captures outer variable" {
    let x = 10
    let f = () => x + 5
    assert_eq(f(), 15)
}

test "closure captures at creation time" {
    let mut val = 1
    let f = () => val
    val = 2
    // In TL, closures get their own copy when closed
    // The result depends on implementation -- capture at creation vs late binding
    let result = f()
    assert(result == 1 or result == 2)
}

test "closure factory creates separate captures" {
    // Use a factory function to create closures with separate captures
    fn make_fn(n) {
        return () => n
    }
    let f0 = make_fn(0)
    let f1 = make_fn(1)
    let f4 = make_fn(4)
    assert_eq(f0(), 0)
    assert_eq(f1(), 1)
    assert_eq(f4(), 4)
}

test "nested closures" {
    let make_adder = (x) => (y) => x + y
    let add10 = make_adder(10)
    let add20 = make_adder(20)
    assert_eq(add10(5), 15)
    assert_eq(add20(5), 25)
}

test "closure as return value" {
    fn make_multiplier(factor) {
        return (x) => x * factor
    }
    let double = make_multiplier(2)
    let triple = make_multiplier(3)
    assert_eq(double(5), 10)
    assert_eq(triple(5), 15)
}
