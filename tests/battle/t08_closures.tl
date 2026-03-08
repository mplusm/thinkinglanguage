// t08 — Closures
// Tests: expression closures, block closures, capture, higher-order, map/filter

test "expression closure" {
    let double = (x) => x * 2
    assert_eq(double(5), 10)
}

test "expression closure with add" {
    let add = (a, b) => a + b
    assert_eq(add(3, 4), 7)
}

test "block closure with return type" {
    let calc = (x) -> int64 {
        let y = x + 1
        y * 2
    }
    assert_eq(calc(3), 8)
}

test "capture variable" {
    let base = 10
    let add_base = (x) => x + base
    assert_eq(add_base(5), 15)
    assert_eq(add_base(0), 10)
}

test "closure as argument" {
    fn apply(f, x) {
        f(x)
    }
    assert_eq(apply((x) => x * 3, 7), 21)
}

test "closure returning closure" {
    let make_adder = (n) => (x) => x + n
    let add5 = make_adder(5)
    assert_eq(add5(10), 15)
    assert_eq(add5(0), 5)
}

test "closure in map builtin" {
    let nums = [1, 2, 3]
    let result = map(nums, (x) => x * 10)
    assert_eq(result[0], 10)
    assert_eq(result[1], 20)
    assert_eq(result[2], 30)
}

test "closure in filter builtin" {
    let nums = [1, 2, 3, 4, 5, 6]
    let evens = filter(nums, (x) => x % 2 == 0)
    assert_eq(len(evens), 3)
    assert_eq(evens[0], 2)
    assert_eq(evens[1], 4)
    assert_eq(evens[2], 6)
}

test "closure in reduce builtin" {
    let nums = [1, 2, 3, 4]
    let total = reduce(nums, 0, (acc, x) => acc + x)
    assert_eq(total, 10)
}

test "closure with map method" {
    let result = [1, 2, 3].map((x) => x * 2)
    assert_eq(len(result), 3)
    assert_eq(result[0], 2)
    assert_eq(result[1], 4)
    assert_eq(result[2], 6)
}

test "closure with filter method" {
    let result = [1, 2, 3, 4, 5].filter((x) => x > 3)
    assert_eq(len(result), 2)
    assert_eq(result[0], 4)
    assert_eq(result[1], 5)
}

test "closure identity" {
    let id = (x) => x
    assert_eq(id(42), 42)
    assert_eq(id("hello"), "hello")
    assert_eq(id(true), true)
}

test "block closure returning string" {
    let fmt = (name, age) -> string {
        let result = name + " is " + str(age)
        result
    }
    assert_eq(fmt("Alice", 30), "Alice is 30")
}

test "nested closures" {
    let outer = (x) => (y) => x + y
    let inner = outer(5)
    assert_eq(inner(10), 15)
}

test "closure with zero" {
    let zero = () => 0
    assert_eq(zero(), 0)
}
