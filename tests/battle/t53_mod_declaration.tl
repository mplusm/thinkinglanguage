// t53 — Mod Declaration Concepts
// Tests: function scoping and nesting (mod declarations require file system)

test "nested function scoping" {
    fn outer_fn() {
        fn inner_fn() { 42 }
        inner_fn()
    }
    assert_eq(outer_fn(), 42)
}

test "function calling function" {
    fn helper(x) { x * 2 }
    fn main_fn(x) { helper(x) + 1 }
    assert_eq(main_fn(5), 11)
}

test "deeply nested functions" {
    fn level1() {
        fn level2() {
            fn level3() { 99 }
            level3()
        }
        level2()
    }
    assert_eq(level1(), 99)
}

test "function isolation" {
    fn make_adder(n) {
        fn add(x) { x + n }
        add
    }
    let add5 = make_adder(5)
    assert_eq(add5(10), 15)
}

test "multiple functions in sequence" {
    fn a() { 1 }
    fn b() { a() + 1 }
    fn c() { b() + 1 }
    assert_eq(c(), 3)
}
