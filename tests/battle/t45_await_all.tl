test "await_all multiple tasks" {
    fn a() { 1 }
    fn b() { 2 }
    fn c() { 3 }
    let tasks = [spawn(a), spawn(b), spawn(c)]
    let results = await_all(tasks)
    assert_eq(len(results), 3)
    // Results should contain 1, 2, 3 (order may vary by implementation)
    let total = sum(results)
    assert_eq(total, 6)
}

test "await_all single task" {
    fn work() { 42 }
    let results = await_all([spawn(work)])
    assert_eq(len(results), 1)
    assert_eq(results[0], 42)
}
