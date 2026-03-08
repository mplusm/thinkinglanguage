test "parallel for basic" {
    let items = [10, 20, 30]
    let mut results = []
    parallel for item in items {
        results = push(results, item)
    }
    // All items should be processed (order may vary)
    assert_eq(len(results), 3)
    assert_eq(sum(results), 60)
}
