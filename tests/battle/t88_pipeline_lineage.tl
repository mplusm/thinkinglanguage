test "data lineage concept" {
    // Test tracking data through transformations
    let input = [1, 2, 3, 4, 5]
    let step1 = input.clone() |> map((x) => x * 2)
    let step2 = step1.clone() |> filter((x) => x > 4)
    // Verify intermediate results for lineage before consuming step2
    assert_eq(len(step2), 3)
    let step3 = step2 |> sum()
    assert_eq(step3, 24)
}
