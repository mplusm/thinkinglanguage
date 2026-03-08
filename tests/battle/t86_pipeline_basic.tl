test "pipeline construct" {
    // Simple pipeline test - create and verify it exists
    let data = [1, 2, 3, 4, 5]
    let result = data
        |> map((x) => x * 2)
        |> filter((x) => x > 4)
        |> sum()
    assert_eq(result, 24)
}

test "multi-stage transform" {
    let words = ["hello", "world", "foo"]
    let result = words
        |> map((w) => w.to_upper())
        |> filter((w) => w.len() > 3)
    assert(result == ["HELLO", "WORLD"])
}
