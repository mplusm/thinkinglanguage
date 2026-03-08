test "pipeline performance tracking" {
    let input = range(100)
    let mut count = 0
    for x in input {
        count = count + 1
    }
    assert_eq(count, 100)
}

test "pipeline item counting" {
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    let filtered = data |> filter((x) => x % 2 == 0)
    assert_eq(len(filtered), 5)
    let mapped = filtered |> map((x) => x * x)
    assert_eq(len(mapped), 5)
}
