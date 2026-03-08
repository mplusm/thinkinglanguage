test "pipe to function" {
    fn double(x) { x * 2 }
    let r = 5 |> double()
    assert_eq(r, 10)
}

test "pipe chain" {
    let r = [1, 2, 3, 4, 5] |> filter((x) => x % 2 == 0) |> map((x) => x * 10)
    assert(r == [20, 40])
}

test "pipe with sum" {
    let r = [1, 2, 3, 4, 5] |> filter((x) => x > 2) |> sum()
    assert_eq(r, 12)
}

test "pipe with map" {
    let r = [1, 2, 3] |> map((x) => x + 10)
    assert(r == [11, 12, 13])
}
