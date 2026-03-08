// t56 — Move Semantics with Pipe Operator
// Tests: pipe |> moves value, chained pipes, pipe to custom functions

test "pipe moves value" {
    let data = [1, 2, 3, 4, 5]
    let result = data |> map((x) => x * 2)
    // After pipe, data is moved
    // Verify the result is correct using element access
    assert_eq(len(result), 5)
    assert_eq(result[0], 2)
    assert_eq(result[4], 10)
}

test "chained pipe with sum" {
    let result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        |> filter((x) => x % 2 == 0)
        |> map((x) => x * x)
        |> sum()
    assert_eq(result, 220)
}

test "pipe to custom function" {
    fn increment_all(items) {
        map(items, (x) => x + 1)
    }
    let result = [10, 20, 30] |> increment_all()
    assert(result == [11, 21, 31])
}

test "pipe filter then sum" {
    let result = [1, 2, 3, 4, 5] |> filter((x) => x > 2) |> sum()
    assert_eq(result, 12)
}

test "pipe with transform chain" {
    fn negate(x) { -x }
    let result = 5 |> negate()
    assert_eq(result, -5)
}

test "pipe map preserves order" {
    let result = [3, 1, 2] |> map((x) => x * 100)
    assert_eq(result[0], 300)
    assert_eq(result[1], 100)
    assert_eq(result[2], 200)
}

test "pipe chain multiple stages" {
    let result = range(10)
        |> filter((x) => x >= 3)
        |> filter((x) => x < 7)
        |> map((x) => x * 2)
        |> sum()
    // 3+4+5+6 doubled = 6+8+10+12 = 36
    assert_eq(result, 36)
}

test "use after move is error" {
    let data = [1, 2, 3]
    let result = data |> sum()
    assert_eq(result, 6)
    let mut caught = false
    try {
        let x = len(data)
    } catch e {
        caught = true
    }
    assert(caught)
}
