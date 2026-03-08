// t60 — Move Semantics in Loops
// Tests: loop variables not moved, pipe inside loop bodies, accumulation patterns

test "loop variable not moved" {
    let items = [1, 2, 3]
    let mut total = 0
    for x in items {
        total = total + x
    }
    assert_eq(total, 6)
}

test "pipe inside loop" {
    let mut results = []
    for i in range(3) {
        let batch = [i, i + 1]
        let doubled = batch |> map((x) => x * 2)
        results = push(results, sum(doubled))
    }
    assert(results == [2, 6, 10])
}

test "accumulate via pipe in loop" {
    let mut total = 0
    for i in range(5) {
        let data = [i]
        let s = data |> sum()
        total = total + s
    }
    // 0 + 1 + 2 + 3 + 4 = 10
    assert_eq(total, 10)
}

test "fresh variable each iteration" {
    let mut sums = []
    for i in range(4) {
        let batch = [i * 2, i * 2 + 1]
        sums = push(sums, batch |> sum())
    }
    // [0+1, 2+3, 4+5, 6+7] = [1, 5, 9, 13]
    assert(sums == [1, 5, 9, 13])
}

test "nested loops with pipe" {
    let mut total = 0
    for i in range(3) {
        for j in range(3) {
            let pair = [i, j]
            total = total + (pair |> sum())
        }
    }
    // Each i appears 3 times, each j appears 3 times
    // sum of i*3 for i in 0..3 = 0+3+6 = 9
    // sum of j*3 for j in 0..3 = 0+3+6 = 9
    // total = 9 + 9 = 18
    assert_eq(total, 18)
}

test "while loop with pipe" {
    let mut count = 0
    let mut total = 0
    while count < 4 {
        let data = [count, count + 1]
        total = total + (data |> sum())
        count = count + 1
    }
    // [0+1] + [1+2] + [2+3] + [3+4] = 1 + 3 + 5 + 7 = 16
    assert_eq(total, 16)
}
