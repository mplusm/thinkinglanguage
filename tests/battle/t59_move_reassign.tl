// t59 — Move and Reassignment
// Tests: reassign after move clears moved state, move + reassign in loops

test "reassign after move" {
    let mut data = [1, 2, 3]
    let result = data |> sum()
    assert_eq(result, 6)
    // Reassign clears the moved state
    data = [4, 5, 6]
    assert_eq(sum(data), 15)
}

test "move then reassign in loop" {
    let mut total = 0
    for i in range(3) {
        let mut batch = [i * 10, i * 10 + 1]
        let s = batch |> sum()
        total = total + s
        batch = []  // reassign after move
    }
    assert_eq(total, 63)
}

test "reassign different type after move" {
    let mut x = [1, 2, 3]
    let moved = x |> sum()
    // Reassign to a new list
    x = [100]
    assert_eq(x[0], 100)
    assert_eq(len(x), 1)
}

test "multiple moves with reassign" {
    let mut data = [1]
    let r1 = data |> sum()
    assert_eq(r1, 1)

    data = [2, 3]
    let r2 = data |> sum()
    assert_eq(r2, 5)

    data = [4, 5, 6]
    let r3 = data |> sum()
    assert_eq(r3, 15)
}

test "reassign in conditional" {
    let mut x = [10, 20]
    let s = x |> sum()
    assert_eq(s, 30)

    if s > 0 {
        x = [99]
    } else {
        x = [0]
    }
    assert_eq(x[0], 99)
}

test "move and clone interplay" {
    let original = [1, 2, 3]
    let backup = original.clone()
    // Move original via pipe
    let result = original |> sum()
    assert_eq(result, 6)
    // backup is still accessible
    assert(backup == [1, 2, 3])
}
