test "undefined variable resolves to none" {
    // In TL, undefined globals resolve to none rather than throwing
    let x = undefined_var_xyz
    assert_eq(x, nil)
}

test "wrong type in arithmetic" {
    let mut caught = false
    try {
        let x = "hello" + 5
    } catch e {
        caught = true
    }
    assert(caught)
}

test "index out of bounds" {
    let mut caught = false
    try {
        let x = [1, 2, 3]
        let y = x[10]
    } catch e {
        caught = true
    }
    assert(caught)
}

test "negative index out of bounds" {
    let mut caught = false
    try {
        let x = [1, 2]
        let y = x[-10]
    } catch e {
        caught = true
    }
    assert(caught)
}

test "call non-function" {
    let mut caught = false
    try {
        let x = 42
        x()
    } catch e {
        caught = true
    }
    assert(caught)
}

test "access field on non-struct returns none" {
    // Field access on non-struct returns none rather than throwing
    let x = 42
    let y = x.field
    assert_eq(y, nil)
}

test "division by zero" {
    let mut caught = false
    try {
        let x = 10 / 0
    } catch e {
        caught = true
    }
    assert(caught)
}

test "use after pipe move" {
    let mut caught = false
    try {
        let data = [1, 2, 3]
        let s = data |> sum()
        // data is now moved
        print(data)
    } catch e {
        caught = true
    }
    assert(caught)
}

test "ref mutation blocked" {
    let mut caught = false
    try {
        let x = [1, 2, 3]
        let r = &x
        r[0] = 99
    } catch e {
        caught = true
    }
    assert(caught)
}

test "assert_eq fails on mismatch" {
    let mut caught = false
    try {
        assert_eq(1, 2)
    } catch e {
        caught = true
    }
    assert(caught)
}

test "assert false fails" {
    let mut caught = false
    try {
        assert(false)
    } catch e {
        caught = true
    }
    assert(caught)
}
