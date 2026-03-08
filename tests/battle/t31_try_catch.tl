test "catch thrown string" {
    let mut caught = ""
    try {
        throw "oops"
    } catch e {
        caught = e
    }
    assert_eq(caught, "oops")
}

test "catch runtime error" {
    let mut caught = ""
    try {
        let x = 1 / 0
    } catch e {
        caught = e
    }
    assert_eq(caught, "Division by zero")
}

test "try without error" {
    let mut result = 0
    try {
        result = 42
    } catch e {
        result = -1
    }
    assert_eq(result, 42)
}

test "catch and continue" {
    let mut val = 0
    try {
        throw "err"
    } catch e {
        val = 1
    }
    val = val + 1
    assert_eq(val, 2)
}
