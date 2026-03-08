test "throw string" {
    let mut msg = ""
    try { throw "boom" } catch e { msg = e }
    assert_eq(msg, "boom")
}

test "throw number" {
    let mut val = 0
    try { throw 42 } catch e { val = e }
    assert_eq(val, 42)
}

test "throw in function" {
    fn fail() { throw "failed" }
    let mut caught = false
    try { fail() } catch e { caught = true }
    assert(caught)
}

test "rethrow" {
    let mut msg = ""
    try {
        try {
            throw "inner"
        } catch e {
            throw "outer"
        }
    } catch e2 {
        msg = e2
    }
    assert_eq(msg, "outer")
}
