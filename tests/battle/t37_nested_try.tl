test "nested try catch" {
    let mut log = ""
    try {
        try {
            throw "inner"
        } catch e {
            log = log + e + " "
            throw "outer"
        }
    } catch e2 {
        log = log + e2
    }
    assert_eq(log, "inner outer")
}

test "deeply nested" {
    let mut depth = 0
    try {
        try {
            try {
                throw "deep"
            } catch e {
                depth = 1
                throw e
            }
        } catch e {
            depth = 2
            throw e
        }
    } catch e {
        depth = 3
    }
    assert_eq(depth, 3)
}

test "inner catch no rethrow" {
    let mut outer_caught = false
    try {
        try {
            throw "handled"
        } catch e {
            // handled, no rethrow
        }
    } catch e {
        outer_caught = true
    }
    assert(not outer_caught)
}
