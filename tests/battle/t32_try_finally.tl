test "finally runs on success" {
    let mut log = ""
    try {
        log = log + "try "
    } catch e {
        log = log + "catch "
    } finally {
        log = log + "finally"
    }
    assert_eq(log, "try finally")
}

test "finally runs on error" {
    let mut log = ""
    try {
        log = log + "try "
        throw "err"
    } catch e {
        log = log + "catch "
    } finally {
        log = log + "finally"
    }
    assert_eq(log, "try catch finally")
}

test "finally runs even with empty catch" {
    let mut ran = false
    try {
        let x = 1
    } catch e {
        // no-op
    } finally {
        ran = true
    }
    assert(ran)
}
