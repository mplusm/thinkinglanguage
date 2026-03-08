test "is_error on normal value" {
    assert(not is_error(42))
    assert(not is_error("hello"))
}

test "error_type on DataError" {
    let mut etype = ""
    try {
        throw DataError::NotFound("x")
    } catch e {
        etype = error_type(e)
    }
    assert(etype != "")
}
