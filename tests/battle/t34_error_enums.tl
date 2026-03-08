test "DataError NotFound" {
    let mut caught = nil
    try {
        throw DataError::NotFound("item missing")
    } catch e {
        caught = e
    }
    assert(caught != nil)
}

test "NetworkError TimeoutError" {
    let mut caught = nil
    try {
        throw NetworkError::TimeoutError("timed out")
    } catch e {
        caught = e
    }
    assert(caught != nil)
}

test "ConnectorError AuthError" {
    let mut caught = nil
    try {
        throw ConnectorError::AuthError("bad credentials")
    } catch e {
        caught = e
    }
    assert(caught != nil)
}
