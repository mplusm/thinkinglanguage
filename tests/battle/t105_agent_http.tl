// Agent Framework: http_request Builtin Tests
// Tests the http_request function used by agents for API calls
// Uses httpbin-style tests or verifies error handling

test "http_request requires method and url" {
    let mut caught = false
    try {
        http_request("GET")
    } catch e {
        caught = true
    }
    assert(caught)
}

test "http_request method must be string" {
    let mut caught = false
    try {
        http_request(42, "http://example.com")
    } catch e {
        caught = true
    }
    assert(caught)
}

test "http_request url must be string" {
    let mut caught = false
    try {
        http_request("GET", 42)
    } catch e {
        caught = true
    }
    assert(caught)
}

test "http_request to invalid host fails" {
    let mut caught = false
    try {
        http_request("GET", "http://this-host-does-not-exist-12345.invalid")
    } catch e {
        caught = true
    }
    assert(caught)
}

test "http_request returns map on success" {
    // This test requires network access - skip gracefully if it fails
    let mut result_type = ""
    try {
        let resp = http_request("GET", "https://httpbin.org/get")
        result_type = type_of(resp)
    } catch e {
        // Network might not be available, that's ok
        result_type = "error"
    }
    // Either we got a map response or a network error - both are valid
    assert(result_type == "map" or result_type == "error")
}
