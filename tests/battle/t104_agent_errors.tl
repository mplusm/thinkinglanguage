// Agent Framework: Error Handling Tests
// Tests that run_agent fails gracefully without API keys
// No LLM API calls succeed, but errors are caught properly

test "run_agent requires agent type" {
    let mut caught = false
    try {
        run_agent("not_an_agent", "hello")
    } catch e {
        caught = true
    }
    assert(caught)
}

test "run_agent requires string message" {
    agent bot {
        model: "gpt-4o"
    }
    let mut caught = false
    try {
        run_agent(bot, 42)
    } catch e {
        caught = true
    }
    assert(caught)
}

test "run_agent with no api key fails gracefully" {
    agent bot {
        model: "gpt-4o"
    }
    let mut caught = false
    let mut err_msg = ""
    try {
        run_agent(bot, "hello")
    } catch e {
        caught = true
        err_msg = str(e)
    }
    // Should fail because no API key is set
    assert(caught)
}

test "stream_agent requires 3 args" {
    agent bot {
        model: "gpt-4o"
    }
    let mut caught = false
    try {
        stream_agent(bot, "hello")
    } catch e {
        caught = true
    }
    assert(caught)
}

test "stream_agent requires agent type" {
    let mut caught = false
    try {
        stream_agent("not_agent", "hello", (chunk) => print(chunk))
    } catch e {
        caught = true
    }
    assert(caught)
}
