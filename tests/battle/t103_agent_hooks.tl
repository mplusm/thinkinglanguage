// Agent Framework: Lifecycle Hooks Tests
// Tests on_tool_call and on_complete hooks are stored as callable functions
// No LLM API calls required

test "agent hooks stored as functions" {
    fn search(q) { "result" }
    agent bot {
        model: "gpt-4o",
        tools {
            search: { description: "Search", parameters: {} }
        },
        on_tool_call {
            print("tool called")
        }
        on_complete {
            print("done")
        }
    }
    assert_eq(type_of(bot), "agent")
    // Hooks are stored as global functions with mangled names
    assert_eq(type_of(__agent_bot_on_tool_call__), "function")
    assert_eq(type_of(__agent_bot_on_complete__), "function")
}

test "on_tool_call hook callable" {
    agent bot2 {
        model: "gpt-4o",
        on_tool_call {
            "tool: " + tool_name + " result: " + tool_result
        }
    }
    // Hook receives tool_name, query, tool_result as parameters
    let result = __agent_bot2_on_tool_call__("search", "query", "found it")
    assert_eq(result, "tool: search result: found it")
}

test "on_complete hook callable" {
    agent bot3 {
        model: "gpt-4o",
        on_complete {
            "completed: " + result
        }
    }
    let r = __agent_bot3_on_complete__("hello world")
    assert_eq(r, "completed: hello world")
}

test "agent without hooks" {
    agent simple {
        model: "gpt-4o"
    }
    assert_eq(type_of(simple), "agent")
    // No hooks means no mangled globals
}

test "agent with only on_tool_call" {
    agent partial {
        model: "gpt-4o",
        on_tool_call {
            print("tool used")
        }
    }
    assert_eq(type_of(__agent_partial_on_tool_call__), "function")
}
