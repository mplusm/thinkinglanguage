// Agent Framework: Definition & Structure Tests
// Tests agent declaration syntax, type_of, and print representation
// No LLM API calls required

test "minimal agent definition" {
    agent minimal {
        model: "gpt-4o"
    }
    assert_eq(type_of(minimal), "agent")
}

test "agent print representation" {
    agent my_bot {
        model: "gpt-4o"
    }
    assert_eq(str(my_bot), "<agent my_bot>")
}

test "agent with system prompt" {
    agent helper {
        model: "gpt-4o",
        system: "You are a helpful assistant."
    }
    assert_eq(type_of(helper), "agent")
}

test "agent with max_turns" {
    agent limited {
        model: "gpt-4o",
        max_turns: 3
    }
    assert_eq(type_of(limited), "agent")
}

test "agent with base_url" {
    agent local {
        model: "llama3",
        base_url: "http://localhost:11434/v1",
        max_turns: 2
    }
    assert_eq(str(local), "<agent local>")
}

test "agent with claude model" {
    agent claude_bot {
        model: "claude-sonnet-4-20250514"
    }
    assert_eq(type_of(claude_bot), "agent")
}

test "agent with all config options" {
    agent full_bot {
        model: "gpt-4o",
        system: "Be concise.",
        max_turns: 10,
        temperature: 0.7,
        max_tokens: 1000,
        output_format: "json"
    }
    assert_eq(type_of(full_bot), "agent")
    assert_eq(str(full_bot), "<agent full_bot>")
}
