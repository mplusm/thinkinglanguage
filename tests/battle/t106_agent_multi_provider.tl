// Agent Framework: Multi-Provider Configuration Tests
// Tests different model/provider configurations
// No LLM API calls required - structural tests only

test "openai model detection" {
    agent gpt_bot {
        model: "gpt-4o"
    }
    assert_eq(type_of(gpt_bot), "agent")
}

test "anthropic model detection" {
    agent claude_bot {
        model: "claude-sonnet-4-20250514"
    }
    assert_eq(type_of(claude_bot), "agent")
}

test "custom model with base_url" {
    agent ollama_bot {
        model: "llama3",
        base_url: "http://localhost:11434/v1"
    }
    assert_eq(type_of(ollama_bot), "agent")
}

test "agent with api_key" {
    agent keyed_bot {
        model: "gpt-4o",
        api_key: "sk-test-key-12345"
    }
    assert_eq(type_of(keyed_bot), "agent")
}

test "agent with custom temperature" {
    agent creative_bot {
        model: "gpt-4o",
        temperature: 0.9,
        max_tokens: 2000
    }
    assert_eq(type_of(creative_bot), "agent")
}

test "agent with json output format" {
    agent json_bot {
        model: "gpt-4o",
        output_format: "json"
    }
    assert_eq(type_of(json_bot), "agent")
}

test "multiple agents in same scope" {
    agent bot_a {
        model: "gpt-4o"
    }
    agent bot_b {
        model: "claude-sonnet-4-20250514"
    }
    assert_eq(type_of(bot_a), "agent")
    assert_eq(type_of(bot_b), "agent")
    assert_eq(str(bot_a), "<agent bot_a>")
    assert_eq(str(bot_b), "<agent bot_b>")
}

test "agent with full tool and system" {
    fn lookup(id) { "item " + id }
    agent full_agent {
        model: "gpt-4o",
        system: "You help look up items by ID.",
        tools {
            lookup: {
                description: "Look up an item by its ID",
                parameters: {
                    type: "object",
                    properties: {
                        id: { type: "string", description: "The item ID" }
                    },
                    required: ["id"]
                }
            }
        },
        max_turns: 5,
        temperature: 0.3
    }
    assert_eq(type_of(full_agent), "agent")
}
