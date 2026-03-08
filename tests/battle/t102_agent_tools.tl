// Agent Framework: Tool Registration Tests
// Tests that agent tool definitions are properly parsed and stored
// No LLM API calls required

fn search(query) {
    "result for: " + query
}

fn get_weather(city) {
    "sunny in " + city
}

fn calculate(expr) {
    "42"
}

test "agent with single tool" {
    agent bot {
        model: "gpt-4o",
        tools {
            search: {
                description: "Search the web",
                parameters: {}
            }
        }
    }
    assert_eq(type_of(bot), "agent")
}

test "agent with multiple tools" {
    agent bot {
        model: "gpt-4o",
        tools {
            search: {
                description: "Search the web",
                parameters: {}
            },
            get_weather: {
                description: "Get weather for a city",
                parameters: {}
            },
            calculate: {
                description: "Calculate a math expression",
                parameters: {}
            }
        }
    }
    assert_eq(type_of(bot), "agent")
}

test "agent with tool parameters" {
    agent bot {
        model: "gpt-4o",
        tools {
            search: {
                description: "Search the web",
                parameters: {
                    type: "object",
                    properties: {
                        query: { type: "string", description: "The search query" }
                    },
                    required: ["query"]
                }
            }
        }
    }
    assert_eq(type_of(bot), "agent")
}

test "agent with multi-param tool" {
    agent bot {
        model: "gpt-4o",
        tools {
            get_weather: {
                description: "Get weather",
                parameters: {
                    type: "object",
                    properties: {
                        city: { type: "string", description: "City name" },
                        unit: { type: "string", description: "celsius or fahrenheit" }
                    },
                    required: ["city"]
                }
            }
        }
    }
    assert_eq(type_of(bot), "agent")
}

test "tool functions exist and callable" {
    // Verify tool functions work independently when defined in test scope
    fn my_search(query) { "result for: " + query }
    fn my_weather(city) { "sunny in " + city }
    assert_eq(my_search("hello"), "result for: hello")
    assert_eq(my_weather("Paris"), "sunny in Paris")
}
