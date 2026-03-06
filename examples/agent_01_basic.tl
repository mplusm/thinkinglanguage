// Agent Framework Example
// Requires: TL_OPENAI_KEY or TL_ANTHROPIC_KEY environment variable

// Define a tool function that the agent can call
fn search(query) {
    // In a real scenario, this would call an API
    "Result for: " + query + " — Paris is the capital of France."
}

fn get_weather(city) {
    "Weather in " + city + ": 22°C, sunny"
}

// Define an agent with model, system prompt, and tools
agent research_bot {
    model: "gpt-4o",
    system: "You are a helpful research assistant. Use the search tool to find information.",
    tools {
        search: {
            description: "Search for information on a topic",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "The search query" }
                },
                required: ["query"]
            }
        },
        get_weather: {
            description: "Get the current weather for a city",
            parameters: {
                type: "object",
                properties: {
                    city: { type: "string", description: "The city name" }
                },
                required: ["city"]
            }
        }
    },
    max_turns: 5
}

// Run the agent (requires API key)
// let result = run_agent(research_bot, "What is the capital of France?")
// println(result.response)
// println("Turns used: " + string(result.turns))

println("Agent 'research_bot' defined successfully")
println("To run: let result = run_agent(research_bot, \"your question\")")
