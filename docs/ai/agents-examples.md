# Agent Framework Examples

Practical examples showing how to build AI agents with TL, from simple to advanced.

## Example 1: Minimal Agent (No Tools)

The simplest possible agent -- just an LLM with a system prompt.

```tl
agent greeter {
    model: "gpt-4o-mini",
    system: "You are a friendly greeting bot. Keep responses under 20 words."
}

let result = run_agent(greeter, "Hello!")
println(result.response)
```

## Example 2: Single Tool Agent

An agent that can look up information using a tool function.

```tl
fn get_capital(country) {
    // In production, this could query a database or API
    let capitals = {
        "France": "Paris",
        "Japan": "Tokyo",
        "Brazil": "Brasilia",
        "Australia": "Canberra"
    }
    let cap = capitals[country]
    if cap != none {
        "The capital of " + country + " is " + cap + "."
    } else {
        "Capital not found for: " + country
    }
}

agent geo_bot {
    model: "gpt-4o-mini",
    system: "You answer geography questions. Always use the get_capital tool before answering.",
    tools {
        get_capital: {
            description: "Look up the capital city of a country",
            parameters: {
                type: "object",
                properties: {
                    country: { type: "string", description: "The country name" }
                },
                required: ["country"]
            }
        }
    },
    max_turns: 3
}

let result = run_agent(geo_bot, "What is the capital of Japan?")
println(result.response)
println("Turns: " + string(result.turns))
```

## Example 3: Multi-Tool Research Agent

An agent with multiple tools that can search, fetch web pages, and do calculations.

```tl
fn web_search(query) {
    let resp = http_request(
        "GET",
        "https://api.search.example.com/search?q=" + query,
        {"Authorization": "Bearer " + env("SEARCH_KEY")},
        none
    )
    if resp.status == 200 {
        resp.body
    } else {
        "Search failed: HTTP " + string(resp.status)
    }
}

fn fetch_url(url) {
    let resp = http_request("GET", url, none, none)
    if resp.status == 200 {
        // Truncate long responses
        let body = resp.body
        if len(body) > 2000 {
            string_slice(body, 0, 2000) + "... [truncated]"
        } else {
            body
        }
    } else {
        "Failed to fetch: HTTP " + string(resp.status)
    }
}

fn calculate(expression) {
    string(eval(expression))
}

agent researcher {
    model: "gpt-4o",
    system: "You are a research assistant. Use tools to gather information, then synthesize a clear answer.",
    tools {
        web_search: {
            description: "Search the web for information",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Search query" }
                },
                required: ["query"]
            }
        },
        fetch_url: {
            description: "Fetch the content of a web page",
            parameters: {
                type: "object",
                properties: {
                    url: { type: "string", description: "URL to fetch" }
                },
                required: ["url"]
            }
        },
        calculate: {
            description: "Evaluate a mathematical expression",
            parameters: {
                type: "object",
                properties: {
                    expression: { type: "string", description: "Math expression (e.g., '2 + 3 * 4')" }
                },
                required: ["expression"]
            }
        }
    },
    max_turns: 8
}

let result = run_agent(researcher, "How many seconds are in a leap year?")
println(result.response)
```

## Example 4: Local LLM via Ollama

Use a locally-running LLM through an OpenAI-compatible endpoint.

```tl
fn list_files(directory) {
    // This would use real file I/O in production
    "Documents:\n- report.pdf\n- notes.txt\n- data.csv"
}

agent local_assistant {
    model: "llama3",
    base_url: "http://localhost:11434/v1",
    system: "You are a local file assistant. Help users find and manage their files.",
    tools {
        list_files: {
            description: "List files in a directory",
            parameters: {
                type: "object",
                properties: {
                    directory: { type: "string", description: "Directory path" }
                },
                required: ["directory"]
            }
        }
    },
    max_turns: 5
}

let result = run_agent(local_assistant, "What files do I have in my Documents folder?")
println(result.response)
```

## Example 5: Agent with Logging (Lifecycle Hooks)

Track every tool call and the final result using lifecycle hooks.

```tl
fn search(query) {
    "Results for '" + query + "': Found 3 relevant documents."
}

fn summarize(text) {
    "Summary: " + string_slice(text, 0, 50) + "..."
}

agent logged_researcher {
    model: "gpt-4o",
    system: "You are a research assistant. Search first, then summarize your findings.",
    tools {
        search: {
            description: "Search for documents on a topic",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Search query" }
                },
                required: ["query"]
            }
        },
        summarize: {
            description: "Summarize a piece of text",
            parameters: {
                type: "object",
                properties: {
                    text: { type: "string", description: "Text to summarize" }
                },
                required: ["text"]
            }
        }
    },
    max_turns: 6,
    on_tool_call {
        println("[TOOL] " + tool_name)
        println("  args: " + tool_args)
        println("  result: " + tool_result)
        println("")
    }
    on_complete {
        println("---")
        println("Agent completed in " + string(result.turns) + " turns")
        println("Response length: " + string(len(result.response)) + " chars")
    }
}

let result = run_agent(logged_researcher, "Find information about quantum computing")
println("\nFinal response:")
println(result.response)
```

**Expected output:**
```
[TOOL] search
  args: {"query": "quantum computing"}
  result: Results for 'quantum computing': Found 3 relevant documents.

---
Agent completed in 2 turns
Response length: 142 chars

Final response:
Based on my search, I found 3 relevant documents about quantum computing...
```

## Example 6: Data Pipeline Agent

An agent that can query data and generate insights.

```tl
fn query_sales(region) {
    // Simulate database query
    if region == "north" {
        json_stringify({"total": 150000, "orders": 342, "avg_order": 438.60})
    } else if region == "south" {
        json_stringify({"total": 98000, "orders": 215, "avg_order": 455.81})
    } else {
        json_stringify({"total": 0, "orders": 0, "avg_order": 0})
    }
}

fn query_inventory(product) {
    json_stringify({"product": product, "stock": 1250, "reorder_point": 500})
}

agent data_analyst {
    model: "gpt-4o",
    system: "You are a data analyst. Query sales and inventory data to answer business questions. Present numbers clearly.",
    tools {
        query_sales: {
            description: "Query sales data for a region (north, south, east, west)",
            parameters: {
                type: "object",
                properties: {
                    region: { type: "string", description: "Sales region" }
                },
                required: ["region"]
            }
        },
        query_inventory: {
            description: "Check inventory levels for a product",
            parameters: {
                type: "object",
                properties: {
                    product: { type: "string", description: "Product name" }
                },
                required: ["product"]
            }
        }
    },
    max_turns: 8,
    temperature: 0.3
}

let result = run_agent(data_analyst, "Compare sales between north and south regions")
println(result.response)
```

## Example 7: Embeddings + Similarity Search

Use embeddings to find the most relevant document for a query.

```tl
// Document corpus
let documents = [
    "Machine learning is a subset of artificial intelligence",
    "The stock market closed higher today on strong earnings",
    "New climate report warns of rising sea levels",
    "Python is the most popular language for data science",
    "Interest rates are expected to remain steady"
]

// Embed all documents
let doc_embeddings = []
for doc in documents {
    doc_embeddings = doc_embeddings + [embed(doc)]
}

// Embed the query
let query = embed("What programming language is used for AI?")

// Find most similar document
let best_score = -1.0
let best_idx = 0
for i in range(len(documents)) {
    let score = similarity(query, doc_embeddings[i])
    println(string(i) + ": " + string(score) + " - " + documents[i])
    if score > best_score {
        best_score = score
        best_idx = i
    }
}

println("\nMost relevant: " + documents[best_idx])
println("Score: " + string(best_score))
```

## Example 8: Error Handling

Robust agent usage with error handling.

```tl
fn unreliable_api(endpoint) {
    let resp = http_request("GET", endpoint, none, none)
    if resp.status != 200 {
        throw "API returned " + string(resp.status)
    }
    resp.body
}

agent cautious_bot {
    model: "gpt-4o-mini",
    system: "You fetch data from APIs. If a tool returns an error, explain the issue to the user.",
    tools {
        unreliable_api: {
            description: "Fetch data from an API endpoint",
            parameters: {
                type: "object",
                properties: {
                    endpoint: { type: "string", description: "API endpoint URL" }
                },
                required: ["endpoint"]
            }
        }
    },
    max_turns: 3
}

try {
    let result = run_agent(cautious_bot, "Fetch data from https://api.example.com/data")
    println(result.response)
} catch e {
    println("Agent failed: " + e)
}
```

## Example 9: Claude via Anthropic API

Using Claude models with the native Anthropic API.

```tl
fn get_fact(topic) {
    "Interesting fact about " + topic + ": it was discovered in 1905."
}

agent claude_agent {
    model: "claude-sonnet-4-20250514",
    system: "You are a trivia assistant. Use the get_fact tool to look up facts.",
    tools {
        get_fact: {
            description: "Get an interesting fact about a topic",
            parameters: {
                type: "object",
                properties: {
                    topic: { type: "string", description: "The topic to look up" }
                },
                required: ["topic"]
            }
        }
    },
    max_turns: 3
}

// Requires TL_ANTHROPIC_KEY environment variable
let result = run_agent(claude_agent, "Tell me something interesting about relativity")
println(result.response)
```

## Example 10: Combining Agents with Pipelines

Use agent results in TL's data pipeline system.

```tl
fn classify_sentiment(text) {
    let response = ai_complete(
        "Classify the sentiment of this text as 'positive', 'negative', or 'neutral'. " +
        "Respond with only one word.\n\nText: " + text
    )
    response
}

// Read customer feedback
let data = read_csv("feedback.csv")

// Use AI to classify each row
data
    |> with(sentiment = classify_sentiment(feedback_text))
    |> filter(sentiment == "negative")
    |> sort(date, "desc")
    |> head(10)
    |> show()
```

## Example 11: Agent with MCP Server Tools

Use external MCP servers instead of defining tools in TL. The agent auto-discovers all tools
from connected servers. Requires `--features mcp`.

```tl
// Connect to a filesystem MCP server
let fs = mcp_connect("npx", ["-y", "@modelcontextprotocol/server-filesystem", "/home/user/docs"])

// Agent gets all filesystem tools automatically
agent file_assistant {
    model: "gpt-4o",
    system: "You help users find and read files. Use your tools to browse directories and read file contents.",
    mcp_servers: [fs],
    max_turns: 5
}

let result = run_agent(file_assistant, "Find all CSV files and show me the first one")
println(result.response)

mcp_disconnect(fs)
```

The `mcp_servers` field merges external tools with any native `tools { }` block. This lets you
combine TL functions with MCP server capabilities in a single agent.

## Example 12: Agent with Multiple MCP Servers

Connect to several MCP servers simultaneously -- tools from all servers are merged.

```tl
// File system tools
let fs = mcp_connect("npx", ["-y", "@modelcontextprotocol/server-filesystem", "/data"])

// Database tools (HTTP-based MCP server)
let db = mcp_connect("http://localhost:8080/mcp")

agent analyst {
    model: "gpt-4o",
    system: "You are a data analyst. You can read files and query databases to answer questions.",
    mcp_servers: [fs, db],
    max_turns: 8
}

let result = run_agent(analyst, "Read the schema from config.json, then query the users table")
println(result.response)

mcp_disconnect(fs)
mcp_disconnect(db)
```

## Example 13: MCP + Native Tools Combined

Mix MCP server tools with locally-defined TL tool functions in the same agent.

```tl
let fs = mcp_connect("npx", ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"])

// A local TL function as a tool
fn summarize(text) {
    let words = split(text, " ")
    if len(words) > 50 {
        join(slice(words, 0, 50), " ") + "..."
    } else {
        text
    }
}

agent reader {
    model: "gpt-4o",
    system: "Read files and summarize them for the user.",
    mcp_servers: [fs],
    tools {
        summarize: {
            description: "Summarize a long text to ~50 words",
            parameters: {
                type: "object",
                properties: {
                    text: { type: "string", description: "Text to summarize" }
                },
                required: ["text"]
            }
        }
    },
    max_turns: 5
}

let result = run_agent(reader, "Read /tmp/report.txt and give me a summary")
println(result.response)
mcp_disconnect(fs)
```

## Example 14: MCP Client Without Agents

Use MCP servers directly without the agent framework -- call tools, read resources, and use prompts manually.

```tl
let client = mcp_connect("npx", ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"])

// Discover available tools
let tools = mcp_list_tools(client)
println("Available tools:")
for tool in tools {
    println("  " + tool["name"] + " - " + tool["description"])
}

// Call a tool directly
let result = mcp_call_tool(client, "list_directory", {"path": "/tmp"})
println(result)

// Check server health
let info = mcp_server_info(client)
println("Server: " + info["name"] + " v" + info["version"])
mcp_ping(client)

// Resources (if the server supports them)
let resources = mcp_list_resources(client)
for r in resources {
    let content = mcp_read_resource(client, r["uri"])
    println(r["name"] + ": " + content)
}

mcp_disconnect(client)
```

## Running the Examples

### Prerequisites

1. Set up API keys:
```bash
# For OpenAI models
export TL_OPENAI_KEY="sk-..."

# For Anthropic models
export TL_ANTHROPIC_KEY="sk-ant-..."

# For local models (Ollama)
# No API key needed, just ensure Ollama is running
```

2. For MCP examples (11-14), build with MCP support:
```bash
cargo build --release --features mcp
```

3. Run an example:
```bash
tl run examples/agent_01_basic.tl
```

### Testing Without API Keys

You can test agent definitions without API keys -- the agent struct is created locally. Only `run_agent()` requires an API connection:

```tl
// This works without any API key
agent bot {
    model: "gpt-4o",
    system: "You are helpful."
}

println(type_of(bot))    // "agent"
println(bot)             // <agent bot>

// This requires an API key
// let result = run_agent(bot, "Hello")
```
