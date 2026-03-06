# AI Agent Framework

TL provides a first-class `agent` construct for building AI agents that can use tools, call LLMs, and run multi-turn conversations autonomously. Agents are defined declaratively and executed with a single function call.

## Quick Start

```tl
// 1. Define a tool function
fn get_weather(city) {
    "Weather in " + city + ": 22°C, sunny"
}

// 2. Define an agent
agent weather_bot {
    model: "gpt-4o",
    system: "You are a weather assistant. Use the get_weather tool to answer questions.",
    tools {
        get_weather: {
            description: "Get current weather for a city",
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

// 3. Run the agent
let result = run_agent(weather_bot, "What's the weather in Tokyo?")
println(result.response)
println("Completed in " + string(result.turns) + " turns")
```

**Requirements:** Set the `TL_OPENAI_KEY` or `TL_ANTHROPIC_KEY` environment variable before running.

## Table of Contents

- [Agent Definition](#agent-definition)
- [Tool Functions](#tool-functions)
- [Running Agents](#running-agents)
- [Multi-Provider Support](#multi-provider-support)
- [Lifecycle Hooks](#lifecycle-hooks)
- [LLM Functions](#llm-functions)
- [HTTP Requests](#http-requests)
- [Embeddings](#embeddings)
- [Environment Variables](#environment-variables)
- [Error Handling](#error-handling)
- [Limitations](#limitations)

## Agent Definition

The `agent` keyword defines an agent with its configuration:

```tl
agent <name> {
    model: "<model-name>",              // required
    system: "<system-prompt>",          // optional
    tools { ... },                      // optional
    max_turns: <integer>,               // optional (default: 10)
    temperature: <float>,               // optional
    max_tokens: <integer>,              // optional
    base_url: "<url>",                  // optional
    api_key: "<key>",                   // optional
    on_tool_call { ... },               // optional lifecycle hook
    on_complete { ... }                 // optional lifecycle hook
}
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `model` | string | yes | -- | The LLM model identifier (e.g., `"gpt-4o"`, `"claude-sonnet-4-20250514"`) |
| `system` | string | no | none | System prompt that guides agent behavior |
| `tools` | block | no | none | Tool definitions the agent can call |
| `max_turns` | integer | no | `10` | Maximum conversation turns before stopping |
| `temperature` | float | no | provider default | Sampling temperature (0.0 = deterministic, 1.0 = creative) |
| `max_tokens` | integer | no | provider default | Maximum tokens in each LLM response |
| `base_url` | string | no | provider default | Custom API endpoint (any OpenAI-compatible URL) |
| `api_key` | string | no | from env vars | API key (overrides environment variables) |
| `on_tool_call` | block | no | none | Code to run after each tool call |
| `on_complete` | block | no | none | Code to run when the agent produces a final response |

### Minimal Agent

Only `model` is required:

```tl
agent simple_bot {
    model: "gpt-4o-mini"
}

let result = run_agent(simple_bot, "Say hello")
println(result.response)
```

### Agent Values

Agents are first-class values in TL:

```tl
agent bot { model: "gpt-4o" }

println(type_of(bot))       // "agent"
println(bot)                // <agent bot>
```

## Tool Functions

Tools connect TL functions to the agent's LLM. When the LLM decides to use a tool, TL automatically calls the corresponding function and feeds the result back.

### Defining Tools

Each tool needs:
1. A TL function with the same name
2. A tool definition in the agent's `tools` block

```tl
// Step 1: Define the TL function
fn search(query) {
    // Real implementation would call an API
    let resp = http_request("GET", "https://api.example.com/search?q=" + query, none, none)
    json_parse(resp.body)
}

// Step 2: Reference it in the agent
agent researcher {
    model: "gpt-4o",
    system: "You are a research assistant.",
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
        }
    }
}
```

### Tool Definition Format

Tool definitions follow the OpenAI function-calling JSON schema format:

```tl
tools {
    <function_name>: {
        description: "<what this tool does>",
        parameters: {
            type: "object",
            properties: {
                <param_name>: {
                    type: "<string|number|integer|boolean|array|object>",
                    description: "<what this parameter is>"
                }
            },
            required: ["<param1>", "<param2>"]
        }
    }
}
```

### Multiple Tools

Agents can have any number of tools:

```tl
fn search(query) { "Results for: " + query }
fn calculate(expression) { string(eval(expression)) }
fn get_time() { "2024-01-15 10:30 UTC" }

agent assistant {
    model: "gpt-4o",
    system: "You are a helpful assistant with access to search, calculator, and clock.",
    tools {
        search: {
            description: "Search the web",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Search query" }
                },
                required: ["query"]
            }
        },
        calculate: {
            description: "Evaluate a math expression",
            parameters: {
                type: "object",
                properties: {
                    expression: { type: "string", description: "Math expression to evaluate" }
                },
                required: ["expression"]
            }
        },
        get_time: {
            description: "Get the current time",
            parameters: {
                type: "object",
                properties: {}
            }
        }
    },
    max_turns: 10
}
```

### How Tool Dispatch Works

When you call `run_agent(agent, message)`, the following loop runs:

1. The user message is sent to the LLM along with tool definitions.
2. If the LLM responds with **text**, the loop ends and returns the response.
3. If the LLM responds with **tool calls**, for each call:
   a. TL looks up the function by name in the current scope.
   b. JSON arguments are converted to TL values (strings, numbers, bools, lists, maps).
   c. The function is called with those arguments.
   d. The return value is converted back to a string and sent to the LLM as a tool result.
4. Steps 1-3 repeat until the LLM produces a text response or `max_turns` is exceeded.

### Argument Conversion

When the LLM calls a tool, JSON arguments are converted to TL values:

| JSON Type | TL Type |
|-----------|---------|
| `"string"` | `string` |
| `123` | `int` |
| `1.5` | `float` |
| `true`/`false` | `bool` |
| `null` | `none` |
| `[1, 2, 3]` | `list` |
| `{"a": 1}` | `map` |

For functions with named parameters, JSON object values are passed as positional arguments in the order they appear:

```tl
// If the LLM calls: search({"query": "hello", "limit": 5})
// TL calls: search("hello", 5)
fn search(query, limit) { ... }
```

## Running Agents

### run_agent

```tl
let result = run_agent(agent, message)
```

**Parameters:**
- `agent` -- an agent value defined with the `agent` keyword
- `message` -- a string containing the user's message

**Returns:** a map with two fields:
- `result.response` -- the agent's final text response (string)
- `result.turns` -- number of conversation turns used (integer)

### Example

```tl
fn get_capital(country) {
    "The capital of " + country + " is Paris."
}

agent geo_bot {
    model: "gpt-4o-mini",
    system: "You answer geography questions. Use tools when needed.",
    tools {
        get_capital: {
            description: "Look up the capital city of a country",
            parameters: {
                type: "object",
                properties: {
                    country: { type: "string", description: "Country name" }
                },
                required: ["country"]
            }
        }
    },
    max_turns: 3
}

let result = run_agent(geo_bot, "What is the capital of France?")
println(result.response)   // The LLM's answer incorporating the tool result
println(result.turns)      // e.g., 2 (one tool call + one final response)
```

## Multi-Provider Support

TL agents work with any OpenAI-compatible API endpoint.

### Provider Detection

The provider is detected from the model name:
- Models starting with `"claude"` use the **Anthropic** API
- All other models use the **OpenAI** Chat Completions API

```tl
// Uses Anthropic API
agent claude_bot {
    model: "claude-sonnet-4-20250514"
}

// Uses OpenAI API
agent gpt_bot {
    model: "gpt-4o"
}
```

### Custom Endpoints (base_url)

Use `base_url` to point to any OpenAI-compatible API:

```tl
// Local Ollama instance
agent local_bot {
    model: "llama3",
    base_url: "http://localhost:11434/v1"
}

// Azure OpenAI
agent azure_bot {
    model: "gpt-4o",
    base_url: "https://myinstance.openai.azure.com/openai/deployments/gpt-4o/v1",
    api_key: "my-azure-key"
}

// Any OpenAI-compatible provider
agent together_bot {
    model: "meta-llama/Llama-3-70b-chat-hf",
    base_url: "https://api.together.xyz/v1",
    api_key: "your-together-key"
}
```

When `base_url` is set, TL always uses the OpenAI-compatible protocol regardless of the model name. This means you can route Claude models through an OpenAI-compatible proxy:

```tl
agent proxied_claude {
    model: "claude-sonnet-4-20250514",
    base_url: "http://my-proxy:8080/v1"  // uses OpenAI protocol, not Anthropic
}
```

### API Key Resolution

API keys are resolved in this order:

1. Explicit `api_key` field in the agent definition
2. `TL_LLM_KEY` environment variable (generic, works for any provider)
3. Provider-specific environment variable:
   - `TL_ANTHROPIC_KEY` for Claude models
   - `TL_OPENAI_KEY` for GPT and other models

## Lifecycle Hooks

Lifecycle hooks let you observe and react to agent events.

### on_tool_call

Runs after each tool call completes. Has access to three implicit variables:

- `tool_name` -- name of the tool that was called (string)
- `tool_args` -- the raw arguments string (string)
- `tool_result` -- the return value from the tool function (string)

```tl
agent bot {
    model: "gpt-4o",
    tools {
        search: {
            description: "Search for information",
            parameters: { type: "object", properties: { query: { type: "string" } }, required: ["query"] }
        }
    },
    on_tool_call {
        println("[TOOL] " + tool_name + " called")
        println("[ARGS] " + tool_args)
        println("[RESULT] " + tool_result)
    }
}
```

### on_complete

Runs when the agent produces its final text response. Has access to one implicit variable:

- `result` -- the result map with `response` and `turns` fields

```tl
agent bot {
    model: "gpt-4o",
    on_complete {
        println("Agent finished in " + string(result.turns) + " turns")
        println("Response length: " + string(len(result.response)))
    }
}
```

### Combined Example

```tl
fn search(query) { "Found: " + query }

agent logged_bot {
    model: "gpt-4o",
    system: "Use the search tool to answer questions.",
    tools {
        search: {
            description: "Search for information",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Search query" }
                },
                required: ["query"]
            }
        }
    },
    max_turns: 5,
    on_tool_call {
        println("[LOG] Tool '" + tool_name + "' returned: " + tool_result)
    }
    on_complete {
        println("[LOG] Completed in " + string(result.turns) + " turns")
    }
}
```

### Hook Storage

Lifecycle hooks are stored as global functions with mangled names:
- `on_tool_call` becomes `__agent_<name>_on_tool_call__`
- `on_complete` becomes `__agent_<name>_on_complete__`

These are regular functions and can be called directly for testing:

```tl
agent bot {
    model: "gpt-4o",
    on_tool_call {
        println("called: " + tool_name)
    }
}

// Call the hook directly (useful for testing)
__agent_bot_on_tool_call__("my_tool", "args", "result")
// Prints: "called: my_tool"
```

## LLM Functions

TL also provides standalone LLM functions for direct API access without the agent framework.

### ai_complete

Single-shot text completion:

```tl
let response = ai_complete("Explain quantum computing in one sentence")
println(response)

// With a specific model
let response = ai_complete("Hello", "gpt-4o-mini")
```

**Parameters:**
- `prompt` (string, required) -- the prompt text
- `model` (string, optional) -- model identifier, defaults to `"claude-sonnet-4-20250514"`

### ai_chat

Multi-turn chat conversation:

```tl
let response = ai_chat("gpt-4o", "You are a helpful tutor.", [
    ["user", "What is 2+2?"],
    ["assistant", "2+2 equals 4."],
    ["user", "And 3+3?"]
])
println(response)
```

**Parameters:**
- `model` (string, required) -- model identifier
- `system` (string, optional) -- system prompt
- `messages` (list, optional) -- conversation history as `[role, content]` pairs

### When to Use What

| Use Case | Function |
|----------|----------|
| Simple one-shot prompt | `ai_complete(prompt)` |
| Multi-turn chat without tools | `ai_chat(model, system, messages)` |
| Autonomous agent with tools | `agent` + `run_agent()` |

## HTTP Requests

The `http_request` builtin enables agents (and any TL code) to make HTTP calls:

```tl
let resp = http_request("GET", "https://api.example.com/data", none, none)
println(resp.status)    // 200
println(resp.body)      // response body as string
```

### Signature

```tl
http_request(method, url, headers, body) -> map
```

**Parameters:**
- `method` (string) -- HTTP method: `"GET"`, `"POST"`, `"PUT"`, `"DELETE"`, `"PATCH"`, `"HEAD"`
- `url` (string) -- the request URL
- `headers` (map or none) -- optional request headers
- `body` (string or none) -- optional request body

**Returns:** a map with:
- `status` (int) -- HTTP status code
- `body` (string) -- response body text

### Examples

```tl
// GET request
let resp = http_request("GET", "https://httpbin.org/get", none, none)

// POST with JSON body and headers
let headers = {"Content-Type": "application/json", "Authorization": "Bearer token123"}
let body = json_stringify({"name": "Alice", "age": 30})
let resp = http_request("POST", "https://api.example.com/users", headers, body)

// Parse JSON response
let data = json_parse(resp.body)
```

### Using HTTP in Tool Functions

A common pattern is wrapping HTTP calls in tool functions for agents:

```tl
fn search_api(query) {
    let resp = http_request(
        "GET",
        "https://api.search.com/v1/search?q=" + query,
        {"Authorization": "Bearer " + env("SEARCH_API_KEY")},
        none
    )
    if resp.status == 200 {
        let data = json_parse(resp.body)
        json_stringify(data.results)
    } else {
        "Search failed with status " + string(resp.status)
    }
}

agent web_researcher {
    model: "gpt-4o",
    tools {
        search_api: {
            description: "Search the web for information",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Search query" }
                },
                required: ["query"]
            }
        }
    }
}
```

## Embeddings

Generate vector embeddings from text using the OpenAI embeddings API:

```tl
let emb = embed("Hello, world!")
println(type_of(emb))          // "tensor"
println(tensor_shape(emb))     // [1536] (for text-embedding-3-small)
```

### Signature

```tl
embed(text, model?, api_key?) -> tensor
```

**Parameters:**
- `text` (string, required) -- text to embed
- `model` (string, optional) -- embedding model, defaults to `"text-embedding-3-small"`
- `api_key` (string, optional) -- API key, defaults to `TL_OPENAI_KEY` env var

**Returns:** a tensor (1D vector) containing the embedding.

### Similarity Search

Combine embeddings with the `similarity` function:

```tl
let doc1 = embed("Machine learning is a subset of AI")
let doc2 = embed("Deep learning uses neural networks")
let query = embed("What is artificial intelligence?")

let sim1 = similarity(query, doc1)
let sim2 = similarity(query, doc2)

println("Doc 1 similarity: " + string(sim1))
println("Doc 2 similarity: " + string(sim2))
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TL_OPENAI_KEY` | API key for OpenAI models (GPT series) and embeddings |
| `TL_ANTHROPIC_KEY` | API key for Anthropic models (Claude series) |
| `TL_LLM_KEY` | Generic API key -- works for any provider (checked first) |
| `TL_LLM_BASE_URL` | Default base URL for all LLM calls (overridden by agent's `base_url`) |

### Setting Up

```bash
# For OpenAI models
export TL_OPENAI_KEY="sk-..."

# For Anthropic models
export TL_ANTHROPIC_KEY="sk-ant-..."

# Or use a single key for any provider
export TL_LLM_KEY="your-key"

# Optional: custom endpoint
export TL_LLM_BASE_URL="http://localhost:11434/v1"
```

### Resolution Priority

For agent API keys:
1. `api_key` field in the agent definition
2. `TL_LLM_KEY` environment variable
3. Provider-specific: `TL_ANTHROPIC_KEY` (claude models) or `TL_OPENAI_KEY` (others)

For embeddings:
1. Explicit `api_key` argument to `embed()`
2. `TL_OPENAI_KEY` environment variable

## Error Handling

### max_turns Exceeded

If the agent doesn't produce a text response within `max_turns`, an error is raised:

```tl
agent limited_bot {
    model: "gpt-4o",
    max_turns: 2
}

// If the LLM keeps calling tools for 2 turns without producing text,
// run_agent returns an error
try {
    let result = run_agent(limited_bot, "Do something complex")
} catch e {
    println("Agent error: " + e)  // "Agent exceeded maximum turns (2)"
}
```

### Missing API Key

If no API key is available, the LLM call fails with a descriptive error:

```tl
try {
    let result = run_agent(bot, "Hello")
} catch e {
    println(e)  // "No API key found. Set TL_OPENAI_KEY or TL_LLM_KEY"
}
```

### Tool Function Errors

If a tool function throws an error, the error message is sent back to the LLM as the tool result, allowing it to handle the situation:

```tl
fn risky_operation(input) {
    if input == "" {
        throw "Input cannot be empty"
    }
    "Success: " + input
}
```

## Limitations

- **WASM:** Agents, `http_request`, and `embed` are not available in the WASM/browser environment.
- **Streaming:** Agent responses are not streamed -- the full response is returned after completion.
- **Conversation state:** Each `run_agent` call starts a fresh conversation. There is no built-in conversation persistence.
- **Embeddings provider:** Only OpenAI embeddings are supported. Anthropic does not offer an embeddings API.
- **Tool schemas:** Tool parameter schemas use the OpenAI function-calling JSON schema format. This is used for all providers (including Anthropic).
- **Map literals in expressions:** TL's `{ }` syntax starts a block in expression position. To pass maps to functions, construct them with variable assignment or use the `with {}` pattern.
