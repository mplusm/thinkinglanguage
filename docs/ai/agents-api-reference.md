# Agent Framework API Reference

Complete reference for all agent-related builtins, types, and syntax in TL.

## Syntax

### agent

Defines an AI agent.

```
agent <name> {
    model: <string>,
    system: <string>,
    tools {
        <tool_name>: {
            description: <string>,
            parameters: {
                type: "object",
                properties: {
                    <param>: { type: <string>, description: <string> }
                },
                required: [<string>, ...]
            }
        }
    },
    max_turns: <integer>,
    temperature: <float>,
    max_tokens: <integer>,
    base_url: <string>,
    api_key: <string>,
    on_tool_call { <statements> }
    on_complete { <statements> }
}
```

All fields except `model` are optional. Commas between fields are optional.

---

## Builtin Functions

### run_agent

```tl
run_agent(agent, message) -> map
```

Executes an agent's tool-use loop.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `agent` | agent | An agent value defined with the `agent` keyword |
| `message` | string | The user's input message |

**Returns:** map with fields:
| Field | Type | Description |
|-------|------|-------------|
| `response` | string | The agent's final text response |
| `turns` | int | Number of conversation turns used |

**Errors:**
- `"run_agent(agent, message) expects 2 arguments"` -- wrong argument count
- `"run_agent() first arg must be an agent"` -- first argument is not an agent value
- `"run_agent() second arg must be a string"` -- second argument is not a string
- `"Agent exceeded maximum turns (N)"` -- agent hit the turn limit without producing a text response
- LLM API errors (network, auth, rate limiting)

---

### ai_complete

```tl
ai_complete(prompt, model?) -> string
```

Single-shot LLM completion.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `prompt` | string | -- | The prompt text |
| `model` | string | `"claude-sonnet-4-20250514"` | Model identifier |

**Returns:** The LLM's text response.

**Retries:** Up to 3 attempts with exponential backoff (500ms, 1000ms, 1500ms).

---

### ai_chat

```tl
ai_chat(model, system?, messages?) -> string
```

Multi-turn LLM chat without tool support.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `model` | string | -- | Model identifier |
| `system` | string | none | System prompt |
| `messages` | list | `[]` | List of `[role, content]` pairs |

**Returns:** The LLM's text response.

---

### http_request

```tl
http_request(method, url, headers, body) -> map
```

Makes an HTTP request.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `method` | string | HTTP method: `"GET"`, `"POST"`, `"PUT"`, `"DELETE"`, `"PATCH"`, `"HEAD"` |
| `url` | string | Request URL |
| `headers` | map or none | Request headers as key-value pairs |
| `body` | string or none | Request body |

**Returns:** map with fields:
| Field | Type | Description |
|-------|------|-------------|
| `status` | int | HTTP status code (e.g., 200, 404, 500) |
| `body` | string | Response body text |

**Not available in WASM.**

---

### embed

```tl
embed(text, model?, api_key?) -> tensor
```

Generates a vector embedding using the OpenAI embeddings API.

**Parameters:**
| Name | Type | Default | Description |
|------|------|---------|-------------|
| `text` | string | -- | Text to embed |
| `model` | string | `"text-embedding-3-small"` | OpenAI embedding model |
| `api_key` | string | `TL_OPENAI_KEY` env var | API key |

**Returns:** A 1D tensor containing the embedding vector.

**Not available in WASM.**

---

### similarity

```tl
similarity(tensor_a, tensor_b) -> float
```

Computes cosine similarity between two tensors.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `tensor_a` | tensor | First vector |
| `tensor_b` | tensor | Second vector |

**Returns:** Float between -1.0 and 1.0. 1.0 = identical direction, 0.0 = orthogonal, -1.0 = opposite.

---

## Types

### agent

An agent definition value.

```tl
agent bot { model: "gpt-4o" }

type_of(bot)       // "agent"
string(bot)        // "<agent bot>"
```

Agents are first-class values -- they can be stored in variables and passed to functions.

### LlmResponse (internal)

The LLM client returns one of:
- `LlmResponse::Text(string)` -- a text response (agent loop ends)
- `LlmResponse::ToolUse(Vec<ToolCall>)` -- one or more tool call requests (agent loop continues)

This type is internal to the runtime and not directly exposed to TL code.

### ToolCall (internal)

Each tool call contains:
- `id` -- unique identifier for the call
- `name` -- name of the tool function to call
- `input` -- JSON object with the function arguments

---

## Opcodes and Builtin IDs

For compiler/VM developers:

| Identifier | Value | Description |
|------------|-------|-------------|
| `Op::AgentExec` | 67 | Compiles agent definition from constants |
| `BuiltinId::AiComplete` | 38 | `ai_complete()` |
| `BuiltinId::AiChat` | 39 | `ai_chat()` |
| `BuiltinId::Embed` | 182 | `embed()` |
| `BuiltinId::HttpRequest` | 183 | `http_request()` |
| `BuiltinId::RunAgent` | 184 | `run_agent()` |

---

## Provider Protocol Details

### OpenAI Chat Completions

Used for: all non-Claude models, or any model when `base_url` is set.

- Endpoint: `{base_url}/chat/completions` (default: `https://api.openai.com/v1/chat/completions`)
- Auth: `Authorization: Bearer {api_key}`
- Tool format: `tools: [{ type: "function", function: { name, description, parameters } }]`
- Tool response: `role: "tool"` messages with `tool_call_id`

### Anthropic Messages API

Used for: Claude models when no `base_url` is set.

- Endpoint: `https://api.anthropic.com/v1/messages`
- Auth: `x-api-key: {api_key}`
- Tool format: `tools: [{ name, description, input_schema }]`
- Tool response: `role: "user"` message with `type: "tool_result"` content blocks

The agent framework handles these differences transparently. You write the same agent definition regardless of provider.

---

## Map Literal Syntax

Inside `agent` tool definitions, TL supports JSON-like map literals with `{ key: value }` syntax:

```tl
tools {
    my_tool: {
        description: "A tool",
        parameters: {
            type: "object",
            properties: {
                name: { type: "string", description: "A name" }
            },
            required: ["name"]
        }
    }
}
```

This syntax supports:
- String keys: `"key": value`
- Identifier keys: `key: value`
- Keyword keys: `type: value` (keywords like `type`, `model`, `match`, etc. are valid map keys)
- Nested maps: `{ outer: { inner: value } }`
- Lists as values: `required: ["a", "b"]`
- Commas between entries are optional

This map literal syntax is only available within agent `tools` blocks and is not a general expression form (in expression position, `{` starts a code block).
