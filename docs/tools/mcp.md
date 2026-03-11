# MCP Integration

TL provides full [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) support — both as a **client** (connecting to MCP servers to use their tools, resources, and prompts) and as a **server** (exposing TL functions to external AI tools like Claude Desktop, Cursor, etc.).

**Requires:** Build with `--features mcp`.

## Quick Start

### As a Client

Connect to any MCP server and call its tools from TL:

```tl
// Connect to an MCP server (subprocess)
let client = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")

// Discover tools
let tools = mcp_list_tools(client)
for tool in tools {
    println(tool["name"] + ": " + tool["description"])
}

// Call a tool
let result = mcp_call_tool(client, "read_file", { "path": "/tmp/hello.txt" })
println(result)

// Clean up
mcp_disconnect(client)
```

### As a Server

Expose TL functions as MCP tools for external AI clients:

```tl
fn summarize(text) {
    "Summary of: " + text
}

fn word_count(text) {
    str(len(split(text, " ")))
}

mcp_serve([
    {
        name: "summarize",
        description: "Summarize the given text",
        handler: summarize,
        input_schema: {
            type: "object",
            properties: { text: { type: "string", description: "Text to summarize" } },
            required: ["text"]
        }
    },
    {
        name: "word_count",
        description: "Count words in text",
        handler: word_count,
        input_schema: {
            type: "object",
            properties: { text: { type: "string", description: "Text to count" } },
            required: ["text"]
        }
    }
])
```

### With AI Agents

Give agents access to MCP tools alongside native TL functions:

```tl
let fs = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")
let db = mcp_connect("http://localhost:3000/mcp")

agent data_analyst {
    model: "claude-sonnet-4-20250514",
    system: "You analyze data files and query databases.",
    mcp_servers: [fs, db],
    max_turns: 10
}

let result = run_agent(data_analyst, "Find all CSV files in /tmp and summarize their contents")
println(result.response)

mcp_disconnect(fs)
mcp_disconnect(db)
```

## Table of Contents

- [Building with MCP](#building-with-mcp)
- [Client Functions](#client-functions)
  - [mcp_connect](#mcp_connect)
  - [mcp_list_tools](#mcp_list_tools)
  - [mcp_call_tool](#mcp_call_tool)
  - [mcp_list_resources](#mcp_list_resources)
  - [mcp_read_resource](#mcp_read_resource)
  - [mcp_list_prompts](#mcp_list_prompts)
  - [mcp_get_prompt](#mcp_get_prompt)
  - [mcp_server_info](#mcp_server_info)
  - [mcp_ping](#mcp_ping)
  - [mcp_disconnect](#mcp_disconnect)
- [Server Functions](#server-functions)
  - [mcp_serve](#mcp_serve)
- [Agent Integration](#agent-integration)
- [Transports](#transports)
- [Security](#security)
- [Error Handling](#error-handling)
- [Timeouts](#timeouts)
- [Examples](#examples)

## Building with MCP

MCP support is behind the `mcp` feature flag:

```sh
# Build with MCP
cargo build --release --features mcp

# Build with MCP + other features
cargo build --release --features "mcp,sqlite,async-runtime"

# Install from source with MCP
cargo install thinkinglanguage --features mcp
```

Without `--features mcp`, calling any MCP builtin returns an error: `"MCP not available. Build with --features mcp"`.

## Client Functions

### mcp_connect

```tl
mcp_connect(command, ...args) -> mcp_client
mcp_connect(url) -> mcp_client
```

Connects to an MCP server. Automatically detects the transport:
- If the first argument starts with `http://` or `https://` — connects via **HTTP** (Streamable HTTP transport)
- Otherwise — spawns a **subprocess** and connects via **stdio**

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `command` | string | Executable path or HTTP URL |
| `...args` | string | Additional arguments passed to the subprocess |

**Returns:** An `mcp_client` value representing the connection.

**Examples:**

```tl
// Subprocess (stdio transport)
let client = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")
let client = mcp_connect("./my-mcp-server")
let client = mcp_connect("python", "-m", "my_mcp_server")

// HTTP transport
let client = mcp_connect("http://localhost:8080/mcp")
let client = mcp_connect("https://mcp.example.com/api")
```

**Errors:**
- `"MCP not available. Build with --features mcp"` — MCP feature not enabled
- `"Permission denied"` — sandbox mode blocked the subprocess command
- `"Connection failed"` — subprocess failed to start or handshake failed
- `"Timeout"` — connection took longer than 30 seconds

---

### mcp_list_tools

```tl
mcp_list_tools(client) -> list
```

Lists all tools exposed by the connected MCP server.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |

**Returns:** A list of maps, each with:
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Tool name |
| `description` | string | What the tool does |
| `input_schema` | map | JSON Schema describing the tool's parameters |

**Example:**

```tl
let client = mcp_connect("./my-server")
let tools = mcp_list_tools(client)

for tool in tools {
    println(tool["name"] + " — " + tool["description"])
}
```

---

### mcp_call_tool

```tl
mcp_call_tool(client, tool_name, arguments?) -> string
```

Calls a tool on the connected MCP server.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |
| `tool_name` | string | Name of the tool to call |
| `arguments` | map | Tool arguments (optional, defaults to empty) |

**Returns:** The tool's text response as a string.

**Example:**

```tl
let client = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")

// Call with arguments
let content = mcp_call_tool(client, "read_file", { "path": "/tmp/data.txt" })
println(content)

// Call without arguments
let listing = mcp_call_tool(client, "list_directory", { "path": "/tmp" })
```

**Errors:**
- `"Tool error: ..."` — the server returned an error for the tool call
- `"Timeout"` — tool call took longer than 60 seconds
- `"Transport closed"` — connection was lost

---

### mcp_list_resources

```tl
mcp_list_resources(client) -> list
```

Lists resources exposed by the server.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |

**Returns:** A list of maps, each with:
| Field | Type | Description |
|-------|------|-------------|
| `uri` | string | Resource URI (e.g., `"tl://table/users"`) |
| `name` | string | Resource name |
| `description` | string or none | Optional description |
| `mime_type` | string or none | Optional MIME type |

---

### mcp_read_resource

```tl
mcp_read_resource(client, uri) -> list
```

Reads a resource by URI.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |
| `uri` | string | Resource URI |

**Returns:** A list of content items, each with:
| Field | Type | Description |
|-------|------|-------------|
| `uri` | string | Content URI |
| `mime_type` | string or none | Content MIME type |
| `text` | string or none | Text content (if text resource) |

**Example:**

```tl
let resources = mcp_list_resources(client)
for r in resources {
    let contents = mcp_read_resource(client, r["uri"])
    for c in contents {
        println(c["text"])
    }
}
```

---

### mcp_list_prompts

```tl
mcp_list_prompts(client) -> list
```

Lists prompt templates exposed by the server.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |

**Returns:** A list of maps, each with:
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Prompt name |
| `description` | string or none | Optional description |
| `arguments` | list or none | List of `{name, description, required}` maps |

---

### mcp_get_prompt

```tl
mcp_get_prompt(client, name, arguments?) -> map
```

Retrieves a prompt template with optional arguments.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `client` | mcp_client | A connected MCP client |
| `name` | string | Prompt name |
| `arguments` | map | Prompt arguments (optional) |

**Returns:** A map with:
| Field | Type | Description |
|-------|------|-------------|
| `description` | string or none | Prompt description |
| `messages` | list | List of `{role, content}` maps |

**Example:**

```tl
let result = mcp_get_prompt(client, "code_review", { "language": "rust", "code": "fn main() {}" })
for msg in result["messages"] {
    println(msg["role"] + ": " + msg["content"])
}
```

---

### mcp_server_info

```tl
mcp_server_info(client) -> map
```

Returns information about the connected server from the initial handshake.

**Returns:** A map with:
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Server name |
| `version` | string | Server version |

Returns `none` if no server info is available.

---

### mcp_ping

```tl
mcp_ping(client) -> bool
```

Pings the server to check if the connection is alive.

**Returns:** `true` if the server responds, `false` otherwise.

---

### mcp_disconnect

```tl
mcp_disconnect(client) -> none
```

Gracefully disconnects from the MCP server. After disconnecting, all operations on the client will return errors.

Calling `mcp_disconnect` on an already-disconnected client is safe (no-op).

---

## Server Functions

### mcp_serve

```tl
mcp_serve(tools) -> none
```

Starts a TL MCP server over stdio. This function **blocks** until the client disconnects. External MCP clients (Claude Desktop, Cursor, MCP Inspector) can connect to the TL process's stdin/stdout.

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `tools` | list | List of tool definition maps |

Each tool definition map has:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Tool name |
| `description` | string | yes | What the tool does |
| `handler` | function | yes | TL function to call when the tool is invoked |
| `input_schema` | map | no | JSON Schema for the tool's parameters |

**Example:**

```tl
fn analyze_data(query) {
    let data = read_csv("data.csv")
    let result = data |> filter(eval(query)) |> collect()
    json_stringify(result)
}

fn list_tables() {
    json_stringify(["users", "orders", "products"])
}

mcp_serve([
    {
        name: "analyze_data",
        description: "Run a filter query against the dataset",
        handler: analyze_data,
        input_schema: {
            type: "object",
            properties: {
                query: { type: "string", description: "Filter expression" }
            },
            required: ["query"]
        }
    },
    {
        name: "list_tables",
        description: "List available data tables",
        handler: list_tables,
        input_schema: { type: "object", properties: {} }
    }
])
```

### Using with Claude Desktop

Add to your Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "tl-data": {
      "command": "tl",
      "args": ["run", "my_server.tl"]
    }
  }
}
```

### Using with Cursor

Add to your Cursor MCP settings:

```json
{
  "mcpServers": {
    "tl-data": {
      "command": "tl",
      "args": ["run", "my_server.tl"]
    }
  }
}
```

---

## Agent Integration

Agents can use MCP tools alongside native TL functions via the `mcp_servers` field.

### Syntax

```tl
agent <name> {
    model: "<model>",
    tools { ... },                    // native TL tools (optional)
    mcp_servers: [client1, client2],  // MCP server connections (optional)
    max_turns: 10
}
```

### How It Works

1. When `run_agent` is called, TL discovers all tools from each MCP server via `mcp_list_tools`.
2. MCP tools are merged with native TL tools into a single tool list for the LLM.
3. When the LLM calls a tool:
   - If the tool name belongs to an MCP server, the call is dispatched via `mcp_call_tool`.
   - If the tool name matches a TL function, it's called directly.
4. The result is fed back to the LLM for the next turn.

### Example: Multi-Server Agent

```tl
// Connect to multiple MCP servers
let files = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/data")
let github = mcp_connect("npx", "-y", "@modelcontextprotocol/server-github")
let db = mcp_connect("http://localhost:3000/mcp")

// Native TL function
fn format_report(title, body) {
    "# " + title + "\n\n" + body
}

agent analyst {
    model: "claude-sonnet-4-20250514",
    system: "You are a data analyst. Use available tools to research and produce reports.",

    tools {
        format_report: {
            description: "Format a markdown report",
            parameters: {
                type: "object",
                properties: {
                    title: { type: "string", description: "Report title" },
                    body: { type: "string", description: "Report body" }
                },
                required: ["title", "body"]
            }
        }
    },

    mcp_servers: [files, github, db],
    max_turns: 15,

    on_tool_call {
        println("[" + tool_name + "] " + tool_result)
    }
}

let result = run_agent(analyst, "Analyze the sales data in /data/sales.csv and compare with last quarter's GitHub issues")
println(result.response)

mcp_disconnect(files)
mcp_disconnect(github)
mcp_disconnect(db)
```

---

## Transports

TL supports two MCP transports:

### Stdio (Subprocess)

The default transport. TL spawns the MCP server as a child process and communicates over stdin/stdout using newline-delimited JSON.

```tl
let client = mcp_connect("./my-server", "--arg1", "--arg2")
```

- The server binary must support MCP over stdio
- Subprocess is cleaned up automatically on disconnect or when the client is dropped
- Security policy applies (see [Security](#security))

### Streamable HTTP

For remote MCP servers. Detected automatically when the argument starts with `http://` or `https://`.

```tl
let client = mcp_connect("http://localhost:8080/mcp")
let client = mcp_connect("https://mcp.example.com/api")
```

- Uses HTTP POST + Server-Sent Events (SSE) for bidirectional communication
- Session management handled automatically (MCP-Session-Id header)

---

## Security

MCP client connections respect TL's `SecurityPolicy`:

- **Sandbox mode** (`--sandbox` CLI flag): Blocks all subprocess spawning by default. Use `SecurityPolicy.allowed_commands` to whitelist specific MCP server binaries.
- **Permissive mode** (default): All connections allowed.
- **HTTP connections**: Not subject to subprocess restrictions (no process is spawned).

```tl
// In sandbox mode, this will fail unless the command is whitelisted:
let client = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp")
// Error: "Permission denied: Command 'npx' is not allowed by security policy"

// HTTP connections work in any mode:
let client = mcp_connect("http://localhost:8080/mcp")  // always allowed
```

---

## Error Handling

All MCP functions can throw errors. Use `try`/`catch` for graceful handling:

```tl
try {
    let client = mcp_connect("./nonexistent-server")
    let tools = mcp_list_tools(client)
} catch e {
    println("MCP error: " + e)
}
```

### Error Types

| Error | Cause |
|-------|-------|
| Permission denied | Sandbox policy blocked the command |
| Connection failed | Subprocess couldn't start or handshake failed |
| Protocol error | Invalid MCP message or unexpected server behavior |
| Tool error | The tool returned an error result |
| Transport closed | Connection was lost or client was disconnected |
| Timeout | Operation exceeded its time limit |

---

## Timeouts

All MCP operations have built-in timeouts to prevent blocking:

| Operation | Timeout | Description |
|-----------|---------|-------------|
| Connect (handshake) | 30 seconds | Initial connection and MCP handshake |
| Tool calls | 60 seconds | `mcp_call_tool` execution |
| Metadata operations | 10 seconds | `mcp_list_tools`, `mcp_list_resources`, `mcp_list_prompts`, `mcp_read_resource`, `mcp_get_prompt`, `mcp_ping`, `mcp_server_info` |

If an operation exceeds its timeout, a `"Timeout"` error is thrown.

---

## Examples

### Filesystem Explorer

```tl
let fs = mcp_connect("npx", "-y", "@modelcontextprotocol/server-filesystem", "/home/user")

// List available tools
let tools = mcp_list_tools(fs)
println("Available tools: " + str(len(tools)))

// Read a file
let content = mcp_call_tool(fs, "read_file", { "path": "/home/user/notes.txt" })
println(content)

// List directory
let files = mcp_call_tool(fs, "list_directory", { "path": "/home/user/documents" })
println(files)

mcp_disconnect(fs)
```

### Database Query Server

Expose TL's data engine as an MCP server:

```tl
fn query(sql) {
    let result = postgres_query("postgresql://user:pass@localhost/db", sql)
    let rows = result |> collect()
    json_stringify(rows)
}

fn list_tables() {
    let result = postgres_query("postgresql://user:pass@localhost/db",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    let tables = result |> collect()
    json_stringify(tables)
}

mcp_serve([
    {
        name: "query",
        description: "Run a SQL query against the database",
        handler: query,
        input_schema: {
            type: "object",
            properties: { sql: { type: "string", description: "SQL query" } },
            required: ["sql"]
        }
    },
    {
        name: "list_tables",
        description: "List all tables in the database",
        handler: list_tables,
        input_schema: { type: "object", properties: {} }
    }
])
```

### Server Info and Health Check

```tl
let client = mcp_connect("http://remote-server:8080/mcp")

// Check server info
let info = mcp_server_info(client)
if info != none {
    println("Connected to: " + info["name"] + " v" + info["version"])
}

// Health check
if mcp_ping(client) {
    println("Server is alive")
} else {
    println("Server is not responding")
}

mcp_disconnect(client)
```

### Resource Browser

```tl
let client = mcp_connect("./my-data-server")

// Browse resources
let resources = mcp_list_resources(client)
for r in resources {
    println(r["uri"] + " — " + (r["description"] or "no description"))
    let contents = mcp_read_resource(client, r["uri"])
    for c in contents {
        if c["text"] != none {
            println("  " + c["text"])
        }
    }
}

// Browse prompts
let prompts = mcp_list_prompts(client)
for p in prompts {
    println("Prompt: " + p["name"])
    if p["arguments"] != none {
        for arg in p["arguments"] {
            let req = if arg["required"] { " (required)" } else { "" }
            println("  - " + arg["name"] + req)
        }
    }
}

mcp_disconnect(client)
```

---

## API Reference Summary

| Function | Description |
|----------|-------------|
| `mcp_connect(cmd, ...args)` | Connect to MCP server (subprocess or HTTP) |
| `mcp_list_tools(client)` | List server's tools |
| `mcp_call_tool(client, name, args?)` | Call a tool |
| `mcp_list_resources(client)` | List server's resources |
| `mcp_read_resource(client, uri)` | Read a resource |
| `mcp_list_prompts(client)` | List server's prompts |
| `mcp_get_prompt(client, name, args?)` | Get a prompt template |
| `mcp_server_info(client)` | Get server name/version |
| `mcp_ping(client)` | Check if server is alive |
| `mcp_disconnect(client)` | Disconnect from server |
| `mcp_serve(tools)` | Run TL as an MCP server (stdio) |

## MCP Client Type

```tl
let client = mcp_connect("./server")
type_of(client)    // "mcp_client"
println(client)    // <mcp_client>
```

The `mcp_client` type is first-class — it can be stored in variables, passed to functions, and included in data structures.
