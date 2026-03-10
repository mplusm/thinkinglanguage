# Architecture Research: MCP Protocol Integration

**Domain:** MCP Protocol Integration (Rust Language Runtime)
**Researched:** 2026-03-10
**Confidence:** HIGH

## Summary

MCP (Model Context Protocol) is Anthropic's open standard for connecting LLM applications with external tools and data sources. It uses JSON-RPC 2.0 over two transports: stdio (newline-delimited JSON via subprocess stdin/stdout) and Streamable HTTP (POST + optional SSE). TL needs both an MCP client (connect to external MCP servers to import tools into agents) and an MCP server (expose TL functions to external MCP clients).

The integration touches three major areas: (1) a new `tl-mcp` crate that owns all protocol logic, (2) modifications to the existing agent framework in `tl-compiler/src/vm.rs` and `tl-interpreter/src/lib.rs` to merge MCP-sourced tools with locally-declared tools, and (3) parser/AST extensions for `mcp_servers` field in agent blocks plus `mcp_connect`/`mcp_serve` builtins.

**Primary recommendation:** Build the `tl-mcp` crate as a self-contained protocol layer with no dependency on tl-compiler or tl-interpreter. It exports `McpClient` and `McpServer` structs that the VM/interpreter consume through well-defined trait-free interfaces. Use OS threads (not tokio) for stdio transport to match TL's existing concurrency model.

## System Overview

```
                                  TL Runtime
 +--------------------------------------------------------------------+
 |                                                                    |
 |  +----------+    +----------+    +---------+    +--------+         |
 |  | tl-parser |-->| tl-ast   |-->| tl-comp  |-->| vm.rs  |         |
 |  | (agent   |   | (AgentDef|   | (compile |   | (exec  |         |
 |  |  block)  |   |  +mcp    |   |  agent)  |   | agent  |         |
 |  +----------+   |  servers)|   +---------+   | loop)  |         |
 |                  +----------+        |         +---+----+         |
 |                                      |             |              |
 |                                      v             v              |
 |                              +-------+-------------+------+       |
 |                              |         tl-mcp             |       |
 |                              |                            |       |
 |                              |  +----------+ +----------+ |       |
 |                              |  |McpClient | |McpServer | |       |
 |                              |  |          | |          | |       |
 |                              |  | connect  | | register | |       |
 |                              |  | list_    | | tools    | |       |
 |                              |  |  tools   | | handle   | |       |
 |                              |  | call_    | | requests | |       |
 |                              |  |  tool    | |          | |       |
 |                              |  +----+-----+ +-----+----+ |       |
 |                              |       |             |       |       |
 |                              |  +----+-----+ +-----+----+ |       |
 |                              |  |Transport | |Transport | |       |
 |                              |  | Layer    | | Layer    | |       |
 |                              |  +----------+ +----------+ |       |
 |                              +-----------------------------+       |
 |                                    |               |               |
 +------------------------------------+---------------+---------------+
                                      |               |
                              +-------v---+    +------v------+
                              | External  |    | External    |
                              | MCP Server|    | MCP Client  |
                              | (subprocess|   | (connects   |
                              |  or HTTP) |    |  over HTTP) |
                              +-----------+    +-------------+
```

## Component Responsibilities

| Component | Responsibility | Location |
|-----------|---------------|----------|
| **JSON-RPC codec** | Serialize/deserialize JSON-RPC 2.0 messages (requests, responses, notifications) | `tl-mcp/src/jsonrpc.rs` |
| **Transport trait** | Abstract read/write of JSON-RPC messages over stdio or HTTP | `tl-mcp/src/transport.rs` |
| **Stdio transport** | Spawn subprocess, newline-delimited JSON on stdin/stdout, reader thread | `tl-mcp/src/transport/stdio.rs` |
| **HTTP transport** | POST requests, optional SSE response streams, session management | `tl-mcp/src/transport/http.rs` |
| **McpClient** | Connect to MCP server, initialize, list tools, call tools, disconnect | `tl-mcp/src/client.rs` |
| **McpServer** | Accept connections, register TL functions as MCP tools, handle requests | `tl-mcp/src/server.rs` |
| **Value conversion** | Convert between serde_json::Value and TL tool schemas/results | `tl-mcp/src/convert.rs` |
| **VM integration** | McpClient/McpServer as VmValue variants, BuiltinId dispatch | `tl-compiler/src/vm.rs` |
| **Interpreter integration** | McpClient/McpServer as Value variants, builtin dispatch | `tl-interpreter/src/lib.rs` |
| **Parser integration** | `mcp_servers` field in agent blocks | `tl-parser/src/lib.rs` |
| **AST integration** | McpServerRef in AgentDef | `tl-ast/src/lib.rs` |
| **Agent integration** | Merge MCP tools with local tools in exec_agent_loop | `tl-compiler/src/vm.rs` |
| **CLI integration** | `tl mcp serve` subcommand | `tl-cli/src/main.rs` |

## Recommended Crate Structure

```
crates/tl-mcp/
  Cargo.toml
  src/
    lib.rs              -- Public API: McpClient, McpServer, McpTool, McpError
    jsonrpc.rs          -- JSON-RPC 2.0 message types (Request, Response, Notification, Error)
    protocol.rs         -- MCP-specific methods: initialize, tools/list, tools/call, etc.
    client.rs           -- McpClient state machine (Disconnected -> Initializing -> Ready -> Closed)
    server.rs           -- McpServer: register tools, dispatch incoming requests
    convert.rs          -- serde_json::Value <-> MCP tool schemas, content types
    error.rs            -- McpError enum
    transport/
      mod.rs            -- Transport trait definition
      stdio.rs          -- StdioTransport: subprocess spawn, reader thread, newline framing
      http.rs           -- HttpTransport: reqwest POST, optional SSE parsing
```

### Dependencies for tl-mcp

```toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.12", features = ["blocking", "rustls-tls"], optional = true }

[features]
default = ["http"]
http = ["dep:reqwest"]
```

No dependency on tokio, tl-compiler, tl-interpreter, tl-ast, or tl-ai. The crate is a pure protocol library. The VM and interpreter depend on tl-mcp, not the other way around.

## Architectural Patterns

### 1. JSON-RPC 2.0 Codec

MCP uses JSON-RPC 2.0 with three message types. All messages are UTF-8 JSON.

```rust
/// A JSON-RPC 2.0 request (has id, expects response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,       // Always "2.0"
    pub id: JsonRpcId,         // String or integer, must be unique per session
    pub method: String,        // e.g., "initialize", "tools/list", "tools/call"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// A JSON-RPC 2.0 response (has id matching request).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: JsonRpcId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC 2.0 notification (no id, no response expected).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcId {
    Number(i64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}
```

### 2. Transport Abstraction

```rust
/// Messages that can be sent/received over a transport.
#[derive(Debug, Clone)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Notification(JsonRpcNotification),
}

/// A bidirectional transport for JSON-RPC messages.
pub trait Transport: Send {
    /// Send a message. Blocks until written.
    fn send(&self, msg: &JsonRpcMessage) -> Result<(), McpError>;

    /// Receive the next message. Blocks until one arrives or connection closes.
    fn recv(&self) -> Result<Option<JsonRpcMessage>, McpError>;

    /// Close the transport.
    fn close(&self) -> Result<(), McpError>;
}
```

### 3. Client State Machine

McpClient follows a strict lifecycle: `Disconnected -> Initializing -> Ready -> Closed`.

```
  connect()       initialize()      close()
      |                |               |
      v                v               v
 Disconnected --> Initializing --> Ready --> Closed
                       |              |
                       |   list_tools()
                       |   call_tool()
                       |   ping()
                       +------+-------+
```

States:
- **Disconnected**: No transport. `connect()` spawns subprocess or opens HTTP, moves to Initializing.
- **Initializing**: Transport open. Sends `initialize` request, receives response, sends `initialized` notification, moves to Ready.
- **Ready**: Can call `list_tools()`, `call_tool()`, `ping()`.
- **Closed**: Transport shut down. No operations possible.

### 4. Server Handler Pattern

McpServer receives requests and dispatches them to registered TL functions.

```rust
pub struct McpServer {
    tools: Vec<McpTool>,
    server_info: ServerInfo,
    handler: Box<dyn Fn(&str, &serde_json::Value) -> Result<serde_json::Value, String> + Send>,
}
```

The handler callback is provided by the VM or interpreter. When `tools/call` arrives, the server looks up the tool by name, validates input against inputSchema, and calls the handler with (tool_name, arguments). The handler dispatches to the TL function.

### 5. Tool Schema Format

MCP tools are represented as:

```rust
/// An MCP tool definition (matches protocol schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

/// An MCP tool call result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Vec<ContentItem>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "isError")]
    pub is_error: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentItem {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image { data: String, #[serde(rename = "mimeType")] mime_type: String },
}
```

## Data Flow

### Client: Connecting to an External MCP Server

```
TL Code                     tl-mcp                        External MCP Server
-------                     ------                        -------------------
mcp_connect("cmd", [...])
  |
  v
  VM/BuiltinId::McpConnect
  |
  v
  McpClient::connect_stdio()
    |
    +-- spawn subprocess ---------> child process starts
    +-- create StdioTransport
    |     (reader thread for stdout)
    +-- send initialize request ---> {"jsonrpc":"2.0","id":1,"method":"initialize",...}
    |                           <--- {"jsonrpc":"2.0","id":1,"result":{...capabilities...}}
    +-- send initialized notif ---> {"jsonrpc":"2.0","method":"notifications/initialized"}
    +-- store capabilities
    |
  <-- return VmValue::McpClient(Arc<McpClient>)

mcp_list_tools(client)
  |
  v
  VM/BuiltinId::McpListTools
  |
  v
  McpClient::list_tools()
    |
    +-- send tools/list ----------> {"jsonrpc":"2.0","id":2,"method":"tools/list"}
    |                           <--- {"jsonrpc":"2.0","id":2,"result":{"tools":[...]}}
    +-- parse tool definitions
    |
  <-- return VmValue::List of McpTool maps

mcp_call_tool(client, "tool_name", {args})
  |
  v
  VM/BuiltinId::McpCallTool
  |
  v
  McpClient::call_tool("tool_name", args)
    |
    +-- send tools/call ----------> {"jsonrpc":"2.0","id":3,"method":"tools/call",
    |                                "params":{"name":"tool_name","arguments":{...}}}
    |                           <--- {"jsonrpc":"2.0","id":3,"result":
    |                                  {"content":[{"type":"text","text":"..."}]}}
    +-- extract content
    |
  <-- return VmValue::String (or Map for structured)

mcp_disconnect(client)
  |
  v
  McpClient::close()
    +-- close stdin to child
    +-- wait/SIGTERM/SIGKILL
```

### Server: Exposing TL Functions to External Clients

```
TL Code                      tl-mcp                       External MCP Client
-------                      ------                       -------------------
mcp_serve(tools, port)
  |
  v
  VM/BuiltinId::McpServe
  |
  v
  McpServer::start_http(tools, handler, port)
    |
    +-- bind HTTP endpoint ---------> listening on port
    |
    |                             <--- POST initialize request
    +-- handle initialize
    +-- send initialize response ---->
    |
    |                             <--- POST tools/list
    +-- return registered tools ----->
    |
    |                             <--- POST tools/call {name, arguments}
    +-- dispatch to handler
    |     handler calls VM function
    +-- return tool result --------->
```

### Agent Integration: MCP Tools Merged with Local Tools

```
agent mybot {
  model: "claude-sonnet-4-20250514",
  mcp_servers: [fs_client, db_client],  // MCP clients
  tools {
    local_tool: { description: "...", parameters: {...} }
  },
  ...
}

exec_agent_loop:
  1. Gather local tools from agent_def.tools
  2. For each MCP client in agent_def.mcp_servers:
     a. Call client.list_tools() to get remote tool definitions
     b. Prefix tool names: "mcp:<server_name>:<tool_name>" (or flat if no collision)
     c. Convert McpTool -> OpenAI/Anthropic tool format for LLM
  3. Merge all tools into tools_json for chat_with_tools()
  4. In tool dispatch:
     - If tool_name starts with "mcp:<server>:":
         Call mcp_client.call_tool(original_name, arguments)
     - Else:
         Call execute_tool_call() (existing local dispatch)
  5. Return result to LLM as before
```

## Threading Model

### Stdio Transport

The stdio transport requires concurrent reading (server's stdout) and writing (server's stdin). Since TL uses OS threads (not async), the design uses:

```
Main Thread                          Reader Thread
-----------                          -------------
  |                                      |
  | spawn child process                  |
  | create channels:                     |
  |   tx_incoming: Sender<JsonRpcMessage>|
  |   rx_incoming: Receiver<...>         |
  |                                      |
  | start reader thread --------------->|
  |                                      | loop {
  |                                      |   read line from child stdout
  |                                      |   parse JSON-RPC message
  |                                      |   tx_incoming.send(msg)
  |                                      | }
  |                                      |
  | send(msg):                           |
  |   write JSON + newline to stdin      |
  |   (protected by Mutex<ChildStdin>)   |
  |                                      |
  | recv():                              |
  |   rx_incoming.recv()                 |
  |   (blocks until reader sends msg)   |
  |                                      |
  | request-response matching:           |
  |   send request with id=N            |
  |   loop recv() until response.id==N  |
  |   (buffer notifications/other)      |
```

Key design decisions:

1. **One reader thread per stdio connection.** Each McpClient spawns exactly one reader thread that reads from the child process's stdout. This matches TL's existing pattern (spawn() uses std::thread::spawn).

2. **std::sync::mpsc channels for message passing.** The reader thread sends parsed messages through a channel. The main thread receives them. This is the same pattern as TL's Channel(Arc<VmChannel>) type.

3. **Mutex<ChildStdin> for writing.** Writing to stdin is protected by a mutex so multiple concurrent operations (if TL adds them) are serialized.

4. **Request-response correlation by id.** The client maintains a monotonic counter for request IDs. When sending a request, it loops on recv() until a response with the matching id arrives. Any notifications received while waiting are buffered for later processing.

5. **No tokio dependency.** The stdio transport uses std::process::Command, std::thread, and std::sync::mpsc. This avoids adding tokio as a dependency to tl-mcp.

### HTTP Transport

For the Streamable HTTP transport:

1. **reqwest::blocking::Client** for POST requests (already used by tl-ai and tl-data connectors).
2. **SSE parsing**: Read response body line-by-line for `text/event-stream` responses. Extract `data:` fields as JSON-RPC messages.
3. **Session management**: Store `MCP-Session-Id` header from initialize response and include in subsequent requests.
4. **No long-lived connections needed for basic usage.** Each tools/call is a separate POST. SSE is only needed if the server streams responses.

### Server Threading

For `mcp_serve` (HTTP server mode):

1. Use a simple blocking HTTP server (tiny-http or a minimal reqwest-based approach) listening on a port.
2. Each incoming request is handled on the listener thread (synchronous).
3. Tool dispatch calls into the VM/interpreter on the server thread.
4. For stdio server mode (`tl mcp serve`): read from stdin, write to stdout, single-threaded.

## Integration Points

### 1. VmValue / Value Variants

```rust
// In tl-compiler/src/value.rs:
#[cfg(feature = "mcp")]
McpClient(Arc<tl_mcp::McpClient>),

// In tl-interpreter (Value enum):
#[cfg(feature = "mcp")]
McpClient(Arc<tl_mcp::McpClient>),
```

McpServer does not need a VmValue variant because the server runs as a blocking call (like `mcp_serve(tools, port)` which blocks the thread serving requests). If non-blocking server is needed later, a VmValue::McpServer variant can be added.

### 2. BuiltinId Additions (216-222)

| BuiltinId | Name | Signature | Purpose |
|-----------|------|-----------|---------|
| 216 | McpConnect | `mcp_connect(transport, command_or_url, [args])` | Connect to MCP server, return McpClient |
| 217 | McpListTools | `mcp_list_tools(client)` | List available tools from server |
| 218 | McpCallTool | `mcp_call_tool(client, name, arguments)` | Call a tool on the server |
| 219 | McpDisconnect | `mcp_disconnect(client)` | Gracefully disconnect |
| 220 | McpServe | `mcp_serve(tools_map, port_or_"stdio")` | Start MCP server exposing TL functions |
| 221 | McpServerInfo | `mcp_server_info(client)` | Get server capabilities and info |
| 222 | McpPing | `mcp_ping(client)` | Ping the server |

### 3. Parser: `mcp_servers` in Agent Blocks

Extend parse_agent() to recognize `mcp_servers: [expr1, expr2, ...]` as a new field. These expressions evaluate to McpClient values at runtime.

```
agent mybot {
  model: "claude-sonnet-4-20250514",
  mcp_servers: [fs_client, weather_client],
  tools {
    local_fn: { description: "...", parameters: {...} }
  }
}
```

At compile time, `mcp_servers` becomes a list of expressions in the AgentDef AST node. At runtime, the VM evaluates them and passes the McpClient values to exec_agent_loop.

### 4. AST: AgentDef Extension

```rust
// In tl-ast Stmt::Agent:
pub struct AgentFields {
    // ... existing fields ...
    pub mcp_servers: Vec<Expr>,  // NEW: expressions evaluating to McpClient values
}
```

### 5. exec_agent_loop Modification

The key integration point. The existing loop in vm.rs already:
1. Builds `tools_json` from `agent_def.tools`
2. Calls `chat_with_tools()` with the tools
3. Dispatches tool calls via `execute_tool_call()`

The modification:

```rust
fn exec_agent_loop(&mut self, agent_def: &AgentDef, ..., mcp_clients: &[Arc<McpClient>]) {
    // 1. Build local tools_json (existing)
    let mut tools_json = build_local_tools_json(&agent_def.tools);

    // 2. For each MCP client, list tools and merge
    let mut mcp_tool_map: HashMap<String, (usize, String)> = HashMap::new(); // tool_name -> (client_idx, original_name)
    for (idx, client) in mcp_clients.iter().enumerate() {
        let remote_tools = client.list_tools()?;
        for tool in &remote_tools {
            let key = format!("mcp_{}_{}", idx, tool.name);
            mcp_tool_map.insert(key.clone(), (idx, tool.name.clone()));
            tools_json.push(json!({
                "type": "function",
                "function": {
                    "name": key,
                    "description": tool.description,
                    "parameters": tool.input_schema
                }
            }));
        }
    }

    // 3. In tool dispatch loop:
    for tc in &tool_calls {
        if let Some((client_idx, original_name)) = mcp_tool_map.get(&tc.name) {
            // MCP tool: call remote server
            let result = mcp_clients[*client_idx].call_tool(&original_name, &tc.input)?;
            results.push((tc.name.clone(), result));
        } else {
            // Local tool: existing dispatch
            let result_str = self.execute_tool_call(&tc.name, &tc.input)?;
            results.push((tc.name.clone(), result_str));
        }
    }
}
```

### 6. CLI: `tl mcp serve`

New subcommand that reads a TL file, evaluates it to collect exported functions, then starts an MCP server exposing those functions as tools.

```
tl mcp serve <file.tl> [--port 3000] [--stdio]
```

- `--stdio`: Read JSON-RPC from stdin, write to stdout (for use as subprocess by MCP clients)
- `--port N`: Start HTTP server on port N

### 7. Feature Gate

```toml
# In tl-compiler/Cargo.toml:
[features]
mcp = ["dep:tl-mcp"]

# In tl-cli/Cargo.toml:
[features]
mcp = ["tl-compiler/mcp", "tl-interpreter/mcp"]
```

## Build Order

The build order reflects dependency chains. Each layer depends only on the layer above it.

### Layer 1: Protocol Foundation (no TL dependencies)

Build `tl-mcp` crate with:
1. **jsonrpc.rs** -- JSON-RPC 2.0 message types with serde
2. **protocol.rs** -- MCP method constants, capability structures, initialize/tools types
3. **error.rs** -- McpError enum
4. **convert.rs** -- McpTool struct, ToolResult, ContentItem

**Test:** Round-trip serialize/deserialize all message types. Verify against MCP spec examples.

### Layer 2: Transport Layer

5. **transport/mod.rs** -- Transport trait
6. **transport/stdio.rs** -- StdioTransport with subprocess spawn, reader thread, newline framing
7. **transport/http.rs** -- HttpTransport with reqwest::blocking::Client, SSE line parsing

**Test:** Integration test spawning a mock MCP server (simple echo), verify message exchange. Test HTTP transport against a mock HTTP endpoint.

### Layer 3: Client

8. **client.rs** -- McpClient state machine: connect(), initialize handshake, list_tools(), call_tool(), close()

**Test:** Connect to a real MCP server (e.g., the filesystem reference server from modelcontextprotocol/servers), list tools, call a tool, disconnect.

### Layer 4: Server

9. **server.rs** -- McpServer: register tools, handle initialize/tools-list/tools-call dispatching

**Test:** Start server in stdio mode, send initialize + tools/list + tools/call via stdin, verify responses on stdout.

### Layer 5: VM/Interpreter Integration

10. Add `VmValue::McpClient` variant (feature-gated)
11. Add `BuiltinId` entries 216-222
12. Implement `call_builtin` handlers for mcp_connect, mcp_list_tools, mcp_call_tool, mcp_disconnect, mcp_serve
13. Mirror in interpreter

**Test:** TL scripts that call `mcp_connect()`, `mcp_list_tools()`, `mcp_call_tool()`.

### Layer 6: Agent Integration

14. Extend AST `AgentFields` with `mcp_servers: Vec<Expr>`
15. Extend parser `parse_agent()` to recognize `mcp_servers: [...]`
16. Extend compiler to compile mcp_servers expressions and pass to agent exec
17. Modify `exec_agent_loop` to merge MCP tools with local tools and dispatch accordingly

**Test:** Agent with mcp_servers field connecting to a mock MCP server, executing tools.

### Layer 7: CLI and Server Mode

18. Add `tl mcp serve` subcommand
19. Implement stdio server mode (read stdin, write stdout)
20. Implement HTTP server mode (optional, can use tiny-http or blocking approach)

**Test:** End-to-end: one TL process serves functions via `tl mcp serve --stdio`, another connects as client.

### Dependency Graph

```
Layer 1: jsonrpc, protocol, error, convert  (pure serde_json)
  |
Layer 2: transport/stdio, transport/http    (+ std::process, reqwest)
  |
Layer 3: client                             (depends on 1+2)
  |
Layer 4: server                             (depends on 1+2)
  |
Layer 5: vm.rs + interpreter integration    (tl-compiler depends on tl-mcp)
  |
Layer 6: parser + AST + agent integration   (tl-parser, tl-ast changes)
  |
Layer 7: CLI subcommand                     (tl-cli changes)
```

### Critical Path

The minimum viable integration is Layers 1-3 + Layer 5 (builtins only, no agent integration). This gives TL users `mcp_connect()` / `mcp_list_tools()` / `mcp_call_tool()` as builtins, letting them manually wire MCP tools into agents.

Layer 6 (agent integration with `mcp_servers` field) is a quality-of-life improvement that can follow.

Layer 4 + Layer 7 (server mode) is independent and can be built in parallel with Layer 6.

## Stdio Transport: MCP vs LSP Framing

**Important distinction:** MCP stdio uses **newline-delimited** JSON, NOT Content-Length framing.

Per the MCP spec (2025-11-25): "Messages are delimited by newlines, and MUST NOT contain embedded newlines."

This is simpler than LSP's Content-Length header framing. The existing tl-lsp uses the `lsp-server` crate which handles Content-Length internally. For MCP stdio, the implementation is:

```rust
// Writing: serialize to JSON + newline
fn send_stdio(stdin: &mut ChildStdin, msg: &JsonRpcMessage) -> Result<(), McpError> {
    let json = serde_json::to_string(msg)?;
    // JSON-RPC messages MUST NOT contain embedded newlines
    debug_assert!(!json.contains('\n'));
    writeln!(stdin, "{}", json)?;
    stdin.flush()?;
    Ok(())
}

// Reading: one line = one message
fn recv_stdio(reader: &mut BufReader<ChildStdout>) -> Result<Option<JsonRpcMessage>, McpError> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 { return Ok(None); } // EOF
    let msg: JsonRpcMessage = serde_json::from_str(line.trim())?;
    Ok(Some(msg))
}
```

## Protocol Version Support

The MCP specification has multiple versions:
- `2024-11-05` -- Original version (HTTP+SSE transport, now deprecated)
- `2025-03-26` -- Added Streamable HTTP
- `2025-06-18` -- Added elicitation, tasks
- `2025-11-25` -- Current latest (icons, structured output, tasks matured)

**Recommendation:** Support protocol version `2025-03-26` as minimum (widely supported) and `2025-11-25` as target. During initialize, send `2025-11-25`; if server responds with older version, fall back. Only tools capability is required for the initial integration -- resources, prompts, sampling, and elicitation can be added later.

## Error Handling

MCP defines two categories of errors:

1. **Protocol errors** -- Standard JSON-RPC error codes:
   - `-32700`: Parse error
   - `-32600`: Invalid request
   - `-32601`: Method not found
   - `-32602`: Invalid params
   - `-32603`: Internal error

2. **Tool execution errors** -- Returned in the result with `isError: true`, not as JSON-RPC errors.

The McpError enum should cover both:

```rust
pub enum McpError {
    Transport(String),           // Connection/IO failures
    Protocol(i32, String),       // JSON-RPC error response
    Timeout,                     // Request timed out
    NotConnected,                // Client not in Ready state
    InvalidResponse(String),     // Malformed response
    ToolError(String),           // Tool returned isError: true
}
```

## Process Lifecycle Management

For stdio transport, the client must manage the child process:

1. **Spawn**: `std::process::Command::new(program).args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()`
2. **Initialize**: Send initialize request, wait for response, send initialized notification
3. **Operate**: Send requests, receive responses
4. **Shutdown**:
   a. Drop/close the stdin handle (signals EOF to child)
   b. Wait up to 5 seconds for child to exit
   c. Send SIGTERM if still alive (on Unix)
   d. Wait up to 5 seconds
   e. Send SIGKILL if still alive

stderr from the child process should be captured and logged (the MCP spec says servers MAY write informational/debug/error messages to stderr).

## Open Questions

1. **Tool name collision handling**: When multiple MCP servers expose tools with the same name, the current design prefixes with `mcp_<idx>_`. Should this use the server name instead? The server name comes from serverInfo in the initialize response. **Recommendation:** Use server name if available, fall back to index.

2. **MCP server in agent syntax**: The `mcp_servers: [expr]` syntax requires the user to manually call `mcp_connect()` before defining the agent. An alternative is `mcp_servers: [{ command: "npx", args: ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"] }]` which auto-connects. **Recommendation:** Start with the explicit approach (user calls mcp_connect first), add auto-connect syntax later.

3. **Tool schema validation**: Should tl-mcp validate tool call arguments against the inputSchema before sending? **Recommendation:** No, let the server validate. The LLM generates arguments that should match the schema, and server-side validation gives better error messages.

4. **Concurrent tool calls**: Currently exec_agent_loop calls tools sequentially. With MCP, tools can be I/O-bound (network calls). Should MCP tool calls be parallelized? **Recommendation:** Sequential initially, parallel as a future optimization (spawn a thread per MCP call_tool).

5. **Connection pooling**: Should McpClient connections persist across agent runs, or reconnect each time? **Recommendation:** Persist. The McpClient value lives in a TL variable; as long as the variable is alive, the connection stays open.

## Sources

### Primary (HIGH confidence)
- [MCP Specification 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25) -- Protocol revision, message types, capabilities
- [MCP Architecture Overview](https://modelcontextprotocol.io/docs/learn/architecture) -- Host/client/server roles, layers, transport mechanisms
- [MCP Transports Specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) -- stdio (newline-delimited), Streamable HTTP (POST + SSE), session management
- [MCP Lifecycle Specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) -- Initialize handshake, version negotiation, capability negotiation, shutdown
- [MCP Tools Specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) -- tools/list, tools/call, tool schema, tool results, error handling
- [MCP Base Protocol](https://modelcontextprotocol.io/specification/2025-11-25/basic) -- JSON-RPC 2.0 message format, JSON Schema usage
- [Official Rust SDK (rmcp)](https://github.com/modelcontextprotocol/rust-sdk) -- Crate structure, trait-based handler pattern, transport implementations

### Secondary (MEDIUM confidence)
- [MCP Client Development Guide](https://github.com/cyanheads/model-context-protocol-resources/blob/main/guides/mcp-client-development-guide.md) -- Community guide for client implementation patterns
- [Why MCP Deprecated SSE](https://blog.fka.dev/blog/2025-06-06-why-mcp-deprecated-sse-and-go-with-streamable-http/) -- Context on transport evolution
- [rust-mcp-sdk on crates.io](https://crates.io/crates/rust-mcp-sdk) -- Community Rust implementation reference

### Codebase (PRIMARY -- verified by reading source)
- `tl-compiler/src/vm.rs` lines 7117-7272: `exec_agent_loop()` -- existing agent tool dispatch
- `tl-compiler/src/vm.rs` lines 7275-7330: `execute_tool_call()`, `json_to_vm_args()`, `json_value_to_vm()` -- tool call dispatch and value conversion
- `tl-compiler/src/chunk.rs` lines 421-664: `BuiltinId` enum, currently 0-215
- `tl-compiler/src/value.rs` lines 21-95: `VmValue` enum with feature-gated variants
- `tl-stream/src/agent.rs` lines 1-48: `AgentDef`, `AgentTool` structs
- `tl-ai/src/llm.rs` lines 1-100: `LlmClient`, `LlmResponse`, `ToolCall`, `chat_with_tools()`
- `tl-lsp/src/server.rs` lines 1-68: LSP stdio via `lsp-server` crate (Content-Length framing -- different from MCP)
- `tl-cli/Cargo.toml`: Feature flag pattern for optional capabilities
- `tl-compiler/Cargo.toml`: Feature-gated optional dependencies pattern

## Metadata

**Confidence breakdown:**
- JSON-RPC codec design: HIGH -- directly from MCP specification
- Transport abstraction: HIGH -- directly from MCP specification
- Stdio threading model: HIGH -- matches TL's existing OS thread + channel pattern
- Client state machine: HIGH -- directly from MCP lifecycle spec
- Agent integration design: HIGH -- based on reading exec_agent_loop source
- Server design: MEDIUM -- less well-defined in spec, based on general patterns
- Build order: HIGH -- based on actual crate dependency analysis
- BuiltinId allocation: HIGH -- verified current max is 215

**Research date:** 2026-03-10
**Valid until:** 2026-06-10 (MCP spec is stable, TL codebase may shift)
