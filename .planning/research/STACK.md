# Stack Research: MCP Protocol Integration (Rust)

**Researched:** 2026-03-10
**Domain:** MCP (Model Context Protocol) client + server integration in a Rust language runtime
**Confidence:** HIGH (official SDK verified, spec verified, dep compatibility checked)

## Summary

The MCP ecosystem in Rust has matured around one official SDK: **rmcp** (v1.1.0, released 2026-03-04), maintained under `modelcontextprotocol/rust-sdk`. This is the clear choice for TL's MCP integration. It provides both client and server implementations, supports all three transports (stdio, Streamable HTTP, legacy SSE), and uses tokio ^1 + serde ^1 -- both already in TL's dependency tree.

**Critical dependency note:** rmcp 1.1.0 requires `reqwest ^0.13.2`. TL currently uses reqwest 0.12 across 7 crates. The reqwest 0.12 -> 0.13 upgrade is a prerequisite. The breaking changes are manageable: TLS default changes (rustls now default, which TL already uses via `rustls-tls` feature), `query`/`form` are now feature-gated, and some deprecated methods removed. The `blocking` feature remains available.

**Primary recommendation:** Use rmcp 1.1.0 as the MCP SDK. Upgrade reqwest from 0.12 to 0.13 workspace-wide. Create a new `tl-mcp` crate feature-gated behind `mcp`.

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| rmcp | 1.1.0 | MCP protocol client + server SDK | Official SDK from modelcontextprotocol org. Handles JSON-RPC 2.0 framing, protocol negotiation, tool/resource/prompt dispatch, all transports. Actively maintained (22+ releases, hit 1.0 on 2026-03-03). |
| rmcp-macros | (bundled) | `#[tool]` proc-macro for tool definitions | Part of rmcp workspace. Generates tool router boilerplate from annotated methods. |
| tokio | ^1 | Async runtime for transports | Already in TL (5 crates). rmcp requires tokio ^1 for async I/O, child process management, and HTTP serving. |
| serde + serde_json | ^1 | JSON serialization for JSON-RPC messages | Already in TL (10+ crates). rmcp uses serde ^1 / serde_json ^1. Zero friction. |
| reqwest | 0.13 | HTTP client for Streamable HTTP transport | rmcp 1.1.0 requires ^0.13.2. TL must upgrade from 0.12. Used for SSE client and Streamable HTTP client transports. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| schemars | ^1.0 | JSON Schema generation for MCP tool input schemas | Required by rmcp for `#[tool]` macro. MCP tools expose JSON Schema for parameter validation. Not currently in TL -- must add. |
| axum | 0.8 | HTTP server for Streamable HTTP server transport | Already in tl-registry. rmcp's `transport-streamable-http-server` can integrate with axum for serving MCP over HTTP. |
| tokio-util | ^0.7 | Codec utilities for stream framing | Already in tl-data. rmcp uses for async stream utilities. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `tl mcp serve` | CLI subcommand to run TL as MCP server | Exposes TL functions as MCP tools over stdio or HTTP |
| `tl mcp list` | CLI subcommand to list configured MCP servers | Shows available MCP server connections from config |
| MCP Inspector | Protocol debugging | Official web tool at https://github.com/modelcontextprotocol/inspector for testing MCP servers |

## rmcp Feature Flags (Prescriptive Selection)

rmcp has granular feature flags. For TL's needs, use these specific combinations:

### For `tl-mcp` crate (new crate)

```toml
[dependencies.rmcp]
version = "1.1"
default-features = false
features = [
    "client",                                    # MCP client functionality
    "server",                                    # MCP server functionality
    "macros",                                    # #[tool] proc-macro
    "transport-io",                              # AsyncRead/AsyncWrite transport
    "transport-child-process",                   # Spawn MCP servers as subprocesses (client)
    "transport-streamable-http-server",          # Serve MCP over HTTP (server)
    "transport-streamable-http-client-reqwest",  # Connect to HTTP MCP servers (client)
    "transport-sse-client",                      # Legacy SSE client (backward compat)
]
```

### Feature Flag Reference (complete list from rmcp)

| Flag | Purpose | TL Needs? |
|------|---------|-----------|
| `client` | Client-side MCP (connect to servers) | YES -- agent tool discovery |
| `server` | Server-side MCP (expose tools) | YES -- expose TL functions |
| `macros` | `#[tool]` proc-macro | YES -- ergonomic tool definitions |
| `transport-async-rw` | Generic AsyncRead/AsyncWrite | Pulled in by other transport features |
| `transport-io` | I/O stream transport | YES -- base for stdio |
| `transport-child-process` | TokioChildProcess for spawning servers | YES -- stdio client |
| `transport-sse-client` | Legacy SSE client transport | YES -- backward compat with older servers |
| `transport-sse-server` | Legacy SSE server transport | NO -- use Streamable HTTP instead |
| `transport-streamable-http-client` | Streamable HTTP client (generic) | Pulled in by reqwest variant |
| `transport-streamable-http-client-reqwest` | Streamable HTTP client via reqwest | YES -- HTTP client transport |
| `transport-streamable-http-server` | Streamable HTTP server | YES -- HTTP server transport |
| `transport-streamable-http-server-session` | Session management for HTTP server | OPTIONAL -- add later if needed |
| `auth` | OAuth 2.1 support | NO initially -- add when needed |
| `schemars` | JSON Schema generation | Likely pulled in by `macros` |

## Installation (Cargo.toml additions)

### Step 0: Workspace-wide reqwest upgrade (prerequisite)

Every crate currently using `reqwest = "0.12"` must upgrade to `0.13`:

```toml
# In each of: tl-ai, tl-compiler, tl-data, tl-interpreter, tl-package, tl-registry, tl-stream
reqwest = { version = "0.13", default-features = false, features = ["blocking", "json", "rustls-tls"] }
```

Key migration notes for reqwest 0.12 -> 0.13:
- `rustls-tls` feature still exists, still works (TL already uses this -- good)
- `blocking` feature still exists, still works
- `json` feature still exists, still works
- `query` method now requires `query` feature flag (check if TL uses `.query()`)
- TLS default changed to rustls (TL already specifies rustls-tls explicitly -- no impact)

### Step 1: New tl-mcp crate

```toml
# crates/tl-mcp/Cargo.toml
[package]
name = "tl-mcp"
version = "0.3.0"
edition = "2024"

[dependencies]
rmcp = { version = "1.1", default-features = false, features = [
    "client", "server", "macros",
    "transport-io", "transport-child-process",
    "transport-streamable-http-server",
    "transport-streamable-http-client-reqwest",
    "transport-sse-client",
] }
schemars = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "process", "io-util", "sync", "macros"] }

# Internal TL crates
tl-errors = { path = "../tl-errors" }
tl-types = { path = "../tl-types" }
```

### Step 2: Feature gate in tl-cli

```toml
# In crates/tl-cli/Cargo.toml
tl-mcp = { path = "../tl-mcp", optional = true }

[features]
mcp = ["dep:tl-mcp"]
```

## Key rmcp API Surface (for implementation planning)

### Server Side (expose TL functions as MCP tools)

```rust
use rmcp::{ServerHandler, tool, model::*};

// 1. Define a struct that holds TL runtime context
struct TlMcpServer { /* interpreter/VM handle */ }

// 2. Implement ServerHandler trait
impl ServerHandler for TlMcpServer {
    fn get_info(&self) -> ServerInfo { /* capabilities */ }
    // tool_list and tool_call are auto-generated by #[tool_handler] macro
}

// 3. Define tools with #[tool] macro
#[tool(description = "Execute a TL expression")]
async fn eval(&self, expression: String) -> Result<CallToolResult, McpError> { ... }

// 4. Serve over stdio (for subprocess mode)
let server = TlMcpServer::new();
let transport = rmcp::transport::io::stdio(); // reads stdin, writes stdout
server.serve(transport).await;

// 5. Or serve over HTTP (for network mode)
// Uses axum integration via transport-streamable-http-server
```

### Client Side (connect to external MCP servers)

```rust
use rmcp::transport::child_process::TokioChildProcess;
use tokio::process::Command;

// 1. Spawn MCP server as subprocess
let child = TokioChildProcess::new(
    Command::new("npx").args(["-y", "@modelcontextprotocol/server-filesystem", "/tmp"])
);
let client = ().serve(child).await?;

// 2. List available tools
let tools = client.list_tools(Default::default()).await?;

// 3. Call a tool
let result = client.call_tool(CallToolRequestParam {
    name: "read_file".into(),
    arguments: serde_json::json!({"path": "/tmp/data.csv"}),
}).await?;
```

### Transport Selection

| Transport | Use Case | rmcp API |
|-----------|----------|----------|
| stdio (server) | `tl mcp serve --stdio` | `rmcp::transport::io::stdio()` |
| stdio (client) | Connect to subprocess MCP server | `TokioChildProcess::new(Command::new(...))` |
| Streamable HTTP (server) | `tl mcp serve --http :8080` | axum router + rmcp handler |
| Streamable HTTP (client) | Connect to remote MCP server | reqwest-based transport |

## MCP Protocol Key Facts

| Aspect | Detail |
|--------|--------|
| Protocol | JSON-RPC 2.0 over transport |
| Latest spec | 2025-11-25 |
| Stdio framing | Newline-delimited JSON (NOT Content-Length headers like LSP) |
| HTTP transport | Streamable HTTP (POST+GET, optional SSE streaming) |
| Legacy transport | HTTP+SSE (deprecated since 2025-03-26, still widely used) |
| Core primitives | Tools, Resources, Prompts |
| Session mgmt | Optional MCP-Session-Id header for HTTP transport |
| New in 2025-11-25 | Tasks (async execution), OAuth 2.1, Extensions framework |

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|------------------------|
| rmcp 1.1 (official SDK) | rust-mcp-sdk | Never for TL. rust-mcp-sdk is community-maintained, larger API surface, not official. rmcp is canonical. |
| rmcp 1.1 (official SDK) | Hand-roll JSON-RPC | Never for TL. MCP protocol has substantial surface area (initialization handshake, capability negotiation, pagination, notifications, cancellation). SDK handles all of this. Hand-rolling would be 2000+ lines of protocol code. |
| rmcp 1.1 (official SDK) | mcp-protocol-sdk | Never for TL. Less mature, fewer downloads, not official. |
| rmcp 1.1 (official SDK) | Prism MCP (pmcp) | Never for TL. Enterprise-focused, heavier, not official. |
| reqwest 0.13 (via rmcp) | Keep reqwest 0.12 + older rmcp | Avoid. Older rmcp versions (pre-0.16) may use reqwest 0.12 but lack Streamable HTTP support and current spec compliance. |
| schemars 1.x | schemars 0.8 | Never. rmcp and the MCP spec use JSON Schema 2020-12; schemars 1.x is current. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| jsonrpc crate (deprecated) | Unmaintained, recommends jsonrpsee | rmcp (handles JSON-RPC internally) |
| jsonrpsee | Overkill -- designed for Ethereum/blockchain JSON-RPC, not MCP | rmcp (MCP-specific JSON-RPC) |
| Manual JSON-RPC parsing | MCP protocol has complex lifecycle (init, capabilities, pagination, notifications) | rmcp (handles full lifecycle) |
| Content-Length framing for stdio | MCP stdio uses newline-delimited, NOT LSP-style Content-Length | rmcp stdio transport (handles framing) |
| reqwest-eventsource (standalone) | rmcp already includes SSE client via transport-sse-client feature | rmcp transport features |
| Custom SSE implementation | rmcp and axum both handle SSE natively | rmcp transport + axum::response::sse |
| std::process::Command (blocking) | MCP requires async I/O for concurrent message handling | tokio::process::Command via rmcp TokioChildProcess |
| HTTP+SSE server transport | Deprecated since MCP spec 2025-03-26 | Streamable HTTP server transport |

## Version Compatibility

| Existing TL Dep | Version | rmcp 1.1 Requires | Compatible? | Action |
|-----------------|---------|-------------------|-------------|--------|
| tokio | ^1 | ^1 | YES | No change needed |
| serde | ^1 | ^1 | YES | No change needed |
| serde_json | ^1 | ^1 | YES | No change needed |
| reqwest | 0.12 | ^0.13.2 | NO | Upgrade to 0.13 workspace-wide |
| axum | 0.8 | 0.8 (dev dep) | YES | No change needed |
| tokio-util | ^0.7 | ^0.7 | YES | No change needed |
| schemars | (not present) | ^1 (via macros) | N/A | Add new dependency |

### reqwest 0.12 -> 0.13 Migration Checklist

1. **Update version** in 7 Cargo.toml files (tl-ai, tl-compiler, tl-data, tl-interpreter, tl-package, tl-registry, tl-stream)
2. **Check for `.query()` usage** -- now requires `query` feature flag in 0.13
3. **TLS features**: TL uses `rustls-tls` explicitly -- this still works in 0.13
4. **`blocking` feature**: Still available, same API
5. **`json` feature**: Still available, same API
6. **Deprecated methods**: Check for any usage of removed methods (e.g., `trust-dns` renamed to `hickory-dns`)
7. **Test all connectors**: ClickHouse, Snowflake, BigQuery, Databricks use reqwest blocking

## Architecture Decision: SDK vs Hand-Roll

**Decision: Use rmcp SDK**

Rationale:
- MCP protocol has ~20 JSON-RPC methods (initialize, tools/list, tools/call, resources/list, resources/read, prompts/list, prompts/get, notifications/*, completion/complete, logging/*, ping, etc.)
- Protocol lifecycle includes capability negotiation, version negotiation, pagination, cancellation
- Transport layer handles framing, reconnection, session management
- Hand-rolling would be 2000-3000 lines of protocol code that rmcp already provides
- rmcp is the official SDK maintained by the MCP specification authors
- rmcp hit 1.0 on 2026-03-03 -- it is stable

## Architecture Decision: New Crate vs Extend Existing

**Decision: New `tl-mcp` crate**

Rationale:
- MCP is a distinct protocol concern, separate from AI (tl-ai), streaming (tl-stream), or interpreter
- Feature-gating `mcp` at the crate boundary is clean
- Avoids pulling rmcp + schemars + tokio::process into crates that don't need them
- Follows TL's existing pattern (tl-lsp, tl-registry, tl-gpu are all standalone crates)
- tl-mcp depends on tl-errors and tl-types for VmValue conversion, similar to other crates

## Open Questions

1. **reqwest 0.13 `.query()` usage**: Need to audit TL codebase for `.query()` calls on reqwest clients. If found, add `query` feature flag. (LOW risk -- TL mostly uses `.json()` bodies)
   - Confidence: MEDIUM -- needs code audit

2. **schemars 1.x compatibility with rmcp macros**: rmcp's `#[tool]` macro generates JSON Schema via schemars. Need to confirm rmcp 1.1 uses schemars 1.x (not 0.8). Search results indicate it does.
   - Confidence: MEDIUM -- search results suggest 1.x but not explicitly verified in Cargo.toml

3. **Concurrent MCP sessions**: When TL agent connects to multiple MCP servers simultaneously, each connection runs in its own tokio task. rmcp handles this natively but TL's VM is single-threaded. Tool call results will need to be marshaled back to the VM thread.
   - Confidence: HIGH -- this is an architecture concern, not a library concern

## Sources

### Primary (HIGH confidence)
- [MCP Specification 2025-11-25 Transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) -- Official spec, verified via WebFetch
- [rmcp GitHub Repository](https://github.com/modelcontextprotocol/rust-sdk) -- Official Rust SDK
- [rmcp crates.io](https://crates.io/crates/rmcp) -- Version 1.1.0, released 2026-03-04
- [rmcp docs.rs](https://docs.rs/crate/rmcp/latest) -- API documentation

### Secondary (MEDIUM confidence)
- [reqwest 0.13 changelog](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md) -- Breaking changes verified via WebSearch
- [schemars 1.1.0](https://docs.rs/crate/schemars/latest) -- Current version verified
- [MCP spec anniversary blog](http://blog.modelcontextprotocol.io/posts/2025-11-25-first-mcp-anniversary/) -- 2025-11-25 spec features

### Tertiary (LOW confidence)
- Feature flag enumeration from [agenterra-rmcp lib.rs features page](https://lib.rs/crates/agenterra-rmcp/features) -- fork, not canonical rmcp, but feature list is comprehensive and matches other sources
- [Shuttle rmcp tutorial](https://www.shuttle.dev/blog/2025/07/18/how-to-build-a-stdio-mcp-server-in-rust) -- Community tutorial, patterns verified against official examples

## Metadata

**Confidence breakdown:**
- rmcp as SDK choice: HIGH -- official, 1.0+ stable, actively maintained, only viable choice
- reqwest upgrade requirement: HIGH -- verified rmcp ^0.13.2 dep vs TL's 0.12
- Transport feature flags: MEDIUM -- enumerated from fork's lib.rs page, consistent with official docs
- MCP protocol spec: HIGH -- fetched directly from modelcontextprotocol.io
- schemars version: MEDIUM -- docs.rs shows 1.1.0 current, rmcp likely uses ^1 but not directly verified in Cargo.toml

**Research date:** 2026-03-10
**Valid until:** 2026-04-10 (30 days -- rmcp is post-1.0 stable)
