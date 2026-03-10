# TL MCP Integration

## What This Is

Full Model Context Protocol (MCP) support for ThinkingLanguage — client, server, and agent integration with both stdio and HTTP+SSE transports. TL programs can connect to external MCP servers and use their tools, expose TL's data engineering capabilities as an MCP server for external AI tools, and TL agents can auto-discover and dispatch to MCP tools in their tool loop.

## Core Value

TL agents gain access to the entire MCP ecosystem without building each integration natively, and any AI tool gains access to TL's data engine via MCP server.

## Requirements

### Validated

- ✓ Agent framework with tool dispatch, multi-turn conversation, lifecycle hooks — existing
- ✓ JSON tool schemas via serde_json::Value — existing
- ✓ HTTP client (reqwest blocking + rustls) — existing
- ✓ Async primitives: spawn/await, channels, tokio (feature-gated) — existing
- ✓ LSP server with Content-Length stdio framing — existing (reusable codec)
- ✓ Sandbox/permission system (check_permission) — existing
- ✓ 15+ database connectors, DataFusion table ops, pipelines, tensors — existing

### Active

- [ ] MCP Client: connect to external MCP servers via stdio (subprocess) and HTTP+SSE
- [ ] MCP Server: expose TL functions/builtins as MCP tools, serve resources via HTTP+SSE and stdio
- [ ] Agent Integration: `mcp_servers` field in agent blocks, auto-import tools, transparent dispatch in agent loop
- [ ] JSON-RPC 2.0 codec: Content-Length framing (stdio), request/response ID matching
- [ ] MCP protocol state machine: initialize handshake, capability negotiation, tool/resource/prompt listing
- [ ] Process spawning: launch MCP server subprocesses, manage stdin/stdout handles, lifecycle (health, restart, shutdown)
- [ ] Server lifecycle management: spawn, monitor, graceful shutdown on drop
- [ ] Transport abstraction: stdio and HTTP+SSE behind unified interface
- [ ] SSE streaming: server-sent events for HTTP transport (both client and server sides)
- [ ] Resource support: list/read resources from MCP servers, expose TL data as resources
- [ ] Prompt support: list/get prompts from MCP servers, expose TL prompts
- [ ] CLI integration: `tl mcp` subcommands (connect, inspect, serve, list)
- [ ] Config file: MCP server definitions in tl.toml or tl-mcp.json
- [ ] New VmValue variants: McpClient, McpServer handles
- [ ] BuiltinId entries: mcp_connect, mcp_disconnect, mcp_call_tool, mcp_list_tools, mcp_list_resources, mcp_read_resource, mcp_serve, etc.
- [ ] Sandbox enforcement: MCP operations gated behind "network" and "process" permissions

### Out of Scope

- MCP sampling (server-initiated LLM calls) — complex trust model, defer to future
- MCP roots (filesystem root declarations) — niche feature, defer
- Custom MCP protocol extensions — stick to spec
- GUI/desktop MCP integration — TL is CLI/programmatic
- OAuth/complex auth flows for MCP — basic auth (API keys, headers) only for now

## Context

**MCP Protocol:** JSON-RPC 2.0 based protocol for AI tool interop. Two transports: stdio (subprocess with Content-Length framing, identical to LSP) and HTTP+SSE (streamable HTTP with server-sent events). Core capabilities: tools, resources, prompts.

**Existing Infrastructure:**
- tl-lsp already implements Content-Length stdio framing — codec logic reusable
- Agent exec_agent_loop already dispatches tool calls by name lookup — extend to MCP dispatch
- reqwest HTTP client available for HTTP+SSE client side
- axum already used in tl-registry — reusable for HTTP+SSE server side
- serde_json pervasive — JSON-RPC encoding trivial
- No process spawning exists in TL currently — must be added

**Architecture:** New `tl-mcp` crate alongside tl-ai/tl-stream. Feature-gated `mcp` in tl-compiler. Threading model: dedicated reader thread per stdio server connection with channel-based request/response matching (fits existing OS thread + channel concurrency model).

**Key Files:**
- Agent execution: `tl-compiler/src/vm.rs` (exec_agent_loop), `tl-interpreter/src/lib.rs`
- Agent definition: `tl-stream/src/agent.rs` (AgentDef, AgentTool)
- LLM client: `tl-ai/src/llm.rs` (chat_with_tools, ToolCall, LlmResponse)
- Builtin IDs: `tl-compiler/src/chunk.rs` (BuiltinId enum, next available: 216+)
- LSP stdio codec: `tl-lsp/src/` (Content-Length framing reference)
- Registry HTTP server: `tl-registry/` (axum reference)
- Parser agent block: `tl-parser/src/lib.rs` (parse_agent)
- Value types: `tl-compiler/src/value.rs` (VmValue enum)

## Constraints

- **Rust edition**: 2024 — no `ref` in implicitly-borrowing patterns
- **Feature-gated**: MCP support behind `mcp` feature flag to keep default binary lean
- **Transport compatibility**: Must work with existing MCP servers (filesystem, postgres, etc.) without modification
- **Threading**: Stdio transport needs dedicated reader thread; avoid requiring tokio for basic MCP (use std::thread + channels)
- **BuiltinId**: u16 repr, next available 216+, must implement TryFrom<u16>
- **VmValue size**: Currently 464 bytes — new variants must be Arc-wrapped to avoid bloat
- **Backward compatible**: No breaking changes to existing agent syntax; `mcp_servers` is additive

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| New tl-mcp crate | Isolation from core, clean dependency boundary | -- Pending |
| Stdio + HTTP+SSE from start | Both transports are mainstream in MCP ecosystem | -- Pending |
| Dedicated reader thread (not tokio) for stdio | Fits existing concurrency model, no forced async dependency | -- Pending |
| Feature-gated `mcp` | Keeps default binary lean, avoids pulling in server deps | -- Pending |
| Full ecosystem (client + server + agent) | TL's data engine as MCP server is unique value proposition | -- Pending |
| Arc-wrapped McpClient/McpServer VmValue | Prevents bloating 464-byte VmValue | -- Pending |

---
*Last updated: 2026-03-10 after initialization*
