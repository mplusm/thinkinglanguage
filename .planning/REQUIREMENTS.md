# Requirements: TL MCP Integration

**Defined:** 2026-03-10
**Core Value:** TL agents gain access to the entire MCP ecosystem without building each integration natively, and any AI tool gains access to TL's data engine via MCP server.

## v1 Requirements

### Protocol Foundation

- [ ] **PROTO-01**: TL can send and receive JSON-RPC 2.0 messages (request, response, notification) with correct framing
- [ ] **PROTO-02**: TL performs the 3-step MCP lifecycle handshake (initialize → response → initialized notification) before any operations
- [ ] **PROTO-03**: TL negotiates protocol version with MCP servers, falls back gracefully if server counters with older version
- [ ] **PROTO-04**: TL declares and respects capabilities during initialization, only uses features the peer declared
- [ ] **PROTO-05**: TL can send and respond to ping requests for keep-alive
- [ ] **PROTO-06**: TL handles both protocol errors (JSON-RPC error codes) and tool errors (isError flag in results)

### Transport

- [ ] **TRANS-01**: TL can connect to MCP servers as subprocesses via stdio (newline-delimited JSON, dedicated reader thread)
- [ ] **TRANS-02**: TL can run as an MCP server over stdio (read stdin, write stdout, redirect print() to stderr)
- [ ] **TRANS-03**: TL can connect to remote MCP servers via Streamable HTTP (POST + SSE, session management)
- [ ] **TRANS-04**: TL can serve as a remote MCP server via Streamable HTTP (axum endpoint, SSE streaming, Origin validation)

### Tools

- [ ] **TOOL-01**: TL can list available tools from an MCP server with cursor-based pagination
- [ ] **TOOL-02**: TL can call tools on an MCP server by name with JSON arguments and receive results
- [ ] **TOOL-03**: TL can expose TL functions as MCP tools with name, description, and JSON Schema inputSchema
- [ ] **TOOL-04**: TL can handle incoming tools/call requests, dispatch to TL functions, return results
- [ ] **TOOL-05**: TL generates valid JSON Schema inputSchema from TL function parameter definitions
- [ ] **TOOL-06**: TL can send and handle cancellation notifications for long-running requests
- [ ] **TOOL-07**: TL supports progress tracking via progressToken in requests and progress notifications

### Resources & Prompts

- [ ] **RSRC-01**: TL MCP server exposes registered DataFusion tables, schemas, and connections as MCP resources with `tl://` URI scheme
- [ ] **RSRC-02**: TL MCP server supports resource templates (RFC 6570 URI templates) for parameterized data access
- [ ] **RSRC-03**: TL MCP server exposes prompt templates for common data workflows (EDA, data quality, pipeline building)
- [ ] **RSRC-04**: TL MCP server streams execution logs to clients via logging capability (RFC 5424 severity levels)
- [ ] **RSRC-05**: TL MCP server provides autocompletion for resource and prompt arguments via completion/complete
- [ ] **RSRC-06**: TL MCP server emits listChanged notifications when tools, resources, or prompts are added/removed

### Agent Integration

- [ ] **AGNT-01**: TL agents auto-discover tools from connected MCP servers and merge them into the agent tool namespace alongside native TL functions
- [ ] **AGNT-02**: TL agents can connect to multiple MCP servers simultaneously with tool name conflict resolution
- [ ] **AGNT-03**: TL declares filesystem roots to MCP servers via roots/list and notifies on changes
- [ ] **AGNT-04**: TL handles sampling/createMessage requests from MCP servers, routing them to the configured LLM provider
- [ ] **AGNT-05**: TL supports sampling with tools — server-provided tool definitions in sampling requests with full tool-use loops

### Infrastructure

- [ ] **INFR-01**: TL can spawn and manage subprocess lifecycles (stdin/stdout handles, graceful shutdown, zombie prevention)
- [ ] **INFR-02**: mcp_connect is gated behind sandbox permissions (allow_subprocess + command whitelist in SecurityPolicy)
- [ ] **INFR-03**: VmValue::McpClient variant exists (Arc-wrapped) with display, equality, and clone support
- [ ] **INFR-04**: BuiltinId entries 216+ wired in both VM and interpreter for all MCP operations
- [ ] **INFR-05**: CLI subcommands: `tl mcp serve <file.tl> [--stdio|--port N]` and `tl mcp list`
- [ ] **INFR-06**: reqwest upgraded from 0.12 to 0.13 workspace-wide (7 crates) with all connectors verified
- [ ] **INFR-07**: tl-mcp crate created with clean dependency boundary (no dep on tl-compiler/tl-ast)
- [ ] **INFR-08**: Bidirectional value conversion between serde_json::Value and TL values with edge case handling (NaN, large numbers, nested maps)

## v2 Requirements

### Tools Enhancements

- **TOOL-V2-01**: Tool annotations (readOnlyHint, destructiveHint, idempotentHint)
- **TOOL-V2-02**: Structured output (outputSchema + structuredContent)

### Resources Enhancements

- **RSRC-V2-01**: Resource subscriptions (live data change notifications)

## Out of Scope

| Feature | Reason |
|---------|--------|
| OAuth 2.1 authorization | Massive scope (PKCE, Dynamic Client Registration, RFC 9728). Bearer token via config sufficient |
| Elicitation | Needs UI layer TL doesn't have (form rendering, URL consent) |
| Tasks (experimental) | Marked experimental in MCP spec. Complex state machine, premature |
| Legacy HTTP+SSE transport | Deprecated since spec 2025-03-26. Only Streamable HTTP supported |
| Audio/image content types | TL is data-focused, not media-focused |
| Dynamic client registration | Only relevant with full OAuth |
| Icons on tools/resources | TL has no GUI |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| PROTO-01 | — | Pending |
| PROTO-02 | — | Pending |
| PROTO-03 | — | Pending |
| PROTO-04 | — | Pending |
| PROTO-05 | — | Pending |
| PROTO-06 | — | Pending |
| TRANS-01 | — | Pending |
| TRANS-02 | — | Pending |
| TRANS-03 | — | Pending |
| TRANS-04 | — | Pending |
| TOOL-01 | — | Pending |
| TOOL-02 | — | Pending |
| TOOL-03 | — | Pending |
| TOOL-04 | — | Pending |
| TOOL-05 | — | Pending |
| TOOL-06 | — | Pending |
| TOOL-07 | — | Pending |
| RSRC-01 | — | Pending |
| RSRC-02 | — | Pending |
| RSRC-03 | — | Pending |
| RSRC-04 | — | Pending |
| RSRC-05 | — | Pending |
| RSRC-06 | — | Pending |
| AGNT-01 | — | Pending |
| AGNT-02 | — | Pending |
| AGNT-03 | — | Pending |
| AGNT-04 | — | Pending |
| AGNT-05 | — | Pending |
| INFR-01 | — | Pending |
| INFR-02 | — | Pending |
| INFR-03 | — | Pending |
| INFR-04 | — | Pending |
| INFR-05 | — | Pending |
| INFR-06 | — | Pending |
| INFR-07 | — | Pending |
| INFR-08 | — | Pending |

**Coverage:**
- v1 requirements: 36 total
- Mapped to phases: 0
- Unmapped: 36 ⚠️ (pending roadmap creation)

---
*Requirements defined: 2026-03-10*
*Last updated: 2026-03-10 after initial definition*
