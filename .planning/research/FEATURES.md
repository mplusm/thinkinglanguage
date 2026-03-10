# Feature Research: MCP Protocol Integration

**Domain:** MCP (Model Context Protocol) Client + Server + Agent Integration for TL
**Researched:** 2026-03-10
**Spec Version Targeted:** 2025-11-25 (latest stable)
**Confidence:** HIGH (derived from official MCP specification at modelcontextprotocol.io)

---

## Protocol Overview

MCP is a JSON-RPC 2.0 based protocol with three roles: **Host** (LLM application), **Client** (connector within host), **Server** (provides context/capabilities). Communication is stateful with capability negotiation at initialization.

**Server-side primitives:** Tools, Resources, Prompts
**Client-side primitives:** Sampling, Roots, Elicitation
**Utilities:** Logging, Progress, Cancellation, Completion (autocompletion), Pagination, Tasks (experimental)
**Transports:** stdio, Streamable HTTP (replaces old HTTP+SSE)
**Auth:** OAuth 2.1 with Protected Resource Metadata (RFC 9728)

All primitives are **optional** -- capability negotiation during `initialize` determines what is available per session. Nothing beyond the base lifecycle (initialize/initialized/shutdown) is strictly required.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Without these, TL cannot interoperate with any existing MCP ecosystem (Claude Desktop, VS Code Copilot, Cursor, etc.).

| # | Feature | Why Expected | Complexity | Spec Status | Notes |
|---|---------|-------------|-----------|-------------|-------|
| **TS-1** | **JSON-RPC 2.0 message layer** | All MCP communication uses JSON-RPC 2.0 with request/response/notification patterns | MEDIUM | REQUIRED | Must handle batching, error codes (-32600 to -32603, plus MCP-specific), `_meta` fields |
| **TS-2** | **Lifecycle: initialize/initialized/shutdown** | Capability negotiation is mandatory first exchange | LOW | REQUIRED | Client sends `initialize` with protocolVersion + capabilities + clientInfo; server responds with its capabilities + serverInfo; client sends `notifications/initialized` |
| **TS-3** | **Version negotiation** | Client and server MUST agree on protocol version | LOW | REQUIRED | Client proposes version, server accepts or counters. Disconnect if incompatible |
| **TS-4** | **Capability negotiation** | Determines which primitives are available per session | LOW | REQUIRED | Both sides declare capabilities object during init. Session must respect negotiated caps |
| **TS-5** | **stdio transport (client)** | Most MCP servers today run as subprocesses via stdio | MEDIUM | REQUIRED (spec says clients SHOULD support) | Launch subprocess, write JSON-RPC to stdin, read from stdout, newline-delimited |
| **TS-6** | **stdio transport (server)** | IDEs/hosts connect to TL as subprocess | MEDIUM | REQUIRED for adoption | Read stdin, write stdout, stderr for logs. No embedded newlines in messages |
| **TS-7** | **Streamable HTTP transport (client)** | Remote MCP servers use HTTP. Replaces old SSE transport | HIGH | REQUIRED for remote servers | POST for sending, GET for SSE listen. Session management (MCP-Session-Id header), `MCP-Protocol-Version` header, Accept: application/json + text/event-stream |
| **TS-8** | **Streamable HTTP transport (server)** | Expose TL as remote MCP server | HIGH | REQUIRED for remote clients | Single endpoint handles POST+GET. SSE streaming for server-initiated messages. Origin validation (DNS rebinding protection), session ID generation |
| **TS-9** | **tools/list + tools/call (client)** | Primary use case: agent discovers and calls external tools | MEDIUM | Server capability: `tools` | List tools with pagination, call by name with JSON arguments, handle text/image/audio/resource_link/embedded resource content types, isError flag |
| **TS-10** | **tools/list + tools/call (server)** | Primary use case: expose TL functions as MCP tools | MEDIUM | Server capability: `tools` | Declare tools with name, description, inputSchema (JSON Schema). Return content array with text/image types. Emit `notifications/tools/list_changed` |
| **TS-11** | **Tool inputSchema (JSON Schema)** | Every tool MUST define parameters via JSON Schema | LOW | REQUIRED for tools | Default to 2020-12 draft. Must be valid JSON Schema object. For no params: `{"type":"object","additionalProperties":false}` |
| **TS-12** | **Error handling (protocol + tool execution)** | Two-tier: JSON-RPC errors for protocol, isError for tool failures | LOW | REQUIRED | Protocol errors: standard codes. Tool errors: `isError: true` in result content. Clients SHOULD feed tool errors to LLM for self-correction |
| **TS-13** | **Cancellation** | Long-running requests need cancellation | LOW | OPTIONAL but expected | `notifications/cancelled` with requestId + optional reason. Fire-and-forget semantics. Handle race conditions gracefully |
| **TS-14** | **Progress tracking** | Long data operations need progress feedback | LOW | OPTIONAL but expected | `progressToken` in request `_meta`, `notifications/progress` with progress/total/message. Progress MUST increase monotonically |
| **TS-15** | **Ping** | Keep-alive / connectivity check | TRIVIAL | REQUIRED | `ping` method, empty response |
| **TS-16** | **Pagination** | Tool/resource/prompt lists can be large | LOW | REQUIRED for list operations | Opaque cursor-based. `nextCursor` in response, `cursor` in follow-up request. Affects tools/list, resources/list, prompts/list, resources/templates/list |

### Differentiators (Competitive Advantage for TL)

These features leverage TL's existing strengths (DataFusion, 15+ connectors, pipeline engine, tensor ops) to create unique value in the MCP ecosystem.

| # | Feature | Value Proposition | Complexity | Spec Status | Notes |
|---|---------|------------------|-----------|-------------|-------|
| **D-1** | **Resources (server): expose data catalog** | TL can expose its registered tables, schemas, connection metadata as MCP resources. No other MCP server has 15+ connector data catalog built in | MEDIUM | Server capability: `resources` | `resources/list`, `resources/read`, `resources/templates/list`. URI schemes: `tl://table/{name}`, `tl://schema/{name}`, `tl://connection/{name}` |
| **D-2** | **Resource subscriptions (server)** | Clients subscribe to table changes, get notified on pipeline updates | MEDIUM | Optional sub-capability: `resources.subscribe` | `resources/subscribe`, `resources/unsubscribe`, `notifications/resources/updated`. Powerful for live data monitoring |
| **D-3** | **Resource templates (server)** | Parameterized data access: `tl://query/{sql}`, `tl://table/{name}/sample/{n}` | LOW | Part of resources | RFC 6570 URI templates. Enables dynamic data exploration without custom tools |
| **D-4** | **Prompts (server): data workflow templates** | Pre-built prompt templates for common data tasks (EDA, data quality check, schema analysis, pipeline building) | MEDIUM | Server capability: `prompts` | `prompts/list`, `prompts/get` with arguments. TL-specific: "analyze_table", "build_pipeline", "data_quality_report" |
| **D-5** | **Tool annotations** | Mark TL data tools as readOnly (queries) vs destructive (writes). Clients use this for safety UIs | LOW | OPTIONAL hints | `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`. Untrusted by spec but valuable for well-known server |
| **D-6** | **Structured output (outputSchema)** | TL tools return typed structured data (table schemas, query results as JSON), not just text blobs | LOW | OPTIONAL | `outputSchema` on tool definition + `structuredContent` in response. Enables programmatic consumption of results |
| **D-7** | **Sampling (client)** | MCP servers can request LLM completions through TL's agent runtime. Enables server-driven agentic loops | HIGH | Client capability: `sampling` | `sampling/createMessage` -- server sends messages + model preferences, client routes to LLM. TL already has multi-provider LLM integration |
| **D-8** | **Sampling with tools** | Servers can provide tool definitions in sampling requests, enabling nested tool loops | HIGH | Sub-capability: `sampling.tools` | Full tool-use loop: server sends tools + messages, client LLM generates tool_use, server executes, sends results back. Complex but powerful |
| **D-9** | **Roots (client)** | Tell MCP servers which directories/files TL has access to. Integrates with TL's sandbox/permissions system | LOW | Client capability: `roots` | `roots/list`, `notifications/roots/list_changed`. Maps naturally to TL SecurityPolicy |
| **D-10** | **Agent auto-discovery of MCP tools** | TL agents transparently discover and use tools from connected MCP servers alongside native TL functions | HIGH | TL-specific integration | Not in MCP spec per se. Agent tool dispatch merges native TL tools + MCP-discovered tools. Single namespace, seamless invocation |
| **D-11** | **Logging (server)** | Stream TL pipeline/query execution logs to MCP clients | LOW | Server capability: `logging` | `logging/setLevel`, `notifications/message`. RFC 5424 severity levels. Enables debugging TL operations from IDE |
| **D-12** | **Completion/autocompletion (server)** | Auto-complete table names, column names, connection names in prompts/resources | MEDIUM | Server capability: `completions` | `completion/complete` with `ref/prompt` or `ref/resource` reference types. Max 100 suggestions per response |
| **D-13** | **listChanged notifications** | Notify clients when available tools/resources/prompts change (e.g., new table registered, pipeline deployed) | LOW | Sub-capability of tools/resources/prompts | `notifications/tools/list_changed`, `notifications/resources/list_changed`, `notifications/prompts/list_changed` |
| **D-14** | **Multi-server client** | Connect to multiple MCP servers simultaneously. Agent sees unified tool namespace | MEDIUM | Architectural pattern | Not in spec but essential for real use. Each connection is independent client instance. Tool name conflicts need namespacing |

### Anti-Features (Deliberately NOT Building in v1)

| # | Feature | Why Requested | Why Problematic for v1 | Alternative |
|---|---------|--------------|----------------------|-------------|
| **AF-1** | **OAuth 2.1 authorization server** | Spec defines full OAuth 2.1 flow for remote MCP servers | Massive scope: PKCE, Dynamic Client Registration, Protected Resource Metadata (RFC 9728), Resource Indicators (RFC 8707). Premature for initial release | Support bearer token auth via config. Full OAuth in v2. Stdio transport needs no auth |
| **AF-2** | **Elicitation** | Servers requesting user input mid-flow (form mode + URL mode) | Complex UI requirements (render forms, handle URL consent, schema validation). Both modes need client UI that TL lacks | Servers can use tool results or sampling to gather info. Add elicitation when TL has UI layer |
| **AF-3** | **Tasks (experimental)** | Async/background execution with polling | Marked experimental in spec. Complex state machine (working/input_required/completed/failed/cancelled). TTL management, task listing, cancellation. Overkill for v1 | Use progress notifications for long ops. Synchronous tool calls are sufficient initially |
| **AF-4** | **HTTP+SSE backward compatibility** | Old 2024-11-05 transport still used by some clients | Supporting two HTTP transport modes doubles complexity. Old transport is being phased out | Implement only Streamable HTTP. Document that old clients should upgrade |
| **AF-5** | **Audio/image content in tool results** | Multimodal tool outputs | TL is data-focused, not media-focused. Binary encoding adds complexity | Return text descriptions, resource links to files. Add binary content types later |
| **AF-6** | **Tool output schema validation** | Validate structured content against outputSchema | Useful but not critical for v1. Adds JSON Schema validation dependency for both sides | Define outputSchema for documentation; skip runtime validation initially |
| **AF-7** | **Dynamic client registration** | OAuth spec feature for auto-registering clients | Only relevant with full OAuth. Complex and optional even in spec | Manual configuration in v1 |
| **AF-8** | **Icons on tools/resources/prompts** | UI display metadata | TL has no GUI. Wasted bytes for CLI/agent use cases | Omit icon fields entirely in v1 |

---

## Feature Dependencies

```
TS-1 (JSON-RPC layer)
 |
 +---> TS-2 (Lifecycle) ---> TS-3 (Version negotiation) ---> TS-4 (Capability negotiation)
 |                                                             |
 |     +------------------------------------------------------+
 |     |                    |                    |
 |     v                    v                    v
 |   TS-5/6 (stdio)    TS-7/8 (HTTP)       TS-15 (Ping)
 |     |                    |
 |     +--------+-----------+
 |              |
 |              v
 |    TS-9/10 (Tools client+server) ---> TS-11 (inputSchema)
 |              |                         |
 |              v                         v
 |    TS-12 (Error handling)        TS-16 (Pagination)
 |              |
 |              +---> TS-13 (Cancellation)
 |              +---> TS-14 (Progress)
 |
 +---> D-1 (Resources server) ---> D-2 (Subscriptions)
 |         |                        D-3 (Resource templates)
 |         v
 |     D-12 (Completion)
 |
 +---> D-4 (Prompts server) ---> D-12 (Completion)
 |
 +---> D-5 (Tool annotations) [depends on TS-9/10]
 +---> D-6 (Structured output) [depends on TS-9/10]
 +---> D-7 (Sampling client) ---> D-8 (Sampling with tools)
 +---> D-9 (Roots client)
 +---> D-10 (Agent auto-discovery) [depends on TS-9, D-14]
 +---> D-11 (Logging server)
 +---> D-13 (listChanged) [depends on TS-9/10, D-1, D-4]
 +---> D-14 (Multi-server client) [depends on TS-5, TS-7, TS-9]
```

**Critical path:** TS-1 -> TS-2/3/4 -> TS-5/6 (stdio) -> TS-9/10 (tools) -> D-10 (agent integration)

---

## MVP Definition

### Launch With (v1) -- MCP Milestone

Must ship together for a coherent, usable MCP integration.

**Client side (connect to external MCP servers):**
- TS-1 through TS-4: JSON-RPC + lifecycle + negotiation
- TS-5: stdio transport (client) -- connect to subprocess MCP servers
- TS-7: Streamable HTTP transport (client) -- connect to remote MCP servers
- TS-9: tools/list + tools/call -- discover and invoke external tools
- TS-11: JSON Schema for tool inputs
- TS-12: Error handling (both tiers)
- TS-13: Cancellation
- TS-14: Progress tracking
- TS-15: Ping
- TS-16: Pagination
- D-9: Roots (expose TL's working directories)
- D-10: Agent auto-discovery (merge MCP tools into agent tool namespace)
- D-14: Multi-server client (connect to N servers simultaneously)

**Server side (expose TL as MCP server):**
- TS-6: stdio transport (server) -- be launched by Claude Desktop, Cursor, etc.
- TS-8: Streamable HTTP transport (server) -- be accessed remotely
- TS-10: tools/list + tools/call -- expose TL functions as tools
- D-1: Resources (expose table catalog, schemas)
- D-3: Resource templates (parameterized data access)
- D-4: Prompts (data workflow templates)
- D-5: Tool annotations (readOnly for queries, destructive for writes)
- D-11: Logging
- D-13: listChanged notifications

### Add After Validation (v1.x)

Add once core is stable and users provide feedback.

- D-2: Resource subscriptions (live data change notifications)
- D-6: Structured output with outputSchema
- D-7: Sampling (server requests LLM completions through TL)
- D-8: Sampling with tools (nested agentic loops)
- D-12: Completion/autocompletion for prompt/resource arguments

### Future Consideration (v2+)

- AF-1: Full OAuth 2.1 authorization (when TL targets enterprise remote deployment)
- AF-2: Elicitation (when TL has UI layer or web interface)
- AF-3: Tasks (when spec stabilizes from experimental)
- AF-4: Legacy HTTP+SSE backward compat (if ecosystem demands it)
- AF-5: Audio/image content types
- AF-6: Output schema runtime validation

---

## Feature Prioritization Matrix

| Feature | User Value | Impl Cost | Risk | Priority | Phase |
|---------|-----------|-----------|------|----------|-------|
| JSON-RPC 2.0 layer (TS-1) | Critical | Medium | Low | P0 | v1 |
| Lifecycle + negotiation (TS-2/3/4) | Critical | Low | Low | P0 | v1 |
| stdio transport client (TS-5) | Critical | Medium | Low | P0 | v1 |
| stdio transport server (TS-6) | Critical | Medium | Low | P0 | v1 |
| Streamable HTTP client (TS-7) | High | High | Medium | P0 | v1 |
| Streamable HTTP server (TS-8) | High | High | Medium | P0 | v1 |
| Tools client (TS-9) | Critical | Medium | Low | P0 | v1 |
| Tools server (TS-10) | Critical | Medium | Low | P0 | v1 |
| inputSchema (TS-11) | Critical | Low | Low | P0 | v1 |
| Error handling (TS-12) | Critical | Low | Low | P0 | v1 |
| Cancellation (TS-13) | Medium | Low | Low | P1 | v1 |
| Progress (TS-14) | Medium | Low | Low | P1 | v1 |
| Ping (TS-15) | Low | Trivial | None | P0 | v1 |
| Pagination (TS-16) | Medium | Low | Low | P1 | v1 |
| Resources server (D-1) | High | Medium | Low | P1 | v1 |
| Resource subscriptions (D-2) | Medium | Medium | Medium | P2 | v1.x |
| Resource templates (D-3) | Medium | Low | Low | P1 | v1 |
| Prompts server (D-4) | Medium | Medium | Low | P1 | v1 |
| Tool annotations (D-5) | Medium | Low | None | P1 | v1 |
| Structured output (D-6) | Medium | Low | Low | P2 | v1.x |
| Sampling client (D-7) | High | High | Medium | P2 | v1.x |
| Sampling with tools (D-8) | Medium | High | High | P2 | v1.x |
| Roots client (D-9) | Medium | Low | Low | P1 | v1 |
| Agent auto-discovery (D-10) | Critical | High | Medium | P0 | v1 |
| Logging server (D-11) | Medium | Low | Low | P1 | v1 |
| Completion (D-12) | Low | Medium | Low | P2 | v1.x |
| listChanged (D-13) | Medium | Low | Low | P1 | v1 |
| Multi-server client (D-14) | High | Medium | Medium | P0 | v1 |

---

## Competitor / Reference Implementation Analysis

### Official Rust SDK (rmcp crate)

- **Crate:** `rmcp` + `rmcp-macros` (published by modelcontextprotocol org)
- **Spec version:** 2025-11-25
- **Transport:** stdio + Streamable HTTP (via tokio)
- **Architecture:** Trait-based handlers (`ServerHandler`, `ClientHandler`), router pattern, `#[tool]` proc macro
- **Dependencies:** tokio, serde, schemars (JSON Schema 2020-12)
- **Status:** Official, actively maintained
- **TL relevance:** Could use as dependency OR use as reference for own implementation. Using as dependency adds tokio requirement (TL already uses tokio for async-runtime feature). Key decision: depend on rmcp vs build custom protocol layer.

**Recommendation: Build custom protocol layer, referencing rmcp for correctness.**
Rationale: TL needs tight integration with its VM, interpreter, agent framework, and data engine. The rmcp abstraction layer would need extensive wrapping. The JSON-RPC protocol itself is simple enough that a custom implementation is lower risk than fighting abstraction mismatches. Total JSON-RPC + lifecycle + transport code is estimated at ~2000 LOC.

### rust-mcp-sdk (community)

- **Crate:** `rust-mcp-sdk`
- **Spec version:** 2025-11-25 with backward compatibility
- **Transport:** stdio + Streamable HTTP + SSE via Axum (hyper-server feature)
- **Features:** Full server + client, proc macros, TLS support
- **Status:** Active community project, full spec compliance claimed
- **TL relevance:** Uses Axum (TL already has axum 0.8 for registry). Alternative reference implementation.

### TypeScript SDK (official)

- **Status:** Most mature, v1.x in production
- **Transport:** stdio + Streamable HTTP (some SSE gaps reported)
- **Features:** Full spec, Zod validation, streaming
- **TL relevance:** Reference for protocol correctness and test interop

### Python SDK (official)

- **Status:** Mature, Pydantic-based, async
- **Transport:** stdio + Streamable HTTP
- **Features:** Decorator-based tool/resource definition
- **TL relevance:** Good reference for server DX patterns. TL's Python FFI bridge could eventually interop with Python MCP servers

---

## MCP Spec Capabilities Reference (2025-11-25)

Complete enumeration of all capability flags exchanged during initialization.

### Server Capabilities

| Capability | Sub-capabilities | Description |
|-----------|------------------|-------------|
| `tools` | `listChanged: bool` | Exposes callable tools |
| `resources` | `subscribe: bool`, `listChanged: bool` | Provides readable resources |
| `prompts` | `listChanged: bool` | Offers prompt templates |
| `logging` | (none) | Emits structured log messages |
| `completions` | (none) | Supports argument autocompletion |
| `tasks` | `list`, `cancel`, `requests.tools.call` | Task-augmented execution (experimental) |
| `experimental` | (any) | Non-standard experimental features |

### Client Capabilities

| Capability | Sub-capabilities | Description |
|-----------|------------------|-------------|
| `roots` | `listChanged: bool` | Provides filesystem roots |
| `sampling` | `tools`, `context` (soft-deprecated) | Supports LLM sampling requests |
| `elicitation` | `form`, `url` | Supports user input requests |
| `tasks` | `list`, `cancel`, `requests.sampling.createMessage`, `requests.elicitation.create` | Task-augmented execution |
| `experimental` | (any) | Non-standard experimental features |

### JSON-RPC Methods Reference

**Client -> Server (requests):**
- `initialize` -- lifecycle handshake
- `ping` -- connectivity check
- `tools/list` -- discover tools (paginated)
- `tools/call` -- invoke a tool
- `resources/list` -- discover resources (paginated)
- `resources/read` -- fetch resource content
- `resources/templates/list` -- discover resource templates
- `resources/subscribe` -- subscribe to resource changes
- `resources/unsubscribe` -- unsubscribe from resource changes
- `prompts/list` -- discover prompts (paginated)
- `prompts/get` -- get prompt content with arguments
- `completion/complete` -- request autocompletion suggestions
- `logging/setLevel` -- set minimum log level
- `tasks/get` -- poll task status
- `tasks/result` -- retrieve task result
- `tasks/list` -- list tasks (paginated)
- `tasks/cancel` -- cancel a task

**Server -> Client (requests):**
- `ping` -- connectivity check
- `sampling/createMessage` -- request LLM completion
- `roots/list` -- request filesystem roots
- `elicitation/create` -- request user input (form or URL mode)

**Client -> Server (notifications):**
- `notifications/initialized` -- initialization complete
- `notifications/cancelled` -- cancel in-progress request
- `notifications/progress` -- progress update
- `notifications/roots/list_changed` -- roots have changed

**Server -> Client (notifications):**
- `notifications/cancelled` -- cancel in-progress request
- `notifications/progress` -- progress update
- `notifications/message` -- log message
- `notifications/tools/list_changed` -- tool list changed
- `notifications/resources/list_changed` -- resource list changed
- `notifications/resources/updated` -- specific resource changed
- `notifications/prompts/list_changed` -- prompt list changed
- `notifications/tasks/status` -- task status changed
- `notifications/elicitation/complete` -- URL elicitation completed

---

## TL-Specific Integration Design Notes

### Agent Tool Namespace Merging (D-10)

Current TL agent tool dispatch: looks up TL functions by name from `AgentTool` definitions.

With MCP: agent tool namespace = native TL functions UNION MCP server tools (from all connected servers).

**Conflict resolution strategy:**
1. Native TL functions take precedence (they are local, fast, trusted)
2. MCP tools are namespaced: `server_name.tool_name` or `tool_name` if unambiguous
3. `tools/list` results are cached; re-fetched on `list_changed` notification
4. Tool descriptions from MCP servers are passed to LLM alongside native tool descriptions

### MCP Server Tool Exposure (TS-10)

TL functions marked with `pub` in a module designated as MCP-exposed become tools.

**Auto-generation of inputSchema:**
- TL function parameters -> JSON Schema properties
- TL types map: `int` -> `integer`, `float` -> `number`, `str` -> `string`, `bool` -> `boolean`, `list` -> `array`, `map` -> `object`
- Optional parameters get `default` in schema
- Typed parameters constrain schema

### Resource URI Scheme (D-1, D-3)

Custom URI scheme `tl://` for TL-specific resources:
- `tl://table/{name}` -- table data/schema
- `tl://connection/{name}` -- connection info
- `tl://pipeline/{name}` -- pipeline definition
- `tl://query?sql={encoded_sql}` -- ad-hoc query result
- `tl://model/{name}` -- ML model info

### Transport Architecture

Both transports share the same protocol handler; only the I/O layer differs:

```
                +------------------+
                | MCP Protocol     |
                | (JSON-RPC,       |
                |  capabilities,   |
                |  handlers)       |
                +--------+---------+
                         |
              +----------+----------+
              |                     |
     +--------+-------+   +--------+--------+
     | stdio transport|   | HTTP transport  |
     | (stdin/stdout) |   | (axum + SSE)    |
     +----------------+   +-----------------+
```

---

## Sources

### Primary (HIGH confidence)
- [MCP Specification 2025-11-25 (overview)](https://modelcontextprotocol.io/specification/2025-11-25) -- full protocol specification
- [MCP Tools spec](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) -- tool definition, inputSchema, annotations, outputSchema, structured content, error handling
- [MCP Resources spec](https://modelcontextprotocol.io/specification/2025-11-25/server/resources) -- resource URIs, templates, subscriptions, annotations, content types
- [MCP Prompts spec](https://modelcontextprotocol.io/specification/2025-11-25/server/prompts) -- prompt templates, arguments, messages
- [MCP Sampling spec](https://modelcontextprotocol.io/specification/2025-11-25/client/sampling) -- createMessage, model preferences, tool use in sampling
- [MCP Roots spec](https://modelcontextprotocol.io/specification/2025-11-25/client/roots) -- filesystem roots, list/change notifications
- [MCP Elicitation spec](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation) -- form mode, URL mode, security considerations
- [MCP Tasks spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks) -- experimental async tasks, state machine, polling
- [MCP Lifecycle spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) -- initialize, capability negotiation, version negotiation, shutdown
- [MCP Transports spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) -- stdio, Streamable HTTP, session management, SSE, backwards compat
- [MCP Logging spec](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/logging) -- log levels, setLevel, notifications
- [MCP Completion spec](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/completion) -- autocompletion for prompts and resources
- [MCP Progress spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress) -- progressToken, notifications
- [MCP Cancellation spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation) -- cancel notifications
- [MCP Pagination spec](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/pagination) -- cursor-based pagination

### Secondary (MEDIUM confidence)
- [Official Rust SDK (rmcp)](https://github.com/modelcontextprotocol/rust-sdk) -- reference implementation, architecture patterns
- [rust-mcp-sdk](https://github.com/rust-mcp-stack/rust-mcp-sdk) -- community Rust implementation with Axum Streamable HTTP
- [MCP 2025-11-25 anniversary blog](http://blog.modelcontextprotocol.io/posts/2025-11-25-first-mcp-anniversary/) -- spec evolution context
- [WorkOS MCP features guide](https://workos.com/blog/mcp-features-guide) -- feature overview and explanations
- [WorkOS MCP 2025-11-25 update](https://workos.com/blog/mcp-2025-11-25-spec-update) -- what changed in latest spec

### Tertiary (LOW confidence)
- [Stainless SDK comparison](https://www.stainless.com/mcp/mcp-sdk-comparison-python-vs-typescript-vs-go-implementations) -- cross-SDK feature comparison
- [MCP tool annotations blog](https://blog.marcnuri.com/mcp-tool-annotations-introduction) -- community explanation of annotations
