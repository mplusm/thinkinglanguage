# Project Research Summary

**Project:** TL MCP Integration
**Domain:** MCP Protocol Integration (Rust Language Runtime)
**Researched:** 2026-03-10
**Confidence:** HIGH

## Executive Summary

MCP integration in TL requires three capabilities: client (connect to external MCP servers), server (expose TL as an MCP server), and agent integration (agents auto-discover and use MCP tools). The protocol is JSON-RPC 2.0 over two transports: stdio (newline-delimited JSON via subprocess) and Streamable HTTP (POST + SSE).

The key stack decision is whether to use **rmcp 1.1.0** (the official Rust MCP SDK, just hit 1.0 on 2026-03-03) or build a custom protocol layer. rmcp handles all protocol complexity (~20 JSON-RPC methods, capability negotiation, transport framing) but requires reqwest 0.13 upgrade and tokio. A custom implementation gives tighter VM integration but costs ~2-3K LoC of protocol code. Both approaches are viable; rmcp is recommended for faster delivery and spec compliance guarantees.

The biggest risks are subprocess lifecycle management (TL has never spawned processes), tool poisoning attacks (malicious tool descriptions with hidden prompt injection), and stdio pipe deadlocks (must separate read/write threads from day one). Security is critical: `mcp_connect` spawns arbitrary processes and must be gated behind sandbox permissions.

## Key Findings

### Recommended Stack

See: [STACK.md](STACK.md)

**Core decision: rmcp 1.1.0 (official MCP SDK)**
- Handles JSON-RPC 2.0, all transports, capability negotiation, tool dispatch
- Requires reqwest 0.13 upgrade (TL currently uses 0.12 across 7 crates)
- Requires schemars 1.x (new dependency for JSON Schema generation)
- Uses tokio (already in TL, feature-gated)

**Core technologies:**
- rmcp 1.1.0: MCP protocol client + server SDK — official, stable, handles full lifecycle
- tokio 1: async runtime for transports — already in TL
- serde_json 1: JSON serialization — already pervasive in TL
- reqwest 0.13: HTTP client for Streamable HTTP — upgrade from 0.12 required
- schemars 1.x: JSON Schema generation for tool definitions — new dependency

**Critical protocol fact:** MCP stdio uses **newline-delimited JSON**, NOT Content-Length framing like LSP. TL's existing LSP codec cannot be reused.

### Expected Features

See: [FEATURES.md](FEATURES.md)

MCP uses capability-based negotiation — everything beyond lifecycle is optional. This enables incremental implementation.

**Must have (table stakes) — 16 features:**
- JSON-RPC 2.0 layer, lifecycle handshake, version/capability negotiation
- Stdio transport (client + server) — most MCP servers run as subprocesses
- Streamable HTTP transport (client + server) — for remote servers
- tools/list + tools/call (client + server) — primary use case
- Error handling, cancellation, progress, ping, pagination

**Should have (differentiators) — 14 features leveraging TL's strengths:**
- Resources: expose DataFusion tables, schemas, connections as MCP resources
- Prompts: data workflow templates (EDA, data quality, pipeline building)
- Tool annotations: mark queries as readOnly, writes as destructive
- Agent auto-discovery: merge MCP tools into agent tool namespace
- Sampling: MCP servers request LLM completions through TL's agent runtime
- Multi-server client: connect to N servers simultaneously

**Defer (anti-features for v1):**
- OAuth 2.1 authorization (massive scope)
- Elicitation (needs UI layer TL doesn't have)
- Tasks (experimental in spec)
- Legacy HTTP+SSE backward compat

### Architecture Approach

See: [ARCHITECTURE.md](ARCHITECTURE.md)

**New `tl-mcp` crate** — self-contained protocol library, no dependency on tl-compiler/tl-ast (same pattern as tl-ai, tl-stream).

**Major components:**
1. Protocol layer — JSON-RPC types, MCP methods, capability structures
2. Transport layer — stdio (subprocess + reader thread) and HTTP (reqwest + SSE)
3. McpClient — state machine (Disconnected → Initializing → Ready → Closed)
4. McpServer — register tools, dispatch incoming requests
5. Value conversion — serde_json::Value ↔ TL values
6. VM/Interpreter integration — VmValue::McpClient, BuiltinId 216-222
7. Agent integration — merge MCP tools in exec_agent_loop, mcp_servers agent field

**Threading model:** Dedicated reader thread per stdio connection with mpsc channels. Matches TL's existing OS thread + channel concurrency model.

**BuiltinId allocation:** 216-222 (McpConnect, McpListTools, McpCallTool, McpDisconnect, McpServe, McpServerInfo, McpPing)

### Critical Pitfalls

See: [PITFALLS.md](PITFALLS.md)

1. **Stdio framing confusion (P1)** — MCP ≠ LSP. Newline-delimited, not Content-Length. Get wrong = zero interop.
2. **Subprocess lifecycle (P3)** — Zombies, orphans, handle leaks. TL has no process management today.
3. **Pipe deadlock (P4)** — Must separate reader/writer threads from day one. Cannot retrofit.
4. **Tool poisoning (S2)** — #1 MCP attack vector. Malicious tool descriptions with hidden prompt injection.
5. **Unrestricted subprocess spawning (S1)** — `mcp_connect` must be sandbox-gated before shipping.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Foundation & Prerequisites
**Rationale:** reqwest upgrade is a prerequisite for rmcp; SecurityPolicy needs subprocess permissions before any MCP code ships.
**Delivers:** reqwest 0.13 workspace-wide, SecurityPolicy subprocess extension, tl-mcp crate skeleton
**Addresses:** S1 (subprocess security), compatibility prereqs
**Avoids:** P3 (process management without security)

### Phase 2: Protocol & Transport Core
**Rationale:** The protocol layer and stdio transport are the foundation everything else builds on. Stdio is the dominant MCP transport today.
**Delivers:** tl-mcp crate with JSON-RPC codec, stdio client transport (subprocess spawn + reader thread), MCP client state machine (connect → initialize → ready)
**Addresses:** TS-1 through TS-6 (JSON-RPC, lifecycle, stdio)
**Avoids:** P1 (stdio framing), P4 (pipe deadlock), P5 (handshake ordering), P7 (ID management)

### Phase 3: Client Builtins & VM Integration
**Rationale:** Once the protocol works, wire it into TL so users can call mcp_connect/mcp_list_tools/mcp_call_tool from TL scripts.
**Delivers:** VmValue::McpClient, BuiltinId 216-219, interpreter mirroring, basic TL scripts using MCP
**Addresses:** TS-9 (tools client), TS-11-16 (schema, errors, pagination)
**Uses:** tl-mcp McpClient API

### Phase 4: HTTP Transport (Client + Server)
**Rationale:** Streamable HTTP enables remote MCP servers and allows TL to be accessed remotely. Independent from agent integration.
**Delivers:** HTTP client transport (reqwest + SSE), HTTP server transport (axum), session management
**Addresses:** TS-7, TS-8 (Streamable HTTP both directions)
**Avoids:** S5 (DNS rebinding — Origin validation), G3 (SSE parsing), G5 (session management)

### Phase 5: Agent Integration
**Rationale:** The highest-value feature — agents transparently use MCP tools. Depends on client builtins being stable.
**Delivers:** mcp_servers field in agent blocks, tool namespace merging in exec_agent_loop, tool list caching
**Addresses:** D-10 (agent auto-discovery), D-14 (multi-server client)
**Avoids:** S2 (tool poisoning — description sanitization), S3 (cross-server exfiltration), S4 (rug pull detection)

### Phase 6: MCP Server (Expose TL as MCP Server)
**Rationale:** TL's unique value — expose DataFusion, connectors, pipelines as MCP tools. Can be built in parallel with Phase 5.
**Delivers:** McpServer, `tl mcp serve` CLI, TL functions as MCP tools, stdio + HTTP server modes
**Addresses:** TS-10 (tools server), D-1 (resources), D-3 (templates), D-4 (prompts), D-5 (annotations)
**Avoids:** P2 (stdout pollution — redirect print() to stderr in MCP mode)

### Phase 7: Resources, Prompts & Polish
**Rationale:** Higher-level MCP capabilities that differentiate TL. Builds on stable server foundation.
**Delivers:** Resource URIs (tl://table/*, tl://connection/*), prompt templates, tool annotations, logging, completions
**Addresses:** D-1 through D-6, D-11-D-13
**Uses:** Existing DataFusion table catalog, connection registry

### Phase 8: Advanced Features & Hardening
**Rationale:** Features that need the core to be stable. Sampling is complex (server-driven LLM calls through TL).
**Delivers:** Sampling client (D-7/D-8), roots (D-9), resource subscriptions (D-2), concurrent tool calls, timeout configuration
**Avoids:** T1-T4 (performance traps), P8 (VM blocking)

### Phase Ordering Rationale

- **Phase 1 before all others:** reqwest upgrade and security groundwork are prerequisites
- **Phase 2 before 3:** Protocol/transport must work before VM integration
- **Phase 3 before 5:** Client builtins must exist before agent integration can merge tools
- **Phase 4 parallel with 5:** HTTP transport and agent integration are independent
- **Phase 6 parallel with 5:** Server mode and agent integration are independent
- **Phase 7 after 6:** Resources/prompts build on server foundation
- **Phase 8 last:** Advanced features need everything stable

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 4 (HTTP transport):** SSE parsing, session management, reconnection — complex protocol details
- **Phase 5 (Agent integration):** Tool poisoning mitigation strategy needs design validation
- **Phase 6 (MCP server):** stdout redirection architecture, TL function → JSON Schema mapping

Phases with standard patterns (skip research-phase):
- **Phase 1 (Prerequisites):** reqwest upgrade is mechanical, SecurityPolicy extension follows existing pattern
- **Phase 2 (Protocol core):** Well-documented spec, reference implementations available
- **Phase 3 (Client builtins):** Follows established BuiltinId pattern exactly

## Key Decision: rmcp SDK vs Custom Implementation

| Factor | rmcp SDK | Custom |
|--------|----------|--------|
| Protocol compliance | Guaranteed (official) | Must verify manually |
| Development speed | Faster (handles ~20 methods) | Slower (~2-3K LoC protocol code) |
| VM integration | Wrapping required | Direct integration |
| Dependencies | reqwest 0.13 + schemars + tokio | serde_json only (+ reqwest for HTTP) |
| Maintenance | Upstream maintains protocol updates | Must track spec changes manually |
| Binary size | Larger (pulls in more deps) | Smaller |

**Recommendation:** Use rmcp. The protocol surface area is substantial (lifecycle, negotiation, pagination, cancellation, progress, notifications). Hand-rolling this is ~2-3K LoC of code that rmcp already provides with spec compliance guarantees. The reqwest upgrade is needed anyway for long-term dep health.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack (rmcp) | HIGH | Official SDK, 1.0+ stable, verified on crates.io |
| Features (spec) | HIGH | Read all 15 MCP spec pages directly |
| Architecture | HIGH | Based on reading TL source + MCP spec |
| Pitfalls | HIGH | CVEs, academic papers, real-world issue reports |

**Overall confidence:** HIGH

### Gaps to Address

- **reqwest 0.13 `.query()` audit** — check if any TL crate uses this method (now feature-gated)
- **rmcp API surface validation** — confirm rmcp's handler traits work with TL's synchronous VM
- **Tool name collision strategy** — needs design validation during Phase 5 planning
- **MCP Inspector testing** — should be used for interop validation throughout

## Sources

### Primary (HIGH confidence)
- MCP Specification 2025-11-25 — all 15 spec pages read directly
- rmcp 1.1.0 on crates.io — official Rust SDK
- MCP security advisories (TypeScript + Python SDK CVEs)

### Secondary (MEDIUM confidence)
- Community implementations (rust-mcp-sdk)
- Academic security research (tool poisoning, cross-server exfiltration)
- reqwest changelog (breaking changes)

### Tertiary (LOW confidence)
- Community blog posts and tutorials
- rmcp feature flag enumeration from fork (agenterra-rmcp)

---
*Research completed: 2026-03-10*
*Ready for roadmap: yes*
