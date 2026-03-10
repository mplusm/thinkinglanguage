# Roadmap: TL MCP Integration

## Overview

Full MCP support for ThinkingLanguage in 8 phases: from prerequisite upgrades and security groundwork, through protocol implementation and transport layers, to full client/server/agent integration with resources, prompts, and sampling. Phases 4-6 can run in parallel once Phase 3 is complete.

## Phases

- [ ] **Phase 1: Prerequisites & Security Foundation** - reqwest upgrade, tl-mcp crate skeleton, sandbox permissions, value conversion
- [ ] **Phase 2: Protocol & Stdio Transport** - JSON-RPC 2.0, MCP lifecycle, stdio client+server, subprocess management
- [ ] **Phase 3: Client Builtins & VM Integration** - VmValue::McpClient, BuiltinId wiring, tools/list, tools/call from TL scripts
- [ ] **Phase 4: HTTP Transport** - Streamable HTTP client (reqwest+SSE) and server (axum+SSE), session management
- [ ] **Phase 5: Agent Integration** - mcp_servers in agent blocks, tool namespace merging, multi-server, roots
- [ ] **Phase 6: MCP Server — Tools** - Expose TL functions as MCP tools, JSON Schema generation, CLI subcommands
- [ ] **Phase 7: Resources, Prompts & Server Capabilities** - tl:// resources, prompt templates, logging, completions, listChanged
- [ ] **Phase 8: Sampling & Hardening** - sampling/createMessage, sampling with tools, performance hardening

## Phase Details

### Phase 1: Prerequisites & Security Foundation
**Goal**: Prepare the workspace for MCP development — upgrade reqwest, create tl-mcp crate, extend SecurityPolicy for subprocess spawning, implement value conversion
**Depends on**: Nothing (first phase)
**Requirements**: INFR-02, INFR-06, INFR-07, INFR-08
**Success Criteria** (what must be TRUE):
  1. `cargo build --workspace` succeeds with reqwest 0.13 across all 7 crates
  2. All existing connector tests pass with reqwest 0.13 (no regressions)
  3. SecurityPolicy has `allow_subprocess` and `allowed_commands` fields, enforced in sandbox mode
  4. `tl-mcp` crate exists with rmcp 1.1 dependency, compiles, and is feature-gated behind `mcp`
  5. `json_to_tl_value()` and `tl_value_to_json()` handle all edge cases (NaN, nested maps, large numbers, nil)
**Research**: Unlikely (mechanical upgrade, established patterns)
**Plans**: TBD

### Phase 2: Protocol & Stdio Transport
**Goal**: Implement MCP protocol core and stdio transport — TL can connect to MCP servers as subprocesses and run as an MCP server over stdio
**Depends on**: Phase 1
**Requirements**: PROTO-01, PROTO-02, PROTO-03, PROTO-04, PROTO-05, PROTO-06, TRANS-01, TRANS-02, INFR-01
**Success Criteria** (what must be TRUE):
  1. TL can connect to an MCP server subprocess (e.g., filesystem server), complete the 3-step handshake, and list its tools
  2. TL can run as an MCP server over stdio — an external MCP client can connect, initialize, and ping it
  3. Subprocess lifecycle is clean: no zombies after disconnect, graceful shutdown on drop
  4. Protocol version negotiation works against servers advertising older versions
  5. Capabilities declared during init are respected — undeclared features are not called
**Research**: Unlikely (well-documented spec, reference implementations available)
**Plans**: TBD

### Phase 3: Client Builtins & VM Integration
**Goal**: Wire MCP client into TL's VM and interpreter — users can call mcp_connect/mcp_list_tools/mcp_call_tool from TL scripts
**Depends on**: Phase 2
**Requirements**: INFR-03, INFR-04, TOOL-01, TOOL-02, TOOL-06, TOOL-07
**Success Criteria** (what must be TRUE):
  1. `let client = mcp_connect("npx @mcp/server-filesystem /tmp")` works in TL scripts and returns an McpClient handle
  2. `mcp_list_tools(client)` returns a list of tool definitions with names, descriptions, and schemas
  3. `mcp_call_tool(client, "tool_name", {args})` invokes a tool and returns the result as a TL value
  4. Cancellation and progress notifications work for long-running tool calls
  5. Both VM and interpreter paths produce identical behavior
**Research**: Unlikely (follows established BuiltinId pattern)
**Plans**: TBD

### Phase 4: HTTP Transport
**Goal**: Add Streamable HTTP transport — TL can connect to remote MCP servers and serve as a remote MCP server over HTTP
**Depends on**: Phase 2
**Requirements**: TRANS-03, TRANS-04
**Success Criteria** (what must be TRUE):
  1. TL can connect to a remote MCP server via HTTP, complete handshake, and call tools
  2. TL can serve as an MCP server over HTTP — external clients can connect via POST+SSE
  3. Session management works (MCP-Session-Id header, session expiry with re-initialization)
  4. Origin validation prevents DNS rebinding attacks on local HTTP servers
**Research**: Likely (SSE parsing, session management, reconnection — complex protocol details)
**Research topics**: SSE client/server libraries for Rust, axum SSE integration patterns, session state management
**Plans**: TBD

### Phase 5: Agent Integration
**Goal**: TL agents transparently discover and use MCP tools alongside native TL functions
**Depends on**: Phase 3
**Requirements**: AGNT-01, AGNT-02, AGNT-03
**Success Criteria** (what must be TRUE):
  1. Agent with `mcp_servers: [client]` auto-imports tools from MCP servers into the agent's tool namespace
  2. Agent tool loop dispatches MCP tool calls to the correct server transparently
  3. Multiple MCP servers can be used simultaneously with tool name conflict resolution (server-prefixed namespacing)
  4. TL declares filesystem roots to MCP servers and notifies on changes
  5. Tool descriptions are sanitized to prevent tool poisoning attacks
**Research**: Likely (tool poisoning mitigation strategy, namespace conflict resolution)
**Research topics**: Tool description sanitization patterns, rug pull detection, multi-server namespace design
**Plans**: TBD

### Phase 6: MCP Server — Tools
**Goal**: Expose TL functions as MCP tools — external AI tools (Claude Desktop, Cursor) can call TL functions via MCP
**Depends on**: Phase 2
**Requirements**: TOOL-03, TOOL-04, TOOL-05, INFR-05
**Success Criteria** (what must be TRUE):
  1. `tl mcp serve script.tl --stdio` launches TL as an MCP server exposing functions from the script as tools
  2. `tl mcp serve script.tl --port 3000` serves over HTTP
  3. TL function parameters are correctly converted to JSON Schema inputSchema
  4. External MCP clients (Claude Desktop, MCP Inspector) can discover and call TL functions
  5. `print()` output is redirected to stderr when running in MCP server mode
**Research**: Likely (TL function → JSON Schema mapping, stdout redirection architecture)
**Research topics**: JSON Schema generation from TL's gradual type system, print() redirection in MCP mode
**Plans**: TBD

### Phase 7: Resources, Prompts & Server Capabilities
**Goal**: Expose TL's data engine as MCP resources and prompts — DataFusion tables, connections, and data workflows discoverable by MCP clients
**Depends on**: Phase 6
**Requirements**: RSRC-01, RSRC-02, RSRC-03, RSRC-04, RSRC-05, RSRC-06
**Success Criteria** (what must be TRUE):
  1. MCP clients can list and read resources via `tl://table/{name}`, `tl://connection/{name}` URIs
  2. Resource templates enable parameterized access (e.g., `tl://table/{name}/sample/{n}`)
  3. Prompt templates are available for common data workflows (EDA, data quality, pipeline building)
  4. Execution logs stream to MCP clients via logging capability
  5. Autocompletion works for table names, column names, and connection names
  6. listChanged notifications fire when tables, connections, or functions change
**Research**: Unlikely (builds on existing DataFusion catalog, established MCP patterns from Phase 6)
**Plans**: TBD

### Phase 8: Sampling & Hardening
**Goal**: Enable server-driven agentic loops through TL and harden performance — MCP servers can request LLM completions through TL's agent runtime
**Depends on**: Phase 5, Phase 6
**Requirements**: AGNT-04, AGNT-05
**Success Criteria** (what must be TRUE):
  1. TL handles sampling/createMessage requests from MCP servers, routing to the configured LLM provider
  2. Sampling with tools works — server-provided tool definitions create nested tool-use loops
  3. Configurable per-tool and per-connection timeouts prevent VM blocking
  4. Tool list caching avoids redundant tools/list calls (refresh only on listChanged notification)
**Research**: Likely (sampling control flow is complex, needs design validation)
**Research topics**: Sampling message routing through TL's LLM providers, nested tool loop termination, timeout strategy
**Plans**: TBD

## Progress

**Execution Order:**
Phases 1 → 2 → 3 → {4, 5, 6} parallel → 7 → 8

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Prerequisites & Security | 4/4 | Complete | 2026-03-10 |
| 2. Protocol & Stdio Transport | 0/TBD | Not started | - |
| 3. Client Builtins & VM | 0/TBD | Not started | - |
| 4. HTTP Transport | 0/TBD | Not started | - |
| 5. Agent Integration | 0/TBD | Not started | - |
| 6. MCP Server — Tools | 0/TBD | Not started | - |
| 7. Resources & Prompts | 0/TBD | Not started | - |
| 8. Sampling & Hardening | 0/TBD | Not started | - |
