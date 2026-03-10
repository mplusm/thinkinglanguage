# Plan: Create tl-mcp Crate Skeleton

**Phase:** 1 — Prerequisites & Security Foundation
**Plan:** 02 of 04
**Requirement:** INFR-07
**Depends on:** Plan 01 (reqwest 0.13 needed for rmcp)
**Estimated scope:** S (< 1 hour)

## Objective

Create the `tl-mcp` crate with rmcp 1.1 dependency, feature-gated behind `mcp`. The crate must compile and be wired into the workspace. Clean dependency boundary: no dep on tl-compiler, tl-ast, or tl-interpreter.

## Context

### Architecture
- tl-mcp follows the same pattern as tl-ai, tl-stream: self-contained protocol crate
- Dependencies: rmcp (MCP SDK), serde_json, tokio, tl-errors (for SecurityPolicy)
- Feature-gated: `mcp` feature in tl-compiler and tl-cli
- Path dependency with version: `tl-mcp = { path = "../tl-mcp", version = "0.3.0" }`

### rmcp Features Needed
- `client` — MCP client functionality
- `server` — MCP server functionality
- `transport-io` — stdio transport
- `transport-child-process` — subprocess spawning
- `transport-streamable-http-server` — HTTP server (axum-based)
- `transport-streamable-http-client-reqwest` — HTTP client (reqwest-based)
- `transport-sse-client` — SSE parsing
- `macros` — derive macros for tool registration

## Tasks

### Task 1: Create crate directory and Cargo.toml
**Action:** Create `crates/tl-mcp/Cargo.toml`:
```toml
[package]
name = "tl-mcp"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "MCP (Model Context Protocol) integration for ThinkingLanguage"

[dependencies]
tl-errors = { path = "../tl-errors", version = "0.3.0" }
rmcp = { version = "1.1", features = [
    "client",
    "server",
    "macros",
    "transport-io",
    "transport-child-process",
    "transport-streamable-http-server",
    "transport-streamable-http-client-reqwest",
    "transport-sse-client",
] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt", "sync", "process", "io-util"] }
thiserror.workspace = true

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### Task 2: Create lib.rs skeleton
**Action:** Create `crates/tl-mcp/src/lib.rs` with module structure placeholders:
```rust
//! MCP (Model Context Protocol) integration for ThinkingLanguage.
//!
//! Provides client and server MCP support over stdio and HTTP transports.

pub mod convert;

// Re-export rmcp types used by other crates
pub use rmcp;
```

### Task 3: Create convert module placeholder
**Action:** Create `crates/tl-mcp/src/convert.rs` as an empty module (populated in Plan 04):
```rust
//! Bidirectional conversion between serde_json::Value and TL values.
```

### Task 4: Add to workspace
**Action:** Add `"crates/tl-mcp"` to the `[workspace] members` list in root `Cargo.toml`.

### Task 5: Add feature gate in tl-compiler
**Action:** In `crates/tl-compiler/Cargo.toml`:
- Add optional dependency: `tl-mcp = { path = "../tl-mcp", version = "0.3.0", optional = true }`
- Add feature: `mcp = ["dep:tl-mcp"]`

### Task 6: Add feature gate in tl-cli
**Action:** In `crates/tl-cli/Cargo.toml`:
- Add feature: `mcp = ["tl-compiler/mcp"]`

### Task 7: Build and verify
**Action:**
- `cargo build -p tl-mcp` — crate compiles standalone
- `cargo build --workspace` — no workspace breakage
- `cargo build -p tl-compiler --features mcp` — feature gate works
**Checkpoint:** All three builds succeed.

## Verification

- [ ] `crates/tl-mcp/` exists with Cargo.toml and src/lib.rs
- [ ] `cargo build -p tl-mcp` compiles cleanly
- [ ] `cargo build --workspace` succeeds
- [ ] `cargo build -p tl-compiler --features mcp` succeeds
- [ ] tl-mcp has NO dependency on tl-compiler, tl-ast, tl-interpreter, tl-parser

## Success Criteria

`tl-mcp` crate exists with rmcp 1.1 dependency, compiles, is feature-gated behind `mcp`, and has a clean dependency boundary (only depends on tl-errors).
