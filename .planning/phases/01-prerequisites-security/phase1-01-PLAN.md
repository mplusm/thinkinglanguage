# Plan: reqwest 0.12 → 0.13 Upgrade

**Phase:** 1 — Prerequisites & Security Foundation
**Plan:** 01 of 04
**Requirement:** INFR-06
**Depends on:** Nothing (can run parallel with Plan 03)
**Estimated scope:** S (< 1 hour)

## Objective

Upgrade reqwest from 0.12 to 0.13 across all 7 crates in the workspace. This is a prerequisite for rmcp 1.1.0 which requires reqwest 0.13.

## Context

### Current State
- 7 crates use reqwest 0.12: tl-ai, tl-stream, tl-compiler, tl-interpreter, tl-data, tl-registry, tl-package
- No crate uses `.query()` (feature-gated in 0.13) — clean upgrade path
- All use `blocking` feature + `rustls-tls` (except tl-registry which is dev-only with just `blocking, json`)
- Features used: `blocking`, `json`, `rustls-tls`

### Files to Modify
1. `crates/tl-ai/Cargo.toml` — line 24
2. `crates/tl-stream/Cargo.toml` — line 21
3. `crates/tl-compiler/Cargo.toml` — line 40
4. `crates/tl-interpreter/Cargo.toml` — line 23
5. `crates/tl-data/Cargo.toml` — line 37
6. `crates/tl-registry/Cargo.toml` — line 28 (dev-dependencies)
7. `crates/tl-package/Cargo.toml` — line 22

### Breaking Changes in reqwest 0.13
- `.query()` is now feature-gated behind `query` feature — NOT USED in TL (verified)
- `default-tls` renamed — TL uses `rustls-tls` exclusively, unaffected
- Minimum Rust version bump — TL is on edition 2024, unaffected

## Tasks

### Task 1: Update all Cargo.toml files
**Action:** Change `version = "0.12"` to `version = "0.13"` in all 7 files listed above.

### Task 2: Build workspace
**Action:** Run `cargo build --workspace` and verify clean compilation.
**Checkpoint:** Build succeeds with zero errors.

### Task 3: Run full test suite
**Action:** Run `RUST_MIN_STACK=16777216 cargo test --workspace --exclude tl-gpu --exclude benchmarks --features "duckdb,snowflake,bigquery,databricks,clickhouse"` and verify no regressions.
**Checkpoint:** Same pass/fail counts as before upgrade (1,331+ passed).

### Task 4: Verify connector functionality
**Action:** Spot-check that HTTP-based connectors still compile with their features:
- `cargo build -p tl-data --features clickhouse,snowflake,bigquery,databricks`
- `cargo build -p tl-package --features registry`
**Checkpoint:** Both build cleanly.

## Verification

- [ ] All 7 Cargo.toml files show reqwest 0.13
- [ ] `cargo build --workspace` succeeds
- [ ] Full test suite passes with no regressions
- [ ] Feature-gated connector builds succeed

## Success Criteria

`cargo build --workspace` succeeds with reqwest 0.13 across all 7 crates, and all existing tests pass without regression.
