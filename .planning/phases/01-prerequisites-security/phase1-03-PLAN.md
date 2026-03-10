# Plan: SecurityPolicy Subprocess Extension

**Phase:** 1 — Prerequisites & Security Foundation
**Plan:** 03 of 04
**Requirement:** INFR-02
**Depends on:** Nothing (can run parallel with Plan 01)
**Estimated scope:** S (< 1 hour)

## Objective

Extend SecurityPolicy with `allow_subprocess` and `allowed_commands` fields so that `mcp_connect` (future Phase 2) can be gated behind sandbox permissions. Subprocess spawning must be denied by default in sandbox mode.

## Context

### Current SecurityPolicy
**File:** `crates/tl-errors/src/security.rs`

```rust
pub struct SecurityPolicy {
    pub allowed_connectors: HashSet<String>,
    pub denied_paths: Vec<String>,
    pub allow_network: bool,
    pub allow_file_read: bool,
    pub allow_file_write: bool,
    pub sandbox_mode: bool,
}
```

- `sandbox()` constructor: network=false, file_write=false, sandbox_mode=true
- `permissive()` constructor: all true, sandbox_mode=false
- `check(permission)` method: matches on "network", "file_read", "file_write", "python", "env_write", "connector:TYPE"
- 4 existing tests

### CLI Integration
**File:** `crates/tl-cli/src/main.rs`
- `--sandbox` flag creates `SecurityPolicy::sandbox()`
- `--allow-connector` adds to `allowed_connectors`

## Tasks

### Task 1: Add subprocess fields to SecurityPolicy
**Action:** In `crates/tl-errors/src/security.rs`, add two fields:
```rust
pub allow_subprocess: bool,
pub allowed_commands: Vec<String>,
```
- `allow_subprocess`: whether subprocess spawning is permitted at all
- `allowed_commands`: whitelist of allowed command names (empty = allow all when subprocess is allowed)

### Task 2: Update constructors
**Action:**
- `permissive()`: `allow_subprocess: true, allowed_commands: vec![]`
- `sandbox()`: `allow_subprocess: false, allowed_commands: vec![]`

### Task 3: Update check() method
**Action:** Add two new permission match arms:
- `"subprocess"` → returns `self.allow_subprocess`
- `"command:CMD"` pattern → returns `!self.sandbox_mode || self.allowed_commands.is_empty() || self.allowed_commands.contains(&cmd)`

### Task 4: Add check_command() convenience method
**Action:** Add a dedicated method for subprocess command validation:
```rust
pub fn check_command(&self, command: &str) -> bool {
    if !self.allow_subprocess {
        return false;
    }
    if self.allowed_commands.is_empty() {
        return true;
    }
    self.allowed_commands.iter().any(|c| c == command)
}
```

### Task 5: Add CLI flags
**Action:** In `crates/tl-cli/src/main.rs`:
- Add `--allow-subprocess` flag (enables subprocess spawning in sandbox)
- Add `--allow-command <CMD>` repeated flag (whitelist specific commands)
- Wire them: when `--allow-subprocess`, set `policy.allow_subprocess = true`
- When `--allow-command CMD`, push to `policy.allowed_commands`

### Task 6: Write tests
**Action:** Add tests in `crates/tl-errors/src/security.rs`:
- `test_sandbox_denies_subprocess` — sandbox mode blocks subprocess
- `test_permissive_allows_subprocess` — permissive allows subprocess
- `test_command_whitelist` — only whitelisted commands pass
- `test_empty_whitelist_allows_all` — empty whitelist = allow all (when subprocess enabled)

### Task 7: Build and run tests
**Action:**
- `cargo test -p tl-errors` — all tests pass
- `cargo build --workspace` — no breakage
**Checkpoint:** All existing + new tests pass.

## Verification

- [ ] SecurityPolicy has `allow_subprocess` and `allowed_commands` fields
- [ ] `sandbox()` sets `allow_subprocess: false`
- [ ] `permissive()` sets `allow_subprocess: true`
- [ ] `check("subprocess")` works correctly
- [ ] `check_command("npx")` validates against whitelist
- [ ] CLI has `--allow-subprocess` and `--allow-command` flags
- [ ] All tests pass (existing + 4 new)

## Success Criteria

SecurityPolicy has `allow_subprocess` and `allowed_commands` fields, enforced in sandbox mode, with CLI flags for opt-in. Future `mcp_connect` will call `check_command()` before spawning.
