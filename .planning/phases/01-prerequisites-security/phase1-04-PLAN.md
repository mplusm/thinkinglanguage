# Plan: Bidirectional Value Conversion

**Phase:** 1 — Prerequisites & Security Foundation
**Plan:** 04 of 04
**Requirement:** INFR-08
**Depends on:** Plan 02 (tl-mcp crate must exist)
**Estimated scope:** M (1–2 hours)

## Objective

Implement robust `json_to_tl_value()` and `tl_value_to_json()` conversion functions in the tl-mcp crate that handle all edge cases: NaN, Infinity, large numbers, Decimal, DateTime, nested maps, nil/None, Secret redaction. These will be the canonical converters used by all MCP operations.

## Context

### Existing Converters (to be superseded for MCP paths)
**VM (tl-compiler/src/vm.rs:159-203):**
- `vm_json_to_value()` / `vm_value_to_json()` — basic, handles Null/Bool/Number/String/Array/Object
- Missing: NaN/Infinity, Decimal, DateTime, Secret, Set, StructInstance, EnumInstance

**Interpreter (tl-interpreter/src/lib.rs:8886-8938):**
- `json_to_value()` / `value_to_json()` — same basic coverage
- Agent-specific variants exist too (`agent_value_to_json`, `agent_json_to_value`)

### Edge Cases to Handle
1. **NaN/Infinity** — JSON has no NaN/Infinity. Must convert to null or string representation.
2. **Large numbers** — i64 overflow from JSON. serde_json::Number may exceed i64 range.
3. **Decimal** — rust_decimal::Decimal must round-trip through JSON (as string to preserve precision).
4. **DateTime** — i64 millis must convert to ISO 8601 string in JSON.
5. **Nested maps** — recursive conversion with proper key handling (Arc<str> in VM, String in interpreter).
6. **Secret** — must be redacted to "***" in JSON output (never leak secret values).
7. **Set** — convert to JSON array (order not guaranteed).
8. **None/nil** — bidirectional null ↔ None.

### Design Decision
The tl-mcp convert module will define **trait-based** conversion that works with serde_json::Value. The VM and interpreter will implement thin adapters that call into tl-mcp's logic. This avoids duplicating edge case handling in two places.

Since tl-mcp cannot depend on tl-compiler or tl-interpreter (clean dependency boundary), we'll define the conversion as standalone functions operating on serde_json::Value with a `TlJsonValue` enum that both VmValue and Value can convert to/from.

## Tasks

### Task 1: Define TlJsonValue intermediate type
**Action:** In `crates/tl-mcp/src/convert.rs`, define:
```rust
/// Intermediate representation for TL values during JSON conversion.
/// Both VmValue and Value can convert to/from this type.
#[derive(Debug, Clone)]
pub enum TlJsonValue {
    Nil,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<TlJsonValue>),
    Map(Vec<(String, TlJsonValue)>),
    Decimal(String),    // String representation to preserve precision
    DateTime(i64),      // Millis since epoch
    Secret,             // Redacted — no value stored
}
```

### Task 2: Implement json_to_tl() conversion
**Action:** In `crates/tl-mcp/src/convert.rs`:
```rust
pub fn json_to_tl(v: &serde_json::Value) -> TlJsonValue
```
- `Null` → `TlJsonValue::Nil`
- `Bool(b)` → `TlJsonValue::Bool(b)`
- `Number` → try `as_i64()` first, then `as_f64()`, handle overflow
- `String` → `TlJsonValue::String`
- `Array` → recursive `TlJsonValue::List`
- `Object` → recursive `TlJsonValue::Map` (preserving key order from serde_json::Map)

### Task 3: Implement tl_to_json() conversion
**Action:** In `crates/tl-mcp/src/convert.rs`:
```rust
pub fn tl_to_json(v: &TlJsonValue) -> serde_json::Value
```
- `Nil` → `Value::Null`
- `Bool` → `Value::Bool`
- `Int` → `Value::Number` (i64)
- `Float` → `Value::Null` if NaN/Infinity, else `Value::Number`
- `String` → `Value::String`
- `List` → recursive `Value::Array`
- `Map` → recursive `Value::Object`
- `Decimal` → `Value::String` (preserves precision)
- `DateTime` → `Value::String` (ISO 8601 via chrono)
- `Secret` → `Value::String("***")`

### Task 4: Add chrono dependency
**Action:** Add `chrono = "0.4"` to `crates/tl-mcp/Cargo.toml` for DateTime formatting.

### Task 5: Write comprehensive tests
**Action:** In `crates/tl-mcp/src/convert.rs`, add tests:
- `test_null_roundtrip` — null ↔ Nil
- `test_bool_roundtrip` — true/false
- `test_int_roundtrip` — small, large, negative, i64::MAX
- `test_float_roundtrip` — normal, negative, fractional
- `test_nan_to_null` — NaN → null
- `test_infinity_to_null` — Infinity → null
- `test_string_roundtrip` — empty, unicode, emoji
- `test_nested_map` — 3 levels deep
- `test_mixed_array` — [int, string, null, nested_object]
- `test_decimal_as_string` — Decimal preserves precision through string
- `test_datetime_iso8601` — millis → ISO 8601 string
- `test_secret_redacted` — Secret → "***"
- `test_empty_map` — {} ↔ empty map
- `test_empty_array` — [] ↔ empty list
- `test_large_number` — numbers near i64 boundary

### Task 6: Build and test
**Action:**
- `cargo test -p tl-mcp` — all conversion tests pass
- `cargo build --workspace` — no breakage
**Checkpoint:** 15+ tests pass.

## Verification

- [ ] `TlJsonValue` enum defined with all variants
- [ ] `json_to_tl()` handles all JSON types correctly
- [ ] `tl_to_json()` handles all TL types correctly
- [ ] NaN/Infinity → null (not panic or invalid JSON)
- [ ] Decimal → string (preserves precision)
- [ ] DateTime → ISO 8601 string
- [ ] Secret → "***" (never leaked)
- [ ] Nested structures convert recursively
- [ ] 15+ tests pass

## Success Criteria

`json_to_tl()` and `tl_to_json()` handle all edge cases (NaN, large numbers, nested maps, nil, Decimal, DateTime, Secret) with comprehensive test coverage. The conversion is canonical — future MCP operations use these functions.
