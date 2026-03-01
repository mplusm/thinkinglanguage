# Contributing to ThinkingLanguage

Thank you for your interest in contributing to ThinkingLanguage! This guide covers how to build, test, and contribute to the project.

## Building

ThinkingLanguage is a Rust workspace using edition 2024. You need Rust 1.85 or later.

```bash
# Clone the repository
git clone https://github.com/mplusm/thinkinglanguage.git
cd thinkinglanguage

# Build the entire workspace
cargo build --workspace

# Build in release mode
cargo build --workspace --release
```

### Feature-Gated Builds

Several crates have optional dependencies behind feature flags:

```bash
# Build with SQLite support
cargo build --features sqlite

# Build with multiple features
cargo build --features "sqlite,async-runtime,notebook"

# LLVM backend requires LLVM 19 installed on your system
cargo build --features llvm-backend

# GPU support requires Vulkan/Metal/DX12 drivers
cargo build --features gpu

# Python FFI requires Python 3.8+ development headers
cargo build --features python
```

## Running Tests

```bash
# Run all tests (excluding feature-gated crates that need external deps)
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu

# With SQLite tests
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu --features sqlite

# With async runtime tests
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu --features async-runtime

# With Python FFI tests (requires Python 3.8+)
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu --features python

# LLVM backend tests (requires LLVM 19)
cargo test -p tl-llvm

# GPU tests (requires GPU hardware)
cargo test -p tl-gpu --features gpu
```

### Test Counts

As of Phase 33 (with dependency upgrade features):
- Base: ~1272 tests (without feature flags)
- With `sqlite`: +5 tests
- With `async-runtime`: ~1252 tests
- With `python`: +44 tests
- With `llvm-backend`: +26 tests
- GPU tests require hardware

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `tl-lexer` | Tokenization using logos |
| `tl-ast` | Abstract syntax tree definitions |
| `tl-parser` | Recursive descent parser |
| `tl-interpreter` | Tree-walking interpreter |
| `tl-compiler` | Bytecode compiler + register-based VM |
| `tl-types` | Type system, type checker |
| `tl-errors` | Error types, diagnostics, source spans |
| `tl-data` | Data engine (DataFusion, Arrow, Parquet) |
| `tl-ai` | AI/ML (ndarray, linfa, ONNX via ort) |
| `tl-stream` | Streaming, pipelines, Kafka |
| `tl-ir` | Intermediate representation optimizer |
| `tl-llvm` | LLVM AOT compilation backend |
| `tl-wasm` | WASM browser backend |
| `tl-gpu` | GPU tensor operations (wgpu) |
| `tl-lsp` | Language Server Protocol server |
| `tl-package` | Package manager and dependency resolution |
| `tl-registry` | Package registry HTTP server |
| `tl-cli` | CLI entry point (`tl` binary) |

## Adding a New Builtin Function

Builtin functions are registered by ID and dispatched in both the VM and interpreter.

### Step 1: Add a BuiltinId

In `crates/tl-compiler/src/chunk.rs`, add a new variant to the `BuiltinId` enum:

```rust
pub enum BuiltinId {
    // ... existing variants ...
    MyNewBuiltin = 182,  // use next available ID
}
```

Also add the string-to-ID mapping in the `from_name` method and ID-to-name in `name`.

### Step 2: Implement in VM

In `crates/tl-compiler/src/vm.rs`, add a handler in the `call_builtin` match:

```rust
BuiltinId::MyNewBuiltin => {
    // args is a slice of VmValue
    let result = /* your implementation */;
    Ok(result)
}
```

### Step 3: Implement in Interpreter

In `crates/tl-interpreter/src/lib.rs`, add the builtin name to the builtins match:

```rust
"my_new_builtin" => {
    // args is a Vec<Value>
    let result = /* your implementation */;
    Ok(result)
}
```

### Step 4: Add to Tab Completion

In `crates/tl-cli/src/main.rs`, add the function name to the `TlHelper::new()` completions list.

### Step 5: Add Tests

Add tests in the relevant test file (usually `crates/tl-compiler/tests/` or as inline `#[test]` functions).

## Adding a New Connector

Follow the pattern established by MySQL and SQLite connectors:

1. **Feature gate**: Add a feature in `crates/tl-cli/Cargo.toml` and propagate to compiler/interpreter crates
2. **Builtins**: Add `BuiltinId` variants for read/write operations
3. **VM implementation**: Wrap connector calls with `#[cfg(feature = "your_connector")]`
4. **Interpreter implementation**: Mirror the VM implementation
5. **Tests**: Add feature-gated integration tests
6. **Documentation**: Update docs/data/connectors.md

## Code Style

- **Rust edition 2024**: Do not use `ref` in implicitly-borrowing match patterns
- **Feature gates**: Use `#[cfg(feature = "...")]` for optional dependencies; never make optional deps required
- **Error handling**: Use `TlError` variants from `tl-errors` for compiler/parser errors; VM runtime errors use string messages or structured error enums
- **No unnecessary allocations**: Prefer `&str` over `String` where lifetime allows, use `Arc<str>` for shared strings in VM values
- **Testing**: Every new feature should have tests; prefer testing via .tl source snippets that exercise the full pipeline (parse → compile → execute)

## Architecture Notes

- **VM instructions**: u32 packed format with opcode + register operands. See `chunk.rs` for encoding.
- **Value types**: `Value` (interpreter) and `VmValue` (VM) are separate types with similar structure
- **Table operations**: The 13 core table ops (filter/select/sort/with/aggregate/join/head/limit/collect/show/describe/write_csv/write_parquet) have special compiler support. Other data ops (Phase 15+) route through the general builtin call path.
- **String interpolation**: `{` inside strings triggers interpolation — keep this in mind when generating JSON in TL code
- **Upvalue closing**: Closures returned from functions have their upvalues promoted from Open to Closed before the stack frame is truncated (Phase 26 fix)

## Pull Request Guidelines

1. **One feature per PR**: Keep changes focused
2. **Tests required**: All new functionality needs test coverage
3. **Feature-gate optional deps**: External service dependencies must be behind feature flags
4. **Update docs**: If your change adds user-facing functionality, update the relevant docs/ file
5. **Run the test suite** before submitting:
   ```bash
   cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu
   ```
6. **Format your code**: `cargo fmt --all`
7. **Check for warnings**: `cargo clippy --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu`

## License

By contributing, you agree that your contributions will be licensed under the MIT OR Apache-2.0 dual license.
