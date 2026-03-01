# Execution Backends

TL supports multiple execution backends, each offering different tradeoffs between startup speed, runtime performance, and feature completeness.

## Bytecode VM (Default)

The default backend. Select explicitly with `--backend vm`.

```
tl run script.tl
tl run script.tl --backend vm
```

- **Architecture**: Register-based bytecode VM with u32 packed instructions.
- **Opcodes**: 66+ opcodes covering arithmetic, control flow, data operations, closures, pattern matching, ownership, and more.
- **Builtins**: 182 builtin functions (BuiltinId 0 through 181) covering data engineering, AI, streaming, security, and connectors.
- **JIT infrastructure**: Cranelift JIT infrastructure for potential future optimization.
- **Characteristics**: Fastest startup time. Bytecode is compiled from source and then interpreted by the VM. This is the most complete and well-tested backend.

Use `--dump-bytecode` to inspect the compiled bytecode:

```
tl run script.tl --dump-bytecode
```

Use `tl disasm` for standalone disassembly:

```
tl disasm script.tl
```

## Interpreter

The original tree-walking interpreter from Phase 0. Select with `--backend interp`.

```
tl run script.tl --backend interp
```

- **Architecture**: Direct AST execution via recursive tree walking.
- **Characteristics**: Slower than the bytecode VM but simpler in implementation. Useful for debugging language behavior since it operates directly on the AST without a compilation step.
- **Use cases**: Debugging, reference implementation, understanding execution flow.

## TL-IR Optimizer (Transparent)

The TL-IR intermediate representation is not a user-selectable backend. It operates transparently within the bytecode compiler to optimize table pipe chains.

- **Scope**: Applies to table pipe chain expressions (e.g., `data |> filter(...) |> select(...) |> sort(...)`).
- **Process**: The compiler detects pipe chain patterns, builds an IR query plan, runs optimizations, then lowers the plan back to bytecode operations.
- **Optimizations**:
  - **Filter merge**: Combines adjacent filter operations into a single filter.
  - **Predicate pushdown**: Moves filter operations closer to the data source.
  - **Column pruning**: Removes unused columns early in the pipeline.
  - **Common subexpression elimination (CSE)**: Deduplicates repeated computations.
- **Fallback**: If the IR path fails for any reason, the compiler falls back to the legacy pipe compilation path. This ensures robustness.
- **Crate**: `tl-ir`, depends only on `tl-ast`.

## LLVM Backend

Ahead-of-time native compilation via LLVM. Requires the `llvm-backend` feature and LLVM 19 installed on the system.

```
cargo build --features llvm-backend
```

### Running with LLVM

```
tl run script.tl --backend llvm
```

### Compiling to Object File

```
tl compile script.tl                  # Produces script.o
tl compile script.tl -o output.o      # Custom output path
tl compile script.tl --emit-ir        # Dump LLVM IR instead
```

### Architecture

- Built on inkwell 0.8 (Rust bindings to LLVM 19) with llvm-sys 191.
- VmValue is 464 bytes with 16-byte alignment; all alloca instructions use `align 16`.
- Three-tier opcode compilation:
  - **Tier 1**: Complex operations compiled as calls to runtime helper functions.
  - **Tier 2**: Operations compiled via dispatch-based code generation.
  - **Tier 3**: Unsupported operations fall back to the VM for execution.
- Uses MCJIT for execution when running with `--backend llvm`.
- Emits object files (`.o`) when using `tl compile`.

### Requirements

- LLVM 19 development libraries installed on the system.
- Build with `cargo build --features llvm-backend`.

## WASM Backend

Browser execution via WebAssembly. Requires the `wasm` feature.

```
cargo build --target wasm32-unknown-unknown --features wasm
```

- **Architecture**: The `tl-wasm` crate compiles to a WebAssembly target using wasm-bindgen.
- **Use case**: Running TL code in web browsers via a playground interface.
- **Characteristics**: Provides a web-based execution environment. The WASM module exposes functions that can be called from JavaScript to evaluate TL source code.

## Choosing a Backend

| Backend | Startup | Runtime Speed | Feature Coverage | Use Case |
|---------|---------|---------------|------------------|----------|
| VM | Fast | Good | Complete | Default, production |
| Interpreter | Fast | Slow | Complete | Debugging |
| LLVM | Slow (compile) | Fastest | Partial (3-tier) | Performance-critical |
| WASM | N/A | Moderate | Partial | Browser execution |
