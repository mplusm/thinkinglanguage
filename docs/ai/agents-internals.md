# Agent Framework Internals

Implementation guide for contributors. Covers the full code path from source text to agent execution.

## Architecture Overview

```
Source Code
    │
    ▼
┌─────────┐    Token::Agent
│  Lexer   │─────────────────────┐
└─────────┘                      │
    │                            ▼
    ▼                     ┌─────────────┐
┌─────────┐               │ parse_agent │
│ Parser  │◄──────────────┤             │
└─────────┘               └─────────────┘
    │                            │
    │                    StmtKind::Agent
    ▼                            │
┌──────────────┐         ┌──────────────┐
│ Interpreter  │         │   Compiler   │
│ exec_agent() │         │compile_agent()│
└──────────────┘         └──────────────┘
    │                            │
    │ Value::Agent          Op::AgentExec
    │                            │
    ▼                            ▼
┌──────────────┐         ┌──────────────┐
│  run_agent   │         │  VM handler  │
│  builtin     │         │handle_agent_ │
└──────────────┘         │    exec()    │
    │                    └──────────────┘
    │                            │
    ▼                            ▼
┌──────────────────────────────────┐
│       exec_agent_loop()          │
│  (shared logic, both backends)   │
│                                  │
│  ┌─────────────────────────┐     │
│  │ chat_with_tools() [LLM] │     │
│  └─────────────────────────┘     │
│         │           │            │
│    Text response  ToolUse        │
│         │           │            │
│      Return      Dispatch        │
│      result      TL fn           │
└──────────────────────────────────┘
```

## File Map

| File | Role |
|------|------|
| `crates/tl-lexer/src/lib.rs` | `Token::Agent` keyword |
| `crates/tl-ast/src/lib.rs` | `StmtKind::Agent` AST node |
| `crates/tl-parser/src/lib.rs` | `parse_agent()`, `parse_map_literal()`, `token_as_key_name()` |
| `crates/tl-ai/src/llm.rs` | `LlmResponse`, `ToolCall`, `chat_with_tools()`, provider detection |
| `crates/tl-ai/src/lib.rs` | Re-exports: `LlmResponse`, `ToolCall`, `chat_with_tools` |
| `crates/tl-stream/src/agent.rs` | `AgentDef`, `AgentTool` structs |
| `crates/tl-stream/src/lib.rs` | Re-exports agent module |
| `crates/tl-interpreter/src/lib.rs` | `Value::Agent`, `exec_agent()`, `exec_agent_loop()` |
| `crates/tl-compiler/src/opcode.rs` | `Op::AgentExec = 67` |
| `crates/tl-compiler/src/chunk.rs` | `BuiltinId::RunAgent = 184` |
| `crates/tl-compiler/src/value.rs` | `VmValue::AgentDef` |
| `crates/tl-compiler/src/compiler.rs` | `compile_agent()`, `compile_agent_hook()` |
| `crates/tl-compiler/src/vm.rs` | `handle_agent_exec()`, `exec_agent_loop()`, `execute_tool_call()` |
| `crates/tl-llvm/src/codegen.rs` | `Op::AgentExec` in VM fallback tier |

## Stage 1: Lexer

**File:** `crates/tl-lexer/src/lib.rs`

A single token addition:

```rust
#[token("agent")]
Agent,
```

The `Display` impl maps it to `"agent"`.

## Stage 2: AST

**File:** `crates/tl-ast/src/lib.rs`

```rust
StmtKind::Agent {
    name: String,
    model: String,
    system_prompt: Option<String>,
    tools: Vec<(String, Expr)>,       // (fn_name, map_expr)
    max_turns: Option<i64>,
    temperature: Option<f64>,
    max_tokens: Option<i64>,
    base_url: Option<String>,
    api_key: Option<String>,
    on_tool_call: Option<Vec<Stmt>>,  // lifecycle hook body
    on_complete: Option<Vec<Stmt>>,   // lifecycle hook body
}
```

Design decisions:
- `tools` stores the raw `Expr` (map expression) -- evaluation happens later in interpreter/compiler.
- Lifecycle hooks store `Vec<Stmt>` (statement bodies), not function definitions. They become functions during interpretation/compilation.
- `max_turns` and `max_tokens` are `i64` at the AST level (matching TL's `Int` type), converted to `u32` at runtime.

## Stage 3: Parser

**File:** `crates/tl-parser/src/lib.rs`

### Entry point

```rust
// In parse_statement() dispatch:
Token::Agent => self.parse_agent(),
```

### parse_agent()

Follows the same pattern as `parse_pipeline()`:

1. Consume `Token::Agent`
2. Read agent name (identifier)
3. Expect `{`
4. Loop over config fields until `}`
5. Return `StmtKind::Agent`

Field dispatch uses string matching on `Token::Ident`:

```rust
Token::Model => { /* parse model: "string" */ }
Token::Ident(s) if s == "system" => { /* parse system: "string" */ }
Token::Ident(s) if s == "tools" => { /* parse tools block */ }
Token::Ident(s) if s == "max_turns" => { /* parse max_turns: int */ }
Token::Ident(s) if s == "on_tool_call" => { /* parse on_tool_call { body } */ }
Token::Ident(s) if s == "on_complete" => { /* parse on_complete { body } */ }
```

Note: `model` uses `Token::Model` (a dedicated keyword), while other fields use `Token::Ident` with string comparison.

### parse_map_literal()

A special parser for JSON-like `{ key: value }` syntax used in tool definitions:

```rust
fn parse_map_literal(&mut self) -> Result<Expr, TlError>
```

Handles:
- Identifier keys: `name: "value"`
- String keys: `"name": "value"`
- Keyword keys: `type: "object"` (via `token_as_key_name()`)
- Nested maps: `{ inner: { nested: true } }`
- Lists: `required: ["a", "b"]`
- Optional trailing commas

### token_as_key_name()

Converts keyword tokens to string names for use as map keys:

```rust
fn token_as_key_name(token: &Token) -> Option<String> {
    match token {
        Token::Type => Some("type".into()),
        Token::Model => Some("model".into()),
        Token::Source => Some("source".into()),
        // ... other keywords
        _ => None,
    }
}
```

This is necessary because `type: "object"` would otherwise fail -- `type` is a keyword (`Token::Type`), not an identifier.

## Stage 4: Interpreter

**File:** `crates/tl-interpreter/src/lib.rs`

### Value::Agent

```rust
Agent(tl_stream::AgentDef),
```

- Display: `<agent {name}>`
- type_of: `"agent"`
- PartialEq: compares by agent name

### exec_agent()

Called from `exec_stmt` when processing `StmtKind::Agent`.

1. Evaluates each tool's map expression → `Value::Map`
2. Extracts `description` (string) and `parameters` (JSON via `agent_value_to_json`) from each tool map
3. Builds `AgentDef` with resolved values
4. Stores `Value::Agent(def)` in environment under the agent name
5. Compiles lifecycle hooks as `Value::Function` with mangled global names

Lifecycle hook storage:
```rust
// on_tool_call → __agent_{name}_on_tool_call__(tool_name, tool_args, tool_result)
// on_complete  → __agent_{name}_on_complete__(result)
```

### exec_agent_loop()

The core agent loop:

```rust
fn exec_agent_loop(&mut self, agent_def: &AgentDef, user_message: &str) -> Result<Value, TlError>
```

1. Detect provider: `"anthropic"` if model starts with `"claude"`, else `"openai"`
2. Build `tools_json`: converts `AgentTool` to OpenAI function-calling format
3. Initialize messages: `[{"role": "user", "content": message}]`
4. Loop up to `max_turns`:
   - Call `chat_with_tools()`
   - `LlmResponse::Text` → build result map, call `on_complete` hook, return
   - `LlmResponse::ToolUse` → for each tool call:
     a. Look up TL function by name in `self.env`
     b. Convert JSON args to `Vec<Value>` via `agent_json_to_values()`
     c. Call function via `call_function_value()`
     d. Stringify result
     e. Call `on_tool_call` hook
     f. Format tool results via `format_tool_result_messages()` and append to messages
5. If max_turns exceeded: error

### Helper functions

| Function | Purpose |
|----------|---------|
| `extract_agent_tool(map)` | Extracts description + parameters from a `Value::Map` |
| `agent_value_to_json(value)` | Recursively converts any `Value` to `serde_json::Value` |
| `agent_json_to_values(json)` | Converts JSON object/array to `Vec<Value>` (positional args) |
| `agent_json_to_value(json)` | Converts a single JSON value to `Value` |
| `call_function_value(func, args)` | Calls a `Value::Function` with explicit scope push/pop |

## Stage 5: Compiler

**File:** `crates/tl-compiler/src/compiler.rs`

### compile_agent()

Called from `compile_stmt` when processing `StmtKind::Agent`.

The compiler encodes the entire agent configuration as a `Constant::AstExprList`:

```rust
fn compile_agent(&mut self, name, model, system_prompt, tools, max_turns,
    temperature, max_tokens, base_url, api_key, on_tool_call, on_complete)
```

1. Pack all config into `Vec<AstExpr>` as `NamedArg` entries:
   ```
   model: "gpt-4o"           → NamedArg("model", AstExpr::String("gpt-4o"))
   tool:search: {desc, params} → NamedArg("tool:search", AstExpr::Map(...))
   on_tool_call: true         → NamedArg("on_tool_call", AstExpr::Bool(true))
   ```
2. Store as `Constant::AstExprList(exprs)` and `Constant::String(name)`
3. Emit `Op::AgentExec A=dest, B=name_const, C=config_const`
4. Emit `Op::SetGlobal` to store agent in globals

### compile_agent_hook()

Compiles lifecycle hooks as separate function prototypes:

```rust
fn compile_agent_hook(&mut self, hook_name: &str, params: &[Param], body: &[Stmt])
```

1. Calls `self.compile_function()` to create a new prototype
2. Emits `Op::SetGlobal` with the mangled name (e.g., `__agent_bot_on_tool_call__`)

Hook parameters:
- `on_tool_call`: `(tool_name, tool_args, tool_result)`
- `on_complete`: `(result)`

## Stage 6: VM

**File:** `crates/tl-compiler/src/vm.rs`

### Op::AgentExec handler

Dispatched in the main opcode loop. Delegates to `handle_agent_exec()`.

### handle_agent_exec()

Reads agent configuration from bytecode constants:

```rust
fn handle_agent_exec(&mut self, frame_idx: usize, b: u8, c: u8) -> Result<VmValue, TlError>
```

1. Read agent name from `constants[b]` (a `Constant::String`)
2. Read config from `constants[c]` (a `Constant::AstExprList`)
3. Iterate `NamedArg` entries:
   - `"tool:*"` prefix → extract tool via `extract_tool_from_ast()`
   - `"model"` → set model string
   - `"system"` → set system prompt
   - etc.
4. Build `AgentDef` and return `VmValue::AgentDef(Arc::new(def))`

### VmValue::AgentDef

```rust
#[cfg(feature = "native")]
AgentDef(Arc<AgentDef>),
```

- Display: `<agent {name}>`
- type_name: `"agent"`
- Only available with `native` feature (not WASM)

### BuiltinId::RunAgent handler

Extracts `AgentDef` and message string from args, calls `exec_agent_loop()`.

### exec_agent_loop() (VM version)

Nearly identical to the interpreter version. Key differences:

| Aspect | Interpreter | VM |
|--------|-------------|-----|
| Value type | `Value` | `VmValue` |
| Map keys | `String` | `Arc<str>` |
| Function lookup | `self.env.get()` | `self.globals.get()` |
| Function call | `call_function_value()` | `self.call_value()` |
| Tool dispatch | Direct function call | `execute_tool_call()` |

### execute_tool_call()

```rust
fn execute_tool_call(&mut self, tool_name: &str, input: &serde_json::Value) -> Result<String, TlError>
```

1. Look up function in `self.globals`
2. Convert JSON args via `json_to_vm_args()`
3. Call via `self.call_value(func, &args)`
4. Format result as string

### JSON↔VmValue conversion

| Function | Direction | Purpose |
|----------|-----------|---------|
| `json_to_vm_args(json)` | JSON → Vec<VmValue> | Convert tool call args (object values → positional) |
| `json_value_to_vm(json)` | JSON → VmValue | Single value conversion |
| `ast_to_json(ast_expr)` | AstExpr → JSON | Convert tool definition maps in bytecode constants |
| `extract_tool_from_ast(expr)` | AstExpr → (name, JSON) | Extract tool description + parameters |

## Stage 7: LLM Client

**File:** `crates/tl-ai/src/llm.rs`

### chat_with_tools()

The core LLM function used by the agent loop:

```rust
pub fn chat_with_tools(
    model: &str,
    system: Option<&str>,
    messages: &[serde_json::Value],
    tools: &[serde_json::Value],
    base_url: Option<&str>,
    api_key: Option<&str>,
) -> Result<LlmResponse, String>
```

Provider selection logic:
```
1. detect_provider(model) → "anthropic" or "openai"
2. If base_url is set → always use OpenAI protocol
3. If TL_LLM_BASE_URL env is set and no explicit base_url → use that URL with OpenAI protocol
4. Otherwise → use native provider API
```

### Response parsing

**OpenAI format:**
```json
{
  "choices": [{
    "message": {
      "content": "text response",
      "tool_calls": [{"id": "...", "function": {"name": "...", "arguments": "{...}"}}]
    }
  }]
}
```
- If `tool_calls` is present → `LlmResponse::ToolUse`
- Otherwise → `LlmResponse::Text`

**Anthropic format:**
```json
{
  "content": [
    {"type": "text", "text": "..."},
    {"type": "tool_use", "id": "...", "name": "...", "input": {...}}
  ]
}
```
- If any content block has `type == "tool_use"` → `LlmResponse::ToolUse`
- Otherwise → concatenate all `text` blocks → `LlmResponse::Text`

### format_tool_result_messages()

Formats tool results for the next LLM call:

**OpenAI:** Separate `role: "tool"` messages per result:
```json
[{"role": "tool", "tool_call_id": "id", "content": "result"}]
```

**Anthropic:** Single `role: "user"` message with `tool_result` content blocks:
```json
[{"role": "user", "content": [{"type": "tool_result", "tool_use_id": "id", "content": "result"}]}]
```

### API key resolution

```rust
fn resolve_api_key(model: &str, explicit_key: Option<&str>) -> Result<String, String>
```

Priority:
1. `explicit_key` parameter (from agent definition or function arg)
2. `TL_LLM_KEY` env var
3. Provider-specific: `TL_ANTHROPIC_KEY` or `TL_OPENAI_KEY`

## Test Locations

| Test File | Tests | Count |
|-----------|-------|-------|
| `crates/tl-parser/src/lib.rs` | `test_parse_agent_*` | 5 |
| `crates/tl-interpreter/src/lib.rs` | `test_interp_agent_*` | 6 |
| `crates/tl-compiler/src/vm.rs` | `test_vm_agent_*` | 6 |
| `crates/tl-compiler/tests/ai_vm_integration.rs` | `test_vm_agent_live_api` | 1 (ignored) |

Total: 18 tests (17 unit + 1 integration).

### Running tests

```bash
# All agent tests
RUSTUP_TOOLCHAIN=stable cargo test agent --workspace --exclude tl-gpu --exclude benchmarks

# Parser tests only
RUSTUP_TOOLCHAIN=stable cargo test -p tl-parser test_parse_agent

# Interpreter tests only
RUSTUP_TOOLCHAIN=stable cargo test -p tl-interpreter --lib test_interp_agent

# VM tests only
RUSTUP_TOOLCHAIN=stable cargo test -p tl-compiler --lib test_vm_agent

# Integration test (requires TL_OPENAI_KEY)
RUSTUP_TOOLCHAIN=stable cargo test -p tl-compiler --test ai_vm_integration test_vm_agent_live_api -- --ignored
```

## Adding a New Agent Feature

To add a new agent config field (e.g., `stop_sequences`):

1. **AST** (`tl-ast/src/lib.rs`): Add field to `StmtKind::Agent`
2. **Parser** (`tl-parser/src/lib.rs`): Add parsing in `parse_agent()`
3. **AgentDef** (`tl-stream/src/agent.rs`): Add field to struct
4. **Interpreter** (`tl-interpreter/src/lib.rs`): Handle in `exec_agent()`, use in `exec_agent_loop()`
5. **Compiler** (`tl-compiler/src/compiler.rs`): Encode in `compile_agent()`
6. **VM** (`tl-compiler/src/vm.rs`): Decode in `handle_agent_exec()`, use in `exec_agent_loop()`
7. **Tests**: Add parser, interpreter, and VM tests
8. **LSP** (`tl-lsp/src/format.rs`): Update `StmtKind::Agent` formatting

To add a new lifecycle hook:

1. **AST**: Add `Option<Vec<Stmt>>` field
2. **Parser**: Add `Token::Ident(s) if s == "on_xxx"` case
3. **Compiler**: Add `compile_agent_hook()` call with appropriate params
4. **VM**: Look up `__agent_{name}_on_xxx__` in globals and call at the right point
5. **Interpreter**: Store as `Value::Function`, call at the right point

## WASM Limitations

The following are gated behind `#[cfg(feature = "native")]` or `#[cfg(not(target_arch = "wasm32"))]`:

- `VmValue::AgentDef` variant
- `Op::AgentExec` handler (returns error in WASM)
- `BuiltinId::RunAgent` handler (returns error in WASM)
- `BuiltinId::HttpRequest` handler (returns error in WASM)
- `BuiltinId::Embed` handler (returns error in WASM)

The agent syntax still parses and compiles in WASM, but execution will fail at runtime with a descriptive error message.
