# Changelog

All notable changes to ThinkingLanguage are documented here, organized by implementation phase.

## Foundation

### Phase 0 — Core Language

- Lexer using logos for tokenization
- Recursive descent parser producing AST
- Tree-walking interpreter with REPL
- Variables (`let`, `let mut`), functions (`fn`), closures (`(x) => expr`)
- Control flow: `if`/`else`, `while`, `for..in`, `match`, `case`
- Pipe operator: `value |> transform() |> result()`
- Types: int64, float64, string, bool, lists, none
- Builtins: print, map, filter, reduce, sum, range, len, and more
- CLI: `tl run <file>`, `tl shell`

### Phase 1 — Data Engine

- Apache DataFusion/Arrow integration for columnar tables
- Schema definitions: `schema User { id: int64, name: string }`
- CSV and Parquet I/O: read_csv, read_parquet, write_csv, write_parquet
- Table pipe operations: filter, select, sort, with, aggregate, join, head, show, describe
- Lazy evaluation with query optimization (predicate pushdown, column pruning)
- PostgreSQL connector: `postgres(conn_str, table_name)`

### Phase 2 — Compiler Backend

- Bytecode compiler targeting register-based VM
- u32 packed instruction format
- Cranelift JIT infrastructure
- Rayon-based parallelism support
- Criterion benchmarks

## AI & Streaming

### Phase 3 — AI Integration

- Tensor type backed by ndarray
- ML model training via linfa (linear regression, logistic regression, random forest, kmeans)
- ONNX inference via ort
- Embeddings and LLM API integration (`ai_complete`, `ai_chat`)
- Model registry for storing and retrieving trained models

### Phase 4 — Streaming & Pipelines

- `pipeline` construct for declarative ETL/ELT workflows
- `stream` construct with windowing (tumbling, sliding, session)
- Source and sink connectors
- Kafka integration (feature-gated)
- Data lineage tracking: `tl lineage`
- Deployment templates: `tl deploy` (Docker, Kubernetes)
- Alerting and metrics callbacks

## Language Core

### Phase 5 — Language Completeness

- Struct definitions and instantiation
- Enum types with variants
- Impl blocks for methods
- try/catch/throw error handling
- Import system
- Standard library: string, list, math functions
- HTTP client
- Test framework with `test "name" { ... }` blocks
- `tl test` CLI command

### Phase 6 — Stdlib & Ecosystem

- Map/Dict type with map_from, key access
- JSON: json_parse, json_stringify
- File I/O: read_file, write_file, append_file, file_exists, list_dir
- Regex: regex_match, regex_find, regex_replace
- Date/Time: now, date_format, date_parse
- Environment variables: env_get, env_set
- Collection and string enhancements

### Phase 7 — Concurrency

- spawn/await for concurrent tasks
- Channels: channel(), send, recv, try_recv
- Combinators: await_all, pmap, timeout
- Error propagation in spawned tasks

### Phase 8 — Iterators & Generators

- Yield-based generators with `yield` keyword
- `next()` for pulling values from generators
- Lazy iterator combinators: gen_map, gen_filter, chain, gen_zip, gen_enumerate
- take, skip, gen_collect for materialization
- For-loop integration with generators
- Method syntax on generators

### Phase 9 — Error Quality & DX

- Statement-level source spans in AST
- Line numbers embedded in bytecode
- VM error enrichment with stack traces
- Bytecode disassembler: `tl disasm`
- REPL improvements: persistent history, multi-line input, tab completion

### Phase 10 — Type System Foundation

- Gradual typing with optional type annotations
- Result type: Ok(value), Err(error)
- Option type: Some(value), None
- `?` operator for error propagation
- Set type: set_from, set_add, set_remove, set_contains, set_union, set_intersection, set_difference

### Phase 11 — Module System

- `use` imports with dot-path syntax
- `pub` visibility modifier
- `mod` declarations for sub-modules
- Directory modules via mod.tl
- tl.toml manifest file
- `tl init` and `tl build` CLI commands

### Phase 12 — Generics & Traits

- Type-erased generic functions and structs
- Trait definitions and implementations
- Trait bounds on generic parameters
- Where clauses
- Built-in trait hierarchy

### Phase 13 — Semantic Analysis & Optimization

- Enhanced type inference
- Type checking pass
- Constant folding optimization
- Dead code elimination
- `tl check` subcommand for static analysis

## Tooling

### Phase 14 — LSP & Developer Tooling

- Language Server Protocol server: `tl lsp`
- VS Code extension with syntax highlighting
- Code formatter: `tl fmt`
- Linter: `tl lint`
- LSP features: completions, hover, go-to-definition, document symbols, diagnostics

### Phase 15 — Data Quality & Connectors

- Data cleaning: fill_null, drop_null, dedup, clamp
- Data profiling: data_profile, row_count, null_rate, is_unique
- Validation: is_email, is_url, is_phone, is_between
- String similarity: levenshtein, soundex
- MySQL connector (feature-gated)
- S3 object storage (feature-gated)
- Redis connector (feature-gated)
- GraphQL query support

### Phase 16 — Package Manager & Registry

- tl-package crate for dependency management
- Dependency sources: version, git, path, registry
- CLI commands: tl add, tl remove, tl install, tl update, tl outdated
- Lock file generation with transitive dependency tracking
- Transitive dependency resolution (BFS with cycle detection)
- Version conflict detection across transitive dependency tree
- `tl update` shows version diffs (added/updated/removed)
- `tl update --dry-run` previews changes without modifying tl.lock
- `tl outdated` shows which dependencies have newer versions available

### Phase 19 — Documentation Generation

- `///` doc comments on functions, structs, enums
- Doc extraction and generation
- Output formats: HTML, Markdown, JSON
- `tl doc` CLI command
- LSP hover integration for doc comments
- Formatter preserves doc comments

## Advanced Language

### Phase 17 — Pattern Matching & Destructuring

- Pattern AST for match expressions
- Guard clauses: `match x { n if n > 0 => ... }`
- Destructuring: `let Point { x, y } = point`
- OR patterns: `match x { 1 or 2 or 3 => "small" }`
- Exhaustiveness checking

### Phase 18 — Closures & Lambdas

- Block-body closures: `(x) -> int { ... }`
- Type aliases: `type Mapper = fn(int) -> int`
- Shorthand closure syntax
- Closure type checking

### Phase 20 — Python FFI Bridge

- pyo3 integration (feature-gated)
- py_import, py_eval, py_call for Python interop
- GetMember/MethodCall dispatch on Python objects
- Tensor to numpy array conversion and vice versa

### Phase 21 — Schema Evolution & Migration

- Schema annotations: @version, @since, @deprecated
- `migrate` statement for data transformation between versions
- Schema registry: schema_register, schema_get, schema_latest, schema_history
- Compatibility checking: schema_check, schema_diff
- `tl migrate` CLI commands (apply, check, diff)

### Phase 22 — Advanced Type System

- Decimal type with 'd' suffix literal
- Typed tables, streams, tensors, pipelines in type system
- Hindley-Milner unification for advanced type inference

### Phase 23 — Security & Access Control

- Secret vault: secret_get, secret_set, secret_delete, secret_list
- Secret type displays as "***" — values never printed in plain text
- Security policies for access control
- Data masking: mask_email, mask_phone, mask_cc, redact
- Hashing: hash (SHA-256, MD5)
- @sensitive annotation
- Sandbox mode: `--sandbox` CLI flag with `--allow-connector` whitelist

### Phase 24 — Async/Await & Runtime

- `async fn` declarations
- Async I/O stubs: async_read_file, async_write_file, async_http_get, async_http_post
- Await-outside-async warning

### Phase 25 — Tokio Async Runtime

- Feature-gated `async-runtime` with full tokio integration
- Async file I/O (tokio::fs), timers (tokio::time), HTTP (reqwest)
- select and race_all combinators
- async_map and async_filter for concurrent collection processing

### Phase 26 — VM Upvalue Fix

- close_upvalues_in_value() in do_call() return path
- Closures, lists, and maps with Open upvalues promoted to Closed before stack truncation
- Fixes closure capture correctness for returned closures

### Phase 27 — Data Error Hierarchy

- DataError enum: ParseError, SchemaError, ValidationError, NotFound
- NetworkError enum: ConnectionError, TimeoutError, HttpError
- ConnectorError enum: AuthError, QueryError, ConfigError
- VM thrown_value preservation for structured errors in catch handlers
- is_error and error_type builtins

### Phase 28 — Ownership & Move Semantics

- Pipe `|>` moves value (source becomes Moved tombstone)
- `.clone()` for deep copy
- `&expr` for read-only references (transparent reads, blocked mutation)
- `parallel for` iteration (rayon-backed)
- Use-after-move detection in compiler and type checker

## Backends

### Phase 29 — TL-IR Intermediate Representation

- tl-ir crate with QueryPlan IR for table pipe chains
- AST to IR builder
- Optimizer passes: filter merge, predicate pushdown, column pruning, CSE
- IR to operations lowering
- Compiler integration with automatic fallback to legacy path

### Phase 30 — LLVM Backend

- tl-llvm crate using inkwell 0.8 (LLVM 19)
- AOT native compilation
- Three-tier opcode support (runtime helpers, dispatch, VM fallback)
- MCJIT execution engine
- Object file emission
- `--backend llvm` CLI flag
- `tl compile` subcommand

### Phase 31 — WASM Backend

- tl-wasm crate for browser execution
- wasm-bindgen integration
- Web playground for running TL in browsers

### Phase 32 — GPU Tensor Support

- tl-gpu crate using wgpu 24 (Vulkan/Metal/DX12/WebGPU)
- GpuDevice singleton initialization
- GpuTensor type with f32 storage
- 5 WGSL compute shaders (matmul, add, mul, relu, sigmoid)
- Binary operator auto-dispatch for GPU tensors
- Feature-gated `gpu`

## Ecosystem

### Phase 33 — Ecosystem & Community

- SQLite connector (rusqlite 0.32 bundled, feature-gated)
  - read_sqlite and write_sqlite builtins
  - Type inference from first row values
  - Transaction batching for writes
- Package registry server (tl-registry crate)
  - Axum 0.8 HTTP server on port 3333
  - Filesystem storage at ~/.tl/registry/
  - Publish, search, download API
  - Registry client in tl-package
- Interactive notebook TUI (feature-gated `notebook`)
  - .tlnb JSON format
  - ratatui 0.29 + crossterm 0.28
  - Persistent VM state across cells
  - Normal/Edit modes

### Phase 34 — AI Agent Framework

- First-class `agent` language construct for defining AI agents
  - Declarative syntax: `agent name { model: "...", tools { ... }, max_turns: N }`
  - Tool definitions with OpenAI function-calling JSON schema format
  - Lifecycle hooks: `on_tool_call { ... }` and `on_complete { ... }`
- Multi-provider LLM support
  - Automatic provider detection from model name (Claude → Anthropic, others → OpenAI)
  - `base_url` field for any OpenAI-compatible endpoint (Ollama, Azure, Together, etc.)
  - `TL_LLM_KEY`, `TL_ANTHROPIC_KEY`, `TL_OPENAI_KEY` env vars with priority resolution
- Tool-use / function-calling in LLM API
  - `LlmResponse::Text` / `LlmResponse::ToolUse` structured responses
  - `ToolCall` type with id, name, and JSON input
  - `chat_with_tools()` function handling both Anthropic and OpenAI tool protocols
  - `format_tool_result_messages()` for provider-specific tool result formatting
- `run_agent(agent, message)` builtin for executing the agent loop
  - Automatic tool dispatch: looks up TL functions by name, converts JSON args
  - Multi-turn conversation management
  - Returns `{response: string, turns: int}` map
- `http_request(method, url, headers, body)` builtin
  - Supports GET, POST, PUT, DELETE, PATCH, HEAD
  - Returns `{status: int, body: string}`
- `embed(text, model?, api_key?)` builtin
  - OpenAI embeddings API (`text-embedding-3-small` default)
  - Returns tensor
- `Expr::Map` / `parse_map_literal()` — JSON-like map syntax in tool definitions
  - Keyword-as-key support (`type`, `model`, etc. as map keys)
- New opcode: `Op::AgentExec = 67`
- New builtins: `BuiltinId::Embed = 182`, `HttpRequest = 183`, `RunAgent = 184`
- New value types: `VmValue::AgentDef`, `Value::Agent`
- WASM: agent syntax parses but execution returns descriptive errors

### Phase 35 — Connector Expansion & Performance

- **Performance optimization** for all existing connectors
  - Batched Arrow conversion (50K rows per batch) across all connectors
  - `register_batches()` method for multi-partition MemTable registration
  - PostgreSQL: server-side cursors (`DECLARE CURSOR` + `FETCH 50000`) for streaming
  - MySQL: chunked batching with 50K row flush threshold
  - SQLite: streaming `query_map` with 50K batch flush
- **DuckDB connector** (feature-gated `duckdb`)
  - Arrow-native reads via IPC bridge (duckdb arrow v54 to DataFusion arrow v53)
  - `read_duckdb(path, query)` and `write_duckdb(table, path, table_name)`
  - Supports file-backed and `:memory:` databases
- **Redshift connector** (always available)
  - Thin wrapper over PostgreSQL cursor code with automatic SSL enforcement
  - `redshift(conn_str, query)` / `read_redshift(conn_str, query)`
- **MSSQL / SQL Server connector** (feature-gated `mssql`)
  - tiberius async client with batched 50K row streaming
  - ADO-style and key=value connection string parsing
  - `mssql(conn_str, query)` / `read_mssql(conn_str, query)`
- **Snowflake connector** (feature-gated `snowflake`)
  - REST API integration (v2/statements)
  - JSON and key=value config format
  - `snowflake(config, query)` / `read_snowflake(config, query)`
- **BigQuery connector** (feature-gated `bigquery`)
  - REST API integration (jobs.query)
  - Access token via config or `TL_BIGQUERY_TOKEN` / `GOOGLE_ACCESS_TOKEN` env vars
  - `bigquery(config, query)` / `read_bigquery(config, query)`
- **Databricks connector** (feature-gated `databricks`)
  - SQL Statement Execution API
  - `databricks(config, query)` / `read_databricks(config, query)`
- **ClickHouse connector** (feature-gated `clickhouse`)
  - HTTP interface with JSONEachRow format
  - `clickhouse(url, query)` / `read_clickhouse(url, query)`
- **MongoDB connector** (feature-gated `mongodb`)
  - Async driver with BSON-to-Arrow flattening
  - Schema inference from first 100 documents
  - `mongo(uri, db, collection, filter)` / `read_mongo()` / `read_mongodb()`
- **SFTP/SCP file transfer** (feature-gated `sftp`)
  - ssh2 (libssh2) for SSH-based file transfer
  - `sftp_download(config, remote, local)` and `sftp_upload(config, local, remote)`
  - `sftp_list(config, path)` — remote directory listing as table (name, size, type, modified)
  - `sftp_read_csv(config, remote)` — read CSV directly from SFTP into table
  - `sftp_read_parquet(config, remote)` — read Parquet directly from SFTP into table
  - Auth: SSH key, password, ssh-agent with automatic fallback
  - JSON and key=value config format
- **PostgreSQL TLS support** — native-tls with fallback to NoTls for cloud/remote servers
- **PostgreSQL error detail** — full error messages via `as_db_error()` (severity, message, SQLSTATE)
- **PostgreSQL fetch optimization** — 1M row cursor fetch + 100K local RecordBatch split
- All connectors support `TL_CONFIG_PATH` / `tl_config.json` named connection resolution
- BuiltinId 202-215 allocated for new connectors
- Structured `ConnectorError` with `AuthError`, `QueryError`, `ConfigError` variants
