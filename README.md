# ThinkingLanguage

[![CI](https://github.com/mplusm/thinkinglanguage/actions/workflows/ci.yml/badge.svg)](https://github.com/mplusm/thinkinglanguage/actions/workflows/ci.yml)

**A purpose-built language for Data Engineering & AI — Modern Problems need Modern Solutions.**

ThinkingLanguage (TL) replaces the fragile Python + SQL + YAML + Spark glue-code stack with a single, coherent language where data pipelines, transformations, AI model training, and real-time streaming are first-class language constructs.

## Highlights

- **Native tables** — columnar data backed by Apache Arrow/DataFusion with pipe-based transforms
- **AI/ML built-in** — tensors, model training (linfa), ONNX inference, embeddings, LLM APIs, AI agents with tool-use
- **Streaming & Pipelines** — ETL/ELT constructs, windowed streams, Kafka integration
- **GPU acceleration** — wgpu-based tensor operations on Vulkan/Metal/DX12/WebGPU
- **Multiple backends** — bytecode VM (default), LLVM AOT native compilation, WASM browser target
- **Gradual typing** — optional type annotations, generics, traits, Result/Option with `?` operator
- **Ownership semantics** — pipe-as-move, `.clone()`, read-only `&ref`, use-after-move detection
- **Rich tooling** — LSP server, VS Code extension, formatter, linter, doc generator, package manager

## Installation

### Quick install (macOS & Linux)

```bash
curl -sSf https://raw.githubusercontent.com/mplusm/thinkinglanguage/main/scripts/install.sh | sh
```

### npx (Node.js)

```bash
# Run directly without installing
npx thinkinglanguage run hello.tl

# Or install globally
npm install -g thinkinglanguage
```

### Pre-built binaries

Download the latest release from [GitHub Releases](https://github.com/mplusm/thinkinglanguage/releases/latest).

**Linux (x86_64):**
```bash
curl -LO https://github.com/mplusm/thinkinglanguage/releases/latest/download/tl-x86_64-unknown-linux-gnu.tar.gz
tar xzf tl-x86_64-unknown-linux-gnu.tar.gz
sudo mv tl /usr/local/bin/
```

**macOS (Apple Silicon):**
```bash
curl -LO https://github.com/mplusm/thinkinglanguage/releases/latest/download/tl-aarch64-apple-darwin.tar.gz
tar xzf tl-aarch64-apple-darwin.tar.gz
sudo mv tl /usr/local/bin/
```

**Windows:** Download `tl-x86_64-pc-windows-msvc.zip` from the [releases page](https://github.com/mplusm/thinkinglanguage/releases/latest), extract, and add `tl.exe` to your PATH.

### From crates.io

```bash
cargo install thinkinglanguage
# With SQLite support
cargo install thinkinglanguage --features sqlite
```

### From source

```bash
git clone https://github.com/mplusm/thinkinglanguage.git
cd thinkinglanguage
cargo build --release --features sqlite
```

## Quick Start

```bash
# Run a script
tl run hello.tl

# Start the REPL
tl shell
```

## A Taste of TL

### Variables and Functions

```
let name = "world"
let nums = [1, 2, 3, 4, 5]

fn greet(who: string) -> string {
    "Hello, {who}!"
}

let doubled = nums |> map((x) => x * 2) |> filter((x) => x > 4)
print(greet(name))
print(doubled)
```

### Data Pipelines

```
let users = read_csv("users.csv")

users
    |> filter(age > 30)
    |> select(name, age, department)
    |> with { senior = age > 35 }
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort(count, "desc")
    |> show()
```

### AI / Machine Learning

```
let data = read_csv("iris.csv")
let model = train_model(data, target: "species", algorithm: "random_forest")
let predictions = predict(model, new_data)

let t = tensor([[1.0, 2.0], [3.0, 4.0]])
let result = t |> matmul(tensor([[5.0], [6.0]]))
```

### AI Agents

```
fn search(query) { http_request("GET", "https://api.example.com/search?q=" + query, none, none).body }

agent research_bot {
    model: "gpt-4o",
    system: "You are a research assistant. Use tools to find information.",
    tools {
        search: {
            description: "Search the web",
            parameters: { type: "object", properties: { query: { type: "string" } }, required: ["query"] }
        }
    },
    max_turns: 5
}

let result = run_agent(research_bot, "What is the capital of France?")
println(result.response)
```

### Streaming

```
pipeline etl_daily {
    schedule: "0 6 * * *"
    steps {
        extract read_csv("raw_data.csv")
        transform |> filter(status == "active") |> select(id, name, score)
        load write_parquet("clean_data.parquet")
    }
}
```

## Architecture

```
Source (.tl)
    │
    ▼
┌─────────┐    ┌──────────┐    ┌─────────┐    ┌───────────┐
│  Lexer  │───▶│  Parser  │───▶│   AST   │───▶│ Compiler  │
│ (logos) │    │(rec.desc)│    │         │    │           │
└─────────┘    └──────────┘    └─────────┘    └─────┬─────┘
                                                    │
                              ┌──────────────────┬──┴──────────────┐
                              ▼                  ▼                 ▼
                        ┌──────────┐      ┌───────────┐    ┌────────────┐
                        │ Bytecode │      │   TL-IR   │    │    LLVM    │
                        │    VM    │      │ Optimizer  │    │  Backend   │
                        │(default) │      │           │    │  (AOT)     │
                        └──────────┘      └───────────┘    └────────────┘
                              │                                   │
                              ▼                                   ▼
                        ┌──────────┐                       ┌────────────┐
                        │   WASM   │                       │   Native   │
                        │ Backend  │                       │  Binary    │
                        └──────────┘                       └────────────┘
```

## Feature Flags

TL uses Cargo feature flags for optional integrations:

| Flag | Description | Dependencies |
|------|-------------|--------------|
| `sqlite` | SQLite connector | rusqlite (bundled) |
| `duckdb` | DuckDB connector (Arrow-native) | duckdb (bundled) |
| `mysql` | MySQL connector | mysql client libs |
| `mssql` | SQL Server / MSSQL connector | tiberius |
| `redis` | Redis connector | redis |
| `s3` | S3 object storage | object_store |
| `snowflake` | Snowflake data warehouse | reqwest |
| `bigquery` | Google BigQuery | reqwest |
| `databricks` | Databricks SQL | reqwest |
| `clickhouse` | ClickHouse analytics DB | reqwest |
| `mongodb` | MongoDB connector | mongodb driver |
| `sftp` | SFTP/SCP file transfer | ssh2 (libssh2) |
| `kafka` | Kafka streaming | rdkafka |
| `python` | Python FFI bridge | pyo3 |
| `gpu` | GPU tensor operations | wgpu, bytemuck |
| `llvm-backend` | LLVM AOT compilation | inkwell, llvm-sys |
| `async-runtime` | Tokio async I/O | tokio, reqwest |
| `notebook` | Interactive notebook TUI | ratatui, crossterm |
| `registry` | Package registry client | reqwest |

PostgreSQL and Redshift connectors are always available (no feature flag needed).

```bash
# Build with specific features
cargo build --release --features "sqlite,gpu,async-runtime"

# Build with all database connectors + SFTP
cargo build --release --features "sqlite,duckdb,mysql,mssql,clickhouse,snowflake,bigquery,databricks,mongodb,redis,sftp"

# Build with all features (except llvm-backend which needs LLVM 19 installed)
cargo build --release --features "sqlite,duckdb,mysql,mssql,redis,s3,sftp,snowflake,bigquery,databricks,clickhouse,mongodb,kafka,python,gpu,async-runtime,notebook,registry"
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `tl run <file>` | Execute a .tl source file |
| `tl shell` | Start the interactive REPL |
| `tl check <file>` | Type-check without executing |
| `tl test <path>` | Run tests in a file or directory |
| `tl fmt <path>` | Format source files |
| `tl lint <path>` | Lint for style and correctness |
| `tl build` | Build the current project (requires tl.toml) |
| `tl init <name>` | Initialize a new TL project |
| `tl doc <path>` | Generate documentation |
| `tl disasm <file>` | Disassemble bytecode |
| `tl compile <file>` | Compile to native object file (LLVM) |
| `tl deploy <file>` | Generate deployment artifacts |
| `tl lineage <file>` | Show data lineage |
| `tl notebook <file>` | Open interactive notebook |
| `tl lsp` | Start the LSP server |
| `tl models list\|info\|delete` | Manage model registry |
| `tl migrate apply\|check\|diff` | Schema migration |
| `tl add <pkg>` | Add a dependency |
| `tl remove <pkg>` | Remove a dependency |
| `tl install` | Install all dependencies |
| `tl update [pkg]` | Update dependencies (with version diffs) |
| `tl update --dry-run` | Preview dependency updates without changes |
| `tl outdated` | Show outdated dependencies |
| `tl publish` | Publish to package registry |
| `tl search <query>` | Search package registry |

## Project Structure

```
crates/
  tl-lexer/          Tokenization (logos)
  tl-ast/            Abstract syntax tree
  tl-parser/         Recursive descent parser
  tl-interpreter/    Tree-walking interpreter
  tl-compiler/       Bytecode compiler + register VM
  tl-types/          Type system and checker
  tl-errors/         Error types and diagnostics
  tl-data/           Data engine (DataFusion, Arrow)
  tl-ai/             AI/ML (ndarray, linfa, ONNX, LLM, agents)
  tl-stream/         Streaming and pipelines
  tl-ir/             Intermediate representation optimizer
  tl-llvm/           LLVM AOT backend (inkwell)
  tl-wasm/           WASM browser backend
  tl-gpu/            GPU tensor operations (wgpu)
  tl-lsp/            Language Server Protocol
  tl-package/        Package manager
  tl-registry/       Package registry server
  tl-cli/            CLI entry point
editors/
  vscode/            VS Code extension
benchmarks/          Criterion benchmarks
examples/            Example .tl programs
docs/                Documentation
```

## Documentation

Detailed documentation is available in the [docs/](docs/) directory:

- **[Getting Started](docs/getting-started/)** — Installation, quickstart, editor setup
- **[Language Guide](docs/language/)** — Syntax, types, functions, structs, generics, error handling
- **[Data Engineering](docs/data/)** — Tables, connectors, streaming, data quality
- **[AI & ML](docs/ai/)** — Tensors, model training, ONNX inference, [AI agents](docs/ai/agents.md)
- **[Tools & Ecosystem](docs/tools/)** — CLI, package manager, notebook, LSP
- **[Advanced Topics](docs/advanced/)** — Backends, Python FFI, security, schema evolution

## Tests

```bash
# Run all tests (excluding feature-gated crates)
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu

# With specific features
cargo test --workspace --exclude tl-llvm --exclude tl-wasm --exclude tl-gpu --features sqlite
```

## License

Apache-2.0
