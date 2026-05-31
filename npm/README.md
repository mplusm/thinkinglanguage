# ThinkingLanguage

A purpose-built language for Data Engineering & AI.

> **Try it in your browser — no install:** [tl.thinkingdbx.com](https://tl.thinkingdbx.com)

This npm package installs the `tl` CLI binary by downloading the appropriate prebuilt release for your platform from [GitHub Releases](https://github.com/mplusm/thinkinglanguage/releases).

## Highlights

- **Native tables** — columnar data on Apache Arrow / DataFusion with pipe-based transforms
- **Data connectors (read & write)** — PostgreSQL, MySQL, Redshift, Snowflake, BigQuery, Databricks, ClickHouse, MongoDB, SQLite, DuckDB, S3, and Apache Iceberg
- **AI/ML built-in** — tensors, model training, ONNX inference, embeddings, LLM APIs, AI agents with tool-use, and MCP client/server
- **Streaming & pipelines** — ETL/ELT constructs, windowed streams, Kafka
- **Gradual typing** — optional annotations, generics, traits, pattern matching, `Result`/`Option` with `?`
- **Ownership semantics** — pipe-as-move, `.clone()`, read-only `&ref`, use-after-move detection
- **Multiple backends** — bytecode VM (default), LLVM AOT native compilation, WASM browser target
- **Rich tooling** — LSP server, VS Code extension, formatter, linter, doc generator, package manager, REPL

## Install

```bash
npx thinkinglanguage --help
```

Or install globally:

```bash
npm install -g thinkinglanguage
tl --help
```

## Supported platforms

- Linux x86_64
- macOS arm64 (Apple Silicon)
- Windows x86_64

## Quick example

```tl
let users = read_csv("users.csv")

users
    |> filter(age > 30)
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort("count", "desc")
    |> show()
```

## Links

- [Playground](https://tl.thinkingdbx.com) — run ThinkingLanguage in your browser
- [Repository](https://github.com/mplusm/thinkinglanguage)
- [Documentation](https://github.com/mplusm/thinkinglanguage/tree/main/docs)
- [Website](https://thinkingdbx.com)

## License

Apache-2.0
