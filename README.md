# ThinkingLanguage

A purpose-built language for Data Engineering & AI — Modern Problems need Modern Solutions.

## Quick Start

```bash
# Build
cargo build --release

# Run a script
cargo run -- run examples/data_01_csv.tl

# Start the REPL
cargo run -- shell
```

## Features

### Phase 0 — Core Language
- Variables (`let`, `let mut`), functions (`fn`), closures (`(x) => expr`)
- Control flow: `if`/`else`, `while`, `for..in`, `match`, `case`
- Pipe operator: `value |> transform() |> result()`
- Types: `int64`, `float64`, `string`, `bool`, lists, `none`
- Builtins: `print`, `map`, `filter`, `reduce`, `sum`, `range`, `len`, ...

### Phase 1 — Data Engine (Apache DataFusion)
- **Columnar tables** backed by Apache Arrow/DataFusion
- **Schema definitions**: `schema User { id: int64, name: string, age: int64 }`
- **CSV/Parquet I/O**: `read_csv()`, `read_parquet()`, `write_csv()`, `write_parquet()`
- **Lazy evaluation** with query optimization (predicate pushdown, column pruning)
- **PostgreSQL connector**: `postgres(conn_str, table_name)`

#### Table Operations (pipe syntax)

```
let users = read_csv("users.csv")

users
    |> filter(age > 30)
    |> select(name, age, department)
    |> sort(age, "desc")
    |> show()

users
    |> with { senior = age > 35 }
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort(count, "desc")
    |> show()

let orders = read_csv("orders.csv")
users
    |> join(orders, on: id == user_id)
    |> aggregate(by: name, total: sum(amount))
    |> show()
```

| Operation | Syntax |
|-----------|--------|
| Filter | `table \|> filter(age > 30)` |
| Select | `table \|> select(name, age)` |
| Sort | `table \|> sort(col, "desc")` |
| Derived columns | `table \|> with { doubled = age * 2 }` |
| Aggregate | `table \|> aggregate(by: dept, total: sum(salary), n: count())` |
| Join | `table \|> join(other, on: id == user_id, kind: "inner")` |
| Head/Limit | `table \|> head(10)` |
| Show | `table \|> show()` |
| Describe | `table \|> describe()` |

## Project Structure

```
crates/
  tl-lexer/         Tokenization (logos)
  tl-ast/           Abstract syntax tree
  tl-parser/        Recursive descent parser
  tl-interpreter/   Tree-walking interpreter
  tl-data/          Data engine (DataFusion, Arrow, Parquet, PostgreSQL)
  tl-errors/        Error types and diagnostics
  tl-cli/           CLI and REPL
examples/
  01-10             Core language examples
  data_01-06        Data engine examples + benchmark
  test_data/        Sample CSV files
```

## Examples

```bash
# Core language
cargo run -- run examples/07_closures_and_pipe.tl

# Data engine
cargo run -- run examples/data_01_csv.tl      # CSV basics
cargo run -- run examples/data_02_parquet.tl   # Parquet I/O
cargo run -- run examples/data_03_pipeline.tl  # Aggregation pipeline
cargo run -- run examples/data_04_join.tl      # Table joins

# Benchmark (1M rows in ~130ms release)
python3 examples/benchmark_generate.py 1000000 /tmp/tl_benchmark.csv
cargo run --release -- run examples/data_06_benchmark.tl
```

## Tests

```bash
cargo test --workspace
```

## License

MIT OR Apache-2.0
