markdown

# ThinkingLanguage (TL) — Architecture & Design Specification

### A Purpose-Built Language for Data Engineering & Artificial Intelligence

**Version:** 0.1.0-draft
**Author:** Mallesh, Founder — ThinkingDBx Private Limited
**Date:** February 2026
**Status:** Design Phase

---

## 1. Vision & Philosophy

ThinkingLanguage (TL) is a compiled, statically-typed programming language designed from the ground up for **data engineering and AI/ML workloads**. It aims to replace the fragile Python + SQL + YAML + Spark glue-code stack with a single, coherent language where data pipelines, transformations, AI model training, and real-time streaming are **first-class language constructs** — not library bolted afterthoughts.

### 1.1 Core Principles

```
1. DATA IS A TYPE, NOT A LIBRARY    — Tables, Streams, Tensors are native types
2. PIPELINES ARE PROGRAMS           — ETL/ELT flows are composable language constructs
3. AI IS A VERB, NOT A FRAMEWORK    — train, predict, embed are keywords
4. PARALLEL BY DEFAULT              — No GIL, no threading hacks, automatic partitioning
5. FAIL LOUD, RECOVER SMART         — Built-in error handling for unreliable data
6. READABLE BEATS CLEVER            — Python-like readability, Rust-like safety
7. FAST WITHOUT TRYING              — Compiled to native code, lazy evaluation, query optimization
```

### 1.2 What TL Replaces

| Today's Stack                        | TL Equivalent                    |
| ------------------------------------ | -------------------------------- |
| Python + Pandas/Polars               | Native `table` type + transforms |
| SQL queries embedded in strings      | Native query syntax              |
| Apache Spark / PySpark               | Built-in distributed execution   |
| Airflow / Dagster / Prefect (YAML)   | Native `pipeline` construct      |
| PyTorch / TensorFlow / scikit-learn  | Native `model` / `train` / `predict` |
| Kafka consumers (Java/Python)        | Native `stream` type             |
| dbt (SQL templating)                 | Native transformations with typing |
| Docker + K8s for pipeline deployment | `tl deploy` CLI command          |

### 1.3 The One-Liner Pitch

> **"Python made data accessible. TL makes it fast, safe, and intelligent — in one language."**

### 1.4 Non-Goals

TL is not trying to be everything. These are explicitly **out of scope**:

```
1. NOT A GENERAL-PURPOSE SYSTEMS LANGUAGE  — Use Rust or C++ for OS kernels, drivers, game engines
2. NOT A WEB FRAMEWORK                    — Use your preferred backend/frontend stack; TL handles data
3. NOT A REPLACEMENT FOR AD-HOC SQL       — SQL interop is the escape hatch for quick queries
4. NOT A NOTEBOOK-FIRST EXPERIENCE        — Code-first, compiled-first; notebooks are secondary
5. NOT COMPETING ON SINGLE-QUERY SPEED    — Polars and DuckDB are already excellent; TL competes
                                             on whole-pipeline optimization and language integration
6. NOT TRYING TO RUN EVERY ML ALGORITHM   — TL delegates to ONNX/XGBoost/etc.; the value is in
                                             the seamless integration, not reimplementing algorithms
```

---

## 2. Language Design

### 2.1 File Extension & Naming

- Source files: `.tl`
- Package manifest: `tl.toml`
- Lock file: `tl.lock`
- Build output: `.tlc` (compiled bytecode) or native binary
- REPL: `tl shell`
- Notebooks: `.tln` (ThinkingLanguage Notebook)

### 2.2 Syntax Overview — A Taste of TL

```tl
// ============================================================
// Example: Complete ETL + ML pipeline in ThinkingLanguage
// ============================================================

// Import connectors and models
use connectors.{postgres, s3, kafka}
use ai.{xgboost, embeddings}

// ---- Schema Definition (first-class types) ----
schema User {
    id: int64
    name: string
    email: string
    signup_date: date
    region: string
    monthly_spend: float64
    is_active: bool
}

schema ChurnPrediction {
    user_id: int64
    churn_probability: float64
    risk_tier: string
    predicted_at: timestamp
}

// ---- Data Sources ----
source users = postgres("analytics_db").table("users") -> User
source events = kafka("event-stream").topic("user_events")

// ---- Transformations ----
transform active_users(src: table<User>) -> table<User> {
    src
    |> filter(is_active == true and signup_date > today() - 90d)
    |> clean(
        nulls: { name: "unknown", monthly_spend: 0.0 },
        duplicates: dedupe(by: email, keep: "latest")
    )
    |> with {
        tenure_days = today() - signup_date
        spend_tier = case {
            monthly_spend > 1000 => "premium"
            monthly_spend > 100  => "standard"
            _                    => "free"
        }
    }
}

// ---- AI Model Training ----
model churn_model = train xgboost {
    data: active_users(users)
    target: "is_active"
    features: [tenure_days, monthly_spend, region]
    split: 80/20
    gpu: auto
    hyperparams: {
        max_depth: 6
        learning_rate: 0.1
        n_estimators: 500
    }
    metrics: [accuracy, f1, auc]
    save_to: s3("models/churn_v1.tlmodel")
}

// ---- Prediction Pipeline ----
transform predict_churn(src: table<User>) -> table<ChurnPrediction> {
    src
    |> active_users()
    |> predict(model: churn_model, output: "churn_probability")
    |> with {
        risk_tier = case {
            churn_probability > 0.8 => "critical"
            churn_probability > 0.5 => "high"
            churn_probability > 0.2 => "medium"
            _                       => "low"
        }
        predicted_at = now()
    }
    |> select(user_id: id, churn_probability, risk_tier, predicted_at)
}

// ---- Pipeline Orchestration ----
pipeline daily_churn_pipeline {
    schedule: cron("0 6 * * *")    // 6 AM daily
    timeout: 30m
    retries: 3

    steps {
        raw       = extract users
        cleaned   = transform active_users(raw)
        predicted = transform predict_churn(cleaned)
        load predicted -> postgres("analytics_db").table("churn_predictions")
        load predicted |> filter(risk_tier == "critical")
            -> webhook("https://api.slack.com/alerts", method: POST)
    }

    on_failure(step, error) {
        alert slack("#data-alerts", "Churn pipeline failed at {step}: {error}")
        alert email("mallesh@thinkingdbx.com")
    }

    on_success {
        log("Pipeline completed. Processed {row_count} users.")
        metrics.emit("churn_pipeline.success", row_count)
    }
}

// ---- Streaming (Real-time) ----
stream process_live_events {
    from: events
    window: tumbling(5m)
    watermark: 30s

    process(batch: stream<Event>) {
        batch
        |> filter(event_type in ["purchase", "signup", "churn_signal"])
        |> aggregate(by: region) {
            event_count = count()
            total_spend = sum(amount)
        }
        |> emit -> kafka("aggregated-events")
    }
}

// ---- Entry Point ----
fn main() {
    print("ThinkingLanguage v0.1 — Data + AI Runtime")
    run daily_churn_pipeline
}
```

### 2.3 Type System

TL uses a **gradual static type system** — strict by default, with optional inference for interactive/REPL use.

#### Primitive Types

```tl
int8, int16, int32, int64          // Signed integers
uint8, uint16, uint32, uint64      // Unsigned integers
float32, float64                   // IEEE 754 floats
bool                               // true / false
string                             // UTF-8 string
bytes                              // Raw byte sequence
date, time, timestamp, duration    // Temporal types (first-class, not a library)
decimal(p, s)                      // Exact decimal for financial data

// Duration Literals (first-class, used throughout pipelines)
// Syntax: integer followed by unit suffix
// 90d (days), 5h (hours), 30m (minutes), 15s (seconds), 100ms (milliseconds)
duration                           // e.g., 90d, 30m, 5s, 100ms
```

#### Data-Native Types (What Makes TL Unique)

```tl
// Table — columnar, lazy-evaluated, partitionable
table<T>                           // Typed table (like a DataFrame with a schema)
table                              // Untyped table (inferred)

// Stream — infinite, windowed, real-time
stream<T>                          // Typed stream
stream                             // Untyped stream

// Tensor — N-dimensional array for AI
tensor<dtype, shape>               // e.g., tensor<float32, [256, 768]>
tensor                             // Dynamic shape

// Model — trained AI model as a value
model                              // Serializable, versionable, deployable

// Schema — structural type for data contracts
schema T { ... }                   // Named schema
```

#### Composite Types

```tl
// Collections
list<T>                            // Dynamic array
set<T>                             // Unique elements
map<K, V>                          // Key-value map

// Struct — general-purpose product type
struct Config {
    max_retries: int
    timeout: duration
    notify: list<string>
}

// Algebraic Types
enum Status { Active, Inactive, Suspended(reason: string) }

// Optional / Nullable (critical for data work)
option<T>                          // Some(value) or None
T?                                 // Shorthand for option<T>

// Result (for error handling)
result<T, E>                       // Ok(value) or Err(error)

// Any (escape hatch — use sparingly)
any                                // Dynamic type, runtime-checked
```

> **`schema` vs `struct`:** Both define product types. `schema` is specialized for table row shapes — it supports annotations (`@sensitive`, `@since`), versioning, evolution, and is the primary type for `table<T>`. `struct` is for general-purpose data (configs, intermediate values, API responses). Both can be used as `table<T>` row types.

### 2.4 Keywords (Reserved)

TL keeps a **small keyword set** (~35 words). Domain-specific operations like `filter`, `sort`, `aggregate`, `dedupe`, `predict`, and `embed` are stdlib functions, not keywords — keeping the language core tight and avoiding reserving common identifiers.

```
// Data Constructs (special syntax)
table    stream    schema    struct    source    sink
extract  transform load      pipeline  with      connector

// AI (special declarative syntax)
model    train     tensor

// Control Flow
if       else      match     case      for       while
in       return    break     continue  yield

// Concurrency & Pipelines
parallel async     emit

// Functions & Modules
fn       use       pub       mod       trait     impl
let      mut       const     type      as        enum

// Primitives
true     false     none      self      _
```

**Stdlib functions (not keywords):** `filter`, `select`, `join`, `aggregate`, `sort`, `limit`, `union`, `partition`, `clean`, `dedupe`, `validate`, `sample`, `predict`, `embed`, `evaluate`, `finetune`, `window`, `tumbling`, `sliding`, `session`, `cron`, `retry`, `count`, `sum`, `avg`, etc.

> **Design rationale:** You can name a variable `batch`, `filter`, `limit`, or `sort` without collision. These words are too common in data engineering to reserve as keywords. The pipe operator `|>` makes them read like keywords even though they're function calls.

### 2.5 Operators

```tl
// Standard
+  -  *  /  %  **              // Arithmetic (** for power)
== != < > <= >=                // Comparison
and or not                     // Logical (words, not symbols)
=                              // Assignment
+=  -=  *=  /=                 // Compound assignment

// Data-Specific (THE DIFFERENTIATORS)
|>                             // Pipe operator (chain transformations)
->                             // Flow-into (load/emit direction)
??                             // Null coalesce (critical for dirty data)
?                              // Optional chaining
..                             // Range (1..100)
@                              // Column reference (disambiguate from local variable)
&                              // Read-only borrow (shared reference)
```

### 2.6 Column Expression Context

Inside data operations — `filter()`, `with {}`, `aggregate() {}`, `select()`, `sort()`, and any function receiving table data via the pipe operator — TL enters a **column expression context**. In this context, bare identifiers resolve to column names from the input table's schema rather than local variables.

This is what makes `filter(age > 25)` work without writing `filter(row => row.age > 25)`. The compiler knows the available columns because the pipe operator carries schema information forward at compile time.

#### How Column Resolution Works

When the compiler encounters a bare identifier inside a data expression context, it follows this resolution order:

1. **Keywords and literals** — `true`, `false`, `none`, etc.
2. **Local variables** — `let` bindings in the enclosing scope
3. **Column names** — fields from the input table's schema

```tl
schema User {
    id: int64
    name: string
    age: int64
    region: string
}

source users = postgres("analytics_db").table("users") -> User

// Inside filter(), `age` and `region` resolve to columns of User
let young_us = users
    |> filter(age < 30 and region == "US")
//          ^^^              ^^^^^^
//          These resolve to User.age and User.region
```

The compiler infers available columns from the pipe input's type. If the input is `table<User>`, every field in `User` becomes available as a bare identifier inside the downstream data expression.

#### Column vs. Local Variable Collision

When a local variable name collides with a column name, **the local variable takes precedence**. To explicitly reference the column, prefix it with `@`:

```tl
let region = "US"   // local variable named `region`

let result = users
    |> filter(region == "US")      // `region` resolves to the local variable
    |> filter(@region == "US")     // `@region` forces column resolution

// Best practice: avoid shadowing column names
let target_region = "US"
let result = users
    |> filter(region == target_region)  // Unambiguous: column vs. local
```

The `@` prefix is only needed inside data expression contexts when disambiguation is required. Outside of data contexts, `@` is not valid.

#### Compile-Time Checking

Column references are checked at compile time. Misspelled column names produce an error with suggestions:

```
Error: Unknown column `agee` in table<User>

  14 │     |> filter(agee > 25)
     │              ^^^^
     │              Unknown column

  Did you mean: `age`?
  Available columns: id, name, age, region
```

This eliminates an entire class of runtime errors that plague SQL-in-strings and DataFrame code, where a typo in a column name only surfaces when the pipeline runs — often hours later, in production.

#### Nested Data Contexts

Data expressions can be nested. Each level resolves columns from its own input:

```tl
let result = orders
    |> join(users, on: orders.user_id == users.id, type: left)
    |> with {
        full_label = "{name}: {order_total}"   // `name` from users, `order_total` from orders
    }
    |> aggregate(by: region) {
        total_revenue = sum(order_total)       // `order_total` still available post-join
        user_count = count_distinct(user_id)
    }
```

After a `join`, columns from both tables are available. If both tables have a column with the same name, you must qualify it: `users.name` vs. `orders.name`.

### 2.7 Closures & Anonymous Functions

TL supports closures (anonymous functions) for general-purpose programming. These are distinct from the implicit column expression context used in data operations — closures are explicit functions with parameters and a body.

#### Single-Expression Closures

The `=>` arrow defines a single-expression closure:

```tl
let double = (x: int64) => x * 2
let greet = (name: string) => "Hello, {name}!"

// Type inference works for parameters when context is available
let nums = [1, 2, 3, 4, 5]
let doubled = nums |> map((x) => x * 2)        // x inferred as int64
```

#### Block-Body Closures

For multi-statement closures, use a block body with an explicit return type:

```tl
let process = (x: int64) -> int64 {
    let doubled = x * 2
    let adjusted = doubled + 1
    adjusted    // last expression is the return value
}

let classify = (score: float64) -> string {
    if score > 0.8 { "high" }
    else if score > 0.5 { "medium" }
    else { "low" }
}
```

#### Function Type Signatures

Function types are declared with the `fn` keyword:

```tl
type Mapper = fn(int64) -> int64
type Predicate = fn(string) -> bool
type Reducer = fn(float64, float64) -> float64

fn apply_twice(f: fn(int64) -> int64, value: int64) -> int64 {
    f(f(value))
}

let result = apply_twice((x) => x * 2, 3)   // result = 12
```

#### Closures Capture Their Environment

Closures capture variables from the enclosing scope by reference (immutable) or by value (`move`):

```tl
let threshold = 100
let multiplier = 2.5

// Captures `threshold` and `multiplier` from enclosing scope
let compute = (x: float64) => x * multiplier + threshold

// Explicit move for ownership transfer (e.g., sending to another thread)
let data = load("snapshot.parquet")
let task = move || {
    data |> filter(active == true) |> collect()
}
```

#### Closures vs. Column Expressions

In pipe data contexts, you do **not** need closures — the column expression context (Section 2.6) handles it implicitly. Closures are for general-purpose higher-order programming:

```tl
// Data context — implicit column resolution, no closure needed
let active = users |> filter(age > 25 and is_active == true)

// General-purpose — explicit closure required
let numbers = [10, 20, 30, 40, 50]
let big = numbers |> filter((n) => n > 25)

// Higher-order functions with closures
fn retry_with(action: fn() -> result<table, DataError>, max: int) -> result<table, DataError> {
    for attempt in 1..=max {
        match action() {
            Ok(data) => return Ok(data)
            Err(e) if attempt < max => log("Retry {attempt}: {e}")
            Err(e) => return Err(e)
        }
    }
}
```

### 2.8 Module System

TL uses a file-based module system inspired by Rust. Modules provide namespace isolation, visibility control, and composable code organization for projects of any size.

#### One File = One Module

Every `.tl` file is implicitly a module. The module name is the file name (without extension):

```
src/
├── main.tl           // module: main
├── schemas.tl        // module: schemas
└── transforms.tl     // module: transforms
```

#### Directory Modules

A directory containing a `mod.tl` file forms a **module group**. The `mod.tl` file declares which submodules are exposed:

```
src/
├── main.tl
├── data/
│   ├── mod.tl              // declares submodules of `data`
│   ├── transforms.tl       // module: data.transforms
│   ├── quality.tl          // module: data.quality
│   └── connectors/
│       ├── mod.tl           // declares submodules of `data.connectors`
│       ├── postgres.tl      // module: data.connectors.postgres
│       └── kafka.tl         // module: data.connectors.kafka
└── models/
    ├── mod.tl
    └── churn.tl             // module: models.churn
```

```tl
// src/data/mod.tl
pub mod transforms
pub mod quality
pub mod connectors
```

#### Visibility with `pub`

Items are private to their module by default. Use `pub` to make them visible outside:

```tl
// src/data/transforms.tl

// Public — accessible from other modules
pub fn clean_users(src: table<User>) -> table<User> {
    src |> remove_nulls() |> dedupe_emails()
}

pub schema CleanedUser {
    id: int64
    name: string
    email: string
}

// Private — only accessible within this module
fn remove_nulls(src: table<User>) -> table<User> {
    src |> filter(email != none)
}

fn dedupe_emails(src: table<User>) -> table<User> {
    src |> dedupe(by: email, keep: "latest")
}
```

#### Imports with `use`

The `use` keyword brings items into scope:

```tl
// Import a single item
use data.transforms.clean_users

// Import multiple items from the same module
use data.transforms.{clean_users, CleanedUser}

// Wildcard import (available but discouraged — pollutes namespace)
use data.transforms.*

// Aliased import
use data.connectors.postgres as pg

// Usage after import
let users = pg.connect(env("DB_URL")).table("users")
let cleaned = clean_users(users)
```

#### Re-exports

A module can re-export items from its submodules, creating a clean public API:

```tl
// src/data/mod.tl
pub mod transforms
pub mod quality
pub mod connectors

// Re-export commonly used items at the `data` level
pub use transforms.clean_users
pub use quality.validate_schema
pub use connectors.postgres
```

```tl
// src/main.tl — consumers see a flat, convenient API
use data.{clean_users, validate_schema, postgres}
```

#### Third-Party Packages

External packages are declared in `tl.toml` and resolve through the package registry:

```toml
[dependencies]
postgres-connector = "1.2"
kafka-connector = "0.8"
xgboost = "2.0"
data-quality-toolkit = { version = "0.5", git = "https://github.com/org/dqt.git" }
```

```tl
// Third-party imports use the package name as the root
use postgres_connector.connect
use data_quality_toolkit.validators.{email_check, range_check}
```

#### Complete Multi-File Example

```
churn-pipeline/
├── tl.toml
├── src/
│   ├── main.tl
│   ├── schemas/
│   │   ├── mod.tl
│   │   ├── user.tl
│   │   └── prediction.tl
│   ├── transforms/
│   │   ├── mod.tl
│   │   ├── clean.tl
│   │   └── features.tl
│   └── pipelines/
│       ├── mod.tl
│       └── daily_churn.tl
```

```tl
// src/schemas/mod.tl
pub mod user
pub mod prediction
pub use user.User
pub use prediction.ChurnPrediction

// src/schemas/user.tl
pub schema User {
    id: int64
    name: string
    email: string
    signup_date: date
    region: string
    monthly_spend: float64
    is_active: bool
}

// src/transforms/clean.tl
use schemas.User

pub fn clean_users(src: table<User>) -> table<User> {
    src
    |> filter(is_active == true)
    |> clean { nulls: { name: fill("unknown"), monthly_spend: fill(0.0) } }
    |> dedupe(by: email, keep: "latest")
}

// src/main.tl
use schemas.{User, ChurnPrediction}
use transforms.clean.clean_users
use pipelines.daily_churn.daily_churn_pipeline

fn main() {
    run daily_churn_pipeline
}
```

### 2.9 Generics

TL supports parametric polymorphism through generics. Generic types and functions enable reusable, type-safe code without sacrificing compile-time checking.

#### Generic Functions

Define type parameters in angle brackets after the function name:

```tl
fn first<T>(items: list<T>) -> T? {
    if items.len() > 0 { Some(items[0]) } else { none }
}

fn zip<A, B>(left: list<A>, right: list<B>) -> list<(A, B)> {
    left.iter().zip(right.iter()).collect()
}

// Usage — type parameter is inferred
let name = first(["Alice", "Bob", "Carol"])   // name: string?
let num = first([10, 20, 30])                 // num: int64?
```

#### Generic Structs

Structs can be parameterized over types:

```tl
struct Pair<A, B> {
    first: A
    second: B
}

struct TimeSeries<T> {
    timestamps: list<timestamp>
    values: list<T>
    metadata: map<string, string>
}

let price_series = TimeSeries<float64> {
    timestamps: [ts("2026-01-01"), ts("2026-01-02")],
    values: [100.5, 102.3],
    metadata: { "source": "market_feed", "currency": "USD" },
}
```

#### Trait Bounds

Constrain type parameters to types that implement specific traits using the `:` syntax:

```tl
fn summarize<T: Numeric>(values: list<T>) -> float64 {
    let total = values |> sum()
    cast(total, float64) / values.len()
}

fn find_max<T: Comparable>(items: list<T>) -> T? {
    if items.len() == 0 { return none }
    let mut best = items[0]
    for item in items {
        if item > best { best = item }
    }
    Some(best)
}

// Multiple bounds with +
fn display_sorted<T: Comparable + Displayable>(items: list<T>) -> list<string> {
    items |> sort() |> map((x) => x.to_string())
}
```

#### Where Clauses

For complex bounds, use a `where` clause to keep the function signature readable:

```tl
fn merge_tables<L, R, K>(left: table<L>, right: table<R>, key: fn(L) -> K) -> table<(L, R)>
where
    K: Comparable + Hashable,
    L: Serializable,
    R: Serializable,
{
    left |> join(right, on: key(left) == key(right), type: inner)
}

fn aggregate_metric<T, M>(data: table<T>, metric_fn: fn(list<T>) -> M) -> M
where
    T: Serializable,
    M: Numeric + Displayable,
{
    let rows = data |> collect()
    metric_fn(rows)
}
```

#### Built-In Trait Hierarchy

TL provides a set of built-in traits that form a hierarchy for constraining generic types:

| Trait            | Description                                    | Implemented By                            |
| ---------------- | ---------------------------------------------- | ----------------------------------------- |
| `Numeric`        | Supports arithmetic operations (+, -, *, /)    | `int*`, `uint*`, `float*`, `decimal`      |
| `Comparable`     | Supports ordering (<, >, <=, >=)               | `Numeric` types, `string`, `date`, `timestamp`, `duration` |
| `Hashable`       | Can be used as map keys or in `dedupe`         | `Numeric` types, `string`, `bool`, `date` |
| `Displayable`    | Can be converted to a human-readable string    | All primitive types, `enum` variants      |
| `Serializable`   | Can be serialized to/from Parquet, JSON, Arrow | All schema-defined types, primitives      |
| `Default`        | Has a meaningful default value                 | All primitives, `list`, `map`, `set`      |

Traits form a hierarchy: `Numeric` implies `Comparable`, and `Comparable` implies `Hashable`.

#### Built-In Generic Types

The core data types `table<T>`, `stream<T>`, and `list<T>` are themselves generic. They are sugar for built-in parameterized types, and follow the same rules:

```tl
// These are all generic types
let users: table<User> = load("users.parquet")
let events: stream<ClickEvent> = kafka("clicks").subscribe()
let names: list<string> = users |> select(name) |> collect()
let cache: map<string, float64> = {}
let maybe: option<int64> = Some(42)     // equivalent to int64?
```

User-defined generics compose naturally with built-in generic types:

```tl
fn top_n<T: Comparable>(data: table<T>, sort_col: fn(T) -> float64, n: int) -> table<T> {
    data |> sort(sort_col, desc) |> limit(n)
}

let top_spenders = top_n(users, (u) => u.monthly_spend, 100)
```

---

## 3. Compiler Architecture

### 3.1 High-Level Pipeline

```
                    ThinkingLanguage Compiler Architecture
  ┌──────────────────────────────────────────────────────────────────┐
  │                                                                  │
  │   Source Code (.tl)                                              │
  │       │                                                          │
  │       ▼                                                          │
  │   ┌──────────┐     ┌──────────┐     ┌──────────────┐            │
  │   │  LEXER   │────▶│  PARSER  │────▶│  AST         │            │
  │   │ (Logos)  │     │(Chumsky/ │     │ (Abstract    │            │
  │   │          │     │ Custom)  │     │  Syntax Tree)│            │
  │   └──────────┘     └──────────┘     └──────┬───────┘            │
  │                                            │                     │
  │                                            ▼                     │
  │                                    ┌───────────────┐             │
  │                                    │  SEMANTIC      │             │
  │                                    │  ANALYSIS      │             │
  │                                    │  - Type Check  │             │
  │                                    │  - Schema Valid│             │
  │                                    │  - Borrow Check│             │
  │                                    └───────┬───────┘             │
  │                                            │                     │
  │                                            ▼                     │
  │                                    ┌───────────────┐             │
  │                                    │  TL-IR         │             │
  │                                    │  (Intermediate │             │
  │                                    │  Representation│             │
  │                                    │  + Query Plan) │             │
  │                                    └───────┬───────┘             │
  │                                            │                     │
  │                         ┌──────────────────┼──────────────┐      │
  │                         │                  │              │      │
  │                         ▼                  ▼              ▼      │
  │                 ┌──────────────┐  ┌──────────────┐ ┌──────────┐ │
  │                 │ OPTIMIZER    │  │ OPTIMIZER    │ │ OPTIMIZER│ │
  │                 │ (Data Paths) │  │ (AI Paths)   │ │ (General)│ │
  │                 │              │  │              │ │          │ │
  │                 │ - Predicate  │  │ - Kernel     │ │ - Dead   │ │
  │                 │   pushdown   │  │   fusion     │ │   code   │ │
  │                 │ - Column     │  │ - Memory     │ │ - Inline │ │
  │                 │   pruning    │  │   planning   │ │ - Const  │ │
  │                 │ - Join       │  │ - GPU/CPU    │ │   fold   │ │
  │                 │   reorder    │  │   dispatch   │ │          │ │
  │                 │ - Partition  │  │ - Batch size │ │          │ │
  │                 │   planning   │  │   tuning     │ │          │ │
  │                 └──────┬───────┘  └──────┬───────┘ └────┬─────┘ │
  │                        │                 │              │       │
  │                        └─────────────────┼──────────────┘       │
  │                                          │                      │
  │                                          ▼                      │
  │                                  ┌───────────────┐              │
  │                                  │  CODE          │              │
  │                                  │  GENERATION    │              │
  │                                  └───────┬───────┘              │
  │                                          │                      │
  │                    ┌─────────────────┬────┴────┬──────────┐     │
  │                    ▼                 ▼         ▼          ▼     │
  │             ┌────────────┐  ┌────────────┐ ┌───────┐ ┌──────┐  │
  │             │   LLVM     │  │ Cranelift  │ │ WASM  │ │ CUDA │  │
  │             │   (Native) │  │ (Fast JIT) │ │ (Web) │ │ (GPU)│  │
  │             └─────┬──────┘  └─────┬──────┘ └───┬───┘ └──┬───┘  │
  │                   │               │            │        │      │
  │                   ▼               ▼            ▼        ▼      │
  │             ┌──────────────────────────────────────────────┐    │
  │             │             RUNTIME EXECUTABLES               │    │
  │             │  .bin (native)  .wasm (web)  .ptx (gpu)      │    │
  │             └──────────────────────────────────────────────┘    │
  │                                                                  │
  └──────────────────────────────────────────────────────────────────┘
```

### 3.2 Compiler Stages — Detailed

#### Stage 1: Lexer (Tokenizer)

**Implementation:** Rust using `logos` crate (fastest lexer generator)

Converts source text into a stream of tokens:

```
source code: `let x = users |> filter(age > 25)`

tokens: [LET, IDENT("x"), EQUALS, IDENT("users"), PIPE, IDENT("filter"),
         LPAREN, IDENT("age"), GT, INT(25), RPAREN]
```

Key design decisions:
- Significant newlines (like Python) OR semicolons — TBD, leaning toward optional semicolons
- Indentation-aware for pipeline blocks
- UTF-8 source encoding
- String interpolation: `"Hello {name}, you have {count} records"`

#### Stage 2: Parser

**Implementation:** Recursive descent parser (hand-written in Rust for best error messages)

Produces an Abstract Syntax Tree (AST). Key grammar constructs:

```
program        → declaration*
declaration    → schema_decl | source_decl | transform_decl |
                 pipeline_decl | model_decl | stream_decl | fn_decl
transform_decl → "transform" IDENT "(" params ")" "->" type "{" pipe_expr "}"
pipe_expr      → expr ("|>" expr)*
expr           → filter_expr | derive_expr | join_expr | aggregate_expr | ...
```

Priority: **World-class error messages.** Bad error messages kill language adoption. Every parse error should suggest a fix.

```
Error: Expected `->` return type after transform parameters

  12 │ transform clean_users(src: table<User>) {
     │                                         ^
     │                                         ╰── Add return type: -> table<User>

Hint: Every transform must declare its output schema.
```

#### Stage 3: Semantic Analysis

- **Type checking** — verify all operations are type-safe
- **Schema validation** — ensure table schemas match between pipeline stages
- **Lineage tracking** — track data flow for governance/debugging
- **Borrow checking (simplified)** — prevent data races in parallel execution
- **Connector validation** — verify database/API connections at compile time (optional)

#### Stage 4: TL-IR (Intermediate Representation)

This is the **secret weapon**. TL-IR is not just code IR — it's also a **query plan**, similar to Apache Spark's Catalyst optimizer or Substrait.

```
TL-IR Node Types:
├── Scan(source, columns, predicate)      // Read data
├── Filter(input, predicate)              // Row filtering
├── Project(input, expressions)           // Column selection/derivation
├── Aggregate(input, group_by, aggs)      // Aggregation
├── Join(left, right, condition, type)    // Joins
├── Sort(input, keys)                     // Ordering
├── Limit(input, count)                   // Row limiting
├── Sink(input, destination)              // Write data
├── ModelTrain(data, config)              // AI training
├── ModelPredict(input, model)            // AI inference
├── StreamWindow(input, window_type)      // Streaming window
└── Parallel(inputs, strategy)            // Parallel execution
```

#### Stage 5: Optimization

**Data Path Optimizations** (inspired by query engines):

| Optimization         | Description                                    | Impact  |
| -------------------- | ---------------------------------------------- | ------- |
| Predicate Pushdown   | Move filters closer to data source             | 10-100x |
| Column Pruning       | Only read needed columns from parquet/DB       | 2-10x   |
| Join Reordering      | Optimize join order based on table statistics   | 5-50x   |
| Partition Pruning    | Skip irrelevant data partitions                | 10-100x |
| Common Subexpression | Reuse repeated computations                    | 2-5x    |
| Lazy Evaluation      | Build full plan before executing               | 2-10x   |

**AI Path Optimizations:**

| Optimization      | Description                                     | Impact  |
| ----------------- | ----------------------------------------------- | ------- |
| Kernel Fusion     | Fuse multiple tensor operations into one kernel  | 2-5x   |
| Memory Planning   | Pre-allocate tensor memory, minimize copies      | 2-3x   |
| Mixed Precision   | Auto-downcast to float16 where safe              | 2x     |
| Batch Tuning      | Auto-tune batch size for hardware                | 1.5-3x |
| GPU Dispatch      | Automatically move operations to GPU when beneficial | 5-50x |

#### Stage 6: Code Generation

Multiple backends for different use cases:

| Backend         | Use Case                  | Tool       |
| --------------- | ------------------------- | ---------- |
| **LLVM**        | Production native binary  | `inkwell`  |
| **Cranelift**   | Fast JIT for REPL/dev     | `cranelift`|
| **WASM**        | Browser / edge execution  | `wasm-pack`|
| **CUDA/ROCm**   | GPU tensor operations     | Custom PTX |
| **Bytecode**    | Interpreted mode (debug)  | Custom VM  |

---

## 4. Runtime Architecture

### 4.1 Execution Engine

```
                    TL Runtime Architecture
  ┌─────────────────────────────────────────────────────────┐
  │                    TL Runtime                            │
  │                                                          │
  │  ┌────────────────────────────────────────────────────┐  │
  │  │                 Scheduler                           │  │
  │  │  Manages task execution, parallelism, resources     │  │
  │  └────────┬──────────┬──────────────┬─────────────────┘  │
  │           │          │              │                     │
  │           ▼          ▼              ▼                     │
  │  ┌─────────────┐ ┌────────────┐ ┌───────────────────┐   │
  │  │   DATA      │ │   AI       │ │   STREAM          │   │
  │  │   ENGINE    │ │   ENGINE   │ │   ENGINE          │   │
  │  │             │ │            │ │                    │   │
  │  │ - Columnar  │ │ - Tensor   │ │ - Window mgmt    │   │
  │  │   storage   │ │   runtime  │ │ - Watermarks     │   │
  │  │ - Vectorized│ │ - GPU mgr  │ │ - Checkpoints    │   │
  │  │   execution │ │ - Model    │ │ - Backpressure   │   │
  │  │ - Query     │ │   registry │ │ - Event-time     │   │
  │  │   optimizer │ │ - Auto-    │ │   processing     │   │
  │  │ - Memory    │ │   batching │ │                    │   │
  │  │   pools     │ │            │ │                    │   │
  │  └──────┬──────┘ └─────┬──────┘ └────────┬───────────┘   │
  │         │              │                  │               │
  │         ▼              ▼                  ▼               │
  │  ┌──────────────────────────────────────────────────┐    │
  │  │              CONNECTOR LAYER                      │    │
  │  │                                                    │    │
  │  │  ┌───────┐ ┌───────┐ ┌─────┐ ┌─────┐ ┌────────┐ │    │
  │  │  │Postgres│ │ MySQL │ │ S3  │ │Kafka│ │BigQuery│ │    │
  │  │  └───────┘ └───────┘ └─────┘ └─────┘ └────────┘ │    │
  │  │  ┌──────┐ ┌─────────┐ ┌──────┐ ┌───────┐        │    │
  │  │  │Redis │ │Snowflake│ │ HTTP │ │Parquet│  ...    │    │
  │  │  └──────┘ └─────────┘ └──────┘ └───────┘        │    │
  │  └──────────────────────────────────────────────────┘    │
  │                                                          │
  │  ┌──────────────────────────────────────────────────┐    │
  │  │              MEMORY MANAGER                       │    │
  │  │  - Arena allocator for table operations           │    │
  │  │  - Pool allocator for tensor operations           │    │
  │  │  - Zero-copy sharing between engines              │    │
  │  │  - Spill-to-disk for large datasets               │    │
  │  └──────────────────────────────────────────────────┘    │
  │                                                          │
  └─────────────────────────────────────────────────────────┘
```

### 4.2 Memory Model

TL uses an **ownership-based memory model** inspired by Rust but simplified for data workloads. Where Rust requires reasoning about lifetimes and borrows at fine granularity, TL restricts its ownership model to four rules — enough to guarantee memory safety and data-race freedom at compile time, without exposing lifetime annotations.

#### The Four Ownership Rules

**Rule 1: Every value has exactly one owner.**
A value is owned by the binding it is assigned to. When the owner goes out of scope, the value is dropped and its memory is reclaimed.

```tl
let users = load("users.parquet")   // `users` owns the Arrow table
// `users` is the sole owner — no other binding can mutate or free this data
```

**Rule 2: Pipe operations (`|>`) move the value — the original binding is consumed.**
The pipe operator transfers ownership from the left-hand side to the right-hand side. After a pipe, the source binding is no longer valid.

```tl
let users = load("users.parquet")          // `users` owns the data
let active = users |> filter(age > 25)     // ownership MOVES to `active`
                                            // `users` is now consumed
let result = active |> collect()           // ownership MOVES to `result`
                                            // `active` is now consumed
```

**Rule 3: To use a value in multiple places, explicitly clone or borrow.**
Use `.clone()` for a deep copy (new independent owner), or `&` for a read-only reference.

```tl
let users = load("users.parquet")

// Deep copy — `copy` is a new, independent owner
let copy = users.clone()

// Both can be piped independently
let active   = users |> filter(age > 25)   // consumes `users`
let inactive = copy  |> filter(age <= 25)  // consumes `copy`
```

**Rule 4: Inside `parallel` blocks, the runtime partitions data — each partition is independently owned.**
No locks, no atomics, no data races — the compiler guarantees it.

```tl
parallel for shard in users.partition(by: "region") {
    // Each `shard` is independently owned — no locks needed
    let processed = shard |> transform_fn()
    save(processed, "output/{shard.key}.parquet")
}
// `users` is consumed by the partition — cannot be used after
```

#### Consumed Value Errors

If you attempt to use a binding after its value has been moved, the compiler rejects with a clear error:

```
error[E0301]: use of moved value `users`
  --> pipeline.tl:4:13
   |
 2 | let active = users |> filter(age > 25)
   |              ----- value moved here
 3 |
 4 | let count = users |> count()
   |             ^^^^^ value used after move
   |
   = help: consider `users.clone()` to create an independent copy,
           or restructure your pipeline to chain operations in a single pipe
```

#### Lazy Evaluation and `collect()`

Most table operations are **lazy** — they build a plan, not immediate results:

```tl
let users = load("users.parquet")                  // lazy: registers source
let active = users |> filter(age > 25)             // lazy: appends filter to plan
let with_tier = active |> with { tier = ... }      // lazy: appends with to plan

// Nothing has executed yet.

let result = with_tier |> collect()                // MATERIALIZES: executes full plan
```

Materialization boundaries (where execution is triggered):

| Boundary                           | Behavior                                         |
| ---------------------------------- | ------------------------------------------------ |
| `collect()`                        | Explicit — executes the plan, returns concrete table |
| `load ... ->`                      | Implicit — data must materialize to write to sink |
| `print()` / `show()`              | Implicit — data must materialize to display       |
| `predict(model: ...)`             | Implicit — inference requires concrete data       |
| `parallel for ... in`             | Implicit — partitioning requires concrete data    |
| Pipeline step boundaries           | Implicit — each step materializes before the next |

#### Key Principles

- **Tables are columnar** — Apache Arrow format in memory
- **Lazy by default** — operations build a plan, `collect()` executes
- **Zero-copy** where possible — slicing a table doesn't copy data
- **Automatic spill** — if data exceeds RAM, spill to disk transparently
- **Arena allocation** — per-pipeline memory pools, freed together

### 4.3 Concurrency Model

TL uses **structured concurrency** with task-based parallelism:

```tl
// Automatic parallelism — compiler decides
let result = large_table
    |> filter(active == true)           // Automatically parallel across partitions
    |> aggregate(by: region) { ... }    // Parallel aggregation with merge

// Explicit parallelism
parallel {
    task a = extract(source_postgres)
    task b = extract(source_s3)
    task c = extract(source_api)
}
// All tasks complete before continuing
let combined = union(a, b, c)

// Async for I/O
async fn fetch_api(url: string) -> result<table, Error> {
    let response = http.get(url).await?
    parse_json(response.body)
}
```

**No GIL. No mutexes exposed to users. No threading API.**
Parallelism is either automatic (data partitioning) or structured (task blocks).

---

## 5. Standard Library

### 5.1 Core Modules

```
tl.std
├── core                    // Primitives, operators, builtins
│   ├── types               // Type definitions and conversions
│   ├── math                // Mathematical functions
│   ├── string              // String operations
│   └── datetime            // Date/time operations (first-class)
│
├── data                    // Data engine
│   ├── table               // Table type and operations
│   ├── schema              // Schema definitions and validation
│   ├── quality             // Data quality checks (nulls, ranges, uniqueness)
│   ├── format              // Parquet, CSV, JSON, Avro parsers
│   └── catalog             // Data catalog / metadata registry
│
├── ai                      // AI engine
│   ├── model               // Model type, save/load, versioning
│   ├── train               // Training loops, hyperparameter tuning
│   ├── predict             // Batch and real-time inference
│   ├── embeddings          // Text/image embeddings
│   ├── llm                 // LLM integration (prompt, complete, chat)
│   ├── vision              // Image classification, detection
│   └── metrics             // Accuracy, F1, AUC, RMSE, etc.
│
├── stream                  // Stream engine
│   ├── source              // Stream sources
│   ├── window              // Tumbling, sliding, session windows
│   ├── watermark           // Event-time processing
│   └── sink                // Stream sinks
│
├── connect                 // Connector layer
│   ├── postgres            // PostgreSQL
│   ├── mysql               // MySQL
│   ├── s3                  // AWS S3 / MinIO
│   ├── kafka               // Apache Kafka
│   ├── bigquery            // Google BigQuery
│   ├── snowflake           // Snowflake
│   ├── redis               // Redis
│   ├── http                // REST API client
│   ├── graphql             // GraphQL client
│   └── filesystem          // Local/HDFS/GCS files
│
├── ops                     // Operations
│   ├── pipeline            // Pipeline orchestration
│   ├── schedule            // Cron / interval scheduling
│   ├── monitor             // Metrics, logging, alerting
│   ├── lineage             // Data lineage tracking
│   └── deploy              // Deployment utilities
│
└── test                    // Testing framework
    ├── assert              // Data assertions
    ├── mock                // Mock data sources
    ├── fixture             // Test data generators
    └── benchmark           // Performance benchmarking
```

### 5.2 Built-in Functions (Most Used)

```tl
// Data Operations
count(), sum(), avg(), min(), max(), median(), stddev()
first(), last(), nth(n)
distinct(), unique()
coalesce(a, b, c)          // First non-null value
cast(value, type)
hash(value)

// String
upper(), lower(), trim(), split(), replace(), regex_match()
contains(), starts_with(), ends_with()
levenshtein(), soundex()   // Fuzzy matching built-in

// Date/Time
today(), now(), epoch()
date_add(), date_diff(), date_trunc()
to_date(), to_timestamp()
extract(year | month | day | hour from timestamp)

// Data Quality
is_null(), is_not_null(), is_unique()
is_between(low, high)
is_email(), is_url(), is_phone()    // Common validation patterns
assert_schema(table, expected_schema)
data_profile(table)                  // Statistical profile of all columns

// AI
predict(model, input)
embed(text, model: "sentence-transformer")
similarity(vec_a, vec_b)
cluster(table, k: int, features: [...])
anomaly_detect(table, features: [...])
```

---

## 6. Toolchain & Developer Experience

### 6.1 CLI Tool: `tl`

```bash
# Project Management
tl init my-project               # Create new TL project
tl build                         # Compile project
tl run main.tl                   # Compile and run
tl run --watch main.tl           # Hot-reload on changes
tl test                          # Run test suite
tl bench                         # Run benchmarks

# REPL & Interactive
tl shell                         # Start interactive REPL
tl notebook                      # Start notebook server
tl playground                    # Browser-based playground

# Package Management
tl add postgres-connector        # Add dependency
tl remove redis-connector        # Remove dependency
tl update                        # Update all dependencies
tl publish                       # Publish package to registry

# DevOps
tl deploy pipeline.tl --target docker   # Generate Dockerfile
tl deploy pipeline.tl --target k8s      # Generate K8s manifests
tl lint                                  # Lint source code
tl fmt                                   # Format source code
tl doc                                   # Generate documentation

# Data Tools
tl inspect data.parquet          # Preview data file
tl profile data.csv              # Statistical profile
tl lineage pipeline.tl           # Show data lineage graph
tl explain pipeline.tl           # Show query plan
```

### 6.2 Project Structure

```
my-project/
├── tl.toml                      # Project manifest
├── tl.lock                      # Dependency lock file
├── src/
│   ├── main.tl                  # Entry point
│   ├── schemas/
│   │   ├── user.tl              # User schema
│   │   └── events.tl            # Event schemas
│   ├── transforms/
│   │   ├── clean.tl             # Cleaning transforms
│   │   └── features.tl          # Feature engineering
│   ├── models/
│   │   └── churn.tl             # ML model definitions
│   ├── pipelines/
│   │   ├── daily_etl.tl         # Daily ETL pipeline
│   │   └── streaming.tl         # Real-time pipeline
│   └── connectors/
│       └── custom_api.tl        # Custom connector
├── tests/
│   ├── test_clean.tl            # Transform tests
│   └── test_pipeline.tl         # Integration tests
├── data/
│   └── samples/                 # Sample data for testing
└── deploy/
    ├── Dockerfile
    └── k8s.yaml
```

### 6.3 `tl.toml` Manifest

```toml
[project]
name = "churn-pipeline"
version = "0.1.0"
edition = "2026"
authors = ["Mallesh "]
description = "Customer churn prediction pipeline"

[dependencies]
postgres-connector = "1.2"
kafka-connector = "0.8"
xgboost = "2.0"
sentence-transformers = "1.1"

[build]
target = "native"              # native | wasm | bytecode
optimization = "release"       # debug | release | max
gpu = "auto"                   # auto | cuda | rocm | none

[runtime]
max_memory = "8GB"
parallelism = "auto"           # auto | 1 | 2 | ... | N
spill_to_disk = true
spill_path = "/tmp/tl-spill"

[pipeline.daily_etl]
schedule = "0 6 * * *"
timeout = "30m"
retries = 3
alert_on_failure = ["slack:#data-team"]
```

### 6.4 IDE Support

**Day 1 requirement:** VS Code extension with:

- Syntax highlighting
- LSP (Language Server Protocol) for autocomplete, go-to-definition, hover types
- Schema-aware autocomplete (suggest column names after `|> filter(`)
- Inline data preview (hover over a table variable to see sample rows)
- Pipeline visualization (see DAG of your pipeline in sidebar)
- Integrated profiler (see execution time per pipeline stage)
- Error diagnostics with fix suggestions

---

## 7. Implementation Language & Tech Stack

### 7.1 Core Implementation

| Component             | Language    | Key Libraries/Tools                        |
| --------------------- | ----------- | ------------------------------------------ |
| **Lexer**             | Rust        | `logos` (fastest lexer generator)           |
| **Parser**            | Rust        | Hand-written recursive descent              |
| **Type Checker**      | Rust        | Custom                                      |
| **IR & Optimizer**    | Rust        | Custom + Apache DataFusion concepts         |
| **LLVM Backend**      | Rust        | `inkwell` (safe LLVM bindings)              |
| **Cranelift Backend** | Rust        | `cranelift` (for JIT/REPL)                  |
| **Runtime**           | Rust        | `tokio` (async), `rayon` (data parallel)    |
| **Memory Manager**    | Rust        | `arrow-rs` (Apache Arrow), custom allocator |
| **GPU Backend**       | Rust + CUDA | `cudarc` or custom PTX generation           |
| **CLI**               | Rust        | `clap` (CLI parser)                         |
| **LSP Server**        | Rust        | `tower-lsp`                                 |
| **Package Registry**  | Rust        | `axum` (web framework)                      |
| **WASM Backend**      | Rust        | `wasm-pack`, `wasm-bindgen`                 |

### 7.2 Why Rust?

1. **Performance** — Zero-cost abstractions, no GC, matches C/C++
2. **Safety** — Memory safety without garbage collection (perfect for a data runtime)
3. **Ecosystem** — `arrow-rs`, `datafusion`, `cranelift`, `inkwell` already exist
4. **WASM** — First-class WASM compilation support
5. **Concurrency** — `tokio` + `rayon` give you both async and data parallelism
6. **Precedent** — Mojo's compiler, Ruff (Python linter), Polars, Deno all chose Rust

### 7.3 Key Rust Crates (Dependencies)

```toml
[dependencies]
# Compiler
logos = "0.13"                    # Lexer generator
inkwell = "0.4"                   # LLVM bindings
cranelift = "0.104"               # JIT compiler

# Runtime
tokio = { version = "1", features = ["full"] }   # Async runtime
rayon = "1.8"                     # Data parallelism
arrow = "50"                      # Apache Arrow (columnar memory)
datafusion = "35"                 # Query execution (can borrow from)
parquet = "50"                    # Parquet file support

# Connectors
sqlx = "0.7"                      # Postgres, MySQL, SQLite
rdkafka = "0.36"                  # Kafka
aws-sdk-s3 = "1.0"               # S3
reqwest = "0.11"                  # HTTP client

# AI
ort = "2.0"                       # ONNX Runtime bindings
candle-core = "0.4"               # Tensor operations (by Hugging Face)
tokenizers = "0.15"               # Text tokenization

# CLI & Tooling
clap = "4"                        # CLI argument parser
tower-lsp = "0.20"                # Language Server Protocol
serde = "1"                       # Serialization
```

---

## 8. Benchmarking Targets

TL's performance goals are defined against two baselines: the Python/Pandas ecosystem (where large speedups are expected) and modern Rust/C++-based alternatives (where the goal is parity, not dominance). All benchmarks will be published with reproducible scripts, pinned versions, and hardware specs.

### 8.1 vs Python / Pandas

| Benchmark                            | Python Baseline       | TL Target   | Target Speedup |
| ------------------------------------ | --------------------- | ----------- | -------------- |
| 1B row CSV parse                     | ~45s (Pandas)         | < 4s        | 10-15x         |
| 1B row filter + aggregate            | ~30s (Pandas)         | < 2s        | 15-20x         |
| 1B row join (two tables)             | ~60s (Pandas)         | < 5s        | 12-15x         |
| ETL pipeline (extract+transform+load)| ~5min (Airflow+Pandas)| < 30s       | 10x            |
| End-to-end ML pipeline (see 8.3)     | ~8min (Python)        | < 60s       | 8x             |
| Embedding generation (100K docs)     | ~300s (Python)        | < 45s       | 6-7x           |
| Stream processing throughput         | ~10K events/s (Python)| 500K events/s| 50x           |
| Cold start (pipeline boot)           | 3-5s (Python)         | < 100ms     | 30x+           |

### 8.2 vs Modern Alternatives (Polars, DuckDB, DataFusion, Flink)

Polars, DuckDB, and DataFusion are Rust/C++-based and highly optimized. TL targets **parity on raw data operations** with advantages in ergonomics and cross-boundary optimization.

| Benchmark                            | Best Modern Tool     | TL Target vs That Tool | Notes                                |
| ------------------------------------ | -------------------- | ---------------------- | ------------------------------------ |
| 1B row CSV parse                     | Polars (~3s)         | Within 1.2x           | Both use Arrow; similar I/O bound    |
| 1B row filter + aggregate            | DuckDB (~1.5s)       | Within 1.2x           | Vectorized execution, similar approach|
| 1B row join (two tables)             | DataFusion (~4s)     | Within 1.3x           | Hash join strategies comparable      |
| TPC-H SF100 (full suite)            | DuckDB               | Within 1.5x           | DuckDB is best-in-class here         |
| Stream processing (50K events/sec)  | Flink (~200K events/s)| Match                 | Flink is JVM but heavily optimized   |
| DataFrame API ergonomics             | Polars               | Advantage TL           | First-class syntax vs library API    |
| End-to-end pipeline (data + ML)      | N/A (requires glue)  | Advantage TL           | See Section 8.3                      |

### 8.3 End-to-End ML Pipeline (Reframed)

TL does not reimplement XGBoost's core algorithm. The advantage is in the **end-to-end pipeline** — eliminating serialization and process boundaries:

```
Python end-to-end ML pipeline:
  pandas.read_csv()            →  45s   (CSV parse, Python objects)
  DataFrame transforms         →  30s   (feature engineering, GIL-bound)
  df.to_numpy()                →   5s   (serialization to NumPy)
  xgboost.train()             → 120s   (native XGBoost — already fast)
  model.predict()              →  15s   (batch inference)
  pandas.to_sql()              →  60s   (write results back)
  ─────────────────────────────────────
  Total:                        ~275s   (+ Airflow scheduling overhead)

TL end-to-end ML pipeline:
  load + filter + with         →   4s   (compiled, Arrow-native, fused)
  train xgboost { ... }       → 110s   (same XGBoost core, zero-copy data handoff)
  predict + with + save        →   6s   (compiled, Arrow-native, pipelined)
  ─────────────────────────────────────
  Total:                        ~120s   (no serialization boundaries)
```

The speedup is **~2.3x on the full pipeline**. The XGBoost training step itself is comparable. TL's win is eliminating ~155 seconds of overhead from format conversions, serialization, and process boundaries.

### 8.4 What TL Optimizes That Others Don't

TL's real performance advantage is **cross-boundary optimization**. In a Python stack, the pipeline is separate programs: Pandas extracts, Airflow orchestrates, another script engineers features, XGBoost trains, and a final script loads to a database. Each boundary involves serialization and process startup — and no optimizer can see across them.

TL's compiler sees the entire pipeline as a single program. It can:

- **Fuse extract and transform** — push filters into the connector (generate WHERE clauses for PostgreSQL)
- **Eliminate intermediate materialization** — skip temp files between transform and train
- **Pipeline I/O with compute** — start feature engineering on early partitions while later ones load
- **Share subexpressions** — if two steps compute the same derived column, compute it once
- **Optimize across the predict boundary** — fuse post-prediction transforms with the inference loop

This is TL's structural advantage — not raw single-query speed, but whole-pipeline optimization.

### 8.5 Benchmark Methodology

> **Reproducibility commitment:** Every benchmark will be published with pinned versions, exact hardware specs, dataset generation scripts, and a single `tl bench run` command to reproduce. Results will be published on `benchmarks.thinkinglang.dev`.

Reference: Compare against Polars (DataFrame performance), DuckDB (analytical queries), Apache DataFusion (query engine), Apache Flink (stream processing), and ONNX Runtime (AI inference).

---

## 9. Development Roadmap

### Phase 0: Foundation (Month 1-2)

**Goal:** Prove the concept compiles and runs

```
Deliverables:
  ✦ Language specification document (formal grammar)
  ✦ Lexer — tokenize .tl files
  ✦ Parser — produce AST for core subset:
      - let bindings
      - basic types (int, float, string, bool)
      - functions
      - if/else, match
      - pipe operator |>
  ✦ Tree-walking interpreter (slow but correct)
  ✦ 10 working code examples
  ✦ Basic REPL (tl shell)

Team: 1-2 people
```

### Phase 1: Data Engine (Month 3-5)

**Goal:** Tables and transformations work end-to-end

```
Deliverables:
  ✦ table<T> type with Arrow-based columnar storage
  ✦ schema definitions
  ✦ Core operations: filter, select, with, aggregate, join, sort
  ✦ Pipe operator chaining
  ✦ CSV and Parquet file reading/writing
  ✦ Lazy evaluation engine
  ✦ Basic query optimizer (predicate pushdown, column pruning)
  ✦ PostgreSQL connector
  ✦ 1B Row Challenge benchmark passing

Team: 2-3 people
```

### Phase 2: Compiler Backend (Month 6-8)

**Goal:** Compiled execution, real performance

```
Deliverables:
  ✦ TL-IR design and implementation
  ✦ Cranelift JIT backend (for REPL, fast iteration)
  ✦ LLVM backend (for production builds)
  ✦ Automatic parallelization of table operations
  ✦ Memory manager with arena allocation
  ✦ Spill-to-disk for large datasets
  ✦ Benchmark suite (vs Python, Polars, Spark)
  ✦ Performance within 2x of Polars on standard benchmarks

Team: 3-4 people
```

### Phase 3: AI Integration (Month 9-12)

**Goal:** AI/ML as native language features

```
Deliverables:
  ✦ tensor type with GPU support
  ✦ model type (save, load, version)
  ✦ train keyword for built-in algorithms (XGBoost, linear, random forest)
  ✦ predict keyword for batch inference
  ✦ embed keyword for text/image embeddings
  ✦ ONNX Runtime integration for model portability
  ✦ LLM integration (ai.complete, ai.chat for API-based models)
  ✦ Auto-batching for inference
  ✦ Model registry (local + S3)

Team: 3-5 people
```

### Phase 4: Streaming & Pipelines (Month 13-16)

**Goal:** Production pipeline orchestration

```
Deliverables:
  ✦ stream type with windowing
  ✦ Kafka connector (source + sink)
  ✦ pipeline construct with scheduling
  ✦ Retry, timeout, error handling
  ✦ Alerting (Slack, email, webhook)
  ✦ Data lineage tracking
  ✦ tl deploy for Docker and Kubernetes
  ✦ Monitoring dashboard

Team: 4-6 people
```

### Phase 5: Ecosystem & Community (Month 17-20)

**Goal:** Ready for external adoption

```
Deliverables:
  ✦ Package registry (packages.thinkinglang.dev)
  ✦ VS Code extension with full LSP
  ✦ tl notebook (interactive notebooks)
  ✦ Comprehensive documentation site
  ✦ Tutorial series (20+ tutorials)
  ✦ Playground (browser-based, WASM)
  ✦ 10+ connector packages
  ✦ Open source launch (GitHub + announcement)
  ✦ Community Discord / forum

Team: 5-8 people
```

### Phase 6: Production & Scale (Month 21-24)

**Goal:** Enterprise-ready

```
Deliverables:
  ✦ Distributed execution (multi-node pipelines)
  ✦ Role-based access control for data sources
  ✦ Audit logging
  ✦ SOC 2 compliance considerations
  ✦ Enterprise connectors (Salesforce, SAP, Oracle)
  ✦ Performance: within 1.5x of hand-written Rust for data operations
  ✦ 1.0 stable release

Team: 8-12 people
```

### Phase 7: Language Completeness & Runtime (Implemented)

**Goal:** Full language feature set with production runtime

```
Delivered (our Phases 5-9):
  ✦ Structs, enums, impl blocks, method dispatch
  ✦ Try/catch/throw error handling
  ✦ Import system (basic)
  ✦ Stdlib: string, list, math, map, JSON, file I/O, regex, date/time
  ✦ Concurrency: spawn/await, channels, combinators (pmap, timeout)
  ✦ Iterators & generators: yield, next, lazy combinators
  ✦ Error quality: statement-level spans, stack traces, bytecode disassembler
  ✦ REPL improvements: history, multi-line input, tab completion
  ✦ 437 tests passing
```

### Phase 8: Type System Foundation (Month 25-27)

**Goal:** Gradual static typing — catch errors at compile time, not runtime

```
Deliverables:
  ✦ Type checker pass between parse and compile/interpret
  ✦ Type annotations enforced on function signatures
  ✦ Type inference for let bindings (Hindley-Milner subset)
  ✦ result<T, E> type with ? operator for error propagation
  ✦ option<T> / T? with ?? null coalescing operator
  ✦ Typed table<T>, stream<T>, tensor<dtype, shape>
  ✦ set<T> type
  ✦ Type-aware pattern matching with destructuring
  ✦ Compile-time type error messages with suggestions
  ✦ Gradual: untyped code still works (inferred as `any`)
```

### Phase 9: Module System (Month 28-30)

**Goal:** Multi-file projects with proper namespacing

```
Deliverables:
  ✦ File-based modules: one .tl file = one module
  ✦ Directory modules with mod.tl
  ✦ pub visibility modifier for functions, structs, schemas
  ✦ use imports: single item, multiple items, wildcard, aliased
  ✦ Re-exports (pub use)
  ✦ tl.toml project manifest (project metadata, dependencies, build config)
  ✦ tl init command to scaffold new projects
  ✦ tl build command for multi-file compilation
  ✦ Module-scoped namespaces (no global pollution)
  ✦ Circular dependency detection
```

### Phase 10: Generics & Traits (Month 31-33)

**Goal:** Parametric polymorphism for type-safe reusable code

```
Deliverables:
  ✦ Generic functions: fn first<T>(items: list<T>) -> T?
  ✦ Generic structs: struct Pair<A, B> { first: A, second: B }
  ✦ Trait definitions: trait Connectable { fn connect() -> result<Connection, Error> }
  ✦ Trait implementations: impl Connectable for PostgresSource { ... }
  ✦ Trait bounds: fn process<T: Serializable>(data: T) -> bytes
  ✦ Monomorphization (compile-time specialization)
  ✦ Built-in traits: Display, Debug, Clone, Serialize, Deserialize
  ✦ Where clauses for complex bounds
```

### Phase 11: Semantic Analysis & Optimization (Month 34-36)

**Goal:** Compile-time correctness guarantees and performance

```
Deliverables:
  ✦ Full type checker integrated into compilation pipeline
  ✦ Schema validation: verify table schemas match between pipeline stages
  ✦ Ownership analysis: pipe |> moves values, use-after-move errors
  ✦ Dead code elimination
  ✦ Constant folding and propagation
  ✦ TL-IR intermediate representation (unified code + query plan)
  ✦ Predicate pushdown for table operations
  ✦ Column pruning optimization
  ✦ Borrow checking (simplified — no lifetime annotations)
```

### Phase 12: LSP & Developer Tooling (Month 37-39)

**Goal:** World-class developer experience drives adoption

```
Deliverables:
  ✦ Language Server Protocol (LSP) implementation
  ✦ VS Code extension with syntax highlighting, diagnostics, go-to-definition
  ✦ Auto-completion for types, functions, modules, fields
  ✦ Hover documentation
  ✦ tl fmt — code formatter (opinionated, like gofmt)
  ✦ tl lint — linter with data engineering best practices
  ✦ tl doc — documentation generator from doc comments
  ✦ tl explain — show query plan for pipeline
  ✦ Inline type hints in editor
```

### Phase 13: Data Quality & Connectors (Month 40-42)

**Goal:** Production data engineering with clean/validate and connectors

```
Deliverables:
  ✦ clean block: null handling (fill, drop_row, median), dedup, outlier clamping
  ✦ validate block: row_count, null_rate, uniqueness assertions
  ✦ data_profile() — statistical profile of all columns
  ✦ Validation builtins: is_email(), is_url(), is_phone(), is_between()
  ✦ Fuzzy matching: levenshtein(), soundex()
  ✦ MySQL connector
  ✦ S3 connector (AWS S3 / MinIO)
  ✦ Redis connector
  ✦ GraphQL client
  ✦ Connector trait — user-defined custom connectors
```

---

## 10. Competitive Landscape

```
                          High Performance
                               ▲
                               │
                      Rust ●   │
                               │
                       Mojo ●  │  ● TL (TARGET)
                               │
                      C++ ●    │         ● Julia
                               │
         Low Usability ◄───────┼────────► High Usability
                               │
                    Scala ●    │    ● Python + Polars
                               │
                   DuckDB ●    │       ● dbt
                               │
                   R ●         │           ● Python
                               │
                          Low Performance
```

TL targets the intersection of **high usability** (Python-like syntax, domain constructs) and **near-Rust performance** (compiled, Arrow-native, zero-copy). It does not claim to exceed Rust — Rust allows hand-tuned, allocation-free code a higher-level language cannot match. TL's claim is **80-95% of Rust's data-processing speed** with dramatically higher productivity.

### Direct Competitors & Differentiation

| Language/Tool        | Their Killer Strength                              | TL's Advantage                                   | TL's Honest Trade-off                                  |
| -------------------- | -------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------ |
| **Python**           | Largest ML/data ecosystem in the world             | 10-50x faster, type-safe, compiled pipelines      | Zero ecosystem at launch; Python's breadth is years ahead |
| **Mojo**             | Compiled ML, Python superset, MLIR backend         | Better data engineering (pipelines, streaming, connectors) | Mojo has Modular's resources and Python compat         |
| **Julia**            | Scientific computing, numeric type system, JIT     | AOT compiled, no JIT warmup, deterministic perf   | Julia's numeric computing and dispatch are more mature  |
| **SQL / dbt**        | Declarative, universally understood, warehouse-native | Full programming language + AI + streaming       | SQL's simplicity is a feature for analysts              |
| **Scala + Spark**    | Battle-tested distributed computing at petabyte scale | Simpler syntax, faster single-node, no JVM      | Spark is proven at massive scale; TL's distributed story is unproven |
| **Rust**             | Maximum performance, memory safety, mature ecosystem | Domain-specific abstractions as language primitives | Rust is faster when hand-optimized                     |
| **DuckDB**           | Embedded analytics, zero-config, excellent SQL perf | Full language (not just SQL), ML + streaming      | DuckDB is more mature for pure analytical queries       |
| **Polars**           | Fastest DataFrame library, Rust-based, lazy eval   | First-class language syntax, integrated ML/streaming | Polars is embeddable in Python/Rust; TL requires new language |

### TL's Unique Positioning

**"The only language where data pipelines, SQL-like queries, ML training, and real-time streaming are all first-class language features — not libraries."**

The competitive moat is not beating Polars at DataFrames or DuckDB at SQL — it is offering a **single coherent language** that eliminates glue code. The value is strongest for teams maintaining 4-6 tools (Airflow + dbt + Pandas + Spark + PyTorch + Kafka consumers) and paying a productivity tax on the boundaries between them.

---

## 11. Open Questions & Decisions Needed

These are critical design decisions that need to be resolved:

| #  | Question                                            | Options                                              | Leaning   | Status    |
| -- | --------------------------------------------------- | ---------------------------------------------------- | --------- | --------- |
| 1  | Indentation-based or braces?                        | Python-style indent / Rust-style braces / Both       | Braces    | **Decided** |
| 2  | Semicolons?                                         | Required / Optional / Newline-terminated              | Optional  | **Decided** |
| 3  | Null handling?                                      | Option type only / SQL-style NULL / Both              | Option    | **Decided** |
| 4  | Error handling?                                     | Result type (Rust) / Exceptions (Python) / Both      | Result    | **Decided** |
| 5  | Should TL support OOP?                              | No (functional) / Minimal (traits) / Full classes     | Traits    | **Decided** |
| 6  | License?                                            | Apache 2.0 / MIT / BSL (source-available)            | Apache    | Open      |
| 7  | Column extension keyword?                           | `with` / `derive` / `extend` / `compute`             | `with`    | **Decided** |
| 8  | Distributed execution: built-in or external?        | Built into runtime / Generate Spark/Ray code         | Built-in  | Open      |
| 9  | Batch/stream unification?                           | Separate `table`/`stream` / Unified type             | Separate  | Open      |
| 10 | Primary target: data engineers or data scientists?  | Engineers / Scientists / Both                         | Both      | **Decided** |
| 11 | Python interop: FFI bridge or compile-to-Python?    | FFI bridge / Compile-to-Python / None                | FFI       | **Decided** |
| 12 | Package registry: self-hosted or crates.io-style?   | Own registry / Reuse existing infra                  | Own       | Open      |

---

## 12. Resource Requirements

### 12.1 Team Composition (Ideal)

| Role                              | Count | Priority     | Key Skills                                    |
| --------------------------------- | ----- | ------------ | --------------------------------------------- |
| **Compiler Engineer**             | 2-3   | Critical     | Rust, LLVM, parser design, type systems       |
| **Data Engine Engineer**          | 2     | Critical     | Apache Arrow, query optimization, columnar    |
| **AI/ML Runtime Engineer**        | 1-2   | High         | ONNX, CUDA, tensor operations, model serving  |
| **Streaming Engineer**            | 1     | High         | Kafka, event-time processing, windowing       |
| **Developer Experience / Tooling**| 1-2   | High         | LSP, VS Code extensions, CLI tools, docs      |
| **Connector Engineer**            | 1     | Medium       | Database drivers, API integrations, protocols  |
| **Technical Writer**              | 1     | Medium       | Language documentation, tutorials, examples   |
| **DevRel / Community**            | 1     | Later        | Open source community building, evangelism     |

**Minimum Viable Team (Phase 0-1):** 2-3 people (compiler + data engine focus)

### 12.2 Infrastructure Requirements

```
Development:
  - CI/CD: GitHub Actions (build + test matrix: Linux, macOS, Windows)
  - Benchmarking: Dedicated bare-metal server for reproducible benchmarks
  - GPU: At least 1x NVIDIA A100 or equivalent for AI backend testing
  - Storage: S3-compatible storage for test data (1TB+)

Production Services:
  - Package registry: packages.thinkinglang.dev (Rust web service + S3 backend)
  - Documentation site: docs.thinkinglang.dev (static site generator)
  - Playground: play.thinkinglang.dev (WASM-compiled TL in browser)
  - Telemetry: opt-in anonymous usage analytics (what features are used)
```

### 12.3 Estimated Budget (24 months)

| Category                 | Monthly Cost  | 24-Month Total |
| ------------------------ | ------------- | -------------- |
| Team (5 avg headcount)   | $75,000       | $1,800,000     |
| Cloud infrastructure     | $3,000        | $72,000        |
| GPU instances (AI dev)   | $2,000        | $48,000        |
| Tools & licenses         | $500          | $12,000        |
| Travel & conferences     | $1,500        | $36,000        |
| **Total**                | **$82,000**   | **$1,968,000** |

> Note: Costs assume a lean startup model. Salaries reflect senior Rust/compiler engineers. GPU costs assume spot instances where possible.

---

## 13. Error Handling — Detailed Design

### 13.1 Philosophy

Data is messy. Networks fail. APIs timeout. Files are malformed. TL's error handling must be **first-class, ergonomic, and data-aware** — not an afterthought bolted onto try/catch.

### 13.2 The `result` Type

TL uses Rust-inspired `result<T, E>` as the primary error mechanism:

```tl
// Functions that can fail return result<T, E>
fn load_users(path: string) -> result<table<User>, DataError> {
    let raw = read_csv(path)?          // ? propagates error automatically
    let validated = raw |> validate_schema(User)?
    Ok(validated)
}

// Caller handles the error
match load_users("users.csv") {
    Ok(users) => process(users)
    Err(DataError::FileNotFound(path)) => log("Missing: {path}")
    Err(DataError::SchemaViolation(details)) => alert("Bad schema: {details}")
    Err(e) => panic("Unexpected: {e}")
}
```

### 13.3 Data-Specific Error Types

```tl
// Built-in error hierarchy for data work
enum DataError {
    // Source errors
    FileNotFound(path: string)
    ConnectionFailed(source: string, reason: string)
    AuthenticationFailed(source: string)
    Timeout(source: string, duration: duration)

    // Schema errors
    SchemaViolation(expected: schema, got: schema, diff: list<string>)
    TypeMismatch(column: string, expected: type, got: type)
    MissingColumn(name: string)

    // Quality errors
    NullsFound(column: string, count: int64, threshold: float64)
    DuplicatesFound(columns: list<string>, count: int64)
    OutOfRange(column: string, value: string, min: string, max: string)  // string representations for display

    // Pipeline errors
    StepFailed(step: string, inner: DataError)
    RetryExhausted(step: string, attempts: int, last_error: DataError)
    TimeoutExceeded(pipeline: string, elapsed: duration, limit: duration)
}
```

### 13.4 The `clean` Block — Data-Aware Error Recovery

Unlike traditional try/catch, TL provides `clean` blocks specifically for data quality issues:

```tl
let users = load("raw_users.csv")
    |> clean {
        nulls: {
            name: fill("UNKNOWN")
            email: drop_row                      // Remove rows with null email
            age: fill(median)                    // Fill with column median
        }
        duplicates: dedupe(by: [email], keep: "latest")
        outliers: {
            age: clamp(0, 150)                   // Cap at reasonable range
            monthly_spend: remove_if(> mean + 3 * stddev)
        }
        type_errors: coerce_or_null              // Try to cast, null if impossible
    }
    |> validate {
        assert row_count > 0, "No users after cleaning"
        assert null_rate(email) == 0.0, "Email must be non-null"
        assert unique(id), "Duplicate IDs found"
    }
```

### 13.5 Pipeline Error Recovery

```tl
pipeline resilient_etl {
    steps {
        raw = extract users
        cleaned = transform clean_users(raw)
        load cleaned -> postgres("analytics_db").table("users")
    }

    on_failure(step, error) {
        match error {
            DataError::ConnectionFailed(_, _) => {
                retry(step, delay: 30s, max: 3)
            }
            DataError::SchemaViolation(_, _, diff) => {
                // Route to dead-letter queue for manual review
                load step.input -> s3("dead-letter/{pipeline}/{step}/{timestamp}")
                alert slack("#data-quality", "Schema drift detected: {diff}")
            }
            _ => {
                alert pagerduty("data-critical", "Pipeline failure: {error}")
                abort
            }
        }
    }
}
```

---

## 14. Security Model

### 14.1 Principles

```
1. NO IMPLICIT NETWORK ACCESS      — All external connections must be declared
2. CREDENTIALS NEVER IN CODE       — Secrets come from environment or vault
3. SANDBOXED EXECUTION             — Pipelines run with minimal permissions
4. AUDIT EVERYTHING                — All data access is logged
5. SCHEMA AS CONTRACT              — Type system prevents data leakage
```

### 14.2 Connection Security

```tl
// Credentials are NEVER hardcoded — resolved from environment or vault
source users = postgres(
    env("POSTGRES_URL")              // Resolved at runtime from environment
) .table("users")

// Or using a secrets vault
source users = postgres(
    secret("vault://prod/postgres/analytics")
) .table("users")

// Connection permissions are declarative
connector analytics_db {
    type: postgres
    url: env("ANALYTICS_DB_URL")
    permissions: [read]              // This connector can ONLY read
    allowed_schemas: ["public"]      // Restricted to specific schemas
    row_limit: 10_000_000           // Safety limit
}
```

### 14.3 Data Masking & Access Control

```tl
// Column-level access control
schema User {
    id: int64
    name: string
    email: string @sensitive          // Marked as PII
    ssn: string @restricted           // Marked as restricted
    age: int64
    region: string
}

// Transforms automatically mask sensitive fields based on role
transform public_users(src: table<User>) -> table<User> {
    src |> mask_sensitive()           // email → "j***@example.com", ssn → "***-**-1234"
}

// Or explicit redaction
transform anonymize(src: table<User>) -> table<User> {
    src |> redact(columns: [email, ssn])   // Completely removes values
        |> hash(columns: [id])             // Pseudonymize the ID
}
```

### 14.4 Sandbox Execution

```tl
// Pipelines declare their required permissions
pipeline daily_etl {
    permissions {
        read: [analytics_db, s3("data-lake/*")]
        write: [analytics_db.table("output"), s3("output/*")]
        network: [webhook("https://hooks.slack.com/*")]
        filesystem: none
        gpu: auto
    }
    // ...
}
// The runtime REJECTS any operation not covered by declared permissions
```

---

## 15. Schema Evolution

### 15.1 Philosophy

Data schemas change. Columns are added, renamed, deprecated, and removed. In production data systems, schema evolution is not exceptional — it is routine. TL treats schema evolution as a **first-class language feature** with compile-time safety, automatic migration, and team-wide governance.

### 15.2 Schema Versioning

Every schema can declare a version with the `@version` annotation. The version is a monotonically increasing integer:

```tl
@version(1)
schema User {
    id: int64
    name: string
    email: string
    signup_date: date
}
```

When you evolve a schema, you increment the version and annotate changes:

```tl
@version(2)
schema User {
    id: int64
    name: string
    email: string
    signup_date: date
    region: string          @since(2)                   // Added in v2
    monthly_spend: float64  @since(2, default: 0.0)     // Added in v2 with default
}
```

### 15.3 Field Lifecycle Annotations

TL provides annotations to manage the full lifecycle of schema fields:

| Annotation                                    | Purpose                                        |
| --------------------------------------------- | ---------------------------------------------- |
| `@since(version)`                             | Field was added in this version                |
| `@since(version, default: value)`             | Added with a backfill default for older data   |
| `@deprecated(version, replacement: "field")`  | Marked for removal; points to successor        |
| `@removed(version)`                           | Field no longer exists in this version         |

```tl
@version(3)
schema User {
    id: int64
    name: string
    email: string
    signup_date: date
    region: string              @since(2)
    monthly_spend: float64      @since(2, default: 0.0)
    full_name: string           @since(3)
    first_name: string          @deprecated(3, replacement: "full_name")
    last_name: string           @deprecated(3, replacement: "full_name")
}
```

The compiler emits warnings when code reads deprecated fields:

```
Warning: Field `first_name` on User is deprecated since v3

  24 │     |> with { greeting = "Hello, {first_name}" }
     │                                     ^^^^^^^^^^
     │                                     Deprecated — use `full_name` instead
```

### 15.4 Migration Blocks

Migration logic is defined with `migrate` blocks that transform data from one version to the next:

```tl
schema User {
    @version(3)
    // ... fields as above ...

    migrate from(1) to(2) {
        region = "UNKNOWN"
        monthly_spend = 0.0
    }

    migrate from(2) to(3) {
        full_name = "{first_name} {last_name}"
    }
}
```

Migrations are **composable**. When the runtime encounters v1 data and the current schema is v3, it applies `1 -> 2` then `2 -> 3` automatically. Each `migrate` block has access to all fields available in the source version and must produce values for all fields added in the target version.

### 15.5 Runtime Behavior

When TL reads data from Parquet files, databases, or streams, it checks the schema version embedded in the data metadata against the current schema version. If they differ, migrations are applied transparently:

```tl
// This Parquet file was written with User v1
// Current schema is User v3 — migrations v1→v2→v3 are applied on read
let users = read_parquet("legacy_users_2024.parquet") -> User

// `users` now has `region`, `monthly_spend`, and `full_name` columns
// populated via migration defaults and migration logic
```

No manual ETL backfill scripts. No "add column with default" SQL migrations. The schema definition **is** the migration.

### 15.6 Compatibility Checking

The `tl schema check` CLI command validates compatibility between schema versions:

```bash
# Check for breaking changes between two versions of a schema file
tl schema check --breaking schemas/user_v2.tl schemas/user_v3.tl

# Output:
# ✓ No breaking changes detected.
# Additions:    full_name (string, since v3)
# Deprecations: first_name (use full_name), last_name (use full_name)
# Removals:     none
```

**Forward compatibility** (old code reads new data): safe if new fields have defaults or are optional. The compiler enforces that `@since` fields must be either nullable (`T?`) or have a `default` value.

**Backward compatibility** (new code reads old data): safe if migrations are defined. The compiler enforces that every version gap has a corresponding `migrate` block.

| Change Type         | Forward Compatible? | Backward Compatible? | Requires Migration? |
| ------------------- | ------------------- | -------------------- | ------------------- |
| Add optional field  | Yes                 | Yes (with default)   | Yes                 |
| Remove field        | No (breaking)       | Yes                  | No                  |
| Rename field        | No (breaking)       | Yes (with migration) | Yes                 |
| Change field type   | No (breaking)       | Depends              | Yes                 |
| Add enum variant    | Yes                 | Yes                  | No                  |
| Remove enum variant | No (breaking)       | No (breaking)        | Yes                 |

### 15.7 Complete Example: User Schema v1 to v3

```tl
// ---- Version 1 (initial release) ----
@version(1)
schema User {
    id: int64
    first_name: string
    last_name: string
    email: string
    created_at: timestamp
}

// ---- Version 2 (add region and spend tracking) ----
@version(2)
schema User {
    id: int64
    first_name: string
    last_name: string
    email: string
    created_at: timestamp
    region: string          @since(2, default: "UNKNOWN")
    monthly_spend: float64  @since(2, default: 0.0)

    migrate from(1) to(2) {
        region = "UNKNOWN"
        monthly_spend = 0.0
    }
}

// ---- Version 3 (consolidate name fields, add status) ----
@version(3)
schema User {
    id: int64
    full_name: string                               @since(3)
    first_name: string                              @deprecated(3, replacement: "full_name")
    last_name: string                               @deprecated(3, replacement: "full_name")
    email: string
    created_at: timestamp
    region: string                                  @since(2, default: "UNKNOWN")
    monthly_spend: float64                          @since(2, default: 0.0)
    status: enum { Active, Inactive, Suspended }    @since(3, default: Active)

    migrate from(1) to(2) {
        region = "UNKNOWN"
        monthly_spend = 0.0
    }

    migrate from(2) to(3) {
        full_name = "{first_name} {last_name}".trim()
        status = Active
    }
}
```

### 15.8 Schema Registry

For teams managing many schemas across services, TL provides a built-in schema registry:

```bash
# Publish a schema to the team registry
tl schema publish src/schemas/user.tl --registry https://registry.example.com

# Pull latest schemas from the registry
tl schema pull --registry https://registry.example.com

# Diff a local schema against the registry version
tl schema diff src/schemas/user.tl --registry https://registry.example.com

# List all schemas and their versions in the registry
tl schema registry list --registry https://registry.example.com
```

The registry tracks version history, enforces compatibility rules, and prevents accidental breaking changes from being published. It integrates with CI/CD — a `tl schema check --breaking` step can gate deployments.

```toml
# tl.toml — registry configuration
[schema.registry]
url = "https://registry.example.com"
namespace = "analytics"
auto_publish = true         # Publish schemas on `tl build --release`
compatibility = "backward"  # Enforce backward compatibility by default
```

---

## 16. Formal Grammar (EBNF Subset)

This section defines the core grammar formally. Full grammar will be in a separate `grammar.ebnf` file.

```ebnf
(* ============================================================ *)
(* ThinkingLanguage — Core Grammar (EBNF)                       *)
(* ============================================================ *)

program           = { declaration } ;

declaration       = schema_decl
                  | struct_decl
                  | source_decl
                  | transform_decl
                  | pipeline_decl
                  | model_decl
                  | stream_decl
                  | fn_decl
                  | let_binding
                  | use_decl
                  | enum_decl ;

(* --- Use / Import --- *)
use_decl          = "use" module_path [ ".{" ident_list "}" ] ;
module_path       = IDENT { "." IDENT } ;
ident_list        = IDENT { "," IDENT } ;

(* --- Schema & Struct --- *)
schema_decl       = [ "@" "version" "(" INT ")" ] "schema" IDENT "{" { field_decl | migrate_block } "}" ;
struct_decl       = "struct" IDENT [ "<" type_params ">" ] "{" { field_decl } "}" ;
enum_decl         = "enum" IDENT "{" IDENT { "(" param_list ")" } { "," IDENT { "(" param_list ")" } } "}" ;
field_decl        = IDENT ":" type_expr [ { annotation } ] ;
annotation        = "@" IDENT [ "(" expr_list ")" ] ;
migrate_block     = "migrate" "from" "(" INT ")" "to" "(" INT ")" "{" { statement } "}" ;
type_params       = IDENT { "," IDENT } ;

(* --- Source --- *)
source_decl       = "source" IDENT "=" connector_expr [ "->" type_expr ] ;
connector_expr    = IDENT "(" expr_list ")" { "." IDENT "(" expr_list ")" } ;

(* --- Transform --- *)
transform_decl    = "transform" IDENT "(" param_list ")" "->" type_expr
                    "{" pipe_expr "}" ;
pipe_expr         = expr { "|>" expr } ;

(* --- Pipeline --- *)
pipeline_decl     = "pipeline" IDENT "{" pipeline_body "}" ;
pipeline_body     = { pipeline_field }
                    "steps" "{" { step_stmt } "}"
                    [ on_failure_block ]
                    [ on_success_block ] ;
pipeline_field    = IDENT ":" expr ;
step_stmt         = IDENT "=" step_action
                  | "load" expr "->" connector_expr ;
step_action       = "extract" IDENT
                  | "transform" IDENT "(" expr_list ")" ;
on_failure_block  = "on_failure" [ "(" IDENT "," IDENT ")" ] "{" { statement } "}" ;
on_success_block  = "on_success" "{" { statement } "}" ;

(* --- Model --- *)
model_decl        = "model" IDENT "=" "train" IDENT "{" { model_field } "}" ;
model_field       = IDENT ":" expr ;

(* --- Stream --- *)
stream_decl       = "stream" IDENT "{" stream_body "}" ;
stream_body       = { stream_field }
                    "process" "(" param_list ")" "{" pipe_expr "}" ;
stream_field      = IDENT ":" expr ;

(* --- Function --- *)
fn_decl           = [ "pub" ] [ "async" ] "fn" IDENT "(" param_list ")"
                    [ "->" type_expr ] "{" { statement } "}" ;

(* --- Let Binding --- *)
let_binding       = "let" [ "mut" ] IDENT [ ":" type_expr ] "=" expr ;

(* --- Types --- *)
type_expr         = primitive_type
                  | "table" [ "<" type_expr ">" ]
                  | "stream" [ "<" type_expr ">" ]
                  | "tensor" [ "<" type_expr "," shape ">" ]
                  | "model"
                  | "list" "<" type_expr ">"
                  | "set" "<" type_expr ">"
                  | "map" "<" type_expr "," type_expr ">"
                  | "option" "<" type_expr ">"
                  | type_expr "?"
                  | "result" "<" type_expr "," type_expr ">"
                  | "any"
                  | IDENT [ "<" type_expr { "," type_expr } ">" ] ;

primitive_type    = "int8" | "int16" | "int32" | "int64"
                  | "uint8" | "uint16" | "uint32" | "uint64"
                  | "float32" | "float64"
                  | "bool" | "string" | "bytes"
                  | "date" | "time" | "timestamp" | "duration"
                  | "decimal" "(" INT "," INT ")" ;

shape             = "[" INT { "," INT } "]" ;

(* --- Expressions --- *)
expr              = assign_expr ;
assign_expr       = or_expr [ ( "=" | "+=" | "-=" | "*=" | "/=" ) assign_expr ] ;
or_expr           = and_expr { "or" and_expr } ;
and_expr          = compare_expr { "and" compare_expr } ;
compare_expr      = add_expr { ( "==" | "!=" | "<" | ">" | "<=" | ">=" | "in" ) add_expr } ;
add_expr          = mul_expr { ( "+" | "-" ) mul_expr } ;
mul_expr          = power_expr { ( "*" | "/" | "%" ) power_expr } ;
power_expr        = unary_expr [ "**" power_expr ] ;
unary_expr        = [ "not" | "-" ] postfix_expr ;
postfix_expr      = primary { "." IDENT | "(" expr_list ")" | "[" expr "]" | "?" } ;
primary           = INT | FLOAT | STRING | "true" | "false" | "none"
                  | IDENT
                  | "(" expr ")"
                  | list_literal
                  | map_literal
                  | case_expr
                  | with_expr
                  | closure_expr
                  | column_ref ;

(* --- Data Expressions (stdlib functions, not keywords — except `with`) --- *)
with_expr         = "with" "{" { IDENT "=" expr } "}" ;
aggregate_expr    = "aggregate" "(" "by" ":" expr ")" "{" { IDENT "=" expr } "}" ;
select_expr       = "select" "(" field_assign_list ")" ;
join_expr         = "join" "(" IDENT "," "on" ":" expr [ "," "type" ":" join_type ] ")" ;
join_type         = "inner" | "left" | "right" | "full" | "cross" ;

(* --- Case Expression --- *)
case_expr         = "case" "{" { expr "=>" expr } "}" ;

(* --- Closures --- *)
closure_expr      = "(" param_list ")" "=>" expr
                  | "(" param_list ")" "->" type_expr "{" { statement } "}" ;

(* --- Column Reference (disambiguate from local variable) --- *)
column_ref        = "@" IDENT ;

(* --- Literals --- *)
list_literal      = "[" [ expr_list ] "]" ;
map_literal       = "{" [ map_entry { "," map_entry } ] "}" ;
map_entry         = expr ":" expr ;

(* --- Common --- *)
param_list        = [ param { "," param } ] ;
param             = IDENT ":" type_expr ;
expr_list         = [ expr { "," expr } ] ;
field_assign_list = [ field_assign { "," field_assign } ] ;
field_assign      = IDENT [ ":" expr ] ;

(* --- Statements --- *)
statement         = let_binding
                  | expr
                  | return_stmt
                  | if_stmt
                  | match_stmt
                  | for_stmt
                  | while_stmt
                  | parallel_block ;

return_stmt       = "return" [ expr ] ;
if_stmt           = "if" expr "{" { statement } "}" { "else" "if" expr "{" { statement } "}" }
                    [ "else" "{" { statement } "}" ] ;
match_stmt        = "match" expr "{" { expr "=>" expr | "{" { statement } "}" } "}" ;
for_stmt          = "for" IDENT "in" expr "{" { statement } "}" ;
while_stmt        = "while" expr "{" { statement } "}" ;
parallel_block    = "parallel" "{" { "task" IDENT "=" expr } "}" ;

(* --- Tokens --- *)
IDENT             = letter { letter | digit | "_" } ;
INT               = digit { digit } [ "_" digit { digit } ] ;
FLOAT             = INT "." INT [ ( "e" | "E" ) [ "+" | "-" ] INT ] ;
STRING            = '"' { char | "{" expr "}" } '"' ;
DURATION          = INT ( "ms" | "s" | "m" | "h" | "d" ) ;
```

---

## 17. Connector Protocol

### 17.1 Connector Interface

All data connectors implement a standard trait:

```tl
trait Connector {
    fn connect(config: ConnectorConfig) -> result<self, ConnectionError>
    fn disconnect(self) -> result<(), ConnectionError>
    fn health_check(self) -> result<bool, ConnectionError>
}

trait ReadConnector: Connector {
    fn scan(self, table: string, columns: list<string>?, predicate: Expr?)
        -> result<stream<RecordBatch>, ReadError>
    fn schema(self, table: string) -> result<schema, ReadError>
    fn row_count(self, table: string) -> result<int64, ReadError>
    fn partitions(self, table: string) -> result<list<Partition>, ReadError>
}

trait WriteConnector: Connector {
    fn write(self, table: string, data: table, mode: WriteMode)
        -> result<WriteStats, WriteError>
    fn create_table(self, name: string, schema: schema)
        -> result<(), WriteError>
}

trait StreamConnector: Connector {
    fn subscribe(self, topic: string, offset: Offset)
        -> result<stream<Event>, StreamError>
    fn publish(self, topic: string, events: stream<Event>)
        -> result<PublishStats, StreamError>
}

enum WriteMode {
    Append
    Overwrite
    Upsert(keys: list<string>)
    Merge(on: list<string>, when_matched: MergeAction, when_not_matched: MergeAction)
}
```

### 17.2 Writing a Custom Connector

```tl
// my_api_connector.tl — Custom REST API connector
use std.connect.{Connector, ReadConnector, ConnectorConfig}
use std.core.http

pub connector MyAPI: Connector + ReadConnector {
    url: string
    api_key: string
    rate_limit: int = 100   // requests per second

    fn connect(config: ConnectorConfig) -> result<self, ConnectionError> {
        let url = config.get("url")?
        let api_key = config.get_secret("api_key")?
        Ok(MyAPI { url, api_key, rate_limit: config.get_or("rate_limit", 100) })
    }

    fn scan(self, endpoint: string, columns: list<string>?, predicate: Expr?)
        -> result<stream<RecordBatch>, ReadError> {
        let response = http.get("{self.url}/{endpoint}")
            .header("Authorization", "Bearer {self.api_key}")
            .rate_limit(self.rate_limit)
            .paginate(strategy: "cursor", field: "next_cursor")
            .await?
        response |> parse_json() |> to_record_batches()
    }

    fn schema(self, endpoint: string) -> result<schema, ReadError> {
        let sample = self.scan(endpoint, none, none)? |> take(100) |> collect()
        infer_schema(sample)
    }
}
```

---

## 18. Testing Framework

### 18.1 Data Testing Philosophy

Traditional `assert_eq` is insufficient for data work. TL provides **data-aware assertions**:

```tl
use std.test.{describe, it, expect}
use std.test.data.{expect_table, expect_schema, expect_quality}

describe "clean_users transform" {
    it "removes rows with null email" {
        let input = mock_table<User>([
            { id: 1, name: "Alice", email: "alice@test.com", age: 30 },
            { id: 2, name: "Bob",   email: none,             age: 25 },
            { id: 3, name: "Carol", email: "carol@test.com", age: 35 },
        ])

        let result = clean_users(input)

        expect_table(result)
            .row_count(2)
            .contains_column("email")
            .no_nulls(in: "email")
            .column("name").values(["Alice", "Carol"])
    }

    it "clamps age to valid range" {
        let input = mock_table<User>([
            { id: 1, name: "Test", email: "t@t.com", age: -5 },
            { id: 2, name: "Test", email: "t@t.com", age: 200 },
        ])

        let result = clean_users(input)

        expect_table(result)
            .column("age").all_between(0, 150)
    }

    it "preserves schema after cleaning" {
        let input = mock_table<User>(random_rows: 1000)
        let result = clean_users(input)

        expect_schema(result).matches(User)
    }
}
```

### 18.2 Pipeline Testing

```tl
describe "daily_etl pipeline" {
    it "processes end-to-end with mock sources" {
        let mock_db = mock_connector<Postgres>(
            tables: { "users": sample_users(1000) }
        )

        let result = run_pipeline(daily_etl, sources: { users: mock_db })

        expect(result.status).to_be(PipelineStatus::Success)
        expect(result.steps["cleaned"].row_count).to_be_greater_than(0)
        expect(result.duration).to_be_less_than(30s)
    }

    it "handles source connection failure gracefully" {
        let failing_db = mock_connector<Postgres>(fail_after: 0)

        let result = run_pipeline(daily_etl, sources: { users: failing_db })

        expect(result.status).to_be(PipelineStatus::Failed)
        expect(result.error).to_be_instance_of(DataError::ConnectionFailed)
        expect(result.retries).to_be(3)
    }
}
```

### 18.3 Snapshot Testing for Transforms

```tl
it "feature engineering output matches snapshot" {
    let input = load_fixture("test_users.parquet")
    let result = engineer_features(input)

    // First run: saves snapshot. Subsequent runs: compares against it.
    expect_table(result).matches_snapshot("feature_engineering_v1")
}
```

### 18.4 Property-Based Testing

```tl
use std.test.property.{for_all, generators as gen}

describe "filter properties" {
    for_all(gen.table<User>(rows: 1..10000)) { users =>
        it "filter never increases row count" {
            let filtered = users |> filter(age > 25)
            expect(filtered.row_count()).to_be_at_most(users.row_count())
        }

        it "filter preserves schema" {
            let filtered = users |> filter(age > 25)
            expect_schema(filtered).matches_schema(users)
        }

        it "filter is idempotent" {
            let once = users |> filter(age > 25)
            let twice = once |> filter(age > 25)
            expect_table(twice).equals(once)
        }
    }
}
```

---

## 19. Deployment & Operations

### 19.1 Deployment Targets

```bash
# Docker container (most common)
tl deploy pipeline.tl --target docker
# Generates: Dockerfile + optimized binary

# Kubernetes CronJob
tl deploy pipeline.tl --target k8s --schedule "0 6 * * *"
# Generates: k8s manifests (Deployment, CronJob, ConfigMap, Secret refs)

# Serverless function (for lightweight transforms)
tl deploy transform.tl --target lambda
# Generates: AWS Lambda deployment package

# Standalone binary
tl build --release pipeline.tl
# Generates: Single static binary, ~10MB, zero dependencies
```

### 19.2 Generated Dockerfile

```dockerfile
# Auto-generated by: tl deploy --target docker
FROM thinkinglang/runtime:0.1 AS builder
COPY . /app
WORKDIR /app
RUN tl build --release --target native

FROM gcr.io/distroless/cc-debian12
COPY --from=builder /app/target/release/pipeline /pipeline
COPY --from=builder /app/tl.toml /tl.toml
ENV TL_ENV=production
ENTRYPOINT ["/pipeline"]
```

### 19.3 Observability

```tl
// Built-in metrics emission (OpenTelemetry compatible)
pipeline monitored_etl {
    observability {
        metrics: prometheus(port: 9090)    // Exposes /metrics endpoint
        tracing: otlp("http://jaeger:4317") // Distributed tracing
        logging: structured(format: json, level: env("LOG_LEVEL", "info"))
    }

    steps {
        // Each step automatically emits:
        //   tl_step_duration_seconds{step="extract", pipeline="monitored_etl"}
        //   tl_step_rows_processed{step="extract", pipeline="monitored_etl"}
        //   tl_step_errors_total{step="extract", pipeline="monitored_etl"}
        raw = extract users
        cleaned = transform clean_users(raw)
        load cleaned -> postgres("output_db").table("users")
    }
}
```

### 19.4 Data Lineage

```tl
// Lineage is tracked automatically by the compiler
// Query it at runtime or via CLI:

// CLI
// tl lineage daily_etl.tl --format dot > lineage.dot
// tl lineage daily_etl.tl --format json > lineage.json

// Programmatic
let graph = lineage(daily_etl)
// graph.sources => ["postgres://analytics_db/users", "kafka://user_events"]
// graph.sinks   => ["postgres://analytics_db/churn_predictions", "slack://alerts"]
// graph.transforms => [clean_users, predict_churn]
// graph.columns["churn_probability"].derived_from => ["monthly_spend", "tenure_days", "region"]
```

---

## 20. LLM & GenAI Integration

### 20.1 First-Class LLM Support

TL treats LLM interactions as typed, auditable operations — not opaque string concatenation:

```tl
use ai.llm.{prompt, chat, complete}

// Simple completion
let summary = prompt("Summarize this data: {table_preview(users, rows: 5)}")
    .model("claude-sonnet")
    .max_tokens(200)
    .temperature(0.3)
    .await

// Structured output (type-safe LLM responses)
schema Sentiment {
    text: string
    sentiment: string       // "positive" | "negative" | "neutral"
    confidence: float64
    reasoning: string
}

let analyzed = reviews
    |> with {
        analysis = prompt("Analyze sentiment: {text}")
            .model("claude-haiku")
            .output_schema(Sentiment)          // Enforces structured JSON output
            .await
    }
    |> with {
        sentiment = analysis.sentiment
        confidence = analysis.confidence
    }

// Chat with context
let assistant = chat("claude-sonnet")
    .system("You are a data quality analyst.")
    .context(table_preview(users, rows: 20))

let insights = assistant.send("What data quality issues do you see?").await
```

### 20.2 Embedding & Vector Operations

```tl
use ai.embeddings.{embed, vector_store}

// Generate embeddings
let products_with_embeddings = products
    |> with {
        embedding = embed("{name} {description}", model: "sentence-transformer")
    }

// Store in vector database
let store = vector_store("chromadb://localhost:8000/products")
load products_with_embeddings -> store

// Semantic search
let similar = store.query(
    embed("comfortable running shoes"),
    top_k: 10,
    filter: category == "footwear"
)

// RAG (Retrieval-Augmented Generation)
let context = store.query(embed(user_question), top_k: 5)
let answer = prompt("""
    Answer the question based on this context:
    Context: {context}
    Question: {user_question}
""")
.model("claude-sonnet")
.await
```

---

## 21. Interoperability

### 21.1 Python Interop (Escape Hatch)

While TL aims to replace Python for data/AI work, pragmatism demands interop:

```tl
// Call Python libraries from TL
use interop.python

let result = python {
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 6))
    plt.plot(data["x"], data["y"])
    plt.savefig("output.png")
    return "output.png"
}

// Use TL from Python (as a library)
// In Python:
// import thinkinglang as tl
// result = tl.run("my_transform.tl", input=pandas_df)
// df = result.to_pandas()
```

### 21.2 SQL Interop

```tl
// Inline SQL for those who prefer it (compiles to the same TL-IR)
let high_spenders = sql {
    SELECT user_id, name, monthly_spend
    FROM users
    WHERE monthly_spend > 1000
    AND region = 'US'
    ORDER BY monthly_spend DESC
    LIMIT 100
}

// SQL is type-checked against known schemas at compile time
// The above compiles to the same plan as:
let high_spenders = users
    |> filter(monthly_spend > 1000 and region == "US")
    |> sort(monthly_spend, desc)
    |> limit(100)
    |> select(user_id, name, monthly_spend)
```

### 21.3 Export Formats

```tl
// Export TL tables to common formats
let data = users |> filter(active == true)

// File exports
data |> write_parquet("output/users.parquet", compression: "zstd")
data |> write_csv("output/users.csv")
data |> write_json("output/users.json", orient: "records")
data |> write_arrow("output/users.arrow")

// DataFrame protocol (for interop with Arrow-based tools)
let arrow_batch = data |> to_arrow()        // Apache Arrow RecordBatch
let ipc_bytes = data |> to_arrow_ipc()      // Arrow IPC format
```

---

## 22. Success Metrics & KPIs

### 22.1 Technical Metrics

| Metric                                   | Target (v1.0)        | Measured By                     |
| ---------------------------------------- | -------------------- | ------------------------------- |
| CSV parse throughput (1B rows)           | > 300M rows/sec      | Benchmark suite                 |
| Filter + aggregate (1B rows)             | < 2s                 | Benchmark suite                 |
| Cold start time                          | < 100ms              | Benchmark suite                 |
| Compilation speed                        | > 50K lines/sec      | Compiler benchmark              |
| Memory efficiency (vs Python)            | 3-5x less            | Profiler                        |
| Query plan optimization                  | Within 1.5x of hand-tuned SQL | TPC-H benchmarks       |
| Connector latency overhead               | < 5% vs raw driver   | Connector benchmarks            |

### 22.2 Adoption Metrics (First 12 Months Post-Launch)

| Metric                          | Target          |
| ------------------------------- | --------------- |
| GitHub stars                    | 5,000+          |
| Monthly active REPL users       | 1,000+          |
| Published packages              | 50+             |
| Production deployments (known)  | 20+             |
| Discord community members       | 2,000+          |
| Conference talks / blog posts   | 10+             |
| Contributors (non-core team)    | 30+             |

### 22.3 Developer Experience Metrics

| Metric                                | Target           |
| ------------------------------------- | ---------------- |
| Time from install to "Hello World"    | < 5 minutes      |
| Time from zero to working ETL pipeline| < 30 minutes     |
| Error message helpfulness (survey)    | > 4.5/5          |
| LSP response time (autocomplete)      | < 100ms          |
| Documentation coverage                | 100% of public API|

---

## 23. Risk Analysis

### 23.1 Technical Risks

| Risk                                        | Probability | Impact   | Mitigation                                              |
| ------------------------------------------- | ----------- | -------- | ------------------------------------------------------- |
| LLVM backend complexity causes delays       | High        | High     | Start with Cranelift JIT; LLVM as phase 2               |
| Distributed execution is extremely hard     | High        | Medium   | Defer to Phase 6; single-node first                     |
| GPU/CUDA support fragile across hardware    | Medium      | Medium   | ONNX Runtime as abstraction layer; test on CI matrix    |
| Query optimizer correctness bugs            | Medium      | High     | Extensive property-based testing; fuzzing               |
| Apache Arrow version churn breaks things    | Medium      | Low      | Pin versions; abstract behind internal API              |
| Memory manager bugs (use-after-free, leaks) | Medium      | High     | Rust's borrow checker + Miri + valgrind in CI           |

### 23.2 Adoption Risks

| Risk                                        | Probability | Impact   | Mitigation                                              |
| ------------------------------------------- | ----------- | -------- | ------------------------------------------------------- |
| "Why not just use Python?" resistance       | Very High   | High     | Lead with benchmarks; show 10-50x speedups              |
| Lack of ecosystem (not enough connectors)   | High        | High     | Prioritize top 10 connectors; easy connector authoring  |
| Steep learning curve for new syntax         | Medium      | Medium   | Python-like syntax; gradual typing; great tutorials     |
| Mojo captures the mindshare first           | Medium      | Medium   | Differentiate on data engineering, not just AI          |
| Companies won't adopt non-mainstream lang   | High        | High     | Python interop escape hatch; gradual migration path     |
| Insufficient funding to reach v1.0          | Medium      | Critical | Open source early; seek grants/sponsorships             |

### 23.3 Mitigation Strategy Summary

```
PHASE 0-1: Prove the core thesis — data operations in TL are genuinely faster and safer
PHASE 2-3: Show AI integration that Python can't match (compile-time tensor checks)
PHASE 4-5: Build the ecosystem moat (connectors, packages, IDE support)
PHASE 6:   Enterprise features that justify commercial adoption
```

---

## 24. Appendices

### Appendix A: Comparison Code — Python vs TL

**Python (Today's Stack)**
```python
# etl_pipeline.py — 45 lines, 4 libraries, no type safety, fragile
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
import schedule, time, requests

engine = create_engine("postgresql://user:pass@localhost/analytics")
df = pd.read_sql("SELECT * FROM users", engine)
df = df[df["is_active"] == True]
df = df[df["signup_date"] > pd.Timestamp.now() - pd.Timedelta(days=90)]
df["name"] = df["name"].fillna("unknown")
df["monthly_spend"] = df["monthly_spend"].fillna(0.0)
df = df.drop_duplicates(subset=["email"], keep="last")
df["tenure_days"] = (pd.Timestamp.now() - df["signup_date"]).dt.days
df["spend_tier"] = df["monthly_spend"].apply(
    lambda x: "premium" if x > 1000 else ("standard" if x > 100 else "free")
)
X = df[["tenure_days", "monthly_spend"]]  # Can't easily include 'region' without encoding
y = df["is_active"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = GradientBoostingClassifier(max_depth=6, learning_rate=0.1, n_estimators=500)
model.fit(X_train, y_train)
df["churn_probability"] = model.predict_proba(X)[:, 1]
df["risk_tier"] = df["churn_probability"].apply(
    lambda x: "critical" if x > 0.8 else ("high" if x > 0.5 else ("medium" if x > 0.2 else "low"))
)
result = df[["id", "churn_probability", "risk_tier"]]
result.to_sql("churn_predictions", engine, if_exists="replace")
# No scheduling, no error handling, no type safety, no lineage, no alerts
```

**ThinkingLanguage (Future)**
```tl
// churn_pipeline.tl — Same logic: typed, scheduled, monitored, parallel
use connectors.{postgres, s3}
use ai.xgboost

schema User { id: int64, name: string, email: string, signup_date: date,
              region: string, monthly_spend: float64, is_active: bool }

source users = postgres(env("DB_URL")).table("users") -> User

transform active_users(src: table<User>) -> table<User> {
    src
    |> filter(is_active == true and signup_date > today() - 90d)
    |> clean(nulls: { name: "unknown", monthly_spend: 0.0 },
             duplicates: dedupe(by: email, keep: "latest"))
    |> with { tenure_days = today() - signup_date
              spend_tier = case { monthly_spend > 1000 => "premium"
                                  monthly_spend > 100  => "standard"
                                  _                    => "free" } }
}

model churn_model = train xgboost {
    data: active_users(users), target: "is_active",
    features: [tenure_days, monthly_spend, region],
    split: 80/20, gpu: auto,
    hyperparams: { max_depth: 6, learning_rate: 0.1, n_estimators: 500 }
}

pipeline daily_churn {
    schedule: cron("0 6 * * *"), timeout: 30m, retries: 3
    steps {
        predicted = active_users(users) |> predict(model: churn_model)
            |> with { risk_tier = case { churn_probability > 0.8 => "critical"
                                           churn_probability > 0.5 => "high"
                                           churn_probability > 0.2 => "medium"
                                           _                       => "low" } }
        load predicted -> postgres(env("DB_URL")).table("churn_predictions")
    }
    on_failure { alert slack("#data-alerts", "Churn pipeline failed: {error}") }
}
```

### Appendix B: Glossary

| Term              | Definition                                                                 |
| ----------------- | -------------------------------------------------------------------------- |
| **TL**            | ThinkingLanguage — the language described in this document                 |
| **TL-IR**         | ThinkingLanguage Intermediate Representation — the compiler's query plan   |
| **Schema**        | A named structural type defining the shape of data                        |
| **Source**        | A declared external data origin (database, file, API, stream)              |
| **Sink**          | A declared external data destination                                       |
| **Transform**     | A pure function from one table shape to another                            |
| **Pipeline**      | An orchestrated sequence of extract → transform → load steps              |
| **Connector**     | A plugin that knows how to read/write a specific external system          |
| **Pipe operator** | `\|>` — chains data through a sequence of transformations                  |
| **Flow operator** | `->` — directs data into a sink (load direction)                           |
| **Record Batch**  | A chunk of columnar data (Apache Arrow concept)                            |
| **Spill**         | Overflow from RAM to disk when data exceeds memory                        |
| **Watermark**     | A marker in stream processing indicating event-time progress              |
| **Lineage**       | The tracked path of data from source through transformations to sink      |

### Appendix C: Inspirations & Acknowledgments

TL draws inspiration from the best ideas in existing languages and tools:

| Feature                | Inspiration Source          |
| ---------------------- | --------------------------- |
| Pipe operator `\|>`     | Elixir, F#, Hack            |
| Ownership / borrowing  | Rust (simplified)           |
| Pattern matching       | Rust, Scala, Haskell        |
| Columnar tables        | Apache Arrow, Polars, DuckDB|
| Query optimization     | Apache Spark Catalyst, DataFusion |
| Pipeline orchestration | Dagster, Prefect            |
| Stream processing      | Apache Flink, Kafka Streams |
| Error messages         | Elm, Rust                   |
| AI integration         | Mojo (performance), PyTorch (ergonomics) |
| REPL / notebooks       | Python, Julia               |
| Package management     | Cargo (Rust)                |
| Syntax philosophy      | Python (readable), Rust (safe) |

---

**End of Specification**

*ThinkingLanguage is a project of ThinkingDBx Private Limited.*
*This document is a living specification and will evolve as the language is implemented.*

*"Data deserves a language of its own."*
