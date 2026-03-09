# Connectors

TL provides connectors for databases, data warehouses, object stores, and APIs. Some connectors are always available, while others require feature flags to be enabled at compile time.

## Building with Connectors

Enable connectors via Cargo feature flags:

```sh
# Individual connectors
cargo build --release --features "sqlite,duckdb,mssql"

# All database connectors + SFTP
cargo build --release --features "sqlite,duckdb,mysql,mssql,clickhouse,snowflake,bigquery,databricks,mongodb,redis,sftp"
```

## Connection Configuration

All connectors support two ways to provide connection details:

### 1. Direct Connection String

Pass the connection string directly:

```tl
let t = postgres("postgresql://user:pass@host:5432/mydb", "users")
```

### 2. Named Connections via `tl_config.json`

Create a `tl_config.json` file (or set `TL_CONFIG_PATH` to its location):

```json
{
  "connections": {
    "prod_pg": "postgresql://user:pass@prod-host:5432/analytics",
    "warehouse": "account=abc123 user=ETL password=secret database=ANALYTICS warehouse=COMPUTE_WH",
    "local_duck": "/path/to/analytics.duckdb"
  }
}
```

Then reference connections by name:

```tl
let t = postgres("prod_pg", "events")
let s = read_snowflake("warehouse", "SELECT * FROM sales")
let d = read_duckdb("local_duck", "SELECT * FROM metrics")
```

---

## Always Available

### PostgreSQL

Connect to PostgreSQL and read a table or run a custom query:

```tl
// Read an entire table
let users = postgres("postgresql://user:pass@host/db", "users")
users |> filter(active == true) |> show()

// Run a custom SQL query
let result = postgres_query("postgresql://user:pass@host/db", "SELECT * FROM orders WHERE amount > 100")
result |> aggregate(total: sum(amount)) |> show()
```

**Aliases:** `postgres()`, `read_postgres()`

PostgreSQL uses server-side cursors (`DECLARE CURSOR` + `FETCH 50000`) for memory-efficient streaming of large result sets.

### Redshift

Thin wrapper over the PostgreSQL connector with automatic SSL enforcement:

```tl
let data = redshift(
    "postgresql://user:pass@cluster.region.redshift.amazonaws.com:5439/analytics",
    "SELECT * FROM events WHERE event_date > '2024-01-01'"
)
data |> aggregate(by: event_type, count: count()) |> show()
```

**Aliases:** `redshift()`, `read_redshift()`

If `sslmode=require` is not present in the connection string, it is added automatically.

### GraphQL

Execute a GraphQL query against an endpoint:

```tl
let result = graphql_query("https://api.example.com/graphql", "{ users { id name } }")
```

---

## Feature-Gated Connectors

### MySQL

**Feature flag:** `mysql`

```tl
let t = read_mysql("mysql://user:pass@host:3306/db", "SELECT * FROM users")
t |> select(id, name, email) |> show()
```

Uses chunked batching (50K rows per batch) for efficient Arrow conversion.

### SQLite

**Feature flag:** `sqlite`

Uses bundled rusqlite -- no external SQLite installation needed.

```tl
// Read with SQL query
let t = read_sqlite("path/to/db.sqlite", "SELECT * FROM users")
t |> filter(age > 25) |> show()

// Write a table to SQLite
write_sqlite(table, "path/to/db.sqlite", "output_table")
```

- Type inference from first row values
- Writes use transaction batching for performance

### DuckDB

**Feature flag:** `duckdb`

Arrow-native connector with zero-copy reads via IPC bridge:

```tl
// Read from DuckDB (file or :memory:)
let t = read_duckdb("/path/to/analytics.duckdb", "SELECT * FROM sales")
t |> filter(amount > 100) |> show()

// In-memory DuckDB for ad-hoc analytics
let t = read_duckdb(":memory:", "SELECT range AS id, random() AS val FROM range(1000)")
t |> aggregate(avg_val: avg(val)) |> show()

// Write a table to DuckDB
write_duckdb(table, "/path/to/output.duckdb", "results")
```

**Aliases:** `duckdb()`, `read_duckdb()`

DuckDB uses Arrow natively. TL bridges DuckDB's Arrow v54 to DataFusion's Arrow v53 via IPC serialization for type-safe interop.

### MSSQL / SQL Server

**Feature flag:** `mssql`

Connects via the tiberius async client:

```tl
// ADO-style connection string
let t = read_mssql(
    "Server=tcp:localhost,1433;User Id=sa;Password=YourPass;Database=mydb",
    "SELECT * FROM orders"
)
t |> filter(status == 'shipped') |> show()

// Key=value format
let t = read_mssql(
    "host=sql-server.example.com port=1433 user=sa password=YourPass database=mydb",
    "SELECT TOP 1000 * FROM large_table"
)
```

**Aliases:** `mssql()`, `read_mssql()`

Supports both ADO-style (`Server=...;Database=...`) and key=value connection strings. Uses batched streaming (50K rows per batch).

### Redis

**Feature flag:** `redis`

Key-value operations on a Redis server:

```tl
let conn = redis_connect("redis://localhost:6379")
redis_set(conn, "key", "value")
let val = redis_get(conn, "key")
redis_del(conn, "key")
```

### S3

**Feature flag:** `s3`

Register an S3 bucket, then use standard file-reading functions with `s3://` paths:

```tl
register_s3("bucket", "region", "key_id", "secret")

let data = read_csv("s3://bucket/path.csv")
let events = read_parquet("s3://bucket/events.parquet")
```

---

## Cloud Data Warehouses

### Snowflake

**Feature flag:** `snowflake`

Uses the Snowflake SQL REST API (v2/statements):

```tl
// JSON config
let t = read_snowflake(
    """{"account":"abc123","user":"ETL_USER","password":"secret","database":"ANALYTICS","warehouse":"COMPUTE_WH"}""",
    "SELECT * FROM sales WHERE region = 'US'"
)
t |> aggregate(by: product, total: sum(revenue)) |> sort(total, "desc") |> show()

// Key=value config
let t = read_snowflake(
    "account=abc123 user=ETL_USER password=secret database=ANALYTICS warehouse=COMPUTE_WH schema=PUBLIC",
    "SELECT * FROM customers LIMIT 1000"
)
```

**Aliases:** `snowflake()`, `read_snowflake()`

**Config fields:** `account` (required), `user`, `password`, `database`, `warehouse`, `schema`

### BigQuery

**Feature flag:** `bigquery`

Uses the BigQuery REST API (jobs.query):

```tl
// JSON config with access token
let t = read_bigquery(
    """{"project":"my-gcp-project","access_token":"ya29.xxx"}""",
    "SELECT * FROM `dataset.table` WHERE date > '2024-01-01'"
)
t |> show()

// Key=value config (uses TL_BIGQUERY_TOKEN or GOOGLE_ACCESS_TOKEN env var)
let t = read_bigquery(
    "project=my-gcp-project",
    "SELECT user_id, COUNT(*) as cnt FROM `events.clicks` GROUP BY user_id"
)
```

**Aliases:** `bigquery()`, `read_bigquery()`

**Config fields:** `project` (required), `access_token` (optional, falls back to `TL_BIGQUERY_TOKEN` or `GOOGLE_ACCESS_TOKEN` env vars)

### Databricks

**Feature flag:** `databricks`

Uses the Databricks SQL Statement Execution API:

```tl
// JSON config
let t = read_databricks(
    """{"host":"adb-123.azuredatabricks.net","token":"dapi-xxx","warehouse_id":"abc123"}""",
    "SELECT * FROM catalog.schema.table"
)
t |> filter(status == 'active') |> show()

// Key=value config
let t = read_databricks(
    "host=adb-123.azuredatabricks.net token=dapi-xxx warehouse_id=abc123",
    "SELECT * FROM sales"
)
```

**Aliases:** `databricks()`, `read_databricks()`

**Config fields:** `host` (required), `token`, `warehouse_id`

### ClickHouse

**Feature flag:** `clickhouse`

Uses the ClickHouse HTTP interface with JSONEachRow format:

```tl
// URL-based connection
let t = read_clickhouse(
    "http://localhost:8123",
    "SELECT * FROM events WHERE event_date = today()"
)
t |> aggregate(by: event_type, count: count()) |> show()

// With authentication
let t = read_clickhouse(
    "http://user:password@clickhouse-host:8123",
    "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 10000"
)
```

**Aliases:** `clickhouse()`, `read_clickhouse()`

The connection string is the ClickHouse HTTP endpoint URL. Authentication can be embedded in the URL or handled by ClickHouse's default user.

---

## NoSQL

### MongoDB

**Feature flag:** `mongodb`

Async MongoDB driver with BSON-to-Arrow conversion:

```tl
// Read with a filter
let t = read_mongodb(
    "mongodb://user:pass@host:27017",
    "mydb",
    "users",
    """{"active": true}"""
)
t |> select(name, email, age) |> show()

// Read all documents (empty filter)
let t = read_mongodb(
    "mongodb://localhost:27017",
    "analytics",
    "events",
    "{}"
)
t |> filter(event_type == "purchase") |> aggregate(total: sum(amount)) |> show()
```

**Aliases:** `mongo()`, `read_mongo()`, `read_mongodb()`

**Arguments:** `(connection_string, database, collection, filter_json)`

Schema is inferred from the first 100 documents. Nested BSON objects are flattened with dot-notation keys (e.g., `address.city`). All top-level fields across sampled documents are included.

---

## Connection Patterns

All connectors return table values that integrate with TL's pipe operations. A typical pattern is to read from a connector, transform with pipes, and write to another:

```tl
// Cross-database ETL
let source = postgres("postgresql://user:pass@host/db", "raw_events")
source
    |> filter(event_type == "purchase")
    |> select(user_id, amount, timestamp)
    |> write_parquet("purchases.parquet")

// Database to data warehouse
let orders = read_mysql("mysql://user:pass@host/db", "SELECT * FROM orders")
orders
    |> filter(status == "completed")
    |> with { quarter = "Q1" }
    |> show()

// DuckDB for local analytics
let sales = read_csv("sales.csv")
write_duckdb(sales, "analytics.duckdb", "sales")
let report = read_duckdb("analytics.duckdb", "SELECT region, SUM(amount) as total FROM sales GROUP BY region")
report |> sort(total, "desc") |> show()
```

## File Transfer

### SFTP / SCP

**Feature flag:** `sftp`

Transfer files to and from remote servers over SSH. Supports password auth, SSH key auth, and ssh-agent.

**Configuration** (JSON or key=value):

```tl
// JSON config
let server = """{"host":"sftp.example.com","user":"deploy","key_path":"~/.ssh/id_rsa"}"""

// Key=value config
let server = "host=sftp.example.com user=deploy key_path=~/.ssh/id_rsa"

// Password auth
let server = "host=sftp.example.com user=etl password=secret port=2222"
```

**Config fields:** `host` (required), `user` (defaults to current system user), `port` (default 22), `password`, `key_path`/`key`/`identity_file`, `passphrase`

**Download and upload files:**

```tl
// Download a file
sftp_download(server, "/remote/data/export.csv", "./local_export.csv")

// Upload a file
sftp_upload(server, "./results.parquet", "/remote/output/results.parquet")
```

**List remote directory:**

```tl
// Returns a table with columns: name, size, type, modified
sftp_list(server, "/remote/data/")
    |> filter(type == "file")
    |> sort(size, "desc")
    |> show()
```

**Read data files directly from SFTP into tables:**

```tl
// Read CSV from SFTP — no manual download needed
let sales = sftp_read_csv(server, "/incoming/daily_sales.csv")
sales
    |> filter(amount > 100)
    |> aggregate(by: region, total: sum(amount))
    |> show()

// Read Parquet from SFTP
let events = sftp_read_parquet(server, "/data/events.parquet")
events |> describe()
```

**ETL from SFTP to database:**

```tl
// Pull vendor data from SFTP, clean it, load into PostgreSQL
let feed = sftp_read_csv("host=vendor-sftp.com user=etl key_path=~/.ssh/vendor_key", "/outbox/feed.csv")
feed
    |> filter(status != "cancelled")
    |> select(order_id, product, quantity, price)
    |> write_parquet("vendor_feed_clean.parquet")
```

**Auth priority:** SSH key (`key_path`) > password > ssh-agent. If no auth is specified, the ssh-agent is tried automatically. `~` in key paths is expanded to `$HOME`.

**Aliases:** `sftp_list()` / `sftp_ls()`

---

## Performance Notes

All connectors use batched Arrow conversion (50K rows per batch) for:

- **Cache efficiency** -- processing data in CPU cache-friendly chunks
- **DataFusion parallelism** -- multiple batches enable parallel query execution
- **Memory control** -- bounded memory usage even for large result sets

Special optimizations:
- **PostgreSQL** uses server-side cursors for streaming without loading all rows into client memory
- **DuckDB** uses Arrow-native IPC transfer (near zero-copy)
- **MySQL/SQLite** use chunked row iteration with flush-on-threshold

## Feature Flag Summary

| Connector | Feature Flag | Protocol | Auth |
|-----------|-------------|----------|------|
| PostgreSQL | *(always on)* | libpq | Connection string |
| Redshift | *(always on)* | libpq + SSL | Connection string |
| GraphQL | *(always on)* | HTTP | None / custom headers |
| MySQL | `mysql` | MySQL protocol | Connection string |
| SQLite | `sqlite` | File | None (file path) |
| DuckDB | `duckdb` | File / in-memory | None (file path) |
| MSSQL | `mssql` | TDS (tiberius) | ADO string / key=value |
| Redis | `redis` | Redis protocol | Connection URL |
| S3 | `s3` | AWS SDK | Access key + secret |
| Snowflake | `snowflake` | REST API | JSON / key=value config |
| BigQuery | `bigquery` | REST API | Access token / env var |
| Databricks | `databricks` | REST API | API token |
| ClickHouse | `clickhouse` | HTTP | URL-embedded / default |
| MongoDB | `mongodb` | MongoDB wire protocol | Connection URI |
| SFTP/SCP | `sftp` | SSH (libssh2) | Key / password / agent |
