# Connectors

TL provides connectors for databases, object stores, and APIs. Some connectors are always available, while others require feature flags to be enabled at compile time.

## PostgreSQL

Always available. Connect to a PostgreSQL database and retrieve a table:

```tl
let t = postgres("postgresql://user:pass@host/db", "table_name")
t |> filter(active == true) |> show()
```

Returns a table value that integrates with all pipe operations.

## MySQL

**Feature flag:** `mysql`

```tl
let t = read_mysql("mysql://user:pass@host/db", "SELECT * FROM users")
t |> select(id, name, email) |> show()
```

## SQLite

**Feature flag:** `sqlite`

Uses bundled rusqlite -- no external SQLite installation is needed.

```tl
let t = read_sqlite("path/to/db.sqlite", "SELECT * FROM users")
t |> filter(age > 25) |> show()

write_sqlite(table, "path/to/db.sqlite", "table_name")
```

- Type inference is performed from first row values
- Writes use transaction batching for performance

## Redis

**Feature flag:** `redis`

Key-value operations on a Redis server:

```tl
let conn = redis_connect("redis://localhost:6379")
redis_set(conn, "key", "value")
let val = redis_get(conn, "key")
redis_del(conn, "key")
```

## S3

**Feature flag:** `s3`

Register an S3 bucket, then use standard file-reading functions with `s3://` paths:

```tl
register_s3("bucket", "region", "key_id", "secret")

let data = read_csv("s3://bucket/path.csv")
let events = read_parquet("s3://bucket/events.parquet")
```

## GraphQL

Always available. Execute a GraphQL query against an endpoint:

```tl
let result = graphql_query("https://api.example.com/graphql", "{ users { id name } }")
```

## Connection Patterns

All connectors return table values (or, in the case of Redis, connection handles) that integrate with TL's pipe operations. A typical pattern is to read from a connector, transform with pipes, and write to another connector:

```tl
let source = postgres("postgresql://user:pass@host/db", "raw_events")
source
    |> filter(event_type == "purchase")
    |> select(user_id, amount, timestamp)
    |> write_parquet("purchases.parquet")
```
