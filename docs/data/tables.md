# Tables

Tables are columnar data structures backed by Apache Arrow and Apache DataFusion. They provide a powerful, SQL-like interface for data manipulation through TL's pipe operator.

## Creating Tables

Read data from files into table values:

```tl
let users = read_csv("users.csv")
let events = read_parquet("events.parquet")
```

## Table Pipe Operations

Table operations use a column expression context where bare identifiers resolve to column names rather than local variables.

### filter

Select rows matching a condition:

```tl
users |> filter(age > 30)
users |> filter(dept == "engineering" and age > 25)
```

### select

Choose specific columns:

```tl
users |> select(name, age, dept)
```

### sort

Order rows by a column:

```tl
users |> sort(age, "desc")
users |> sort(name, "asc")
```

### with (derived columns)

Add computed columns:

```tl
users |> with { senior = age > 35, label = "user_{id}" }
```

### aggregate

Group and summarize data:

```tl
users |> aggregate(by: dept, count: count(), avg_age: avg(age), total: sum(salary))
```

Available aggregate functions: `count()`, `sum(col)`, `avg(col)`, `min(col)`, `max(col)`.

### join

Combine two tables:

```tl
users |> join(orders, on: id == user_id)
users |> join(orders, on: id == user_id, kind: "left")
```

### head / limit

Take the first N rows:

```tl
users |> head(10)
```

### show

Print the table to stdout:

```tl
users |> show()
```

### describe

Show schema and column statistics:

```tl
users |> describe()
```

### collect

Materialize lazy evaluation into a concrete table:

```tl
let result = users |> filter(age > 30) |> collect()
```

## Writing Tables

Write table data to files:

```tl
users |> write_csv("output.csv")
users |> write_parquet("output.parquet")
```

## Schema Definitions

Define table schemas for validation and documentation:

```tl
schema User { id: int64, name: string, age: int64 }
```

## Lazy Evaluation

Table operations are lazy by default. No computation occurs until a terminal operation triggers execution:

- `show()` -- prints and triggers evaluation
- `collect()` -- materializes the result
- `write_csv()` / `write_parquet()` -- writes output and triggers evaluation

This allows DataFusion to optimize the entire query plan before execution.

## Query Optimization

DataFusion automatically applies optimizations including:

- **Predicate pushdown** -- filters are pushed as close to the data source as possible
- **Column pruning** -- only columns actually used are read from disk

## Column vs Local Variable Disambiguation

When a local variable name collides with a column name inside a pipe expression, use `@column_name` to explicitly reference the column:

```tl
let age = 30
users |> filter(@age > age)
```

Here `@age` refers to the table column, while `age` refers to the local variable (see Section 2.6 of the spec).
