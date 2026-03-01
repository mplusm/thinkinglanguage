# Error Handling

TL provides multiple mechanisms for handling errors: try/catch for exception-style handling, Result and Option types for functional error management, and the `?` operator for concise error propagation.

## try / catch / throw

Use `try`/`catch` for exception-style error handling:

```tl
try {
    let data = read_file("config.json")
    let config = json_parse(data)
    print("Loaded config: {config}")
} catch e {
    print("Error: {e}")
}
```

### Throwing Errors

Use `throw` to raise an error:

```tl
fn divide(a: float, b: float) -> float {
    if b == 0.0 {
        throw "division by zero"
    }
    a / b
}
```

You can throw any value, including structured error enums:

```tl
throw DataError::ValidationError("age must be positive")
```

## Result Type

The Result type represents either success or failure:

```tl
let ok_val = Ok(42)
let err_val = Err("something failed")
```

### Inspecting Results

```tl
let result = Ok(42)

if is_ok(result) {
    let value = unwrap(result)
    print("Got: {value}")
}

if is_err(result) {
    print("Failed!")
}
```

### Unwrapping

`unwrap()` extracts the value from `Ok` or panics on `Err`:

```tl
let value = unwrap(Ok(42))     // 42
let bad = unwrap(Err("oops"))  // runtime error!
```

## Option Type

The Option type represents a value that may or may not exist:

```tl
let present = Some(42)
let absent = None
```

Use Option when a value is legitimately optional, as opposed to Result which indicates an operation that can fail.

## The ? Operator

The `?` operator provides concise error propagation. It unwraps `Ok` values or returns early with the `Err`:

```tl
fn load_config(path: string) -> Result {
    let content = read_file(path)?       // returns Err early if read fails
    let parsed = json_parse(content)?    // returns Err early if parse fails
    Ok(parsed)
}
```

This is equivalent to the more verbose:

```tl
fn load_config(path: string) -> Result {
    let content_result = read_file(path)
    if is_err(content_result) {
        return content_result
    }
    let content = unwrap(content_result)

    let parsed_result = json_parse(content)
    if is_err(parsed_result) {
        return parsed_result
    }
    let parsed = unwrap(parsed_result)

    Ok(parsed)
}
```

## Built-in Error Hierarchy

TL provides three built-in error enums for structured error handling across different domains:

### DataError

For data processing and validation failures:

```tl
enum DataError {
    ParseError(message: string),
    SchemaError(message: string),
    ValidationError(message: string),
    NotFound(message: string),
}
```

Example:

```tl
try {
    let data = read_csv("data.csv")
} catch e {
    match e {
        DataError::ParseError(msg) => print("Parse failed: {msg}"),
        DataError::SchemaError(msg) => print("Schema mismatch: {msg}"),
        DataError::NotFound(msg) => print("Not found: {msg}"),
        _ => print("Other error: {e}"),
    }
}
```

### NetworkError

For network and HTTP failures:

```tl
enum NetworkError {
    ConnectionError(message: string),
    TimeoutError(message: string),
    HttpError(message: string),
}
```

### ConnectorError

For data source connector failures:

```tl
enum ConnectorError {
    AuthError(message: string),
    QueryError(message: string),
    ConfigError(message: string),
}
```

## Error Inspection Builtins

TL provides built-in functions for inspecting errors at runtime:

### is_error()

Check if a value is an error:

```tl
let val = some_operation()
if is_error(val) {
    print("Operation failed")
}
```

### error_type()

Get the type name of an error as a string:

```tl
try {
    connect_to_db("invalid://url")
} catch e {
    let etype = error_type(e)
    print("Error type: {etype}")  // e.g., "ConnectorError::ConfigError"
}
```

## Structured Errors from Data Operations

Data operations (CSV, Parquet, SQL, etc.) throw structured error enums rather than plain strings. This allows precise error handling:

```tl
try {
    let table = read_csv("missing.csv")
} catch e {
    // 'e' is a DataError::NotFound, not a plain string
    match e {
        DataError::NotFound(msg) => print("File not found: {msg}"),
        DataError::ParseError(msg) => print("CSV parse error: {msg}"),
        _ => throw e,  // re-throw unexpected errors
    }
}
```

## Best Practices

1. **Use Result for expected failures.** When a function can reasonably fail (file not found, invalid input, network timeout), return a Result:

    ```tl
    fn parse_age(input: string) -> Result {
        let n = try_parse_int(input)
        if is_err(n) {
            return Err("invalid number")
        }
        let age = unwrap(n)
        if age < 0 or age > 150 {
            return Err("age out of range")
        }
        Ok(age)
    }
    ```

2. **Use try/catch for exceptional cases.** Wrap sections of code where errors are unexpected or where you want to handle multiple failure points at once:

    ```tl
    try {
        let config = load_config("app.json")?
        let db = connect(config["db_url"])?
        run_migration(db)?
    } catch e {
        print("Startup failed: {e}")
    }
    ```

3. **Match on error types for precise handling.** Use the built-in error enums to handle different failure modes differently:

    ```tl
    try {
        let data = fetch_and_process(url)
    } catch e {
        match e {
            NetworkError::TimeoutError(msg) => retry(url),
            NetworkError::ConnectionError(msg) => alert("Network down"),
            DataError::ValidationError(msg) => log("Bad data: {msg}"),
            _ => throw e,
        }
    }
    ```

4. **Use `?` to keep functions clean.** Instead of deeply nested try/catch blocks, use `?` for concise propagation and handle errors at the appropriate level.
