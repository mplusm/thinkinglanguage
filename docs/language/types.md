# Type System

ThinkingLanguage uses a gradual type system -- type annotations are optional, and the compiler infers types when they are omitted.

## Primitive Types

| Type     | Description                | Example          |
|----------|----------------------------|------------------|
| `int`    | 64-bit signed integer      | `42`             |
| `float`  | 64-bit floating point      | `3.14`           |
| `string` | UTF-8 string               | `"hello"`        |
| `bool`   | Boolean                    | `true`, `false`  |
| `none`   | Absence of value           | `none`           |
| `decimal`| Exact decimal arithmetic   | `19.99d`         |

### Decimal

The `decimal` type provides exact arithmetic for financial and precision-sensitive calculations. Append `d` to a numeric literal:

```tl
let price = 19.99d
let tax = 1.50d
let total = price + tax  // 21.49d — exact, no floating-point drift
```

Underscores are allowed in decimal literals for readability: `1_000_000.00d`.

## Collection Types

- `list` -- ordered, indexed collection: `[1, 2, 3]`
- `map` -- key-value pairs: `{"name": "Alice", "age": 30}`
- `set` -- unique unordered elements: `set([1, 2, 3])`

## Data-Native Types

These types are purpose-built for data engineering and AI workflows:

- `table` -- tabular data (backed by Apache Arrow/DataFusion)
- `stream` -- streaming data source with windowing
- `tensor` -- multi-dimensional array for ML (backed by ndarray)
- `pipeline` -- ETL pipeline construct

## Type Annotations

Annotations are always optional. When provided, the compiler verifies them.

### Variables

```tl
let x: int = 42
let name: string = "Alice"
let scores: list<float> = [9.5, 8.3, 7.1]
```

### Functions

```tl
fn add(a: int, b: int) -> int {
    return a + b
}

fn greet(name: string) -> string {
    return "Hello, {name}!"
}
```

### The `any` Type

The `any` type is compatible with every other type. It is the implicit type when no annotation is given:

```tl
fn identity(x: any) -> any {
    return x
}
```

## Type Inference

When annotations are omitted, the compiler infers types from usage:

```tl
let x = 42          // inferred as int
let y = x + 1.0     // inferred as float (int promoted)
let items = [1, 2]  // inferred as list<int>
```

Phase 22 introduced Hindley-Milner unification for more advanced inference across function boundaries.

## Result and Option

### Result<T, E>

Represents success or failure:

```tl
fn divide(a: float, b: float) -> Result<float, string> {
    if b == 0.0 {
        return Err("division by zero")
    }
    return Ok(a / b)
}
```

### Option<T>

Represents presence or absence of a value:

```tl
fn find_user(id: int) -> Option<string> {
    if id == 1 {
        return Some("Alice")
    }
    return None
}
```

### The `?` Operator

Propagate errors concisely with `?`:

```tl
fn process() -> Result<string, string> {
    let value = divide(10.0, 0.0)?  // returns Err early if division fails
    return Ok("result: {value}")
}
```

## Generic Types

Use type parameters for reusable data structures:

```tl
list<int>
map<string, int>
set<float>
```

Define your own generic types:

```tl
struct Pair<A, B> {
    first: A,
    second: B,
}
```

## Type Aliases

Create shorthand names for complex types:

```tl
type IntList = list<int>
type StringMap = map<string, string>
type UserResult = Result<User, string>
```

## Specialized VM Types

These types exist in the runtime for specific purposes:

- `Secret` -- values that display as `***` and are never logged in plaintext
- `Ref` -- read-only reference created with `&expr`
- `GpuTensor` -- GPU-resident tensor (f32 storage, requires `gpu` feature)

## Static Type Checking

Run the type checker without executing code:

```sh
tl check file.tl
```

Use strict mode to require type annotations on all function parameters:

```sh
tl check file.tl --strict
```

The type checker catches type mismatches, use-after-move errors, and unreachable code.
