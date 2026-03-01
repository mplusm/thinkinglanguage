# Quickstart

This guide walks you through your first ThinkingLanguage program, the REPL, core syntax, and data operations.

## Your First Program

Create a file called `hello.tl`:

```
print("Hello, ThinkingLanguage!")
```

Run it:

```sh
tl run hello.tl
```

Output:

```
Hello, ThinkingLanguage!
```

## The REPL

Start an interactive session with:

```sh
tl shell
```

The REPL supports:

- **Basic expressions** -- type any expression and see its result immediately
- **Multi-line input** -- unclosed braces, brackets, or parentheses automatically continue to the next line
- **History** -- previous inputs are saved to `~/.tl_history` and accessible with the up/down arrow keys
- **Tab completion** -- press Tab to complete built-in functions, keywords, and variable names

```
tl> 2 + 3
5
tl> let name = "world"
tl> print("Hello, {name}!")
Hello, world!
```

Type `exit` or press Ctrl-D to quit.

## Basic Syntax Tour

### Variables

```
let x = 42
let mut count = 0
count = count + 1
```

Use `let` for immutable bindings and `let mut` for mutable ones.

### Types

TL has the following built-in types:

```
// Integers and floats
let n = 42
let pi = 3.14159

// Strings with interpolation
let name = "TL"
let greeting = "Hello, {name}!"

// Booleans and none
let active = true
let nothing = none

// Lists
let nums = [1, 2, 3, 4, 5]

// Maps
let config = {"host": "localhost", "port": 8080}
```

String interpolation is triggered by `{` inside string literals. Use variable names or expressions directly: `"result: {x + 1}"`.

### Functions

```
fn add(a, b) {
    a + b
}

print(add(3, 4))   // 7
```

Closures use `=>` for expression bodies and `->` for block bodies:

```
let double = (x) => x * 2
let process = (x) -> {
    let result = x * 2
    result + 1
}
```

### Control Flow

```
// If/else
if x > 10 {
    print("big")
} else {
    print("small")
}

// For loops
for item in [1, 2, 3] {
    print(item)
}

// While loops
let mut i = 0
while i < 5 {
    print(i)
    i = i + 1
}

// Pattern matching
match value {
    0 => print("zero"),
    1 or 2 => print("small"),
    n if n > 100 => print("large: {n}"),
    _ => print("other"),
}
```

### Pipe Operator

The pipe operator `|>` passes the result of one expression as the first argument to the next function. Note that piping moves the value -- the source variable cannot be used after piping.

```
let result = [3, 1, 4, 1, 5]
    |> sort()
    |> take(3)
    |> collect()
```

## Data Operations

ThinkingLanguage is built for data work. Here is a quick example with CSV data:

```
// Read a CSV file into a table
let sales = read_csv("sales.csv")

// Filter, select columns, sort, and display
sales
    |> filter(amount > 100)
    |> select(date, product, amount)
    |> sort(amount, "desc")
    |> show()
```

Tables support a rich set of pipe operations including `filter`, `select`, `sort`, `with` (add columns), `aggregate`, `join`, `head`, `limit`, `describe`, `write_csv`, and `write_parquet`.

## Running Files

```sh
# Run a script
tl run script.tl

# Run tests defined in a file
tl test tests.tl
```

## Initializing a Project

Create a new TL project with a `tl.toml` manifest:

```sh
tl init myproject
cd myproject
```

This sets up the project structure with a manifest file for dependency management and module configuration.

## Build and Check

```sh
# Type-check without running
tl check src/main.tl

# Format source files
tl fmt src/main.tl

# Lint for common issues
tl lint src/main.tl
```

## Execution Backends

TL supports multiple execution backends:

```sh
# Bytecode VM (default, best balance of speed and compatibility)
tl run script.tl --backend vm

# Tree-walking interpreter (useful for debugging)
tl run script.tl --backend interp

# LLVM AOT compilation (fastest, requires llvm-backend feature)
tl run script.tl --backend llvm
```

You can also compile to a native binary with the LLVM backend:

```sh
tl compile script.tl -o script
./script
```

## Next Steps

- **Language Guide** -- in-depth coverage of the type system, modules, generics, traits, and more
- **Data Guide** -- detailed documentation on tables, connectors, pipelines, and streaming
- **AI Guide** -- tensors, ML training, ONNX inference, embeddings, and LLM integration
