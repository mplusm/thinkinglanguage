# Functions

TL supports first-class functions, closures, higher-order functions, and methods on structs. Type annotations are optional thanks to gradual typing.

## Function Definition

Functions are declared with the `fn` keyword:

```tl
fn greet(name: string) -> string {
    "Hello, {name}!"
}
```

### Gradual Typing

Type annotations are optional. When omitted, the function accepts and returns `any`:

```tl
fn add(a, b) {
    a + b
}

// Equivalent to:
fn add_typed(a: int, b: int) -> int {
    a + b
}
```

You can annotate some parameters and leave others untyped:

```tl
fn process(data: string, options) {
    // 'data' is type-checked, 'options' is 'any'
}
```

### Return Values

The last expression in a function body is its return value. You can also use explicit `return`:

```tl
fn max(a: int, b: int) -> int {
    if a > b {
        return a
    }
    b
}
```

## Closures

TL provides two closure syntaxes:

### Single-Expression Closures (=>)

Use `=>` for concise, single-expression closures:

```tl
let double = (x) => x * 2
let add = (a, b) => a + b

// With type annotations
let square = (x: int) => x ** 2
```

### Block-Body Closures (->)

Use `->` for closures with multiple statements:

```tl
let process = (x: int) -> int {
    let y = x * 2
    let z = y + 1
    z
}
```

### Closure Captures

Closures capture variables from their enclosing scope:

```tl
let multiplier = 3
let multiply = (x) => x * multiplier
print(multiply(5))  // 15
```

**Note:** Each closure gets its own copy of captured variables when closed over. Shared mutable state across multiple closures is not supported.

## Higher-Order Functions

Functions are first-class values and can be passed as arguments or returned from other functions:

```tl
fn apply(f: fn(int) -> int, x: int) -> int {
    f(x)
}

let result = apply((x) => x * 2, 5)  // 10
```

### Returning Functions

```tl
fn make_adder(n: int) -> fn(int) -> int {
    (x) => x + n
}

let add5 = make_adder(5)
print(add5(10))  // 15
```

## Type Aliases

Use `type` to create aliases for function types and other complex types:

```tl
type Mapper = fn(int) -> int
type Predicate = fn(int) -> bool

fn transform(data: [int], f: Mapper) -> [int] {
    data |> map(f)
}
```

## Recursion

Recursion works naturally:

```tl
fn factorial(n: int) -> int {
    if n <= 1 { return 1 }
    n * factorial(n - 1)
}

fn fibonacci(n: int) -> int {
    match n {
        0 => 0,
        1 => 1,
        _ => fibonacci(n - 1) + fibonacci(n - 2),
    }
}
```

## Methods via Impl Blocks

Methods are defined in `impl` blocks associated with a struct:

```tl
struct Counter {
    value: int,
}

impl Counter {
    fn new() -> Counter {
        Counter { value: 0 }
    }

    fn increment(self) {
        self.value = self.value + 1
    }

    fn get(self) -> int {
        self.value
    }
}

let mut c = Counter::new()
c.increment()
print(c.get())  // 1
```

The first parameter `self` refers to the instance the method is called on.

## Async Functions

Functions can be declared async for non-blocking I/O:

```tl
async fn fetch_data(url: string) -> string {
    let response = await http_get(url)
    response
}
```

See the async/await documentation for more details.
