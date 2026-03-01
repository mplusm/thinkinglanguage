# Language Basics

ThinkingLanguage (TL) is a Rust-based language designed for Data Engineering and AI. This guide covers the fundamental building blocks of the language.

## Variables

TL supports three kinds of variable bindings:

```tl
let x = 42            // immutable binding
let mut count = 0     // mutable binding
const PI = 3.14       // compile-time constant
```

Immutable bindings cannot be reassigned after initialization. Use `mut` when you need to update a variable's value. `const` declares a constant that is fixed at compile time.

## Primitive Types

| Type    | Description              | Example          |
|---------|--------------------------|------------------|
| int     | 64-bit signed integer    | `42`, `-7`       |
| float   | 64-bit floating point    | `3.14`, `-0.5`   |
| string  | UTF-8 text               | `"hello"`        |
| bool    | Boolean                  | `true`, `false`  |
| none    | Absence of a value       | `none`           |

### Decimal Type

For exact decimal arithmetic (useful in financial calculations), use the `d` suffix:

```tl
let price = 19.99d
let tax = 0.08d
let total = price * (1.0d + tax)  // exact, no floating-point rounding
```

The `d` suffix creates a Decimal value. Underscores can be used as visual separators in numeric literals (e.g., `1_000_000d`).

## String Interpolation

Double-quoted strings support interpolation using curly braces. A `{` character inside a string triggers interpolation:

```tl
let name = "world"
let greeting = "Hello, {name}!"       // "Hello, world!"

let x = 10
let msg = "x is {x} and x+1 is {x + 1}"  // "x is 10 and x+1 is 11"
```

**Note:** Because `{` triggers interpolation, be mindful of this when working with strings that contain literal curly braces.

## Operators

### Arithmetic

```tl
let a = 10 + 3    // 13
let b = 10 - 3    // 7
let c = 10 * 3    // 30
let d = 10 / 3    // 3 (integer division)
let e = 10 % 3    // 1 (modulo)
let f = 2 ** 10   // 1024 (exponentiation)
```

### Comparison

```tl
x == y    // equal
x != y    // not equal
x < y     // less than
x > y     // greater than
x <= y    // less than or equal
x >= y    // greater than or equal
```

### Logical

TL uses word-based logical operators, not symbols:

```tl
true and false   // false
true or false    // true
not true         // false
```

### Null Handling

```tl
let val = maybe_none ?? "default"   // null coalesce: use right side if left is none
let len = obj?.name                 // optional chaining: returns none if obj is none
```

## Control Flow

### if/else

```tl
if x > 0 {
    print("positive")
} else if x == 0 {
    print("zero")
} else {
    print("negative")
}
```

### while

```tl
let mut i = 0
while i < 10 {
    print(i)
    i = i + 1
}
```

### for..in

```tl
for item in [1, 2, 3] {
    print(item)
}

for i in 0..5 {
    print(i)  // prints 0 through 4
}
```

### match

```tl
match value {
    1 => print("one"),
    2 => print("two"),
    _ => print("other"),
}
```

### case

The `case` expression evaluates conditions top-to-bottom and executes the first matching branch:

```tl
case {
    x > 100 => "big",
    x > 10  => "medium",
    _       => "small",
}
```

### break, continue, return

```tl
for i in 0..100 {
    if i == 5 { continue }   // skip this iteration
    if i == 10 { break }     // exit the loop
    print(i)
}

fn early_return(x) {
    if x < 0 { return none }
    return x * 2
}
```

## Ranges

The `..` operator creates a range:

```tl
let r = 1..10   // range from 1 to 9 (exclusive end)

for i in 1..5 {
    print(i)    // prints 1, 2, 3, 4
}
```

## Comments

```tl
// This is a single-line comment

/// This is a doc comment.
/// Doc comments are used to generate documentation.
fn documented_function() {
    // implementation
}
```

## Pipe Operator

The pipe operator `|>` chains transformations, passing the result of the left side as the first argument to the right side:

```tl
let result = value
    |> transform()
    |> validate()
    |> format()
```

This is equivalent to `format(validate(transform(value)))` but reads left-to-right, making data transformation chains clear and readable. The pipe operator is central to TL's data engineering workflows.

**Note:** The pipe operator moves the value (ownership transfer). Use `.clone()` if you need to keep the original value.
