# Generics and Traits

TL supports generic programming through type parameters and traits. Generics use a type-erased implementation with runtime dispatch.

## Generic Functions

Define functions that work with any type using angle-bracket type parameters:

```tl
fn identity<T>(x: T) -> T {
    x
}

let a = identity(42)        // int
let b = identity("hello")   // string
```

### Multiple Type Parameters

```tl
fn pair<A, B>(first: A, second: B) -> [any] {
    [first, second]
}
```

## Generic Structs

Structs can be parameterized over types:

```tl
struct Pair<A, B> {
    first: A,
    second: B,
}

let p = Pair { first: 1, second: "hello" }
print(p.first)    // 1
print(p.second)   // "hello"
```

### Generic Impl Blocks

```tl
impl Pair<A, B> {
    fn swap(self) -> Pair<B, A> {
        Pair { first: self.second, second: self.first }
    }
}
```

## Traits

Traits define shared behavior as a set of method signatures:

### Defining Traits

```tl
trait Display {
    fn display(self) -> string
}

trait Area {
    fn area(self) -> float
}
```

### Implementing Traits

Use `impl Trait for Type` to implement a trait for a specific type:

```tl
struct Point {
    x: float,
    y: float,
}

impl Display for Point {
    fn display(self) -> string {
        "({self.x}, {self.y})"
    }
}

struct Circle {
    center: Point,
    radius: float,
}

impl Display for Circle {
    fn display(self) -> string {
        "Circle at {self.center.display()} with r={self.radius}"
    }
}

impl Area for Circle {
    fn area(self) -> float {
        3.14159 * self.radius ** 2
    }
}
```

### Calling Trait Methods

Trait methods are called like regular methods:

```tl
let p = Point { x: 1.0, y: 2.0 }
print(p.display())   // "(1.0, 2.0)"

let c = Circle { center: p, radius: 5.0 }
print(c.area())      // 78.53975
```

## Trait Bounds

Constrain generic type parameters to types that implement specific traits:

```tl
fn print_it<T: Display>(x: T) {
    print(x.display())
}

print_it(Point { x: 1.0, y: 2.0 })   // prints "(1.0, 2.0)"
```

### Multiple Bounds

Require multiple traits with `+`:

```tl
fn describe<T: Display + Area>(shape: T) -> string {
    "{shape.display()} has area {shape.area()}"
}
```

## Where Clauses

For complex bounds, use `where` clauses for readability:

```tl
fn process<T>(x: T) where T: Display {
    print(x.display())
}

fn compare<A, B>(a: A, b: B) where A: Display, B: Display {
    print("a = {a.display()}, b = {b.display()}")
}
```

## Built-in Trait Hierarchy

TL provides several built-in traits:

| Trait     | Description                                  |
|-----------|----------------------------------------------|
| Display   | Human-readable string representation         |
| Debug     | Developer-oriented string representation     |
| Clone     | Deep copy capability                         |

These traits are automatically available and can be implemented for any user-defined type.

## Type Erasure

TL's generics use type-erased implementation, meaning generic type information is resolved at runtime rather than generating specialized code for each type. This keeps compilation fast and binary sizes small, with a small runtime cost for dispatch.

```tl
// Both calls use the same compiled function body
let x = identity(42)
let y = identity("hello")
```

This approach means that:
- Generic functions are compiled once, not per-type
- Trait method calls go through runtime dispatch
- Type checking with trait bounds happens at compile time where annotations are provided
- Untyped code falls back to dynamic dispatch (gradual typing)
```
