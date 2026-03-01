# Structs, Enums, and Pattern Matching

TL supports user-defined data types through structs and enums, with powerful pattern matching for deconstructing values.

## Structs

Structs group related data into named fields:

### Definition

```tl
struct Point {
    x: float,
    y: float,
}

struct User {
    name: string,
    age: int,
    active: bool,
}
```

### Instantiation

```tl
let p = Point { x: 1.0, y: 2.0 }
let user = User { name: "Alice", age: 30, active: true }
```

### Field Access

```tl
print(p.x)         // 1.0
print(user.name)   // "Alice"
```

### Impl Blocks

Methods are added to structs via `impl` blocks:

```tl
impl Point {
    fn distance(self) -> float {
        sqrt(self.x ** 2 + self.y ** 2)
    }

    fn translate(self, dx: float, dy: float) -> Point {
        Point { x: self.x + dx, y: self.y + dy }
    }

    fn to_string(self) -> string {
        "({self.x}, {self.y})"
    }
}

let p = Point { x: 3.0, y: 4.0 }
print(p.distance())     // 5.0
print(p.to_string())    // "(3.0, 4.0)"
```

Static methods (no `self` parameter) are called with `::` syntax:

```tl
impl Point {
    fn origin() -> Point {
        Point { x: 0.0, y: 0.0 }
    }
}

let o = Point::origin()
```

## Enums

Enums define a type with a fixed set of variants:

### Simple Enums

```tl
enum Color {
    Red,
    Green,
    Blue,
}

let c = Color::Red
```

### Enums with Data

Variants can carry associated data:

```tl
enum Shape {
    Circle(radius: float),
    Rect(w: float, h: float),
    Triangle(a: float, b: float, c: float),
}

let s = Shape::Circle(5.0)
let r = Shape::Rect(3.0, 4.0)
```

## Pattern Matching

The `match` expression provides exhaustive pattern matching:

### Basic Matching

```tl
match color {
    Color::Red => print("red"),
    Color::Green => print("green"),
    Color::Blue => print("blue"),
}
```

### Destructuring Enum Variants

```tl
fn area(shape: Shape) -> float {
    match shape {
        Shape::Circle(r) => 3.14159 * r ** 2,
        Shape::Rect(w, h) => w * h,
        Shape::Triangle(a, b, c) => {
            let s = (a + b + c) / 2.0
            sqrt(s * (s - a) * (s - b) * (s - c))
        },
    }
}
```

### Guards

Add conditions to match arms with `if`:

```tl
match x {
    n if n > 0 => "positive",
    n if n < 0 => "negative",
    _ => "zero",
}
```

### OR Patterns

Combine multiple patterns in a single arm using the `or` keyword:

```tl
match x {
    1 or 2 or 3 => "small",
    4 or 5 or 6 => "medium",
    _ => "big",
}
```

**Important:** TL uses the `or` keyword for OR patterns, NOT the `|` symbol.

### Wildcard Pattern

The `_` pattern matches anything and is used as a catch-all:

```tl
match value {
    0 => "zero",
    1 => "one",
    _ => "something else",
}
```

## Destructuring

### Let Destructuring

Extract fields from structs in `let` bindings:

```tl
let Point { x, y } = point
print("x = {x}, y = {y}")
```

### List Destructuring

```tl
let [first, second, rest] = [1, 2, 3]
```

### In For Loops

```tl
let points = [Point { x: 1.0, y: 2.0 }, Point { x: 3.0, y: 4.0 }]
for Point { x, y } in points {
    print("({x}, {y})")
}
```

## Nested Pattern Matching

Patterns can be nested to match complex data structures:

```tl
enum Expr {
    Num(value: float),
    Add(left: Expr, right: Expr),
    Mul(left: Expr, right: Expr),
}

fn eval(expr: Expr) -> float {
    match expr {
        Expr::Num(v) => v,
        Expr::Add(l, r) => eval(l) + eval(r),
        Expr::Mul(l, r) => eval(l) * eval(r),
    }
}
```

## Exhaustiveness Checking

The TL compiler checks that match expressions cover all possible variants. If you miss a variant, you will get a compile-time warning:

```tl
// Warning: non-exhaustive match — missing Color::Blue
match color {
    Color::Red => "red",
    Color::Green => "green",
}
```

Use `_` as a catch-all if you intentionally want to handle remaining cases uniformly:

```tl
match color {
    Color::Red => "red",
    _ => "not red",
}
```
