# Ownership and Move Semantics

ThinkingLanguage uses ownership and move semantics to manage data safely and efficiently, particularly in data pipelines.

## Pipe-as-Move

The pipe operator `|>` transfers ownership of the value from the source to the next stage. After piping, the source variable is considered moved and cannot be used again.

```tl
let data = read_csv("data.csv")
data |> filter(age > 30) |> show()
// data is now moved — using it here would cause an error
```

This design prevents accidental aliasing of large datasets in pipelines.

## Use-After-Move Detection

Both the compiler and the type checker detect attempts to use a moved variable. If you try to access a variable after it has been piped, you get a clear error message at compile time.

```tl
let table = read_csv("sales.csv")
table |> filter(region == "US") |> write_csv("us_sales.csv")
table |> show()  // ERROR: use of moved value 'table'
```

At runtime, a moved variable holds a `Moved` tombstone value. Accessing it produces a descriptive error.

## Deep Copy with `.clone()`

If you need to use a value after piping it, clone it first:

```tl
let data = read_csv("data.csv")
let backup = data.clone()
data |> filter(age > 30) |> show()
// backup is still valid
backup |> describe()
```

`.clone()` performs a deep copy, recursively duplicating containers, lists, maps, and nested structures.

## Read-Only References

Create a read-only reference with `&`:

```tl
let original = [1, 2, 3]
let ref = &original
```

References are transparent in read contexts -- you can use them with `GetMember`, `GetIndex`, and pass them as function arguments just like the original value. However, references block mutation:

```tl
let data = [1, 2, 3]
let r = &data
print(r[0])    // OK: reading through reference
r[0] = 99      // ERROR: cannot mutate through a reference
```

Use references for shared read-only access to data without transferring ownership.

## Parallel Iteration

The `parallel for` construct runs loop iterations concurrently using rayon:

```tl
parallel for item in collection {
    process(item)
}
```

This is useful for CPU-bound processing of independent items.

## Best Practices

- **Clone before pipe** if you need the value again downstream.
- **Use `&ref`** for shared read-only access to avoid unnecessary copies.
- **Design pipelines linearly** -- each value flows through one chain of operations.
- **Use `parallel for`** for independent, CPU-bound work over collections.
- Let the compiler help you -- move detection catches mistakes early.
