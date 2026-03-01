# Iterators and Generators

ThinkingLanguage supports generators for lazy, on-demand value production and a rich set of iterator combinators.

## Generators

A generator is a function that uses `yield` to produce values one at a time. Execution pauses at each `yield` and resumes when the next value is requested.

```tl
fn count_up(start) {
    let i = start
    while true {
        yield i
        i = i + 1
    }
}

let counter = count_up(1)
print(next(counter))  // 1
print(next(counter))  // 2
print(next(counter))  // 3
```

## Core Functions

- `next(gen)` -- get the next value from a generator
- `is_generator(val)` -- check if a value is a generator
- `iter(list)` -- convert a list into a generator

```tl
let nums = [10, 20, 30]
let gen = iter(nums)
print(next(gen))  // 10
print(next(gen))  // 20
```

## Lazy Evaluation

Generators do not compute values until `next()` is called. This makes them memory-efficient for large or infinite sequences:

```tl
fn fibonacci() {
    let a = 0
    let b = 1
    while true {
        yield a
        let temp = a + b
        a = b
        b = temp
    }
}

// Only computes the first 10 Fibonacci numbers
let fibs = fibonacci()
let first_ten = take(fibs, 10)
```

## Combinators

Combinators transform generators lazily -- no values are computed until consumed.

### gen_map

Apply a function to each yielded value:

```tl
let doubled = gen_map(count_up(1), fn(x) => x * 2)
print(next(doubled))  // 2
print(next(doubled))  // 4
```

### gen_filter

Keep only values matching a predicate:

```tl
let evens = gen_filter(count_up(1), fn(x) => x % 2 == 0)
print(next(evens))  // 2
print(next(evens))  // 4
```

### chain

Concatenate two generators end-to-end:

```tl
let first = iter([1, 2, 3])
let second = iter([4, 5, 6])
let combined = chain(first, second)
// yields 1, 2, 3, 4, 5, 6
```

### gen_zip

Pair up values from two generators:

```tl
let names = iter(["alice", "bob"])
let ages = iter([30, 25])
let pairs = gen_zip(names, ages)
print(next(pairs))  // ["alice", 30]
print(next(pairs))  // ["bob", 25]
```

### gen_enumerate

Yield `[index, value]` pairs:

```tl
let items = iter(["a", "b", "c"])
let enumerated = gen_enumerate(items)
print(next(enumerated))  // [0, "a"]
print(next(enumerated))  // [1, "b"]
```

## Take, Skip, and Collect

- `take(gen, n)` -- get the first `n` values as a list
- `skip(gen, n)` -- skip the first `n` values, return the generator
- `gen_collect(gen)` -- materialize the entire generator into a list

```tl
let nums = count_up(1)
let skipped = skip(nums, 5)
let batch = take(skipped, 3)
print(batch)  // [6, 7, 8]
```

Be careful with `gen_collect` on infinite generators -- it will run forever.

## For-Loop Integration

Generators work naturally with `for` loops:

```tl
fn range(start, end) {
    let i = start
    while i < end {
        yield i
        i = i + 1
    }
}

for i in range(0, 5) {
    print(i)
}
// prints 0, 1, 2, 3, 4
```

## Method Syntax

Generators support method-style chaining for readable pipelines:

```tl
let result = count_up(1)
    .gen_filter(fn(x) => x % 2 == 0)
    .gen_map(fn(x) => x * x)
    .take(5)
// [4, 16, 36, 64, 100]
```
