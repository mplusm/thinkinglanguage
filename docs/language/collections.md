# Collections

TL provides three core collection types: lists, maps (dictionaries), and sets. All collections support iteration and work with the pipe operator for functional transformations.

## Lists

Lists are ordered, dynamically-sized sequences of values:

```tl
let nums = [1, 2, 3, 4, 5]
let mixed = [1, "hello", true, none]  // lists can hold mixed types
let empty = []
```

### Accessing Elements

```tl
let first = nums[0]      // 1 (zero-indexed)
let last = nums[len(nums) - 1]
```

### Modifying Lists

```tl
let mut items = [1, 2, 3]
push(items, 4)            // [1, 2, 3, 4]
let length = len(items)   // 4
```

## Maps / Dicts

Maps are key-value collections:

```tl
let m = map_from([["name", "Alice"], ["age", 30]])
```

### Accessing Values

```tl
let name = m["name"]   // "Alice"
```

### Iterating Maps

```tl
for [k, v] in m {
    print("{k}: {v}")
}
```

## Sets

Sets are unordered collections of unique values:

```tl
let s = set_from([1, 2, 3, 2, 1])  // {1, 2, 3}
```

### Set Operations

```tl
let a = set_from([1, 2, 3])
let b = set_from([2, 3, 4])

set_add(a, 4)
set_remove(a, 1)
set_contains(a, 2)           // true

let union = set_union(a, b)             // {1, 2, 3, 4}
let intersection = set_intersection(a, b)  // {2, 3}
let difference = set_difference(a, b)      // {1}
```

## Iteration

All collections support `for..in` loops:

```tl
// Lists
for item in [10, 20, 30] {
    print(item)
}

// Maps
for [key, value] in my_map {
    print("{key} = {value}")
}

// Ranges
for i in 0..10 {
    print(i)
}
```

## Functional Transformations

TL supports higher-order functions on collections, often used with the pipe operator:

### map

Transform each element:

```tl
let doubled = [1, 2, 3] |> map((x) => x * 2)
// [2, 4, 6]
```

### filter

Keep elements matching a predicate:

```tl
let evens = [1, 2, 3, 4, 5] |> filter((x) => x % 2 == 0)
// [2, 4]
```

### reduce

Combine all elements into a single value:

```tl
let total = [1, 2, 3, 4] |> reduce((acc, x) => acc + x)
// 10
```

### sum

Sum all numeric elements:

```tl
let total = [1, 2, 3, 4, 5] |> sum()
// 15
```

### any / all

Check if any or all elements satisfy a condition:

```tl
let has_negative = [1, -2, 3] |> any((x) => x < 0)   // true
let all_positive = [1, -2, 3] |> all((x) => x > 0)    // false
```

### Chaining Transformations

The pipe operator enables readable transformation chains:

```tl
let result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    |> filter((x) => x % 2 == 0)
    |> map((x) => x ** 2)
    |> sum()
// 220 (4 + 16 + 36 + 64 + 100)
```

## Utility Functions

### zip

Combine two lists into a list of pairs:

```tl
let pairs = zip([1, 2, 3], ["a", "b", "c"])
// [[1, "a"], [2, "b"], [3, "c"]]
```

### enumerate

Pair each element with its index:

```tl
for [i, val] in enumerate(["a", "b", "c"]) {
    print("{i}: {val}")
}
// 0: a
// 1: b
// 2: c
```

### len

Get the length of any collection:

```tl
len([1, 2, 3])        // 3
len("hello")           // 5
len(my_map)            // number of key-value pairs
```

### range

Generate a sequence of numbers:

```tl
let nums = range(0, 5)   // [0, 1, 2, 3, 4]
```

## JSON Interop

Convert between TL values and JSON strings:

```tl
// Parse a JSON string into a TL value
let data = json_parse("{\"name\": \"Alice\", \"age\": 30}")
print(data["name"])  // "Alice"

// Convert a TL value to a JSON string
let json_str = json_stringify(data)
```
