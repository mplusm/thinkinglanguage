// 07 — Closures and Pipe Operator
// Demonstrates: closures, |> pipe, map, filter, chained pipes

// Basic closure
let double = (x) => x * 2
let add_ten = (x) => x + 10
print("double(5) = " + str(double(5)))
print("add_ten(5) = " + str(add_ten(5)))

// Closures capture their environment
let multiplier = 3
let times = (x) => x * multiplier
print("times(7) = " + str(times(7)))

// Pipe operator with named functions
fn square(n: int64) -> int64 { n * n }
let result = 5 |> square()
print("5 |> square() = " + str(result))

// Pipe with closures via map/filter
let nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

let evens = nums |> filter((x) => x % 2 == 0)
print("Evens: " + str(evens))

let doubled = nums |> map((x) => x * 2)
print("Doubled: " + str(doubled))

// Chained pipes
let result = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    |> filter((x) => x % 2 == 0)
    |> map((x) => x * x)
    |> sum()
print("Sum of squares of evens: " + str(result))
