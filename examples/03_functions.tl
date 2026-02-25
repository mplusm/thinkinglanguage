// 03 — Functions
// Demonstrates: fn, typed params, return types, recursion

fn add(a: int64, b: int64) -> int64 {
    a + b
}

fn greet(name: string) -> string {
    "Hello, {name}!"
}

print(add(3, 4))
print(greet("World"))

// Recursion: factorial
fn factorial(n: int64) -> int64 {
    if n <= 1 {
        return 1
    }
    return n * factorial(n - 1)
}

print("5! = " + str(factorial(5)))
print("10! = " + str(factorial(10)))
