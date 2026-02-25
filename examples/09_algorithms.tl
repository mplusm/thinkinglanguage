// 09 — Algorithms
// Demonstrates: fibonacci, binary search, while loops, mutable state

// Fibonacci sequence
fn fib(n: int64) -> int64 {
    if n <= 1 { return n }
    let mut a = 0
    let mut b = 1
    let mut i = 2
    while i <= n {
        let temp = b
        b = a + b
        a = temp
        i = i + 1
    }
    return b
}

print("Fibonacci sequence:")
for i in range(12) {
    print("fib(" + str(i) + ") = " + str(fib(i)))
}

// Binary search
fn binary_search(list, target) {
    let mut lo = 0
    let mut hi = len(list) - 1
    while lo <= hi {
        let mid = (lo + hi) / 2
        let val = list[mid]
        if val == target {
            return mid
        } else if val < target {
            lo = mid + 1
        } else {
            hi = mid - 1
        }
    }
    return -1
}

let sorted = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
print("Search for 23: index " + str(binary_search(sorted, 23)))
print("Search for 99: index " + str(binary_search(sorted, 99)))

// GCD using Euclidean algorithm
fn gcd(a: int64, b: int64) -> int64 {
    let mut x = a
    let mut y = b
    while y != 0 {
        let temp = y
        y = x % y
        x = temp
    }
    return x
}

print("gcd(48, 18) = " + str(gcd(48, 18)))
print("gcd(100, 75) = " + str(gcd(100, 75)))
