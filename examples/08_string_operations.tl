// 08 — String Operations
// Demonstrates: interpolation, concatenation, str/int conversion, repeat

let first = "Think"
let second = "ing"
let third = "Language"

// Concatenation
let full = first + second + third
print(full)

// String interpolation
let version = 1
print("Welcome to {full} v{version}")

// str() conversion
let n = 42
let pi = 3.14
print("Number: " + str(n))
print("Float: " + str(pi))

// int() conversion
let s = "123"
let parsed = int(s)
print("Parsed: " + str(parsed + 1))

// String repeat with *
let separator = "-" * 20
print(separator)

// String length
let msg = "Hello, World!"
print("Length of '{msg}': " + str(len(msg)))

// Build a string in a loop
let mut result = ""
for i in range(5) {
    result = result + str(i)
}
print("Digits: " + result)
