// 02 — Basic Types
// Demonstrates: int64, float64, bool, string, arithmetic, type_of

let x = 42
let pi = 3.14
let active = true
let name = "TL"

print("x = {x}, type = " + type_of(x))
print("pi = " + str(pi) + ", type = " + type_of(pi))
print("active = " + str(active) + ", type = " + type_of(active))
print("name = {name}, type = " + type_of(name))

// Arithmetic
print("10 + 3 = " + str(10 + 3))
print("10 - 3 = " + str(10 - 3))
print("10 * 3 = " + str(10 * 3))
print("10 / 3 = " + str(10 / 3))
print("10 % 3 = " + str(10 % 3))
print("2 ** 8 = " + str(2 ** 8))
