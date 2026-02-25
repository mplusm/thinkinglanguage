// 06 — Lists and Loops
// Demonstrates: list literals, indexing, len, push, range, for-in

let fruits = ["apple", "banana", "cherry"]
print("Fruits: " + str(fruits))
print("First: " + fruits[0])
print("Length: " + str(len(fruits)))

// Build a list with push
let mut numbers = [1, 2, 3]
numbers = push(numbers, 4)
numbers = push(numbers, 5)
print("Numbers: " + str(numbers))

// Iterate with for-in
for fruit in fruits {
    print("I like " + fruit)
}

// Range-based loop
let mut squares = []
for i in range(1, 6) {
    squares = push(squares, i * i)
}
print("Squares: " + str(squares))

// Nested iteration
for i in range(1, 4) {
    let mut row = ""
    for j in range(1, 4) {
        row = row + str(i * j) + " "
    }
    print(row)
}
