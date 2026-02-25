// 04 — Control Flow
// Demonstrates: if/else if/else, while, for, break, continue

// If / else if / else
let score = 85
if score >= 90 {
    print("Grade: A")
} else if score >= 80 {
    print("Grade: B")
} else if score >= 70 {
    print("Grade: C")
} else {
    print("Grade: F")
}

// While loop
let mut count = 0
while count < 5 {
    print("count = " + str(count))
    count = count + 1
}

// For loop with range
let mut total = 0
for i in range(1, 11) {
    total = total + i
}
print("Sum 1..10 = " + str(total))

// Break and continue
let mut n = 0
while true {
    n = n + 1
    if n % 2 == 0 {
        continue
    }
    if n > 10 {
        break
    }
    print("odd: " + str(n))
}
