// t06 — Loops
// Tests: for..in range, for..in list, while, break, continue, nested

test "for range single arg" {
    let mut s = 0
    for i in range(5) {
        s = s + i
    }
    assert_eq(s, 10)
}

test "for range with start and end" {
    let mut s = 0
    for i in range(2, 5) {
        s = s + i
    }
    assert_eq(s, 9)
}

test "for range zero iterations" {
    let mut s = 0
    for i in range(0) {
        s = s + 1
    }
    assert_eq(s, 0)
}

test "for in list" {
    let items = [10, 20, 30]
    let mut s = 0
    for x in items {
        s = s + x
    }
    assert_eq(s, 60)
}

test "for in string list" {
    let words = ["hello", "world"]
    let mut result = ""
    for w in words {
        result = result + w + " "
    }
    assert_eq(result, "hello world ")
}

test "while loop" {
    let mut x = 0
    while x < 5 {
        x = x + 1
    }
    assert_eq(x, 5)
}

test "while with complex condition" {
    let mut a = 1
    let mut b = 1
    while a + b < 100 {
        let temp = a + b
        a = b
        b = temp
    }
    assert(a + b >= 100)
}

test "break" {
    let mut x = 0
    for i in range(100) {
        if i == 5 {
            break
        }
        x = i
    }
    assert_eq(x, 4)
}

test "continue" {
    let mut s = 0
    for i in range(10) {
        if i % 2 == 0 {
            continue
        }
        s = s + i
    }
    assert_eq(s, 25)
}

test "nested loops" {
    let mut count = 0
    for i in range(3) {
        for j in range(3) {
            count = count + 1
        }
    }
    assert_eq(count, 9)
}

test "nested loops with multiplication table" {
    let mut total = 0
    for i in range(1, 4) {
        for j in range(1, 4) {
            total = total + i * j
        }
    }
    assert_eq(total, 36)
}

test "loop building a list" {
    let mut result = []
    for i in range(5) {
        result = push(result, i * i)
    }
    assert_eq(len(result), 5)
    assert_eq(result[0], 0)
    assert_eq(result[1], 1)
    assert_eq(result[2], 4)
    assert_eq(result[3], 9)
    assert_eq(result[4], 16)
}

test "while countdown" {
    let mut n = 10
    let mut steps = 0
    while n > 0 {
        n = n - 1
        steps = steps + 1
    }
    assert_eq(steps, 10)
    assert_eq(n, 0)
}
