// t09 — Lists
// Tests: literals, indexing, push, len, methods, nested, map/filter/reduce

test "list literal" {
    let xs = [1, 2, 3]
    assert_eq(len(xs), 3)
}

test "empty list" {
    let xs = []
    assert_eq(len(xs), 0)
}

test "indexing" {
    let xs = [10, 20, 30]
    assert_eq(xs[0], 10)
    assert_eq(xs[1], 20)
    assert_eq(xs[2], 30)
}

test "negative indexing" {
    let xs = [10, 20, 30]
    assert_eq(xs[-1], 30)
    assert_eq(xs[-2], 20)
    assert_eq(xs[-3], 10)
}

test "push returns new list" {
    let xs = [1, 2]
    let ys = push(xs, 3)
    assert_eq(len(ys), 3)
    assert_eq(ys[2], 3)
}

test "len method" {
    assert_eq([1, 2, 3].len(), 3)
    assert_eq([].len(), 0)
}

test "map method" {
    let result = [1, 2, 3].map((x) => x * 2)
    assert_eq(len(result), 3)
    assert_eq(result[0], 2)
    assert_eq(result[1], 4)
    assert_eq(result[2], 6)
}

test "filter method" {
    let result = [1, 2, 3, 4, 5].filter((x) => x > 3)
    assert_eq(len(result), 2)
    assert_eq(result[0], 4)
    assert_eq(result[1], 5)
}

test "reduce method" {
    let result = [1, 2, 3, 4].reduce(0, (acc, x) => acc + x)
    assert_eq(result, 10)
}

test "reduce with multiply" {
    let result = [1, 2, 3, 4].reduce(1, (acc, x) => acc * x)
    assert_eq(result, 24)
}

test "nested lists" {
    let m = [[1, 2], [3, 4]]
    assert_eq(m[0][0], 1)
    assert_eq(m[0][1], 2)
    assert_eq(m[1][0], 3)
    assert_eq(m[1][1], 4)
}

test "contains method" {
    assert([1, 2, 3].contains(2))
    assert(not [1, 2, 3].contains(5))
}

test "sum builtin" {
    assert_eq(sum([1, 2, 3, 4]), 10)
    assert_eq(sum([]), 0)
}

test "sum method" {
    assert_eq([1, 2, 3, 4, 5].sum(), 15)
}

test "list of strings" {
    let words = ["hello", "world"]
    assert_eq(words[0], "hello")
    assert_eq(words[1], "world")
    assert_eq(len(words), 2)
}

test "list of booleans" {
    let flags = [true, false, true]
    assert_eq(flags[0], true)
    assert_eq(flags[1], false)
    assert_eq(len(flags), 3)
}

test "list built with loop" {
    let mut xs = []
    for i in range(5) {
        xs = push(xs, i * 10)
    }
    assert_eq(len(xs), 5)
    assert_eq(xs[0], 0)
    assert_eq(xs[1], 10)
    assert_eq(xs[2], 20)
    assert_eq(xs[3], 30)
    assert_eq(xs[4], 40)
}

test "slice method" {
    let xs = [10, 20, 30, 40, 50]
    let sub = xs.slice(1, 4)
    assert_eq(len(sub), 3)
    assert_eq(sub[0], 20)
    assert_eq(sub[1], 30)
    assert_eq(sub[2], 40)
}

test "list type" {
    assert_eq(type_of([1, 2, 3]), "list")
    assert_eq(type_of([]), "list")
}

test "chained list operations" {
    let result = [1, 2, 3, 4, 5, 6]
        .filter((x) => x % 2 == 0)
        .map((x) => x * 10)
    assert_eq(len(result), 3)
    assert_eq(result[0], 20)
    assert_eq(result[1], 40)
    assert_eq(result[2], 60)
}

test "map builtin function" {
    let result = map([5, 10, 15], (x) => x + 1)
    assert_eq(result[0], 6)
    assert_eq(result[1], 11)
    assert_eq(result[2], 16)
}

test "filter builtin function" {
    let result = filter([1, 2, 3, 4, 5], (x) => x < 4)
    assert_eq(len(result), 3)
    assert_eq(result[0], 1)
    assert_eq(result[1], 2)
    assert_eq(result[2], 3)
}
