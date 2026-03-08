test "string method chain" {
    let result = "  Hello World  ".trim().to_lower()
    assert_eq(result, "hello world")
}

test "list method chain" {
    let result = [5, 3, 1, 4, 2]
        .filter((x) => x > 2)
        .map((x) => x * 10)
        .sort()
    assert(result == [30, 40, 50])
}

test "list chain with reduce" {
    let result = [1, 2, 3, 4, 5]
        .filter((x) => x % 2 != 0)
        .map((x) => x * x)
        .reduce(0, (acc, x) => acc + x)
    assert_eq(result, 35)
}

test "string split then join" {
    let result = "a-b-c".split("-")
    let joined = join(", ", result)
    assert_eq(joined, "a, b, c")
}

test "chained string operations" {
    let result = "Hello, World!".replace(",", "").to_lower().split(" ")
    assert_eq(result[0], "hello")
    assert_eq(result[1], "world!")
}
