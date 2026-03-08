// t01 — Let Bindings
// Tests: let, let mut, reassignment, shadowing, multiple types
test "let immutable binding" {
    let x = 42
    assert_eq(x, 42)
}

test "let mut allows reassignment" {
    let mut x = 1
    x = 2
    assert_eq(x, 2)
}

test "multiple reassignments" {
    let mut x = 0
    x = 1
    x = 2
    x = 3
    assert_eq(x, 3)
}

test "shadowing with let" {
    let x = 1
    let x = 2
    assert_eq(x, 2)
}

test "shadowing changes type" {
    let x = 42
    let x = "hello"
    assert_eq(x, "hello")
}

test "different types int" {
    let a = 42
    assert_eq(type_of(a), "int64")
}

test "different types float" {
    let b = 3.14
    assert_eq(type_of(b), "float64")
}

test "different types string" {
    let c = "hi"
    assert_eq(type_of(c), "string")
}

test "different types bool" {
    let d = true
    assert_eq(type_of(d), "bool")
}

test "different types none" {
    let e = nil
    assert_eq(type_of(e), "none")
}

test "let with expression" {
    let x = 2 + 3 * 4
    assert_eq(x, 14)
}

test "let mut with accumulation" {
    let mut total = 0
    total = total + 10
    total = total + 20
    total = total + 30
    assert_eq(total, 60)
}

test "multiple let bindings" {
    let a = 1
    let b = 2
    let c = a + b
    assert_eq(c, 3)
}

test "nested shadowing in block" {
    let x = 10
    if true {
        let x = 20
        assert_eq(x, 20)
    }
}
