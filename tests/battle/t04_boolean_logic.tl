// t04 — Boolean Logic
// Tests: and/or short-circuit, not, comparisons, equality

test "greater than" {
    assert(5 > 3)
    assert(not (3 > 5))
    assert(not (5 > 5))
}

test "less than" {
    assert(3 < 5)
    assert(not (5 < 3))
    assert(not (5 < 5))
}

test "greater or equal" {
    assert(5 >= 5)
    assert(5 >= 3)
    assert(not (3 >= 5))
}

test "less or equal" {
    assert(5 <= 5)
    assert(3 <= 5)
    assert(not (5 <= 3))
}

test "equality" {
    assert(5 == 5)
    assert(not (5 == 3))
}

test "inequality" {
    assert(5 != 3)
    assert(not (5 != 5))
}

test "and short-circuit" {
    let x = false and (1 / 0 == 1)
    assert_eq(x, false)
}

test "and both true" {
    let x = true and true
    assert_eq(x, true)
}

test "and first false" {
    let x = false and true
    assert_eq(x, false)
}

test "or short-circuit" {
    let x = true or (1 / 0 == 1)
    assert_eq(x, true)
}

test "or both false" {
    let x = false or false
    assert_eq(x, false)
}

test "or first false second true" {
    let x = false or true
    assert_eq(x, true)
}

test "not operator" {
    assert(not false)
    assert(not not true)
    assert_eq(not true, false)
}

test "string equality" {
    assert("a" == "a")
    assert("a" != "b")
}

test "bool equality" {
    assert(true == true)
    assert(false == false)
    assert(true != false)
}

test "nil equality" {
    assert(nil == nil)
}

test "compound boolean" {
    let a = true
    let b = false
    assert(a and not b)
    assert(a or b)
    assert(not (a and b))
}

test "comparison chaining" {
    let x = 5
    assert(x > 2 and x < 10)
    assert(not (x > 10 or x < 0))
}
