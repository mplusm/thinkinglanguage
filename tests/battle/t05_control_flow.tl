// t05 — Control Flow
// Tests: if/else if/else, nested if, compound conditions

test "basic if true" {
    let x = 10
    let mut result = "no"
    if x > 5 {
        result = "yes"
    }
    assert_eq(result, "yes")
}

test "basic if false" {
    let x = 2
    let mut result = "no"
    if x > 5 {
        result = "yes"
    }
    assert_eq(result, "no")
}

test "if else" {
    let x = 3
    let mut result = ""
    if x > 5 {
        result = "big"
    } else {
        result = "small"
    }
    assert_eq(result, "small")
}

test "else if chain" {
    let x = 5
    let mut result = ""
    if x > 10 {
        result = "big"
    } else if x > 3 {
        result = "medium"
    } else {
        result = "small"
    }
    assert_eq(result, "medium")
}

test "if else assigns big" {
    let x = 20
    let mut r = ""
    if x > 5 {
        r = "big"
    } else {
        r = "small"
    }
    assert_eq(r, "big")
}

test "else if assigns medium" {
    let x = 5
    let mut r = ""
    if x > 10 {
        r = "big"
    } else if x > 3 {
        r = "medium"
    } else {
        r = "small"
    }
    assert_eq(r, "medium")
}

test "nested if" {
    let x = 10
    let y = 20
    let mut result = ""
    if x > 5 {
        if y > 15 {
            result = "both"
        } else {
            result = "only x"
        }
    } else {
        result = "neither"
    }
    assert_eq(result, "both")
}

test "if with boolean variable" {
    let flag = true
    let mut result = "no"
    if flag {
        result = "yes"
    }
    assert_eq(result, "yes")
}

test "if with compound condition" {
    let x = 7
    let mut result = "no"
    if x > 5 and x < 10 {
        result = "yes"
    }
    assert_eq(result, "yes")
}

test "multiple else if" {
    let x = 42
    let mut result = ""
    if x < 0 {
        result = "negative"
    } else if x == 0 {
        result = "zero"
    } else if x < 10 {
        result = "small"
    } else if x < 100 {
        result = "medium"
    } else {
        result = "large"
    }
    assert_eq(result, "medium")
}

test "if with or condition" {
    let x = 15
    let mut result = "no"
    if x < 5 or x > 10 {
        result = "yes"
    }
    assert_eq(result, "yes")
}

test "deeply nested if" {
    let a = 1
    let b = 2
    let c = 3
    let mut result = ""
    if a == 1 {
        if b == 2 {
            if c == 3 {
                result = "all match"
            } else {
                result = "c mismatch"
            }
        } else {
            result = "b mismatch"
        }
    } else {
        result = "a mismatch"
    }
    assert_eq(result, "all match")
}
