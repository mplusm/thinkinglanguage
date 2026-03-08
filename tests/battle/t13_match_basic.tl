test "match int" {
    let r = match 2 {
        1 => "one",
        2 => "two",
        _ => "other"
    }
    assert_eq(r, "two")
}

test "match string" {
    let r = match "hi" {
        "hello" => 1,
        "hi" => 2,
        _ => 0
    }
    assert_eq(r, 2)
}

test "match wildcard" {
    let r = match 99 {
        1 => "one",
        _ => "other"
    }
    assert_eq(r, "other")
}

test "match binding" {
    let r = match 42 {
        val => val + 1
    }
    assert_eq(r, 43)
}

test "match as expression" {
    fn classify(n) {
        match n {
            0 => "zero",
            1 => "one",
            _ => "many"
        }
    }
    assert_eq(classify(0), "zero")
    assert_eq(classify(5), "many")
}
