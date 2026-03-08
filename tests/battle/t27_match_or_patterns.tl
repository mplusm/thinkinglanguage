test "or pattern match" {
    let x = 2
    let r = match x { 1 or 2 or 3 => "small", _ => "big" }
    assert_eq(r, "small")
}

test "or pattern no match" {
    let x = 10
    let r = match x { 1 or 2 or 3 => "small", _ => "big" }
    assert_eq(r, "big")
}

test "or pattern strings" {
    let s = "yes"
    let r = match s { "yes" or "y" or "true" => true, _ => false }
    assert(r)
}

test "or pattern boundary" {
    let x = 3
    let r = match x { 1 or 2 or 3 => "small", 4 or 5 or 6 => "medium", _ => "large" }
    assert_eq(r, "small")
}
