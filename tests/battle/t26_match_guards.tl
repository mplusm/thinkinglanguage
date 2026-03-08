test "positive guard" {
    let x = 5
    let r = match x { n if n > 0 => "pos", n if n < 0 => "neg", _ => "zero" }
    assert_eq(r, "pos")
}

test "negative guard" {
    let x = -3
    let r = match x { n if n > 0 => "pos", n if n < 0 => "neg", _ => "zero" }
    assert_eq(r, "neg")
}

test "zero guard" {
    let x = 0
    let r = match x { n if n > 0 => "pos", n if n < 0 => "neg", _ => "zero" }
    assert_eq(r, "zero")
}

test "guard with enum" {
    enum Result { Ok(int64), Err(string) }
    let r = Result::Ok(150)
    let label = match r {
        Result::Ok(v) if v > 100 => "big",
        Result::Ok(v) => "small",
        Result::Err(e) => e,
        _ => "unknown"
    }
    assert_eq(label, "big")
}

test "guard small value" {
    enum Result { Ok(int64), Err(string) }
    let r = Result::Ok(50)
    let label = match r {
        Result::Ok(v) if v > 100 => "big",
        Result::Ok(v) => "small",
        Result::Err(e) => e,
        _ => "unknown"
    }
    assert_eq(label, "small")
}
