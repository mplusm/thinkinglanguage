test "ok result" {
    let r = Ok(42)
    assert(is_ok(r))
    assert_eq(unwrap(r), 42)
}

test "err result" {
    let r = Err("fail")
    assert(is_err(r))
    assert(not is_ok(r))
}

test "option some via Ok" {
    let x = Ok(10)
    assert(is_ok(x))
    assert_eq(unwrap(x), 10)
}

test "option none" {
    let x = none
    assert_eq(type_of(x), "none")
}

test "match on result" {
    let r = Ok(42)
    let v = match r {
        Result::Ok(x) => x,
        Result::Err(e) => -1,
        _ => -2
    }
    assert_eq(v, 42)
}

test "match on err" {
    let r = Err("oops")
    let v = match r {
        Result::Ok(x) => x,
        Result::Err(e) => "got error",
        _ => "unknown"
    }
    assert_eq(v, "got error")
}
