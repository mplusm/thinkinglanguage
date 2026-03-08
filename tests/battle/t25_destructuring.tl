test "list destructure in match" {
    let data = [1, 2, 3]
    let first = data[0]
    assert_eq(first, 1)
}

test "enum payload extraction" {
    enum Wrapper { Val(int64) }
    let w = Wrapper::Val(42)
    let v = match w { Wrapper::Val(x) => x, _ => 0 }
    assert_eq(v, 42)
}

test "wildcard in destructure" {
    enum Pair { Two(int64, int64) }
    let p = Pair::Two(10, 20)
    let second = match p { Pair::Two(_, y) => y, _ => 0 }
    assert_eq(second, 20)
}

test "nested enum destructure" {
    enum Result { Ok(int64), Err(string) }
    let r = Result::Ok(100)
    let val = match r {
        Result::Ok(v) => v,
        Result::Err(e) => -1,
        _ => -2
    }
    assert_eq(val, 100)
}
