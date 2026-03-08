test "simple enum variant match" {
    enum Color { Red, Green, Blue }
    let c = Color::Green
    let name = match c {
        Color::Red => "red",
        Color::Green => "green",
        Color::Blue => "blue",
        _ => "unknown"
    }
    assert_eq(name, "green")
}

test "enum with payload match all variants" {
    enum Expr { Num(int64), Add(int64, int64), Neg(int64) }
    fn eval(e) {
        match e {
            Expr::Num(n) => n,
            Expr::Add(a, b) => a + b,
            Expr::Neg(n) => 0 - n,
            _ => 0
        }
    }
    assert_eq(eval(Expr::Num(5)), 5)
    assert_eq(eval(Expr::Add(3, 4)), 7)
    assert_eq(eval(Expr::Neg(10)), -10)
}
