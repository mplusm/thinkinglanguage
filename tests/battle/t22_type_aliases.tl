test "basic type alias" {
    type Num = int64
    fn square(n: Num) -> Num { n * n }
    assert_eq(square(5), 25)
}
