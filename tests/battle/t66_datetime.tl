test "now returns datetime" {
    let t = now()
    assert_eq(type_of(t), "datetime")
}
test "datetime comparison" {
    let t1 = now()
    let t2 = now()
    // t2 >= t1 since it's taken after
    assert(t2 >= t1)
}
