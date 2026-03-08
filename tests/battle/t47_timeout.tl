test "timeout with fast task" {
    fn fast() { 42 }
    let t = spawn(fast)
    let r = await t
    assert_eq(r, 42)
}
