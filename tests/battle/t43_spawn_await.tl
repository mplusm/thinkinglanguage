test "spawn and await" {
    fn worker() { 42 }
    let t = spawn(worker)
    let result = await t
    assert_eq(result, 42)
}

test "spawn closure" {
    let x = 10
    let f = () => x + 5
    let t = spawn(f)
    assert_eq(await t, 15)
}

test "spawn with computation" {
    fn compute() {
        let mut s = 0
        for i in range(10) { s = s + i }
        s
    }
    let t = spawn(compute)
    assert_eq(await t, 45)
}

test "await non-task passthrough" {
    assert_eq(await 42, 42)
}
