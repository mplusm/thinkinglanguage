// t58 — References (&expr)
// Tests: &expr creates read-only ref, ref reads transparently, ref blocks mutation

test "ref read list" {
    let x = [1, 2, 3]
    let r = &x
    assert_eq(len(r), 3)
}

test "ref to int" {
    let x = 42
    let r = &x
    assert_eq(r, 42)
}

test "ref to string" {
    let x = "hello"
    let r = &x
    assert_eq(r, "hello")
    assert_eq(len(r), 5)
}

test "ref to bool" {
    let x = true
    let r = &x
    assert(r)
}

test "ref blocks list mutation" {
    let x = [1, 2, 3]
    let r = &x
    let mut caught = false
    try {
        r[0] = 99
    } catch e {
        caught = true
    }
    assert(caught)
}

test "ref allows read access" {
    let items = [10, 20, 30]
    let r = &items
    assert_eq(r[0], 10)
    assert_eq(r[1], 20)
    assert_eq(r[2], 30)
}

test "ref to struct field" {
    struct Pt { x: int64, y: int64 }
    let p = Pt { x: 5, y: 10 }
    let r = &p
    assert_eq(r.x, 5)
    assert_eq(r.y, 10)
}

test "multiple refs to same value" {
    let x = 100
    let r1 = &x
    let r2 = &x
    assert_eq(r1, 100)
    assert_eq(r2, 100)
}
