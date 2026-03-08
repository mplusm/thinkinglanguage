test "send and recv" {
    let ch = channel()
    send(ch, 42)
    assert_eq(recv(ch), 42)
}

test "multiple values" {
    let ch = channel()
    send(ch, 1)
    send(ch, 2)
    send(ch, 3)
    assert_eq(recv(ch), 1)
    assert_eq(recv(ch), 2)
    assert_eq(recv(ch), 3)
}

test "try_recv empty" {
    let ch = channel()
    let r = try_recv(ch)
    assert_eq(r, none)
}

test "try_recv with value" {
    let ch = channel()
    send(ch, "hello")
    let r = try_recv(ch)
    assert_eq(r, "hello")
}

test "channel with spawn" {
    let ch = channel()
    fn producer() {
        send(ch, 10)
        send(ch, 20)
        send(ch, 30)
    }
    let t = spawn(producer)
    let a = recv(ch)
    let b = recv(ch)
    let c = recv(ch)
    await t
    assert_eq(a + b + c, 60)
}
