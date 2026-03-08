test "producer consumer pattern" {
    let ch = channel()

    fn producer() {
        for i in range(5) {
            send(ch, i)
        }
        send(ch, -1)
    }

    let t = spawn(producer)

    let mut total = 0
    let mut done = false
    while not done {
        let val = recv(ch)
        if val == -1 {
            done = true
        } else {
            total = total + val
        }
    }

    await t
    assert_eq(total, 10)
}

test "two-stage pipeline" {
    let ch1 = channel()
    let ch2 = channel()

    fn stage1() {
        send(ch1, 1)
        send(ch1, 2)
        send(ch1, 3)
        send(ch1, -1)
    }

    fn stage2() {
        let mut done = false
        while not done {
            let v = recv(ch1)
            if v == -1 {
                done = true
                send(ch2, -1)
            } else {
                send(ch2, v * 10)
            }
        }
    }

    let t1 = spawn(stage1)
    let t2 = spawn(stage2)

    let mut results = []
    let mut done = false
    while not done {
        let v = recv(ch2)
        if v == -1 {
            done = true
        } else {
            results = push(results, v)
        }
    }

    await t1
    await t2
    assert(results == [10, 20, 30])
}
