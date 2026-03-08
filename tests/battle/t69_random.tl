test "random in range" {
    let r = random()
    assert(r >= 0.0)
    assert(r < 1.0)
}
test "random_int in range" {
    let r = random_int(1, 10)
    assert(r >= 1)
    assert(r <= 10)
}
test "multiple randoms differ" {
    // It's possible but extremely unlikely that 10 random numbers are all the same
    let mut all_same = true
    let first = random()
    for i in range(10) {
        if random() != first { all_same = false }
    }
    // This could theoretically fail but is astronomically unlikely
    assert(not all_same)
}
