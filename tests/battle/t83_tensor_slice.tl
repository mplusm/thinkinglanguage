test "tensor operations on subset" {
    let t = tensor([10.0, 20.0, 30.0, 40.0, 50.0])
    let total = tensor_sum(t)
    assert_eq(total, 150.0)
}

test "tensor mean on small" {
    let t = tensor([10.0, 20.0, 30.0])
    assert_eq(tensor_mean(t), 20.0)
}
