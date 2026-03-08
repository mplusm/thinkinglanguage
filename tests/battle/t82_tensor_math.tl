test "tensor add" {
    let a = tensor([1.0, 2.0, 3.0])
    let b = tensor([4.0, 5.0, 6.0])
    let c = a + b
    let s = tensor_sum(c)
    assert_eq(s, 21.0)
}

test "tensor sub" {
    let a = tensor([10.0, 20.0, 30.0])
    let b = tensor([1.0, 2.0, 3.0])
    let c = a - b
    let s = tensor_sum(c)
    assert_eq(s, 54.0)
}

test "tensor mul" {
    let a = tensor([2.0, 3.0, 4.0])
    let b = tensor([5.0, 6.0, 7.0])
    let c = a * b
    let s = tensor_sum(c)
    assert_eq(s, 56.0)
}

test "tensor sum" {
    let t = tensor([1.0, 2.0, 3.0, 4.0])
    assert_eq(tensor_sum(t), 10.0)
}

test "tensor mean" {
    let t = tensor([2.0, 4.0, 6.0, 8.0])
    assert_eq(tensor_mean(t), 5.0)
}

test "tensor dot product" {
    let a = tensor([1.0, 2.0, 3.0])
    let b = tensor([4.0, 5.0, 6.0])
    let d = tensor_dot(a, b)
    // tensor_dot returns a scalar tensor, so use tensor_sum to extract the value
    assert_eq(tensor_sum(d), 32.0)
}
