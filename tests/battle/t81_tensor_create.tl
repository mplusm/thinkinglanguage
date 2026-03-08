test "create 1d tensor" {
    let t = tensor([1.0, 2.0, 3.0])
    assert_eq(type_of(t), "tensor")
}

test "tensor shape 1d" {
    let t = tensor([1.0, 2.0, 3.0, 4.0])
    let s = tensor_shape(t)
    assert(s == [4])
}

test "tensor reshape" {
    let t = tensor([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    let t2 = tensor_reshape(t, [2, 3])
    let s = tensor_shape(t2)
    assert(s == [2, 3])
}
