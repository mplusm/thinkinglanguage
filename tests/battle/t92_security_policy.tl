test "mask email" {
    let masked = mask_email("alice@domain.com")
    assert_eq(masked, "a***@domain.com")
}

test "mask email short" {
    let masked = mask_email("a@b.com")
    // Should still mask
    assert(masked.contains("***"))
}
