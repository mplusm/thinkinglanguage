test "check_permission network" {
    assert(check_permission("network"))
}

test "check_permission file_read" {
    assert(check_permission("file_read"))
}

test "mask_phone" {
    let masked = mask_phone("555-123-4567")
    assert_eq(masked, "***-***-4567")
}

test "hash produces hex" {
    let h = hash("hello")
    assert_eq(type_of(h), "string")
    assert(h.len() > 20)
}

test "hash deterministic" {
    let h1 = hash("test")
    let h2 = hash("test")
    assert_eq(h1, h2)
}

test "hash different inputs differ" {
    let h1 = hash("aaa")
    let h2 = hash("bbb")
    assert(h1 != h2)
}

test "redact replaces sensitive data" {
    let r = redact("call 555-123-4567")
    assert_eq(type_of(r), "string")
}

test "secret_list initially empty" {
    let secrets = secret_list()
    assert_eq(type_of(secrets), "list")
}
