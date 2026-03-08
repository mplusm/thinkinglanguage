test "secret set and get" {
    secret_set("test_key", "secret_value")
    let s = secret_get("test_key")
    // Secrets display as *** but still exist
    assert(s != nil)
    secret_delete("test_key")
}

test "secret delete" {
    secret_set("temp_key", "temp_val")
    secret_delete("temp_key")
    let s = secret_get("temp_key")
    assert_eq(type_of(s), "none")
}

test "secret display masked" {
    secret_set("masked_key", "my_password")
    let s = secret_get("masked_key")
    // str() on secret should show ***
    assert_eq(str(s), "***")
    secret_delete("masked_key")
}
