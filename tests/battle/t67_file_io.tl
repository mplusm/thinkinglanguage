test "write and read file" {
    let path = "/tmp/tl_battle_test_67.txt"
    write_file(path, "hello TL")
    let content = read_file(path)
    assert_eq(content, "hello TL")
}

test "file_exists" {
    let path = "/tmp/tl_battle_test_67b.txt"
    write_file(path, "test")
    assert(file_exists(path))
}

test "overwrite file" {
    let path = "/tmp/tl_battle_test_67c.txt"
    write_file(path, "first")
    write_file(path, "second")
    assert_eq(read_file(path), "second")
}
