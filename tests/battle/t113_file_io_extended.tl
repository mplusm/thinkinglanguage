// t113 — File I/O Extended
// Tests: append_file, list_dir, write/append/read sequences

test "append_file" {
    let path = "/tmp/tl_battle_113.txt"
    write_file(path, "line1\n")
    append_file(path, "line2\n")
    let content = read_file(path)
    assert(content.contains("line1"))
    assert(content.contains("line2"))
}

test "list_dir current" {
    let items = list_dir(".")
    assert_eq(type_of(items), "list")
    assert(len(items) > 0)
}

test "list_dir tmp" {
    let items = list_dir("/tmp")
    assert(len(items) > 0)
}

test "write then append then read" {
    let path = "/tmp/tl_battle_113b.txt"
    write_file(path, "A")
    append_file(path, "B")
    append_file(path, "C")
    assert_eq(read_file(path), "ABC")
}

test "overwrite clears previous" {
    let path = "/tmp/tl_battle_113c.txt"
    write_file(path, "first content")
    write_file(path, "second")
    assert_eq(read_file(path), "second")
}

test "append to new file" {
    let path = "/tmp/tl_battle_113d.txt"
    write_file(path, "")
    append_file(path, "hello")
    assert_eq(read_file(path), "hello")
}

test "file_exists after write" {
    let path = "/tmp/tl_battle_113e.txt"
    write_file(path, "test data")
    assert(file_exists(path))
}

test "multi-line file" {
    let path = "/tmp/tl_battle_113f.txt"
    write_file(path, "line1\nline2\nline3")
    let content = read_file(path)
    let lines = content.split("\n")
    assert_eq(len(lines), 3)
    assert_eq(lines[0], "line1")
    assert_eq(lines[2], "line3")
}
