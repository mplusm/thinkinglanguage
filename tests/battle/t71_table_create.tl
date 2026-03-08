// t71 — Table Creation
// Tests: reading CSV into a table, verifying table type

test "create table from csv" {
    let path = "/tmp/tl_battle_71.csv"
    write_file(path, "name,age\nAlice,30\nBob,25\nCharlie,35\n")
    let t = read_csv(path)
    assert(t != nil)
}

test "table type" {
    let path = "/tmp/tl_battle_71b.csv"
    write_file(path, "x\n1\n2\n3\n")
    let t = read_csv(path)
    assert_eq(type_of(t), "table")
}

test "table with multiple columns" {
    let path = "/tmp/tl_battle_71c.csv"
    write_file(path, "name,age,city\nAlice,30,NYC\nBob,25,LA\n")
    let t = read_csv(path)
    assert_eq(type_of(t), "table")
}
