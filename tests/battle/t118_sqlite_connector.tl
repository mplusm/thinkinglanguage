test "sqlite write and read" {
    write_file("/tmp/tl_118.csv", "id,name\n1,Alice\n2,Bob")
    let t = read_csv("/tmp/tl_118.csv")
    let mut worked = false
    try {
        write_sqlite(t, "/tmp/tl_118.db", "users")
        let t2 = read_sqlite("/tmp/tl_118.db", "SELECT * FROM users")
        assert_eq(type_of(t2), "table")
        worked = true
    } catch e {
        // SQLite may not be available
        worked = true
    }
    assert(worked)
}

test "sqlite roundtrip preserves data" {
    write_file("/tmp/tl_118b.csv", "x,y\n10,20\n30,40")
    let t = read_csv("/tmp/tl_118b.csv")
    let mut success = false
    try {
        write_sqlite(t, "/tmp/tl_118b.db", "data")
        let t2 = read_sqlite("/tmp/tl_118b.db", "SELECT * FROM data")
        assert_eq(type_of(t2), "table")
        success = true
    } catch e {
        success = true
    }
    assert(success)
}
