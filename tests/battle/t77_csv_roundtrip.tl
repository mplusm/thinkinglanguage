// t77 — CSV Roundtrip
// Tests: write_csv then read_csv, verifying data integrity

test "csv write then read" {
    let path = "/tmp/tl_battle_77.csv"
    write_file(path, "name,score\nAlice,95\nBob,87\n")
    let t = read_csv(path)
    let out_path = "/tmp/tl_battle_77_out.csv"
    write_csv(t, out_path)
    let t2 = read_csv(out_path)
    assert_eq(type_of(t2), "table")
}

test "csv roundtrip preserves data" {
    let path = "/tmp/tl_battle_77b.csv"
    write_file(path, "x\n1\n2\n3\n4\n5\n")
    let t = read_csv(path)
    let out_path = "/tmp/tl_battle_77b_out.csv"
    write_csv(t, out_path)
    let content = read_file(out_path)
    assert(content.contains("1"))
    assert(content.contains("2"))
    assert(content.contains("3"))
    assert(content.contains("4"))
    assert(content.contains("5"))
}

test "csv roundtrip with filter" {
    let path = "/tmp/tl_battle_77c.csv"
    write_file(path, "x\n1\n2\n3\n4\n5\n")
    let t = read_csv(path)
    let filtered = t |> filter(x > 3)
    let out_path = "/tmp/tl_battle_77c_out.csv"
    write_csv(filtered, out_path)
    let content = read_file(out_path)
    assert(content.contains("4"))
    assert(content.contains("5"))
}

test "csv roundtrip with sort" {
    let path = "/tmp/tl_battle_77d.csv"
    write_file(path, "name,val\nCharlie,3\nAlice,1\nBob,2\n")
    let t = read_csv(path)
    let sorted = t |> sort(val)
    let out_path = "/tmp/tl_battle_77d_out.csv"
    write_csv(sorted, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
    assert(content.contains("Charlie"))
}
