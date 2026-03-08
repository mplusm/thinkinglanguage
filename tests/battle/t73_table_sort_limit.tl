// t73 — Table Sort and Limit
// Tests: sorting rows, limiting result count

test "sort ascending" {
    let path = "/tmp/tl_battle_73.csv"
    write_file(path, "name,val\nCharlie,3\nAlice,1\nBob,2\n")
    let t = read_csv(path)
    let sorted = t |> sort(val)
    let out_path = "/tmp/tl_battle_73_out.csv"
    write_csv(sorted, out_path)
    let content = read_file(out_path)
    assert(content.contains("1"))
    assert(content.contains("2"))
    assert(content.contains("3"))
}

test "sort descending" {
    let path = "/tmp/tl_battle_73b.csv"
    write_file(path, "name,val\nCharlie,3\nAlice,1\nBob,2\n")
    let t = read_csv(path)
    let sorted = t |> sort(val, "desc")
    let out_path = "/tmp/tl_battle_73b_out.csv"
    write_csv(sorted, out_path)
    let content = read_file(out_path)
    assert(content.contains("Charlie"))
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
}

test "head limits rows" {
    let path = "/tmp/tl_battle_73c.csv"
    write_file(path, "x\n10\n20\n30\n40\n50\n")
    let t = read_csv(path)
    let top2 = t |> head(2)
    let out_path = "/tmp/tl_battle_73c_out.csv"
    write_csv(top2, out_path)
    let content = read_file(out_path)
    assert(content.contains("10"))
    assert(content.contains("20"))
}

test "limit is alias for head" {
    let path = "/tmp/tl_battle_73d.csv"
    write_file(path, "x\n10\n20\n30\n40\n50\n")
    let t = read_csv(path)
    let top3 = t |> limit(3)
    let out_path = "/tmp/tl_battle_73d_out.csv"
    write_csv(top3, out_path)
    let content = read_file(out_path)
    assert(content.contains("10"))
    assert(content.contains("20"))
    assert(content.contains("30"))
}
