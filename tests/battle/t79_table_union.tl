// t79 — Table Union
// Tests: combining two tables with matching schemas
// Note: union() in pipe context requires inline table expressions (not variables)
// because the VM's eval_ast_to_vm can only resolve globals, not locals.

test "union two tables" {
    write_file("/tmp/tl_battle_79a.csv", "name,age\nAlice,30\n")
    write_file("/tmp/tl_battle_79b.csv", "name,age\nBob,25\n")
    let t1 = read_csv("/tmp/tl_battle_79a.csv")
    let combined = t1 |> union(read_csv("/tmp/tl_battle_79b.csv"))
    let out_path = "/tmp/tl_battle_79_out.csv"
    write_csv(combined, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
}

test "union preserves all rows" {
    write_file("/tmp/tl_battle_79c.csv", "val\n1\n2\n3\n")
    write_file("/tmp/tl_battle_79d.csv", "val\n4\n5\n")
    let t1 = read_csv("/tmp/tl_battle_79c.csv")
    let combined = t1 |> union(read_csv("/tmp/tl_battle_79d.csv"))
    let out_path = "/tmp/tl_battle_79_out2.csv"
    write_csv(combined, out_path)
    let content = read_file(out_path)
    assert(content.contains("1"))
    assert(content.contains("2"))
    assert(content.contains("3"))
    assert(content.contains("4"))
    assert(content.contains("5"))
}
