// t76 — Table With (Computed Columns)
// Tests: adding derived columns via with { col = expr }

test "add computed column" {
    let path = "/tmp/tl_battle_76.csv"
    write_file(path, "name,age\nAlice,30\nBob,25\n")
    let t = read_csv(path)
    let enriched = t |> with { doubled = age * 2 }
    let out_path = "/tmp/tl_battle_76_out.csv"
    write_csv(enriched, out_path)
    let content = read_file(out_path)
    assert(content.contains("doubled"))
    assert(content.contains("60"))
    assert(content.contains("50"))
}

test "add boolean column" {
    let path = "/tmp/tl_battle_76b.csv"
    write_file(path, "name,age\nAlice,30\nBob,25\nCharlie,35\n")
    let t = read_csv(path)
    let enriched = t |> with { senior = age > 28 }
    let out_path = "/tmp/tl_battle_76b_out.csv"
    write_csv(enriched, out_path)
    let content = read_file(out_path)
    assert(content.contains("senior"))
    assert(content.contains("true"))
    assert(content.contains("false"))
}

test "with preserves original columns" {
    let path = "/tmp/tl_battle_76c.csv"
    write_file(path, "x,y\n10,20\n30,40\n")
    let t = read_csv(path)
    let enriched = t |> with { total = x + y }
    let out_path = "/tmp/tl_battle_76c_out.csv"
    write_csv(enriched, out_path)
    let content = read_file(out_path)
    assert(content.contains("x"))
    assert(content.contains("y"))
    assert(content.contains("total"))
    assert(content.contains("30"))
    assert(content.contains("70"))
}
