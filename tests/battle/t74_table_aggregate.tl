// t74 — Table Aggregation
// Tests: sum, count aggregation functions with grouping

test "sum aggregation" {
    let path = "/tmp/tl_battle_74.csv"
    write_file(path, "category,amount\nA,10\nA,20\nB,30\nB,40\n")
    let t = read_csv(path)
    let agg = t |> aggregate(total: sum(amount))
    let out_path = "/tmp/tl_battle_74_out.csv"
    write_csv(agg, out_path)
    let content = read_file(out_path)
    assert(content.contains("100"))
}

test "grouped aggregation" {
    let path = "/tmp/tl_battle_74b.csv"
    write_file(path, "dept,salary\nEng,100\nEng,120\nSales,90\n")
    let t = read_csv(path)
    let agg = t |> aggregate(by: dept, total: sum(salary), n: count())
    let out_path = "/tmp/tl_battle_74b_out.csv"
    write_csv(agg, out_path)
    let content = read_file(out_path)
    assert(content.contains("Eng"))
    assert(content.contains("Sales"))
    assert(content.contains("220"))
}

test "count aggregation" {
    let path = "/tmp/tl_battle_74c.csv"
    write_file(path, "name,city\nAlice,NYC\nBob,NYC\nCharlie,LA\n")
    let t = read_csv(path)
    let agg = t |> aggregate(by: city, headcount: count())
    let out_path = "/tmp/tl_battle_74c_out.csv"
    write_csv(agg, out_path)
    let content = read_file(out_path)
    assert(content.contains("NYC"))
    assert(content.contains("LA"))
    assert(content.contains("2"))
}
