// t72 — Table Filter and Select
// Tests: filtering rows, selecting columns via pipe operations

test "filter table rows" {
    let path = "/tmp/tl_battle_72.csv"
    write_file(path, "name,age\nAlice,30\nBob,25\nCharlie,35\n")
    let t = read_csv(path)
    let filtered = t |> filter(age > 28)
    let out_path = "/tmp/tl_battle_72_out.csv"
    write_csv(filtered, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Charlie"))
    assert(not content.contains("Bob"))
}

test "filter with equality" {
    let path = "/tmp/tl_battle_72b.csv"
    write_file(path, "name,dept\nAlice,Eng\nBob,Sales\nCharlie,Eng\n")
    let t = read_csv(path)
    let filtered = t |> filter(dept == "Eng")
    let out_path = "/tmp/tl_battle_72b_out.csv"
    write_csv(filtered, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Charlie"))
    assert(not content.contains("Bob"))
}

test "select columns" {
    let path = "/tmp/tl_battle_72c.csv"
    write_file(path, "name,age,city\nAlice,30,NYC\nBob,25,LA\n")
    let t = read_csv(path)
    let selected = t |> select(name, age)
    let out_path = "/tmp/tl_battle_72c_out.csv"
    write_csv(selected, out_path)
    let content = read_file(out_path)
    assert(content.contains("name"))
    assert(content.contains("age"))
    assert(not content.contains("city"))
}

test "filter then select" {
    let path = "/tmp/tl_battle_72d.csv"
    write_file(path, "name,age,dept\nAlice,30,Eng\nBob,25,Sales\nCharlie,35,Eng\n")
    let t = read_csv(path)
    let result = t |> filter(dept == "Eng") |> select(name, age)
    let out_path = "/tmp/tl_battle_72d_out.csv"
    write_csv(result, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Charlie"))
    assert(not content.contains("Bob"))
    assert(not content.contains("dept"))
}
