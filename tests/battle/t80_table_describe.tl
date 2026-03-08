// t80 — Table Describe
// Tests: describing table schema

test "describe table" {
    let path = "/tmp/tl_battle_80.csv"
    write_file(path, "x,y,name\n1,10.5,Alice\n2,20.3,Bob\n3,30.1,Charlie\n")
    let t = read_csv(path)
    let desc = t |> describe()
    assert(desc != nil)
}

test "describe includes column names" {
    let path = "/tmp/tl_battle_80b.csv"
    write_file(path, "id,name,age\n1,Alice,30\n")
    let t = read_csv(path)
    let desc = t |> describe()
    assert(desc.contains("id"))
    assert(desc.contains("name"))
    assert(desc.contains("age"))
}

test "describe returns string" {
    let path = "/tmp/tl_battle_80c.csv"
    write_file(path, "score\n100\n200\n")
    let t = read_csv(path)
    let desc = t |> describe()
    assert_eq(type_of(desc), "string")
}
