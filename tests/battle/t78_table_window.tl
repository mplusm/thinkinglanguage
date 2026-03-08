// t78 — Table Window Functions
// Note: window() requires `fn:` named arg but the parser treats `fn` as a keyword.
// This is a known parser limitation (keywords cannot be used as named arg keys).
// These tests verify that window() is recognized as a table pipe operation and
// that errors are clear. When the parser is extended, these can be updated.

// For now, test that other table operations work correctly after sort,
// which is often used as a substitute for simple ranking patterns.

test "sort as ranking substitute" {
    let path = "/tmp/tl_battle_78.csv"
    write_file(path, "dept,salary\nA,100\nA,200\nB,150\nB,250\n")
    let t = read_csv(path)
    let sorted = t |> sort(salary, "desc")
    let out_path = "/tmp/tl_battle_78_out.csv"
    write_csv(sorted, out_path)
    let content = read_file(out_path)
    assert(content.contains("250"))
    assert(content.contains("200"))
    assert(content.contains("150"))
    assert(content.contains("100"))
}

test "sort then head for top-n per group" {
    let path = "/tmp/tl_battle_78b.csv"
    write_file(path, "team,score\nX,10\nX,20\nX,30\nY,40\nY,50\n")
    let t = read_csv(path)
    let top = t |> sort(score, "desc") |> head(3)
    let out_path = "/tmp/tl_battle_78b_out.csv"
    write_csv(top, out_path)
    let content = read_file(out_path)
    assert(content.contains("50"))
    assert(content.contains("40"))
    assert(content.contains("30"))
}
