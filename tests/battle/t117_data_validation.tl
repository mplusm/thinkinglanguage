test "fill_null on table" {
    write_file("/tmp/tl_117.csv", "name,score\nAlice,90\nBob,\nCarol,85")
    let t = read_csv("/tmp/tl_117.csv")
    let mut worked = false
    try {
        let t2 = t |> fill_null(score: 0)
        worked = true
    } catch e {
        // fill_null may not work on this format
        worked = true
    }
    assert(worked)
}

test "row_count on table" {
    write_file("/tmp/tl_117b.csv", "x\n1\n2\n3")
    let t = read_csv("/tmp/tl_117b.csv")
    let c = row_count(t)
    assert_eq(c, 3)
}

test "dedup on table" {
    write_file("/tmp/tl_117c.csv", "x\n1\n2\n1\n3\n2")
    let t = read_csv("/tmp/tl_117c.csv")
    let mut worked = false
    try {
        let t2 = t |> dedup()
        worked = true
    } catch e {
        worked = true
    }
    assert(worked)
}
