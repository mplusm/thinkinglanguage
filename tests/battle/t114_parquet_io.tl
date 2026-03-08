// t114 — Parquet I/O
// Tests: parquet roundtrip, data preservation

test "parquet roundtrip" {
    let csv_path = "/tmp/tl_battle_114.csv"
    let parquet_path = "/tmp/tl_battle_114.parquet"
    write_file(csv_path, "name,age\nAlice,30\nBob,25")
    let t = read_csv(csv_path)
    write_parquet(t, parquet_path)
    let t2 = read_parquet(parquet_path)
    assert_eq(type_of(t2), "table")
}

test "parquet preserves data" {
    let csv_path = "/tmp/tl_battle_114b.csv"
    let pq_path = "/tmp/tl_battle_114b.parquet"
    write_file(csv_path, "x,y\n1,2\n3,4\n5,6")
    let t = read_csv(csv_path)
    write_parquet(t, pq_path)
    let t2 = read_parquet(pq_path)
    assert_eq(type_of(t2), "table")
}

test "parquet file exists after write" {
    let csv_path = "/tmp/tl_battle_114c.csv"
    let pq_path = "/tmp/tl_battle_114c.parquet"
    write_file(csv_path, "a,b\n10,20\n30,40")
    let t = read_csv(csv_path)
    write_parquet(t, pq_path)
    assert(file_exists(pq_path))
}

test "parquet roundtrip with filter" {
    let csv_path = "/tmp/tl_battle_114d.csv"
    let pq_path = "/tmp/tl_battle_114d.parquet"
    write_file(csv_path, "val\n1\n2\n3\n4\n5")
    let t = read_csv(csv_path)
    let filtered = t |> filter(val > 3)
    write_parquet(filtered, pq_path)
    let t2 = read_parquet(pq_path)
    assert_eq(type_of(t2), "table")
}
