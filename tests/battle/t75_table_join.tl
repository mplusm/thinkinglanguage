// t75 — Table Join
// Tests: inner join between two tables
// Note: join() in pipe context requires inline table expressions (not variables)
// because the VM's eval_ast_to_vm can only resolve globals, not locals.

test "inner join" {
    write_file("/tmp/tl_battle_75_users.csv", "id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    write_file("/tmp/tl_battle_75_orders.csv", "order_id,user_id,product\n101,1,Book\n102,2,Pen\n103,1,Notebook\n")
    let users = read_csv("/tmp/tl_battle_75_users.csv")
    let joined = users |> join(read_csv("/tmp/tl_battle_75_orders.csv"), on: id == user_id)
    let out_path = "/tmp/tl_battle_75_out.csv"
    write_csv(joined, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
    assert(content.contains("Book"))
    assert(content.contains("Pen"))
}

test "join preserves all matched rows" {
    write_file("/tmp/tl_battle_75b_left.csv", "id,name\n1,Alice\n2,Bob\n")
    write_file("/tmp/tl_battle_75b_right.csv", "user_id,score\n1,95\n1,87\n2,72\n")
    let left = read_csv("/tmp/tl_battle_75b_left.csv")
    let joined = left |> join(read_csv("/tmp/tl_battle_75b_right.csv"), on: id == user_id)
    let out_path = "/tmp/tl_battle_75b_out.csv"
    write_csv(joined, out_path)
    let content = read_file(out_path)
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
    assert(content.contains("95"))
    assert(content.contains("87"))
    assert(content.contains("72"))
}
