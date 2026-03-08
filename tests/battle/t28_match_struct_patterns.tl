test "enum struct-like match" {
    enum Message { Quit, Text(string), Move(int64, int64) }
    let m = Message::Text("hello")
    let r = match m {
        Message::Quit => "quit",
        Message::Text(s) => s,
        Message::Move(x, y) => "move",
        _ => "unknown"
    }
    assert_eq(r, "hello")
}

test "enum move match" {
    enum Message { Quit, Text(string), Move(int64, int64) }
    let m = Message::Move(10, 20)
    let r = match m {
        Message::Move(x, y) => x + y,
        _ => 0
    }
    assert_eq(r, 30)
}
