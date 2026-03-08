test "question mark on ok" {
    fn get_val() { Ok(42) }
    fn process() {
        let v = get_val()?
        v + 1
    }
    let r = process()
    assert_eq(r, 43)
}

test "question mark on plain value" {
    fn get_val() { 42 }
    fn process() {
        let v = get_val()?
        v + 1
    }
    assert_eq(process(), 43)
}
