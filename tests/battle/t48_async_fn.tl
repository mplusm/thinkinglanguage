test "async fn declaration" {
    async fn fetch_data() {
        42
    }
    let r = fetch_data()
    assert_eq(r, 42)
}

test "async fn with args" {
    async fn add(a, b) {
        a + b
    }
    assert_eq(add(3, 4), 7)
}
