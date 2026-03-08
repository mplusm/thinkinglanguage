test "basic windowed processing" {
    // Simulate windowed aggregation using lists
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    // Window of size 3: [1,2,3], [4,5,6], [7,8,9], leftover [10]
    let mut windows = []
    let mut i = 0
    while i + 3 <= len(data) {
        let window = [data[i], data[i+1], data[i+2]]
        windows = push(windows, sum(window))
        i = i + 3
    }
    assert(windows == [6, 15, 24])
}
