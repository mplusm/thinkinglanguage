test "error in pipeline stage" {
    let mut caught = false
    try {
        let data = [1, 2, 0, 4]
        let result = data |> map((x) => 100 / x)
    } catch e {
        caught = true
    }
    assert(caught)
}

test "pipeline with error recovery" {
    let data = [1, 2, 3, 4, 5]
    let mut result = []
    for x in data {
        try {
            let val = 100 / x
            result = push(result, val)
        } catch e {
            result = push(result, 0)
        }
    }
    assert_eq(len(result), 5)
    assert_eq(result[0], 100)
}
