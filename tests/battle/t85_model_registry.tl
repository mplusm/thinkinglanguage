test "model save and load" {
    let data = [[1.0], [2.0], [3.0]]
    let target = [1.0, 2.0, 3.0]
    let model = train("linear_regression", data, target)
    let path = "/tmp/tl_battle_model_85.bin"
    model_save(model, path)
    assert(file_exists(path))
    let loaded = model_load(path)
    assert(loaded != nil)
    delete_file(path)
}
