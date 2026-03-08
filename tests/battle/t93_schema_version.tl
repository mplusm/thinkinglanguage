test "schema register and get" {
    let schema_def = map_from("id", "int64", "name", "string")
    schema_register("TestUser", 1, schema_def)
    let retrieved = schema_get("TestUser", 1)
    assert(retrieved != nil)
}

test "schema latest" {
    let s1 = map_from("id", "int64")
    let s2 = map_from("id", "int64", "email", "string")
    schema_register("EvolvingType", 1, s1)
    schema_register("EvolvingType", 2, s2)
    let latest = schema_latest("EvolvingType")
    assert(latest != nil)
}

test "schema history" {
    let s1 = map_from("x", "int64")
    schema_register("HistType", 1, s1)
    let hist = schema_history("HistType")
    assert(len(hist) > 0)
}
