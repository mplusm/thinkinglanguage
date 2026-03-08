test "schema compatibility check" {
    let s1 = map_from("id", "int64", "name", "string")
    let s2 = map_from("id", "int64")
    schema_register("MigrateTestBattle", 1, s1)
    schema_register("MigrateTestBattle", 2, s2)
    // Removing a field (name) should have backward compatibility issues
    let issues = schema_check("MigrateTestBattle", 1, 2, "backward")
    assert(len(issues) > 0)
}
