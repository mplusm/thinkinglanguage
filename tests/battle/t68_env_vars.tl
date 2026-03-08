test "set and get env" {
    env_set("TL_BATTLE_TEST", "hello")
    assert_eq(env_get("TL_BATTLE_TEST"), "hello")
}
test "overwrite env" {
    env_set("TL_BATTLE_TEST2", "first")
    env_set("TL_BATTLE_TEST2", "second")
    assert_eq(env_get("TL_BATTLE_TEST2"), "second")
}
test "PATH exists" {
    let path = env_get("PATH")
    assert(len(path) > 0)
}
