test "linear regression on table" {
    let path = "/tmp/tl_battle_84.csv"
    write_file(path, "x,y\n1.0,2.0\n2.0,4.0\n3.0,6.0\n4.0,8.0\n5.0,10.0")
    let data = read_csv(path)
    model lr = train linear {
        data: data,
        target: "y"
    }
    assert(lr != nil)
}
