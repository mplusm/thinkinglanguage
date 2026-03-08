// t112 — DateTime Full Coverage
// Tests: now, today, date_format, date_parse, date_add, date_diff, date_trunc, date_extract

test "now returns datetime" {
    assert_eq(type_of(now()), "datetime")
}

test "today returns datetime" {
    assert_eq(type_of(today()), "datetime")
}

test "date_format year" {
    let d = today()
    let s = date_format(d, "%Y")
    assert_eq(s.len(), 4)
}

test "date_parse" {
    let d = date_parse("2024-06-15 12:30:00", "%Y-%m-%d %H:%M:%S")
    assert_eq(type_of(d), "datetime")
}

test "date_add days" {
    let d = today()
    let d2 = date_add(d, 7, "days")
    assert(d2 > d)
}

test "date_add negative" {
    let d = today()
    let d2 = date_add(d, -7, "days")
    assert(d2 < d)
}

test "date_diff days" {
    let d1 = today()
    let d2 = date_add(d1, 10, "days")
    let diff = date_diff(d1, d2, "days")
    assert(diff == -10 or diff == 10)
}

test "date_trunc to day" {
    let d = date_trunc(now(), "day")
    let s = date_format(d, "%H:%M:%S")
    assert_eq(s, "00:00:00")
}

test "date_extract year" {
    let d = today()
    let year = date_extract(d, "year")
    assert(year >= 2024)
}

test "date_format full" {
    let d = date_parse("2024-01-15 00:00:00", "%Y-%m-%d %H:%M:%S")
    let formatted = date_format(d, "%Y-%m-%d")
    assert_eq(formatted, "2024-01-15")
}

test "date_add then diff roundtrip" {
    let d1 = today()
    let d2 = date_add(d1, 30, "days")
    let diff = date_diff(d1, d2, "days")
    assert(diff == 30 or diff == -30)
}

test "now vs today ordering" {
    let t = now()
    let d = today()
    // today() should be <= now() since today truncates to midnight
    assert(t >= d)
}
