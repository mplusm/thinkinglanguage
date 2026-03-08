test "struct creation" {
    struct Point { x: float64, y: float64 }
    let p = Point { x: 1.0, y: 2.0 }
    assert_eq(p.x, 1.0)
    assert_eq(p.y, 2.0)
}

test "struct with method" {
    struct Rect { w: float64, h: float64 }
    impl Rect { fn area(self) { self.w * self.h } }
    let r = Rect { w: 3.0, h: 4.0 }
    assert_eq(r.area(), 12.0)
}

test "nested struct" {
    struct Point { x: float64, y: float64 }
    struct Line { start: Point, end_pt: Point }
    let l = Line { start: Point { x: 0.0, y: 0.0 }, end_pt: Point { x: 1.0, y: 1.0 } }
    assert_eq(l.start.x, 0.0)
    assert_eq(l.end_pt.y, 1.0)
}

test "struct multiple methods" {
    struct Counter { value: int64 }
    impl Counter {
        fn get(self) { self.value }
        fn next_val(self) { self.value + 1 }
    }
    let c = Counter { value: 10 }
    assert_eq(c.get(), 10)
    assert_eq(c.next_val(), 11)
}

test "struct type_of" {
    struct Foo { x: int64 }
    let f = Foo { x: 1 }
    assert_eq(type_of(f), "struct")
}
