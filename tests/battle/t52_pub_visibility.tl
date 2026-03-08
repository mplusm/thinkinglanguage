// t52 — Pub Visibility
// Tests: pub fn, pub struct within a single file

test "pub fn in same file" {
    pub fn public_add(a, b) { a + b }
    assert_eq(public_add(1, 2), 3)
}

test "pub fn behaves like normal fn locally" {
    pub fn greet(name) { "hello " + name }
    assert_eq(greet("world"), "hello world")
}

test "pub struct in same file" {
    pub struct Point { x: int64, y: int64 }
    let p = Point { x: 1, y: 2 }
    assert_eq(p.x, 1)
    assert_eq(p.y, 2)
}

test "pub struct with methods" {
    pub struct Rect { w: int64, h: int64 }
    impl Rect {
        fn area(self) { self.w * self.h }
    }
    let r = Rect { w: 3, h: 4 }
    assert_eq(r.area(), 12)
}

test "multiple pub fns" {
    pub fn double(x) { x * 2 }
    pub fn triple(x) { x * 3 }
    assert_eq(double(5), 10)
    assert_eq(triple(5), 15)
}
