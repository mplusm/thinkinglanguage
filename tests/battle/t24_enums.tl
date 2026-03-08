test "simple enum" {
    enum Color { Red, Green, Blue }
    let c = Color::Red
    assert_eq(type_of(c), "enum")
}

test "enum with fields" {
    enum Shape { Circle(float64), Rect(float64, float64) }
    let s = Shape::Circle(5.0)
    assert_eq(type_of(s), "enum")
}

test "enum match" {
    enum Dir { North, South, East, West }
    let d = Dir::North
    let name = match d {
        Dir::North => "north",
        Dir::South => "south",
        Dir::East => "east",
        Dir::West => "west",
        _ => "unknown"
    }
    assert_eq(name, "north")
}

test "enum destructure" {
    enum Shape { Circle(int64), Rect(int64, int64) }
    let s = Shape::Circle(5)
    let area = match s {
        Shape::Circle(r) => r,
        Shape::Rect(w, h) => w * h,
        _ => 0
    }
    assert_eq(area, 5)
}

test "enum rect destructure" {
    enum Shape { Circle(int64), Rect(int64, int64) }
    let s = Shape::Rect(3, 4)
    let area = match s {
        Shape::Circle(r) => r * r,
        Shape::Rect(w, h) => w * h,
        _ => 0
    }
    assert_eq(area, 12)
}
