test "struct with methods and match" {
    struct Shape { kind: string, size: float64 }
    impl Shape {
        fn area(self) {
            match self.kind {
                "circle" => 3.14159 * self.size * self.size,
                "square" => self.size * self.size,
                _ => 0.0
            }
        }
        fn describe(self) {
            self.kind + " of size " + str(self.size)
        }
    }

    let circle = Shape { kind: "circle", size: 5.0 }
    let square = Shape { kind: "square", size: 4.0 }

    let circle_area = circle.area()
    assert(circle_area > 78.0)
    assert(circle_area < 79.0)
    assert_eq(square.area(), 16.0)
    assert_eq(circle.describe(), "circle of size 5.0")
}

test "enum AST evaluator" {
    enum Expr {
        Num(int64),
        Add(int64, int64),
        Neg(int64)
    }

    fn eval(e) {
        match e {
            Expr::Num(n) => n,
            Expr::Add(a, b) => a + b,
            Expr::Neg(n) => 0 - n,
            _ => 0
        }
    }

    assert_eq(eval(Expr::Num(42)), 42)
    assert_eq(eval(Expr::Add(3, 4)), 7)
    assert_eq(eval(Expr::Neg(10)), -10)
}

test "functional pipeline" {
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    let result = data
        |> filter((x) => x % 2 == 0)
        |> map((x) => x * x)
        |> reduce(0, (acc, x) => acc + x)
    assert_eq(result, 220)
}

test "closure factory" {
    fn make_adder(x) {
        return (y) => x + y
    }
    let add5 = make_adder(5)
    assert_eq(add5(3), 8)
    assert_eq(add5(10), 15)
}

test "recursive algorithm" {
    fn binary_search(list, target) {
        let mut lo = 0
        let mut hi = len(list) - 1
        while lo <= hi {
            let mid = (lo + hi) / 2
            if list[mid] == target { return mid }
            else if list[mid] < target { lo = mid + 1 }
            else { hi = mid - 1 }
        }
        return -1
    }

    let sorted = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
    assert_eq(binary_search(sorted, 23), 5)
    assert_eq(binary_search(sorted, 99), -1)
    assert_eq(binary_search(sorted, 2), 0)
}
