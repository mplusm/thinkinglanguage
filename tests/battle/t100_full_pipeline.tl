test "end to end data pipeline" {
    // Create data using map_from to avoid { in strings triggering interpolation
    let records = [
        map_from("name", "Alice", "score", 95),
        map_from("name", "Bob", "score", 87),
        map_from("name", "Charlie", "score", 92),
        map_from("name", "Diana", "score", 78),
        map_from("name", "Eve", "score", 99)
    ]

    // Transform: get high scorers
    let high_scorers = records
        |> filter((r) => r["score"] > 90)
        |> map((r) => r["name"])

    assert_eq(len(high_scorers), 3)
    assert(high_scorers.contains("Alice"))
    assert(high_scorers.contains("Charlie"))
    assert(high_scorers.contains("Eve"))
}

test "end to end file processing" {
    // Write CSV manually
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"
    let path = "/tmp/tl_battle_100.csv"
    write_file(path, csv_content)

    // Read and verify
    let content = read_file(path)
    assert(content.contains("Alice"))
    assert(content.contains("Bob"))
    assert(content.contains("Charlie"))

    // Parse lines
    let lines = content.split("\n")
    assert_eq(len(lines), 4)  // header + 3 data lines
}

test "end to end computation" {
    // Fibonacci + map + filter + reduce
    fn fib(n) {
        if n <= 1 { n }
        else { fib(n-1) + fib(n-2) }
    }

    let fibs = range(10)
        |> map((n) => fib(n))
    let even_fibs = fibs |> filter((x) => x % 2 == 0)
    let total = even_fibs |> sum()

    // fib(0..9) = 0,1,1,2,3,5,8,13,21,34
    // even: 0,2,8,34 = 44
    assert_eq(total, 44)
}

test "end to end struct system" {
    struct Student { name: string, grade: int64 }
    impl Student {
        fn is_passing(self) { self.grade >= 60 }
        fn letter(self) {
            match self.grade {
                g if g >= 90 => "A",
                g if g >= 80 => "B",
                g if g >= 70 => "C",
                g if g >= 60 => "D",
                _ => "F"
            }
        }
    }

    let students = [
        Student { name: "Alice", grade: 95 },
        Student { name: "Bob", grade: 72 },
        Student { name: "Charlie", grade: 55 }
    ]

    assert_eq(students[0].letter(), "A")
    assert_eq(students[1].letter(), "C")
    assert_eq(students[2].letter(), "F")
    assert(students[0].is_passing())
    assert(not students[2].is_passing())
}
