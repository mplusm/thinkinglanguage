test "data processing pipeline" {
    // Simulate processing records
    let records = [10, 25, 5, 30, 15, 20, 35, 8, 12, 28]
    let result = records
        .filter((x) => x >= 10)
        .map((x) => x * 2)
        .filter((x) => x <= 60)
        .reduce(0, (acc, x) => acc + x)
    // values >= 10: [10,25,30,15,20,35,12,28] -> *2: [20,50,60,30,40,70,24,56] -> <=60: [20,50,60,30,40,24,56]
    assert_eq(result, 280)
}

test "configuration map" {
    let config = map_from(
        "host", "localhost",
        "port", 8080,
        "debug", true,
        "name", "myapp"
    )
    assert_eq(config["host"], "localhost")
    assert_eq(config["port"], 8080)
    assert(config["debug"])
}

test "fibonacci with memoization" {
    let mut cache = map_from("0", 0, "1", 1)
    fn fib_memo(n) {
        if n <= 1 { return n }
        let mut a = 0
        let mut b = 1
        let mut i = 2
        while i <= n {
            let temp = b
            b = a + b
            a = temp
            i = i + 1
        }
        b
    }
    assert_eq(fib_memo(20), 6765)
    assert_eq(fib_memo(30), 832040)
}

test "word frequency counter" {
    let words = "the cat sat on the mat the cat".split(" ")
    let mut the_count = 0
    for word in words {
        if word == "the" {
            the_count = the_count + 1
        }
    }
    assert_eq(the_count, 3)
}

test "binary search" {
    fn binary_search(lst, target) {
        let mut lo = 0
        let mut hi = len(lst) - 1
        while lo <= hi {
            let mid = (lo + hi) / 2
            if lst[mid] == target { return mid }
            if lst[mid] < target {
                lo = mid + 1
            } else {
                hi = mid - 1
            }
        }
        return -1
    }
    let sorted = [2, 5, 8, 12, 16, 23, 38, 56, 72, 91]
    assert_eq(binary_search(sorted, 23), 5)
    assert_eq(binary_search(sorted, 99), -1)
    assert_eq(binary_search(sorted, 2), 0)
}

test "state machine" {
    let mut state = "idle"
    let events = ["start", "process", "complete", "reset"]
    for event in events {
        if state == "idle" and event == "start" {
            state = "running"
        } else if state == "running" and event == "process" {
            state = "processing"
        } else if state == "processing" and event == "complete" {
            state = "done"
        } else if state == "done" and event == "reset" {
            state = "idle"
        }
    }
    assert_eq(state, "idle")
}
