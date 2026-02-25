// 05 — Match Expressions
// Demonstrates: match with int/string patterns, _ wildcard

fn describe_number(n: int64) -> string {
    match n {
        0 => "zero",
        1 => "one",
        2 => "two",
        3 => "three",
        _ => "many"
    }
}

for i in range(6) {
    print(str(i) + " is " + describe_number(i))
}

// Match on strings
let lang = "TL"
let description = match lang {
    "Python" => "interpreted",
    "Rust" => "compiled",
    "TL" => "the future",
    _ => "unknown"
}
print("{lang} is {description}")

// Match for HTTP status
fn status_text(code: int64) -> string {
    match code {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown Status"
    }
}

print("200: " + status_text(200))
print("404: " + status_text(404))
print("418: " + status_text(418))
