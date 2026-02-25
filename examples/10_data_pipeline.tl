// 10 — Data Pipeline
// Demonstrates: all features combined in a realistic scenario

// Simulated dataset: student scores
let scores = [95, 82, 67, 91, 45, 78, 88, 55, 73, 96]

print("=== Student Score Analysis ===")
print("All scores: " + str(scores))

// Compute statistics using builtins and closures
let total = sum(scores)
let count = len(scores)
let average = total / count
print("Total: " + str(total))
print("Count: " + str(count))
print("Average: " + str(average))

// Find passing and failing scores
let passing = scores |> filter((s) => s >= 60)
let failing = scores |> filter((s) => s < 60)
print("Passing (" + str(len(passing)) + "): " + str(passing))
print("Failing (" + str(len(failing)) + "): " + str(failing))

// Grade using case expression
fn grade(score: int64) -> string {
    case {
        score >= 90 => "A",
        score >= 80 => "B",
        score >= 70 => "C",
        score >= 60 => "D",
        _ => "F"
    }
}

// Grade each score
let grades = scores |> map((s) => grade(s))
print("Grades: " + str(grades))

// Curve scores: add 5 to each, cap at 100
let curved = scores |> map((s) => min(s + 5, 100))
print("Curved: " + str(curved))

// Check if any perfect scores
let has_perfect = any(scores, (s) => s == 100)
print("Any perfect scores? " + str(has_perfect))

// Check if all passing
let all_pass = all(scores, (s) => s >= 60)
print("All passing? " + str(all_pass))

// Pipeline: get sum of passing curved scores
let curved_passing = scores
    |> map((s) => min(s + 5, 100))
    |> filter((s) => s >= 60)
let curved_sum = sum(curved_passing)
let curved_avg = curved_sum / len(curved_passing)
print("Average of passing curved scores: " + str(curved_avg))
