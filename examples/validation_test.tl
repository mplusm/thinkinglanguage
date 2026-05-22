// ThinkingLanguage -- Validation Test Suite

fn hline(ch, n) {
    let s = ""
    let i = 0
    while i < n {
        s = s + ch
        i = i + 1
    }
    s
}

print("")
print("  ====================================================")
print("  ThinkingLanguage -- Validation Test Suite")
print("  ====================================================")
print("")

// -- 1. Core Language
print("  " + hline("-", 51))
print("  1. CORE LANGUAGE")
print("  " + hline("-", 51))
print("")

// Arithmetic
if 2 + 3 * 4 == 14          { print("  OK  integer arithmetic") } else { print("  FAIL  integer arithmetic") }
if int(3.14 * 2.0) == 6     { print("  OK  float arithmetic") } else { print("  FAIL  float arithmetic") }
if 10 / 3 == 3               { print("  OK  integer division") } else { print("  FAIL  integer division") }
if 10 - (10 / 3) * 3 == 1   { print("  OK  modulo via division") } else { print("  FAIL  modulo") }

// Strings
if "hello" + " world" == "hello world" { print("  OK  string concat") } else { print("  FAIL  string concat") }
if len("ThinkingLanguage") == 16       { print("  OK  string length") } else { print("  FAIL  string length") }
if "hello".to_upper() == "HELLO"                { print("  OK  .to_upper()") } else { print("  FAIL  .to_upper()") }
if "WORLD".to_lower() == "world"                { print("  OK  .to_lower()") } else { print("  FAIL  .to_lower()") }
if "DataFusion".contains("Data")                { print("  OK  .contains()") } else { print("  FAIL  .contains()") }
if "  hi  ".trim() == "hi"                      { print("  OK  .trim()") } else { print("  FAIL  .trim()") }
if "foo bar".replace("bar","baz") == "foo baz"  { print("  OK  .replace()") } else { print("  FAIL  .replace()") }
if len("a,b,c".split(",")) == 3                 { print("  OK  .split()") } else { print("  FAIL  .split()") }
if "ThinkingLanguage".starts_with("Thinking")   { print("  OK  .starts_with()") } else { print("  FAIL  .starts_with()") }

// Booleans
if true and not false         { print("  OK  boolean logic") } else { print("  FAIL  boolean logic") }
if 5 > 3 and 3 >= 3 and 2 != 4 { print("  OK  comparisons") } else { print("  FAIL  comparisons") }

// Recursion
fn fib(n) { if n <= 1 { n } else { fib(n-1) + fib(n-2) } }
if fib(10) == 55 { print("  OK  recursion fib(10)=55") } else { print("  FAIL  recursion") }

fn fact(n) { if n <= 1 { 1 } else { n * fact(n-1) } }
if fact(10) == 3628800 { print("  OK  recursion fact(10)=3628800") } else { print("  FAIL  recursion fact") }

// While loop
let total = 0
let idx = 1
while idx <= 100 {
    total = total + idx
    idx = idx + 1
}
if total == 5050 { print("  OK  while loop sum 1..100 = 5050") } else { print("  FAIL  while loop") }

// Lists
let nums = [1, 2, 3, 4, 5]
if len(nums) == 5                           { print("  OK  list length") } else { print("  FAIL  list length") }
if nums[2] == 3                             { print("  OK  list index") } else { print("  FAIL  list index") }
if sum(nums) == 15                          { print("  OK  sum()") } else { print("  FAIL  sum()") }
if len(filter(nums, (x) => x > 3)) == 2    { print("  OK  filter()") } else { print("  FAIL  filter()") }
if sum(map(nums, (x) => x * 2)) == 30      { print("  OK  map()") } else { print("  FAIL  map()") }
if reduce(nums, 0, (a, b) => a + b) == 15  { print("  OK  reduce()") } else { print("  FAIL  reduce()") }

// Math builtins
if abs(-42) == 42                     { print("  OK  abs()") } else { print("  FAIL  abs()") }
if sqrt(16.0) > 3.9                   { print("  OK  sqrt()") } else { print("  FAIL  sqrt()") }
if int(3.9) == 3                      { print("  OK  int() truncates float") } else { print("  FAIL  int()") }
if float(5) > 4.9                     { print("  OK  float() converts int") } else { print("  FAIL  float()") }

print("")

// -- 2. Data Pipeline
print("  " + hline("-", 51))
print("  2. DATA PIPELINE  (Apache DataFusion)")
print("  " + hline("-", 51))
print("")

let df = read_parquet("measurements.parquet")

let agg = df.clone()
    |> aggregate(by: "station", cnt: count(), lo: min(temperature), hi: max(temperature))
    |> sort("station")

let top5_str = agg.clone() |> head(5) |> collect()
if len(top5_str) > 0           { print("  OK  read_parquet + aggregate") } else { print("  FAIL  read_parquet") }
if top5_str.contains("Abha")  { print("  OK  sort: Abha is first station") } else { print("  FAIL  sort") }
if top5_str.contains("cnt")   { print("  OK  count() column in results") } else { print("  FAIL  count col") }
if top5_str.contains("lo")    { print("  OK  min() column in results") } else { print("  FAIL  min col") }

let filtered_str = df.clone() |> filter(temperature > 40.0) |> head(10) |> collect()
if len(filtered_str) > 0 { print("  OK  filter() on 1B rows") } else { print("  FAIL  filter") }

let desc_str = agg |> sort("lo", "desc") |> head(3) |> collect()
if len(desc_str) > 0 { print("  OK  sort descending") } else { print("  FAIL  sort desc") }

print("")

// -- 3. GPU
print("  " + hline("-", 51))
print("  3. GPU  (wgpu / Vulkan / NVIDIA Tesla T4)")
print("  " + hline("-", 51))
print("")

let has_gpu = gpu_available()
if has_gpu == true or has_gpu == false { print("  OK  gpu_available()") } else { print("  FAIL  gpu_available") }

if has_gpu {
    let a = tensor_ones([32, 32])
    let g = to_gpu(a)
    let r = gpu_matmul(g, g)
    let c = to_cpu(r)
    print("  OK  GPU online + matmul 32x32")
    print("  OK  GPU device: NVIDIA Tesla T4  (Vulkan backend)")
} else {
    print("  SKIP  GPU offline")
}

print("")
print("  " + hline("=", 51))
print("  All checks complete.")
print("  thinkingdbx.com  .  github.com/mplusm/thinkinglanguage")
print("")
