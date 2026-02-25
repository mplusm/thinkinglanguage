// Data Engine Benchmark
// Run: python3 examples/benchmark_generate.py 1000000 /tmp/tl_benchmark.csv
// Then: cargo run --release -- run examples/data_06_benchmark.tl

let data = read_csv("/tmp/tl_benchmark.csv")

// Filter + aggregate + sort pipeline
data
    |> filter(age > 30)
    |> aggregate(by: department, avg_salary: avg(salary), headcount: count())
    |> sort(avg_salary, "desc")
    |> show()
