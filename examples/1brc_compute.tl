// Inner compute script — called by 1brc_demo.sh which handles timing
let data = read_parquet("measurements.parquet")

let result = data
    |> aggregate(by: "station", min_t: min(temperature), avg_t: avg(temperature), max_t: max(temperature))
    |> sort("station")

result |> show()
