// 1 Billion Row Challenge — ThinkingLanguage
// Read measurements.csv, compute min/mean/max per station, print sorted.

fn hline(ch, n) {
    let s = ""
    let i = 0
    while i < n { s = s + ch    i = i + 1 }
    s
}

fn fmt_time(s) {
    if s < 60 {
        str(s) + "s"
    } else if s < 3600 {
        str(s / 60) + "m " + str(s - (s / 60) * 60) + "s"
    } else {
        str(s / 3600) + "h " + str((s - (s / 3600) * 3600) / 60) + "m"
    }
}

print("")
print("  ╔══════════════════════════════════════════════════╗")
print("  ║                                                  ║")
print("  ║   1  B I L L I O N  R O W  C H A L L E N G E   ║")
print("  ║         ThinkingLanguage  ×  DataFusion          ║")
print("  ║                                                  ║")
print("  ╚══════════════════════════════════════════════════╝")
print("  thinkingdbx.com  ·  github.com/mplusm/thinkinglanguage")
print("")
print("  Task   : group 1,000,000,000 weather readings")
print("  Goal   : min / mean / max temperature per station")
print("  Engine : Apache DataFusion (columnar, parallel, 4 cores)")
print("")
print("  " + hline("─", 51))

// ── Show the code ────────────────────────────────────────
print("")
print("  THE CODE")
print("  " + hline("·", 51))
print("")
print("  ┌─────────────────────────────────────────────────┐")
print("  │                                                 │")
print("  │  let data = read_parquet(\"measurements.parquet\") │")
print("  │                                                 │")
print("  │  let result = data                              │")
print("  │      |> aggregate(                              │")
print("  │           by: \"station\",                        │")
print("  │           min_t: min(temperature),              │")
print("  │           avg_t: avg(temperature),              │")
print("  │           max_t: max(temperature))              │")
print("  │      |> sort(\"station\")                         │")
print("  │                                                 │")
print("  │  result |> show()                               │")
print("  │                                                 │")
print("  └─────────────────────────────────────────────────┘")
print("")
print("  That's it. 6 lines. Let's run it.")
print("")
print("  " + hline("─", 51))
print("")

// ── Execute ──────────────────────────────────────────────
let file = "measurements.parquet"

print("  ► tl run 1brc.tl")
print("")
print("  Reading  : measurements.parquet  (1B rows)")
print("  Grouping : by station name")
print("  Computing: min / mean / max temperature")
print("")
print("  Running ...")
print("")

let t_start = now()

let data = read_parquet(file)

let result = data
    |> aggregate(by: "station", min_t: min(temperature), avg_t: avg(temperature), max_t: max(temperature))
    |> sort("station")

result |> show()

let t_end = now()
let elapsed = date_diff(t_end, t_start, "second")

print("")
print("  " + hline("─", 51))
print("")
print("  ✓ Done in " + str(elapsed) + "s  —  1,000,000,000 rows processed")
print("")
