// ThinkingLanguage — 1 Billion Row Challenge (GPU Edition)
//
// Workload: batch ML inference — each "batch" is a 2048×2048 matmul
//           representing 2048 samples × 2048 features → predictions.
//           We measure throughput then extrapolate to 1 billion rows.

fn hline(ch, n) {
    let s = ""
    let i = 0
    while i < n { s = s + ch    i = i + 1 }
    s
}

fn progress_bar(done, total, width) {
    let filled = done * width / total
    let b = ""
    let i = 0
    while i < filled { b = b + "█"    i = i + 1 }
    while i < width  { b = b + "░"    i = i + 1 }
    b
}

fn rows_per_sec(batches, batch_rows, seconds) {
    if seconds == 0 { return 0 }
    batches * batch_rows / seconds
}

fn secs_for_billion(rps) {
    if rps == 0 { return 999999 }
    1000000000 / rps
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

fn fmt_millions(n) {
    if n >= 1000000000 {
        str(n / 1000000000) + "." + str((n / 100000) - (n / 1000000000) * 10000) + "B"
    } else if n >= 1000000 {
        str(n / 1000000) + "M"
    } else if n >= 1000 {
        str(n / 1000) + "K"
    } else {
        str(n)
    }
}

// ── Header ────────────────────────────────────────────────────

print("")
print("  ╔══════════════════════════════════════════════════╗")
print("  ║                                                  ║")
print("  ║   1  B I L L I O N  R O W  C H A L L E N G E   ║")
print("  ║       ThinkingLanguage  ×  NVIDIA Tesla T4       ║")
print("  ║                                                  ║")
print("  ╚══════════════════════════════════════════════════╝")
print("  thinkingdbx.com  ·  github.com/mplusm/thinkinglanguage")
print("")
print("  Workload  : Batch ML inference  (2048 samples x 2048 features = 4M pts/batch)")
print("  Operation : Matrix multiply -- the core of every neural network")
print("  Goal      : How fast can we score 1,000,000,000 data points?")
print("")

let has_gpu = gpu_available()
let batch_rows  = 2048
let row_size    = 2048
let points_per_batch = batch_rows * row_size   // 4,194,304 data points per matmul

if has_gpu {
    print("  ┌─────────────────────────────────────────────────┐")
    print("  │  GPU  ████████████████████  ONLINE              │")
    print("  │  NVIDIA Tesla T4  ·  16 GB  ·  320 GB/s BW      │")
    print("  └─────────────────────────────────────────────────┘")
} else {
    print("  GPU: OFFLINE — running CPU baseline only")
}

print("")
print("  " + hline("─", 51))

// ── Phase 1: CPU throughput ───────────────────────────────────

print("")
print("  PHASE 1 — Measuring CPU throughput")
print("  " + hline("·", 51))
print("")

let cpu_batches = 10
let mat = tensor_ones([batch_rows, row_size])

print("  Running " + str(cpu_batches) + " batches × " + str(batch_rows) + "×" + str(row_size) + " matmul on CPU...")
print("")

let cpu_start = now()
let ci = 0
while ci < cpu_batches {
    let _r = tensor_dot(mat, mat)
    ci = ci + 1
    let pbar = progress_bar(ci, cpu_batches, 30)
    print("  CPU  [" + pbar + "]  batch " + str(ci) + "/" + str(cpu_batches))
}
let cpu_s = date_diff(now(), cpu_start, "second")

let cpu_rps   = rows_per_sec(cpu_batches, points_per_batch, cpu_s)
let cpu_1b_s  = secs_for_billion(cpu_rps)

print("")
print("  ✓ CPU benchmark complete")
if cpu_s == 0 {
    print("    Time        : <1s  (need more batches for precise rate)")
    print("    Note        : Extrapolation not available — rerun with larger batch count")
} else {
    print("    Batches     : " + str(cpu_batches))
    print("    Time        : " + str(cpu_s) + "s")
    print("    Throughput  : " + fmt_millions(cpu_rps) + " rows/sec")
    print("    1B rows ETA : " + fmt_time(cpu_1b_s))
}

// ── Phase 2: GPU throughput ───────────────────────────────────

if has_gpu {
    print("")
    print("  " + hline("─", 51))
    print("")
    print("  PHASE 2 — Measuring GPU throughput")
    print("  " + hline("·", 51))
    print("")

    let gpu_batches = 1000
    let gmat = to_gpu(mat)

    print("  Running " + str(gpu_batches) + " batches × " + str(batch_rows) + "×" + str(row_size) + " matmul on GPU...")
    print("")

    let gpu_start = now()
    let gi = 0
    while gi < gpu_batches {
        let _r = gpu_matmul(gmat, gmat)
        gi = gi + 1
        if gi == 1 or gi == 250 or gi == 500 or gi == 750 or gi == 1000 {
            let gpbar = progress_bar(gi, gpu_batches, 30)
            print("  GPU  [" + gpbar + "]  batch " + str(gi) + "/" + str(gpu_batches))
        }
    }
    let gpu_s = date_diff(now(), gpu_start, "second")

    let gpu_rps   = rows_per_sec(gpu_batches, points_per_batch, gpu_s)
    let gpu_1b_s  = secs_for_billion(gpu_rps)

    print("")
    print("  ✓ GPU benchmark complete")
    if gpu_s == 0 {
        let _lb_speedup = cpu_1b_s
        print("    Batches     : " + str(gpu_batches))
        print("    Time        : <1s  (all " + str(gpu_batches) + " batches in under a second)")
        print("    1B rows ETA : well under " + fmt_time(cpu_1b_s / gpu_batches * cpu_batches))
    } else {
        print("    Batches     : " + str(gpu_batches))
        print("    Time        : " + str(gpu_s) + "s")
        print("    Throughput  : " + fmt_millions(gpu_rps) + " rows/sec")
        print("    1B rows ETA : " + fmt_time(gpu_1b_s))
    }

    // ── Head-to-head ─────────────────────────────────────────

    print("")
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║   HEAD-TO-HEAD  —  1,000,000,000 ROWS           ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print("")

    if cpu_s == 0 {
        print("  CPU result too fast to extrapolate — increase cpu_batches")
    } else if gpu_s == 0 {
        let speedup_lb = cpu_s * gpu_batches / cpu_batches
        let gpu0_eta = fmt_time(cpu_1b_s)

        print("  CPU  to process 1B rows: " + gpu0_eta)
        print("  GPU  to process 1B rows: seconds  (extrapolated)")
        print("")
        print("  Speedup  : AT LEAST " + str(speedup_lb) + "x")
        print("")
        print("  ┌─────────────────────────────────────────────────┐")
        print("  │  CPU  " + hline("█", 10) + hline("░", 30) + "  " + gpu0_eta + "  │")
        print("  │  GPU  " + hline("█", 1)  + hline("░", 39) + "  <1s          │")
        print("  └─────────────────────────────────────────────────┘")
        print("")
        print("  Winner ►  NVIDIA Tesla T4  (" + str(speedup_lb) + "x+ faster)")
    } else {
        let cpu_eta = fmt_time(cpu_1b_s)
        let gpu_eta = fmt_time(gpu_1b_s)
        let sx10    = int(float(cpu_1b_s) / float(gpu_1b_s) * 10.0)
        let sw      = sx10 / 10
        let sd      = sx10 - sw * 10

        let _bar_max = cpu_1b_s
        let cpu_w    = 40
        let gpu_w   = gpu_1b_s * 40 / cpu_1b_s
        if gpu_w < 1 { gpu_w = 1 }

        print("  Extrapolated time to process 1,000,000,000 rows:")
        print("")
        print("  CPU  [" + hline("█", cpu_w) + hline("░", 40 - cpu_w) + "]  " + cpu_eta)
        print("  GPU  [" + hline("█", gpu_w) + hline("░", 40 - gpu_w) + "]  " + gpu_eta)
        print("")
        print("  Speedup : " + str(sw) + "." + str(sd) + "x faster on Tesla T4")
        print("")
        print("  ┌─────────────────────────────────────────────────┐")
        print("  │  CPU throughput : " + fmt_millions(cpu_rps) + " rows/sec              │")
        print("  │  GPU throughput : " + fmt_millions(gpu_rps) + " rows/sec              │")
        print("  │  Winner ►  NVIDIA Tesla T4                      │")
        print("  └─────────────────────────────────────────────────┘")
    }
}

print("")
print("  " + hline("═", 51))
print("  thinkingdbx                https://thinkingdbx.com")
print("  ThinkingLanguage           https://github.com/mplusm/thinkinglanguage")
print("  " + hline("─", 51))
print("  Stack : ThinkingLanguage 0.3.1  ·  Apache DataFusion  ·  wgpu 24")
print("  Build : Rust  ·  Vulkan backend  ·  WGSL compute shaders")
print("")
