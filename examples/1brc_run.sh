#!/bin/bash
set -e

DATA=~/measurements.csv
TL=~/thinkinglanguage/target/release/tl
SCRIPT=~/thinkinglanguage/examples/1brc.tl
GEN=~/thinkinglanguage/examples/1brc_generate.py

ROWS=${1:-1000000000}

echo ""
echo "══════════════════════════════════════════════"
echo "  1 Billion Row Challenge — ThinkingLanguage"
echo "══════════════════════════════════════════════"
echo ""

# ── Step 1: Generate data ─────────────────────────
if [ -f "$DATA" ]; then
    SIZE=$(du -sh "$DATA" | cut -f1)
    echo "  Dataset already exists: $DATA ($SIZE)"
else
    echo "  Generating dataset ($ROWS rows)..."
    echo "  This takes ~3-5 minutes."
    echo ""
    time python3 "$GEN" "$ROWS" "$DATA"
fi

echo ""
SIZE=$(du -sh "$DATA" | cut -f1)
LINES=$(wc -l < "$DATA")
echo "  File: $DATA"
echo "  Size: $SIZE"
echo "  Rows: $((LINES - 1)) (excluding header)"
echo ""

# ── Step 2: Run ThinkingLanguage ─────────────────
echo "══════════════════════════════════════════════"
echo "  Running: ThinkingLanguage + DataFusion"
echo "══════════════════════════════════════════════"
echo ""

RUST_MIN_STACK=16777216 time "$TL" run "$SCRIPT"

# ── Step 3: DuckDB comparison ────────────────────
if command -v duckdb &>/dev/null; then
    echo ""
    echo "══════════════════════════════════════════════"
    echo "  Running: DuckDB (reference)"
    echo "══════════════════════════════════════════════"
    echo ""
    time duckdb -c "
        SELECT station,
               round(min(temperature),1) AS min,
               round(avg(temperature),1) AS mean,
               round(max(temperature),1) AS max
        FROM read_csv('$DATA', delim=';', header=true)
        GROUP BY station
        ORDER BY station
        LIMIT 10;
    "
fi
