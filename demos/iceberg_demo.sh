#!/usr/bin/env bash
# ThinkingLanguage — Apache Iceberg Demo
# Run: bash demos/iceberg_demo.sh
#
# Showcases TL's native Apache Iceberg support: one-line reads, pipelines over
# lakehouse data, schema introspection, snapshot history, time-travel, column
# projection, and object-storage reads.
#
# Outputs are illustrative (mock) so the demo runs anywhere. Every code block is
# real, runnable TL — build with `cargo build --release --features iceberg` and
# point it at a real metadata.json (see examples/iceberg_demo.sh) to run live.

set -e

# ── colours (256-colour palette) ─────────────────────────────────────────────
BOLD='\033[1m'
DIM='\033[2m'
ITAL='\033[3m'
RESET='\033[0m'

PURPLE='\033[38;5;141m'
PINK='\033[38;5;213m'
HOT='\033[38;5;207m'
SKY='\033[38;5;117m'
TEAL='\033[38;5;80m'
MINT='\033[38;5;121m'
LIME='\033[38;5;154m'
GOLD='\033[38;5;220m'
CORAL='\033[38;5;209m'
LAVENDER='\033[38;5;189m'
SLATE='\033[38;5;245m'
INK='\033[38;5;238m'
PAPER='\033[38;5;254m'
SOFT='\033[38;5;244m'
ICE='\033[38;5;159m'      # iceberg cyan-white

BOLD_W='\033[1;97m'

pause() { sleep "${1:-1.5}"; }

typewrite() {
  local text="$1"; local delay="${2:-0.022}"; local i=0 len=${#text}
  while [ $i -lt $len ]; do printf '%s' "${text:$i:1}"; sleep "$delay"; i=$((i + 1)); done
  echo
}

# Syntax highlighting
KW='\033[38;5;177m'    # purple — keywords
KW2='\033[38;5;117m'   # sky    — declarations / iceberg builtins
STR='\033[38;5;151m'   # sage   — strings
OP='\033[38;5;213m'    # pink   — operators (|>, =>)
NUM='\033[38;5;215m'   # amber  — kept subtle
CMT='\033[38;5;244m'   # gray   — comments
RST='\033[0m'

hl_block() {
  local code="$1"
  printf '%s\n' "$code" | sed \
    -e "s|\(//.*\)$|$(printf "$CMT")\1$(printf "$RST")|" \
    -e "s/\"[^\"]*\"/$(printf "$STR")&$(printf "$RST")/g" \
    -e "s/\b\(read_iceberg\|iceberg\|iceberg_snapshots\|iceberg_schema\)\b/$(printf "$KW2")\1$(printf "$RST")/g" \
    -e "s/\b\(let\|mut\|if\|else\|for\|in\|while\|return\|fn\|columns\|snapshot_id\)\b/$(printf "$KW")\1$(printf "$RST")/g" \
    -e "s/|>/$(printf "$OP")|>$(printf "$RST")/g" \
    -e "s/=>/$(printf "$OP")=>$(printf "$RST")/g"
}

SECTION_NUM=0
section() {
  local title="$1"
  title="${title#[0-9] · }"; title="${title#[0-9][0-9] · }"
  SECTION_NUM=$((SECTION_NUM + 1))
  local num; num=$(printf '%02d' "$SECTION_NUM")
  local rule; rule=$(printf '─%.0s' {1..50})
  echo; echo
  echo -e "  ${PURPLE}${BOLD}┃${RESET}  ${ICE}${BOLD}${num}${RESET}   ${BOLD_W}${title}${RESET}"
  echo -e "  ${PURPLE}${BOLD}┃${RESET}  ${PURPLE}${rule}${RESET}"
  echo
  pause 1.2
}

step() {
  echo
  echo -e "  ${SKY}${BOLD}❯${RESET} ${PAPER}${ITAL}$1${RESET}"
  pause 0.7
}

show_code() {
  local code="$1"; local highlighted; highlighted=$(hl_block "$code")
  local total; total=$(printf '%s\n' "$code" | wc -l | tr -d ' ')
  local header_rule; header_rule=$(printf '─%.0s' {1..62})
  local n=1
  echo
  echo -e "  ${INK}╭──${RESET} ${TEAL}${BOLD}tl${RESET} ${INK}${header_rule}${RESET} ${SLATE}${total}L${RESET} ${INK}─╮${RESET}"
  while IFS= read -r ln; do
    printf "  ${INK}│${RESET} ${SLATE}%3d${RESET}  %b\n" "$n" "$ln"
    n=$((n + 1)); sleep 0.10
  done <<< "$highlighted"
  local foot_rule; foot_rule=$(printf '─%.0s' {1..74})
  echo -e "  ${INK}╰${foot_rule}╯${RESET}"
  pause 1.4
}

out_header() {
  echo; echo -e "  ${SLATE}▼ output${RESET}"; pause 0.4
}

# print a mock table line-by-line in mint
fake_table() {
  local txt="$1"
  while IFS= read -r ln; do echo -e "  ${MINT}${ln}${RESET}"; sleep 0.05; done <<< "$txt"
  pause 1.2
}

note() {
  echo -e "  ${SLATE}${ITAL}$1${RESET}"
  pause 0.6
}


# ═════════════════════════════════════════════════════════════════════════════
#  TITLE
# ═════════════════════════════════════════════════════════════════════════════

clear
echo; echo
echo -e "    ${ICE}        ❄        ${RESET}     ${PURPLE}████████╗${RESET}  ${PINK}██╗${RESET}"
echo -e "    ${ICE}     ╱╲ ╱╲ ╱╲     ${RESET}     ${PURPLE}╚══██╔══╝${RESET}  ${PINK}██║${RESET}        ${HOT}${BOLD}ThinkingLanguage${RESET}"
echo -e "    ${ICE}    ▔▔▔▔▔▔▔▔▔     ${RESET}        ${PURPLE}██║${RESET}     ${PINK}██║${RESET}"
echo -e "    ${SKY}   ░░░░░░░░░░░    ${RESET}        ${PURPLE}██║${RESET}     ${PINK}██║${RESET}        ${SLATE}native${RESET}"
echo -e "    ${SKY}  ░░░░░░░░░░░░░   ${RESET}        ${PURPLE}██║${RESET}     ${PINK}███████╗${RESET}   ${LAVENDER}${BOLD}Apache Iceberg${RESET}"
echo -e "    ${SLATE}   ░░░░░░░░░░░    ${RESET}       ${PURPLE}╚═╝${RESET}     ${PINK}╚══════╝${RESET}   ${GOLD}${BOLD}Lakehouse Demo${RESET}"
echo
echo -e "    ${INK}$(printf '━%.0s' {1..62})${RESET}"
echo -e "    ${SLATE}${ITAL}read · pipeline · schema · history · time-travel · projection${RESET}"
echo -e "    ${INK}$(printf '━%.0s' {1..62})${RESET}"
echo
echo -e "    ${SLATE}No Spark.  No Trino.  No JVM.  No Python glue.${RESET}"
echo
pause 3


# ═════════════════════════════════════════════════════════════════════════════
#  1 · ONE-LINE READ
# ═════════════════════════════════════════════════════════════════════════════

section "1 · Read an Iceberg table in one line"

step "Point read_iceberg() at a table's metadata.json — that's the whole integration"

CODE_1=$(cat <<'EOF'
// A catalog-less read: metadata + manifests + Parquet → a query-ready table
let orders = read_iceberg("/warehouse/sales/orders/metadata/v3.metadata.json")

orders |> show()
EOF
)
show_code "$CODE_1"
out_header
fake_table "+--------+-----------+-------+---------+
| region | product   | units | revenue |
+--------+-----------+-------+---------+
| NA     | Widget A  | 1240  | 62000   |
| EMEA   | Gadget X  | 2100  | 105000  |
| APAC   | Pro Suite | 560   | 168000  |
| NA     | Pro Suite | 720   | 216000  |
| LATAM  | Widget A  | 410   | 20500   |
+--------+-----------+-------+---------+"
note "One builtin. One binary. The Iceberg spec, read directly into the engine."


# ═════════════════════════════════════════════════════════════════════════════
#  2 · PIPELINE OVER LAKEHOUSE DATA
# ═════════════════════════════════════════════════════════════════════════════

section "2 · Pipe straight into a DataFusion query"

step "Iceberg data is a first-class table — use the same |> pipeline as any source"

CODE_2=$(cat <<'EOF'
let orders = read_iceberg("/warehouse/sales/orders/metadata/v3.metadata.json")

orders
    |> filter(revenue > 50000)
    |> aggregate(by: region, revenue: sum(revenue), units: sum(units))
    |> sort(revenue, "desc")
    |> show()
EOF
)
show_code "$CODE_2"
out_header
fake_table "+--------+---------+-------+
| region | revenue | units |
+--------+---------+-------+
| APAC   | 411000  | 2620  |
| NA     | 384000  | 4080  |
| EMEA   | 312000  | 4540  |
+--------+---------+-------+"
note "Powered by Apache DataFusion — full SQL-grade execution over Iceberg files."


# ═════════════════════════════════════════════════════════════════════════════
#  3 · SCHEMA INTROSPECTION
# ═════════════════════════════════════════════════════════════════════════════

section "3 · Inspect the table schema"

step "iceberg_schema() returns the current schema as a table you can query"

CODE_3=$(cat <<'EOF'
iceberg_schema("/warehouse/sales/orders/metadata/v3.metadata.json") |> show()
EOF
)
show_code "$CODE_3"
out_header
fake_table "+----------+---------+--------+----------+
| field_id | name    | type   | required |
+----------+---------+--------+----------+
| 1        | region  | string | true     |
| 2        | product | string | true     |
| 3        | units   | long   | true     |
| 4        | revenue | long   | true     |
+----------+---------+--------+----------+"
note "Field IDs are Iceberg's stable identity — they survive renames and reorders."


# ═════════════════════════════════════════════════════════════════════════════
#  4 · SNAPSHOT HISTORY
# ═════════════════════════════════════════════════════════════════════════════

section "4 · Walk the snapshot history"

step "Every write creates a snapshot — iceberg_snapshots() exposes the full log"

CODE_4=$(cat <<'EOF'
iceberg_snapshots("/warehouse/sales/orders/metadata/v3.metadata.json")
    |> show()
EOF
)
show_code "$CODE_4"
out_header
fake_table "+---------------------+---------------------+------------+-----------+----------------------+------------+
| snapshot_id         | parent_snapshot_id  | timestamp  | operation | summary              | is_current |
+---------------------+---------------------+------------+-----------+----------------------+------------+
| 8472033118273849001 |                     | 1748600100 | append    | added-records=8      | false      |
| 1903844820017562233 | 8472033118273849001 | 1748603700 | append    | added-records=3 ...  | false      |
| 4410927355128830947 | 1903844820017562233 | 1748687400 | overwrite | total-records=11 ... | true       |
+---------------------+---------------------+------------+-----------+----------------------+------------+"
note "Parent links form the lineage chain. 'is_current' marks the live snapshot."


# ═════════════════════════════════════════════════════════════════════════════
#  5 · TIME-TRAVEL
# ═════════════════════════════════════════════════════════════════════════════

section "5 · Time-travel to an older snapshot"

step "Pass a snapshot_id to read the table exactly as it was — pushed into the scan"

CODE_5=$(cat <<'EOF'
// Read the table as of the very first append (8 rows, before later writes).
// Second arg is the snapshot_id — time-travel, no map literal needed.
let v1 = read_iceberg(
    "/warehouse/sales/orders/metadata/v3.metadata.json",
    8472033118273849001
)

print("rows in snapshot 1: " + str(len(to_rows(v1))))
EOF
)
show_code "$CODE_5"
out_header
echo -e "  ${MINT}rows in snapshot 1: 8${RESET}"
pause 1.0
note "Same table, a point in the past — reproducible reads for audits & backfills."


# ═════════════════════════════════════════════════════════════════════════════
#  6 · COLUMN PROJECTION
# ═════════════════════════════════════════════════════════════════════════════

section "6 · Project only the columns you need"

step "Pass a column list — projection is pushed into the Iceberg scan, fewer bytes read"

CODE_6=$(cat <<'EOF'
// Only region + revenue are read off disk; product/units are never touched.
let slim = read_iceberg(
    "/warehouse/sales/orders/metadata/v3.metadata.json",
    ["region", "revenue"]
)

slim |> aggregate(by: region, revenue: sum(revenue)) |> show()
EOF
)
show_code "$CODE_6"
out_header
fake_table "+--------+---------+
| region | revenue |
+--------+---------+
| APAC   | 411000  |
| NA     | 384000  |
| EMEA   | 312000  |
| LATAM  | 20500   |
+--------+---------+"
note "Projection is pushed down — the 'product'/'units' columns are never scanned."


# ═════════════════════════════════════════════════════════════════════════════
#  7 · OBJECT STORAGE
# ═════════════════════════════════════════════════════════════════════════════

section "7 · Read straight from object storage"

step "s3:// and gs:// just work — the storage scheme is inferred from the URL"

CODE_7=$(cat <<'EOF'
// Object-store credentials/region come from the environment (AWS_REGION,
// AWS_ACCESS_KEY_ID, …) — the same metadata.json read, now over S3.
let orders = read_iceberg(
    "s3://lake/warehouse/sales/orders/metadata/v3.metadata.json"
)

orders |> aggregate(by: region, revenue: sum(revenue)) |> show()
EOF
)
show_code "$CODE_7"
out_header
echo -e "  ${SLATE}connecting to s3://lake … reading metadata + manifests …${RESET}"
pause 1.0
fake_table "+--------+---------+
| region | revenue |
+--------+---------+
| APAC   | 411000  |
| NA     | 384000  |
| EMEA   | 312000  |
| LATAM  | 20500   |
+--------+---------+"
note "Same call, any backend — the storage scheme is inferred from the URL."


# ═════════════════════════════════════════════════════════════════════════════
#  8 · HOW IT WORKS
# ═════════════════════════════════════════════════════════════════════════════

section "8 · Why this is fast (and small)"

echo -e "  ${PAPER}The standard Iceberg-on-Rust path (${ICE}iceberg-datafusion${PAPER}) tracks${RESET}"
echo -e "  ${PAPER}DataFusion 52. ThinkingLanguage runs DataFusion 44.${RESET}"
echo
pause 1.2
echo -e "  ${SLATE}Instead of an 8-version engine upgrade, TL uses the core ${ICE}iceberg${SLATE} crate,${RESET}"
echo -e "  ${SLATE}which emits the ${BOLD}exact Arrow version${RESET}${SLATE} the engine already speaks:${RESET}"
echo
pause 1.0
echo -e "    ${ICE}Iceberg table${RESET}  ${SLATE}──▶${RESET}  ${MINT}Arrow RecordBatch (v53)${RESET}  ${SLATE}──▶${RESET}  ${GOLD}DataFusion 44${RESET}"
echo -e "    ${SLATE}                          ${BOLD}no copy · no IPC bridge · no JVM${RESET}${RESET}"
echo
pause 2


# ═════════════════════════════════════════════════════════════════════════════
#  OUTRO
# ═════════════════════════════════════════════════════════════════════════════

echo; echo
echo -e "  ${INK}$(printf '━%.0s' {1..70})${RESET}"
echo -e "  ${ICE}${BOLD}  ❄ ThinkingLanguage  ×  Apache Iceberg  ${SLATE}${ITAL}— quick reference${RESET}"
echo -e "  ${INK}$(printf '━%.0s' {1..70})${RESET}"
echo
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "read_iceberg(meta)"                          "one-line table read"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "read_iceberg(meta, [\"col\", ...])"            "projection pushdown"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "read_iceberg(meta, snapshot_id)"             "time-travel"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "read_iceberg(meta, [\"col\"], snapshot_id)"    "projection + time-travel"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "read_iceberg(\"s3://...\")"                     "object-storage read"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "iceberg_schema(meta)"                        "schema as a table"
printf "    ${SKY}${BOLD}%-46s${RESET} ${PAPER}%s${RESET}\n" "iceberg_snapshots(meta)"                     "snapshot history"
echo
echo -e "  ${INK}$(printf '━%.0s' {1..70})${RESET}"
echo -e "    ${SLATE}build:${RESET}  ${MINT}cargo build --release --features iceberg${RESET}"
echo -e "    ${SLATE}live:${RESET}   ${MINT}bash examples/iceberg_demo.sh${RESET}   ${SLATE}(generates a real table, runs it)${RESET}"
echo -e "  ${INK}$(printf '━%.0s' {1..70})${RESET}"
echo
