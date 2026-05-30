#!/usr/bin/env bash
# ThinkingLanguage — Apache Iceberg connector demo.
#
# Generates a small Iceberg table with pyiceberg, then reads it *natively* from
# ThinkingLanguage — no Spark, no Trino, no Python glue at query time.
#
#   bash examples/iceberg_demo.sh
#
# Requires the `iceberg` feature:
#   cargo build --release --features iceberg
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TL="${TL:-$ROOT/target/release/tl}"
VENV="${VENV:-/tmp/tl-iceberg-venv}"
DATA_ROOT="${DATA_ROOT:-/tmp/iceberg_demo}"

if [ ! -x "$TL" ]; then
  echo "tl binary not found at $TL — build it first:"
  echo "  cargo build --release --features iceberg"
  exit 1
fi

# One-time: isolated venv with pyiceberg so we can author a real Iceberg table.
if [ ! -x "$VENV/bin/python" ]; then
  echo "Setting up pyiceberg venv at $VENV ..."
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q "pyiceberg[sql-sqlite]" pyarrow
fi

echo "Generating sample Iceberg table ..."
META="$("$VENV/bin/python" "$ROOT/examples/iceberg_generate.py" "$DATA_ROOT" | tail -1)"
echo "  metadata: $META"
echo

export TL_ICEBERG_META="$META"
"$TL" run "$ROOT/examples/data_07_iceberg.tl"
