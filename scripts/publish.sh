#!/usr/bin/env bash
set -euo pipefail

# Publish all ThinkingLanguage crates to crates.io in topological order.
# Usage: ./scripts/publish.sh [--dry-run]

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
    echo "=== DRY RUN MODE ==="
fi

DELAY=30  # seconds between publishes for crates.io indexing

# Topological publish order (dependencies before dependents)
CRATES=(
    tl-errors
    tl-ast
    tl-lexer
    tl-parser
    thinkinglanguage-types
    tl-ir
    tl-package
    tl-ai
    tl-data
    tl-stream
    tl-lsp
    tl-gpu
    tl-compiler
    tl-interpreter
    thinkinglanguage
)

CLI_CARGO="crates/tl-cli/Cargo.toml"
CLI_CARGO_BAK="${CLI_CARGO}.bak"

cleanup() {
    if [[ -f "$CLI_CARGO_BAK" ]]; then
        echo "Restoring tl-cli Cargo.toml..."
        mv "$CLI_CARGO_BAK" "$CLI_CARGO"
    fi
}
trap cleanup EXIT

publish_crate() {
    local crate="$1"
    echo ""
    echo "=== Publishing $crate ==="

    # Before publishing tl-cli, patch out tl-llvm optional dependency
    if [[ "$crate" == "thinkinglanguage" ]]; then
        echo "Patching out tl-llvm dependency from tl-cli..."
        cp "$CLI_CARGO" "$CLI_CARGO_BAK"
        # Remove the tl-llvm dependency line
        sed -i '/^tl-llvm = /d' "$CLI_CARGO"
        # Remove the llvm-backend feature line
        sed -i '/^llvm-backend = /d' "$CLI_CARGO"
    fi

    if [[ -n "$DRY_RUN" ]]; then
        cargo publish --package "$crate" --dry-run --allow-dirty
    else
        cargo publish --package "$crate" --allow-dirty 2>&1 || echo "WARNING: $crate publish failed (may already exist), continuing..."
    fi

    # Restore tl-cli after publish
    if [[ "$crate" == "thinkinglanguage" && -f "$CLI_CARGO_BAK" ]]; then
        echo "Restoring tl-cli Cargo.toml..."
        mv "$CLI_CARGO_BAK" "$CLI_CARGO"
    fi
}

echo "Publishing ${#CRATES[@]} crates to crates.io"
echo "Order: ${CRATES[*]}"
echo ""

for i in "${!CRATES[@]}"; do
    crate="${CRATES[$i]}"
    publish_crate "$crate"

    # Wait for crates.io indexing between publishes (skip after last crate)
    if [[ -z "$DRY_RUN" && $i -lt $(( ${#CRATES[@]} - 1 )) ]]; then
        echo "Waiting ${DELAY}s for crates.io indexing..."
        sleep "$DELAY"
    fi
done

echo ""
echo "=== All crates published successfully! ==="
