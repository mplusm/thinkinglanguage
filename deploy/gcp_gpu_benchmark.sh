#!/usr/bin/env bash
# ThinkingLanguage GPU Benchmark — GCP Spot T4 Setup & Run
# Usage: bash deploy/gcp_gpu_benchmark.sh [create|run|ssh|delete]
#
# Prerequisites: gcloud authenticated as thinkingdbx@gmail.com
#                Project: thinkingmodel-tpu

set -euo pipefail

INSTANCE_NAME="tl-gpu-bench"
ZONE="us-central1-a"
MACHINE_TYPE="n1-standard-4"
GPU_TYPE="nvidia-tesla-t4"
GPU_COUNT=1
IMAGE_FAMILY="ubuntu-accelerator-2204-amd64-with-nvidia-580"
IMAGE_PROJECT="deeplearning-platform-release"
DISK_SIZE="50GB"
PROJECT="thinkingmodel-tpu"

CMD="${1:-help}"

# ── helpers ──────────────────────────────────────────────────────────────────

log()  { echo ""; echo ">>> $*"; }
ok()   { echo "    OK: $*"; }
fail() { echo "    ERROR: $*" >&2; exit 1; }

# ── create ───────────────────────────────────────────────────────────────────

do_create() {
    log "Creating spot T4 instance: $INSTANCE_NAME"

    gcloud compute instances create "$INSTANCE_NAME" \
        --project="$PROJECT" \
        --zone="$ZONE" \
        --machine-type="$MACHINE_TYPE" \
        --accelerator="type=$GPU_TYPE,count=$GPU_COUNT" \
        --image-family="$IMAGE_FAMILY" \
        --image-project="$IMAGE_PROJECT" \
        --boot-disk-size="$DISK_SIZE" \
        --boot-disk-type="pd-ssd" \
        --provisioning-model=SPOT \
        --instance-termination-action=STOP \
        --maintenance-policy=TERMINATE \
        --metadata="install-nvidia-driver=True" \
        --scopes="cloud-platform"

    ok "Instance created"
    log "Waiting 90s for drivers and boot to complete..."
    sleep 90

    log "Installing Rust + Vulkan on instance..."
    gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --project="$PROJECT" --command="
        set -e
        # Rust
        if ! command -v cargo &>/dev/null; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
        fi
        source \$HOME/.cargo/env

        # Vulkan ICD (wgpu needs this on Linux)
        sudo apt-get install -y -qq libvulkan1 mesa-vulkan-drivers vulkan-tools 2>/dev/null || true

        # Verify GPU + Vulkan
        nvidia-smi --query-gpu=name,memory.total --format=csv,noheader
        vulkaninfo --summary 2>/dev/null | grep 'GPU id' || echo '(vulkaninfo not available — wgpu will fall back to GL)'
        echo 'Setup complete'
    "
    ok "Dependencies installed"
}

# ── upload ───────────────────────────────────────────────────────────────────

do_upload() {
    log "Uploading ThinkingLanguage source to instance..."
    REPO_ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"

    # Pack source (exclude build artifacts)
    tar -czf /tmp/tl-source.tar.gz \
        --exclude="$REPO_ROOT/target" \
        --exclude="$REPO_ROOT/.git" \
        -C "$(dirname "$REPO_ROOT")" \
        "$(basename "$REPO_ROOT")"

    gcloud compute scp /tmp/tl-source.tar.gz \
        "$INSTANCE_NAME:/tmp/tl-source.tar.gz" \
        --zone="$ZONE" --project="$PROJECT"

    gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --project="$PROJECT" --command="
        mkdir -p ~/thinkinglanguage
        tar -xzf /tmp/tl-source.tar.gz -C ~/ 2>/dev/null || true
        echo 'Source uploaded'
    "
    ok "Source uploaded"
}

# ── build ────────────────────────────────────────────────────────────────────

do_build() {
    log "Building TL with GPU feature (this takes ~5 min first run)..."
    gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --project="$PROJECT" --command="
        set -e
        source \$HOME/.cargo/env
        cd ~/thinkinglanguage
        RUST_MIN_STACK=16777216 cargo build --release \
            -p tl-cli \
            --features gpu \
            2>&1 | tail -20
        echo 'Build complete'
        ls -lh target/release/tl
    "
    ok "Build complete"
}

# ── run ──────────────────────────────────────────────────────────────────────

do_run() {
    log "Running GPU benchmark on $INSTANCE_NAME..."
    gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --project="$PROJECT" --command="
        set -e
        source \$HOME/.cargo/env
        cd ~/thinkinglanguage
        RUST_MIN_STACK=16777216 ./target/release/tl examples/gpu_benchmark.tl
    "
}

# ── ssh ───────────────────────────────────────────────────────────────────────

do_ssh() {
    log "Opening SSH session to $INSTANCE_NAME..."
    gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --project="$PROJECT"
}

# ── delete ────────────────────────────────────────────────────────────────────

do_delete() {
    log "Deleting instance $INSTANCE_NAME..."
    gcloud compute instances delete "$INSTANCE_NAME" \
        --zone="$ZONE" --project="$PROJECT" --quiet
    ok "Instance deleted"
}

# ── status ────────────────────────────────────────────────────────────────────

do_status() {
    gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$ZONE" --project="$PROJECT" \
        --format="table(name,status,machineType,scheduling.provisioningModel)" 2>/dev/null \
        || echo "Instance not found"
}

# ── help / router ─────────────────────────────────────────────────────────────

case "$CMD" in
    create)  do_create ;;
    upload)  do_upload ;;
    build)   do_build ;;
    run)     do_run ;;
    ssh)     do_ssh ;;
    delete)  do_delete ;;
    status)  do_status ;;
    all)
        # Full setup in one shot: create → upload → build → run
        do_create
        do_upload
        do_build
        do_run
        ;;
    help|*)
        echo ""
        echo "Usage: bash deploy/gcp_gpu_benchmark.sh <command>"
        echo ""
        echo "Commands:"
        echo "  all      Full setup + run in one shot (create → upload → build → run)"
        echo "  create   Provision spot T4 VM + install dependencies"
        echo "  upload   Upload TL source to the instance"
        echo "  build    Build TL with --features gpu on the instance"
        echo "  run      Run gpu_benchmark.tl and stream output"
        echo "  ssh      Open interactive SSH session (for live screen recording)"
        echo "  status   Show instance status"
        echo "  delete   Tear down the instance"
        echo ""
        echo "Typical flow:"
        echo "  bash deploy/gcp_gpu_benchmark.sh all     # first time"
        echo "  bash deploy/gcp_gpu_benchmark.sh ssh     # for live demo / screen recording"
        echo "  bash deploy/gcp_gpu_benchmark.sh delete  # after demo"
        echo ""
        ;;
esac
