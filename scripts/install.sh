#!/bin/sh
# Shell installer for ThinkingLanguage (tl)
# Usage: curl -sSf https://raw.githubusercontent.com/mplusm/thinkinglanguage/main/scripts/install.sh | sh
# Options:
#   VERSION=v0.1.0  - install a specific version (default: latest)
#   INSTALL_DIR=... - install to a custom directory (default: ~/.local/bin)
set -eu

REPO="mplusm/thinkinglanguage"
BINARY_NAME="tl"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

say() {
    printf '%s\n' "$@"
}

err() {
    say "error: $*" >&2
    exit 1
}

detect_platform() {
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "$OS" in
        Linux)
            case "$ARCH" in
                x86_64)  TARGET="x86_64-unknown-linux-gnu" ;;
                *)       err "unsupported Linux architecture: $ARCH. Download manually from https://github.com/$REPO/releases" ;;
            esac
            ;;
        Darwin)
            case "$ARCH" in
                arm64|aarch64) TARGET="aarch64-apple-darwin" ;;
                *)             err "unsupported macOS architecture: $ARCH. Download manually from https://github.com/$REPO/releases" ;;
            esac
            ;;
        *)
            err "unsupported OS: $OS. Download manually from https://github.com/$REPO/releases"
            ;;
    esac

    ARCHIVE="tl-${TARGET}.tar.gz"
}

detect_downloader() {
    if command -v curl > /dev/null 2>&1; then
        DOWNLOADER="curl"
    elif command -v wget > /dev/null 2>&1; then
        DOWNLOADER="wget"
    else
        err "need curl or wget installed"
    fi
}

download() {
    url="$1"
    output="$2"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl -sSfL -o "$output" "$url"
    else
        wget -qO "$output" "$url"
    fi
}

get_latest_version() {
    if [ -n "${VERSION:-}" ]; then
        return
    fi

    say "Fetching latest version..."

    # Use GitHub API redirect to avoid rate limits
    if [ "$DOWNLOADER" = "curl" ]; then
        VERSION="$(curl -sSfI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest" | rev | cut -d'/' -f1 | rev)"
    else
        VERSION="$(wget --spider -S -O /dev/null "https://github.com/$REPO/releases/latest" 2>&1 | grep -i 'Location:' | tail -1 | rev | cut -d'/' -f1 | rev | tr -d '[:space:]')"
    fi

    if [ -z "$VERSION" ]; then
        err "could not determine latest version. Set VERSION manually, e.g.: VERSION=v0.1.0"
    fi

    say "Latest version: $VERSION"
}

detect_sha_cmd() {
    if command -v sha256sum > /dev/null 2>&1; then
        SHA_CMD="sha256sum"
    elif command -v shasum > /dev/null 2>&1; then
        SHA_CMD="shasum -a 256"
    else
        SHA_CMD=""
    fi
}

download_and_verify() {
    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR"' EXIT

    DOWNLOAD_URL="https://github.com/$REPO/releases/download/${VERSION}/${ARCHIVE}"
    CHECKSUMS_URL="https://github.com/$REPO/releases/download/${VERSION}/sha256sums.txt"

    say "Downloading $ARCHIVE..."
    download "$DOWNLOAD_URL" "$TMPDIR/$ARCHIVE"

    detect_sha_cmd
    if [ -n "$SHA_CMD" ]; then
        say "Verifying checksum..."
        download "$CHECKSUMS_URL" "$TMPDIR/sha256sums.txt"
        EXPECTED="$(grep "$ARCHIVE" "$TMPDIR/sha256sums.txt" | awk '{print $1}')"
        if [ -z "$EXPECTED" ]; then
            say "warning: checksum not found for $ARCHIVE, skipping verification"
        else
            ACTUAL="$($SHA_CMD "$TMPDIR/$ARCHIVE" | awk '{print $1}')"
            if [ "$EXPECTED" != "$ACTUAL" ]; then
                err "checksum mismatch! Expected $EXPECTED, got $ACTUAL"
            fi
            say "Checksum verified."
        fi
    else
        say "warning: sha256sum/shasum not found, skipping checksum verification"
    fi
}

install_binary() {
    say "Extracting..."
    tar xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"

    mkdir -p "$INSTALL_DIR"
    mv "$TMPDIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    chmod +x "$INSTALL_DIR/$BINARY_NAME"

    say "Installed $BINARY_NAME to $INSTALL_DIR/$BINARY_NAME"
}

print_path_hint() {
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            say ""
            say "Add $INSTALL_DIR to your PATH:"
            say ""
            say "  export PATH=\"$INSTALL_DIR:\$PATH\""
            say ""
            say "Add that line to ~/.bashrc or ~/.zshrc to make it permanent."
            ;;
    esac
}

main() {
    say "Installing ThinkingLanguage ($BINARY_NAME)..."
    say ""
    detect_platform
    detect_downloader
    get_latest_version
    download_and_verify
    install_binary
    say ""
    say "Done! Run 'tl shell' to start the REPL."
    print_path_hint
}

main
