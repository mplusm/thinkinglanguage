# Installation

This guide covers how to install ThinkingLanguage (TL) from source and configure optional features.

## Prerequisites

- **Rust toolchain** 1.85 or later (required for Rust edition 2024)
- **Cargo** (included with the Rust toolchain)

Install Rust via [rustup](https://rustup.rs/):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Verify your Rust version:

```sh
rustc --version   # must be 1.85+
```

## Install from Source

Clone the repository and install the `tl` binary from the `tl-cli` crate:

```sh
git clone https://github.com/anthropic/thinkinglanguage.git
cd thinkinglanguage
cargo install --path crates/tl-cli
```

This places the `tl` binary in `~/.cargo/bin/`, which should already be on your PATH if you installed Rust via rustup.

Alternatively, build the entire workspace:

```sh
cargo build --release
```

The binary will be at `target/release/tl`.

## Feature Flags

ThinkingLanguage uses Cargo feature flags to control optional functionality. All features are disabled by default to keep the base build lean.

| Feature          | Description                                          | Extra Requirements                     |
|------------------|------------------------------------------------------|----------------------------------------|
| `sqlite`         | SQLite connector (rusqlite bundled)                  | None (statically linked)               |
| `mysql`          | MySQL connector                                      | MySQL client libraries                 |
| `redis`          | Redis connector                                      | None                                   |
| `s3`             | S3 object storage connector                          | None                                   |
| `kafka`          | Kafka streaming connector                            | librdkafka                             |
| `python`         | Python FFI bridge via pyo3                           | Python 3.8+ with development headers   |
| `gpu`            | GPU tensor operations via wgpu                       | Vulkan, Metal, or DX12 drivers         |
| `llvm-backend`   | LLVM AOT native compilation via inkwell              | LLVM 19 installed                      |
| `async-runtime`  | Tokio-backed async I/O (async fs, HTTP, timers)      | None                                   |
| `notebook`       | Interactive notebook TUI (ratatui)                   | Terminal with TUI support              |
| `registry`       | Package registry client                              | None                                   |

## Building with Features

Enable features by passing them as a comma-separated list:

```sh
cargo build --release --features "sqlite,gpu,async-runtime"
```

To install with features:

```sh
cargo install --path crates/tl-cli --features "sqlite,async-runtime,notebook"
```

To enable all connectors:

```sh
cargo build --release --features "sqlite,mysql,redis,s3,kafka"
```

## Platform Notes

### Linux

All features are supported. For the `gpu` feature, ensure Vulkan drivers are installed:

```sh
# Debian/Ubuntu
sudo apt install libvulkan1 mesa-vulkan-drivers

# Fedora
sudo dnf install vulkan-loader mesa-vulkan-drivers
```

For the `llvm-backend` feature, install LLVM 19:

```sh
# Debian/Ubuntu
sudo apt install llvm-19-dev
```

### macOS

All features are supported. GPU acceleration uses Metal, which is available on all modern Macs. No additional GPU drivers are needed.

For the `llvm-backend` feature:

```sh
brew install llvm@19
```

### Windows

All features are supported. GPU acceleration uses DX12, which is included with Windows 10 and later. Vulkan drivers can also be used as an alternative.

For the `llvm-backend` feature, install LLVM 19 from the [LLVM releases page](https://releases.llvm.org/).

## Verify Installation

After installation, verify that the `tl` binary is available:

```sh
tl --version
```

Enter the REPL to confirm everything is working:

```sh
tl shell
```

You should see the TL interactive prompt. Type `exit` or press Ctrl-D to quit.
