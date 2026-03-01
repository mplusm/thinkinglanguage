# Notebook

TL includes an interactive notebook interface for exploratory data engineering and AI work. The notebook feature provides a terminal-based UI (TUI) with persistent state across cells.

Requires the `notebook` feature to be enabled at build time.

## File Format

Notebooks use the `.tlnb` extension and are stored as JSON files. The format consists of an array of cells, where each cell contains TL source code and its output.

## Getting Started

Open or create a notebook:

```
tl notebook mynotebook.tlnb
```

If the file does not exist, a new empty notebook will be created.

## TUI Interface

The notebook TUI is built with ratatui and crossterm, providing a terminal-based interactive experience.

### Modes

- **Normal mode**: Navigate between cells, execute cells, and manage the notebook structure.
- **Edit mode**: Edit the source code within a cell.

### Key Bindings

| Key | Mode | Action |
|-----|------|--------|
| Arrow keys (Up/Down) | Normal | Navigate between cells |
| Enter | Normal | Enter edit mode for current cell |
| Esc | Edit | Return to normal mode |
| Execute shortcut | Normal | Run the current cell |
| Add cell | Normal | Insert a new cell |
| Delete cell | Normal | Remove the current cell |

## Persistent VM State

A single VM instance is shared across all cells in a notebook. This means:

- Variables defined in one cell are available in subsequent cells.
- Functions defined early in the notebook can be called in later cells.
- State accumulates as cells are executed in order.

```
# Cell 1
let data = read_csv("sales.csv")

# Cell 2 (data is available here)
let filtered = data |> filter(row => row.amount > 100)

# Cell 3 (both data and filtered are available)
filtered |> show()
```

## Output Capture

When a cell is executed, any stdout output produced by the code is captured and displayed below the cell in the TUI. This includes output from `print`, `show`, and other functions that write to stdout.

## Export

Convert a notebook to a plain `.tl` source file:

```
tl notebook mynotebook.tlnb --export
```

This concatenates all cell source code into a single `.tl` file, suitable for running with `tl run`.

## Cell Types

Currently, notebooks support code cells containing TL source code. Each cell is an independent unit that can be executed individually, while sharing state with other cells through the persistent VM.

## Building with Notebook Support

The notebook feature must be enabled at compile time:

```
cargo build --features notebook
```
