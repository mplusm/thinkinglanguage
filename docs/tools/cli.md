# CLI Reference

ThinkingLanguage (TL) is a Rust-based language for Data Engineering & AI. The command-line interface is invoked via the `tl` binary.

## Commands

### tl run

Execute a `.tl` source file.

```
tl run <file>
```

| Flag | Description |
|------|-------------|
| `--backend vm\|interp\|llvm` | Execution backend (default: `vm`) |
| `--dump-bytecode` | Show compiled bytecode before execution |
| `--no-check` | Skip type checking |
| `--strict` | Require type annotations on function parameters |
| `--sandbox` | Restrict file write and network access |
| `--allow-connector <type>` | Allow a specific connector in sandbox mode (repeatable) |

Examples:

```
tl run script.tl
tl run script.tl --backend interp
tl run script.tl --sandbox --allow-connector postgres
tl run script.tl --dump-bytecode --strict
```

---

### tl shell

Start the interactive REPL.

```
tl shell
```

| Flag | Description |
|------|-------------|
| `--backend vm\|interp` | Execution backend (default: `vm`) |

Features:
- Tab completion for keywords, builtins, and variables
- Multi-line input support
- Command history persisted at `~/.tl_history`

---

### tl check

Type-check a source file without executing it.

```
tl check <file>
```

| Flag | Description |
|------|-------------|
| `--strict` | Require type annotations on function parameters |

---

### tl test

Run tests in a file or directory. Tests are `test "name" { ... }` blocks inside `.tl` files.

```
tl test <path>
```

| Flag | Description |
|------|-------------|
| `--backend vm\|interp` | Execution backend (default: `vm`) |

Test block syntax:

```
test "addition works" {
    assert_eq(1 + 1, 2)
}
```

---

### tl fmt

Format TL source files.

```
tl fmt <path>
```

| Flag | Description |
|------|-------------|
| `--check` | Check formatting without writing changes (exit code 1 if changes needed) |

---

### tl lint

Lint source files for style and correctness issues.

```
tl lint <path>
```

| Flag | Description |
|------|-------------|
| `--strict` | Require type annotations |

---

### tl build

Build the current project. Requires a `tl.toml` manifest in the working directory.

```
tl build
```

| Flag | Description |
|------|-------------|
| `--backend vm\|interp` | Execution backend (default: `vm`) |
| `--dump-bytecode` | Show compiled bytecode |
| `--no-check` | Skip type checking |
| `--strict` | Require type annotations on function parameters |

---

### tl init

Initialize a new project with a `tl.toml` scaffold and directory structure.

```
tl init <name>
```

---

### tl doc

Generate documentation from doc comments in source files.

```
tl doc <path>
```

| Flag | Description |
|------|-------------|
| `--format html\|markdown\|json` | Output format (default: `html`) |
| `-o, --output <file>` | Output file path |
| `--public-only` | Only document public items |

---

### tl disasm

Disassemble compiled bytecode for a source file.

```
tl disasm <file>
```

---

### tl compile

Compile a source file to a native object file. Requires the `llvm-backend` feature.

```
tl compile <file>
```

| Flag | Description |
|------|-------------|
| `-o, --output <file>` | Output path (default: `<file>.o`) |
| `--emit-ir` | Dump LLVM IR instead of emitting an object file |

---

### tl deploy

Generate deployment artifacts from a pipeline or script.

```
tl deploy <file>
```

| Flag | Description |
|------|-------------|
| `--target docker\|k8s` | Deployment target (default: `docker`) |
| `--output <dir>` | Output directory (default: `./deploy`) |

---

### tl lineage

Show data lineage information for a script.

```
tl lineage <file>
```

| Flag | Description |
|------|-------------|
| `--format dot\|json\|text` | Output format (default: `text`) |

---

### tl notebook

Open an interactive notebook in the terminal. Requires the `notebook` feature.

```
tl notebook <file>
```

| Flag | Description |
|------|-------------|
| `--export` | Export notebook to a `.tl` file instead of opening the TUI |

The file should have a `.tlnb` extension. If it does not exist, it will be created.

---

### tl lsp

Start the Language Server Protocol server on stdin/stdout.

```
tl lsp
```

---

### tl models

Manage registered ML models.

```
tl models list              # List all registered models
tl models info <name>       # Show model metadata
tl models delete <name>     # Delete a model
```

---

### tl migrate

Schema migration commands.

```
tl migrate apply <file>     # Apply schema migrations
tl migrate check <file>     # Check compatibility without applying
tl migrate diff <file>      # Show diff between schema versions
```

| Flag | Description |
|------|-------------|
| `--backend vm\|interp` | Execution backend (for `apply` subcommand) |

---

### Package Manager Commands

Manage project dependencies.

```
tl add <pkg>                # Add a dependency
tl remove <pkg>             # Remove a dependency
tl install                  # Install all dependencies from tl.toml
tl update [pkg]             # Update dependencies (all or specific)
tl publish                  # Publish package to registry
tl search <query>           # Search the package registry
```

Flags for `tl add`:

| Flag | Description |
|------|-------------|
| `--version <ver>` | Specify version |
| `--git <url>` | Git repository URL |
| `--branch <branch>` | Git branch |
| `--path <path>` | Local filesystem path |
