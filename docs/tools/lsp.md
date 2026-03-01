# Language Server Protocol (LSP)

TL ships with a built-in LSP server that provides IDE features for any editor that supports the Language Server Protocol.

## Starting the Server

```
tl lsp
```

The LSP server communicates over stdin/stdout using the standard LSP JSON-RPC protocol.

## Features

### Completions

Context-aware autocompletion for:

- Keywords (`fn`, `let`, `if`, `for`, `match`, `struct`, `enum`, `impl`, `use`, `pub`, etc.)
- Builtin functions
- Variables in scope
- Struct fields (after `.`)
- Enum variants (after `::`)

### Hover

Hovering over an identifier displays:

- Type information
- Doc comments (extracted from `///` comments above definitions)
- Function signatures

### Go to Definition

Jump to the definition of:

- Functions
- Structs
- Enums
- Variables
- Imported modules

### Document Symbols

Displays an outline of the current file showing:

- Functions
- Structs
- Enums
- Impl blocks

### Diagnostics

Real-time feedback shown inline in the editor:

- Parse errors
- Type warnings and errors
- Lint issues

## Editor Setup

### VS Code

A VS Code extension is included in the repository at `editors/vscode/`.

To install:

1. Navigate to the `editors/vscode/` directory.
2. Run `npm install` to install dependencies.
3. Run `npm run package` to build the extension.
4. Install the generated `.vsix` file in VS Code.

The extension provides:

- Syntax highlighting for `.tl` files
- LSP client configured to launch `tl lsp` automatically
- File association for `.tl` source files

### Neovim

Using nvim-lspconfig, add a custom server configuration:

```lua
local lspconfig = require('lspconfig')
local configs = require('lspconfig.configs')

if not configs.tl then
  configs.tl = {
    default_config = {
      cmd = { 'tl', 'lsp' },
      filetypes = { 'tl' },
      root_dir = lspconfig.util.root_pattern('tl.toml', '.git'),
    },
  }
end

lspconfig.tl.setup({})
```

### Emacs

Using lsp-mode:

```elisp
(with-eval-after-load 'lsp-mode
  (add-to-list 'lsp-language-id-configuration '(".*\\.tl$" . "tl"))
  (lsp-register-client
   (make-lsp-client
    :new-connection (lsp-stdio-connection '("tl" "lsp"))
    :activation-fn (lsp-activate-on "tl")
    :server-id 'tl-lsp)))
```

Using eglot:

```elisp
(add-to-list 'eglot-server-programs '(tl-mode "tl" "lsp"))
```

### Sublime Text

Using the LSP package:

1. Install the LSP package from Package Control.
2. Open LSP Settings and add:

```json
{
  "clients": {
    "tl": {
      "enabled": true,
      "command": ["tl", "lsp"],
      "selector": "source.tl"
    }
  }
}
```

## File Types

| Extension | Description |
|-----------|-------------|
| `.tl` | TL source file |
| `.tlnb` | TL notebook file |
| `tl.toml` | Project manifest |
