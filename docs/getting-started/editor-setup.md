# Editor Setup

ThinkingLanguage ships with an LSP server and a VS Code extension to provide a full development experience. This guide covers how to set up your editor.

## VS Code

### Install the Extension

The VS Code extension is located in the `editors/vscode/` directory of the repository. To install it:

1. Open VS Code.
2. Open the Extensions sidebar (Ctrl+Shift+X / Cmd+Shift+X).
3. Click the `...` menu at the top of the sidebar and select **Install from VSIX...**.
4. Navigate to `editors/vscode/` and select the packaged `.vsix` file.

Alternatively, during development you can symlink the extension into your VS Code extensions directory:

```sh
ln -s /path/to/thinkinglanguage/editors/vscode ~/.vscode/extensions/tl-lang
```

### What the Extension Provides

- **Syntax highlighting** for `.tl` files
- **LSP integration** -- the extension automatically starts the `tl lsp` server
- **Snippets** for common constructs (functions, pipelines, match expressions)

### Format on Save

To enable format-on-save, add the following to your VS Code `settings.json`:

```json
{
    "[tl]": {
        "editor.formatOnSave": true,
        "editor.defaultFormatter": "tl-lang.tl"
    }
}
```

## LSP Server

The TL Language Server Protocol server provides rich editor features for any LSP-compatible editor.

### Starting the Server

```sh
tl lsp
```

This starts the LSP server over stdio, which is the standard transport for most editor integrations.

### Features

| Feature             | Description                                                  |
|---------------------|--------------------------------------------------------------|
| **Completions**     | Context-aware suggestions for functions, variables, keywords |
| **Hover**           | Type information and doc comments on hover                   |
| **Go-to-Definition**| Jump to the definition of functions, variables, and types    |
| **Document Symbols**| Outline view of all definitions in the current file          |
| **Diagnostics**     | Inline error and warning messages from the type checker      |

## Neovim

### nvim-lspconfig

If you use [nvim-lspconfig](https://github.com/neovim/nvim-lspconfig), add a custom server configuration:

```lua
local lspconfig = require('lspconfig')
local configs = require('lspconfig.configs')

if not configs.tl then
    configs.tl = {
        default_config = {
            cmd = { 'tl', 'lsp' },
            filetypes = { 'tl' },
            root_dir = lspconfig.util.root_pattern('tl.toml', '.git'),
            settings = {},
        },
    }
end

lspconfig.tl.setup({})
```

You will also want to associate the `.tl` extension with a filetype. Add this to your Neovim config:

```lua
vim.filetype.add({
    extension = {
        tl = 'tl',
    },
})
```

### Other Neovim LSP Clients

For any LSP client plugin, configure it to run the command `tl lsp` for files with the `.tl` extension.

## Other Editors

Any editor that supports the Language Server Protocol can use the TL LSP server. Configure your editor's LSP client with:

- **Command:** `tl lsp`
- **Transport:** stdio
- **File types:** `.tl`
- **Root markers:** `tl.toml`, `.git`

Examples of compatible editors: Helix, Sublime Text (with LSP package), Emacs (with lsp-mode or eglot), Zed, Kate.

## File Associations

| Extension | Description                                      |
|-----------|--------------------------------------------------|
| `.tl`     | ThinkingLanguage source files                    |
| `.tlnb`   | ThinkingLanguage notebook files (JSON format)    |
| `tl.toml` | Project manifest (dependencies, module config)   |

## Formatter

Format source files from the command line:

```sh
tl fmt path/to/file.tl
```

The formatter normalizes indentation, spacing, and brace placement. It can also format an entire directory:

```sh
tl fmt src/
```

Integrate with your editor's format command or format-on-save for the best workflow.

## Linter

Run the linter to catch common issues and style problems:

```sh
tl lint path/to/file.tl
```

The linter checks for unused variables, unreachable code, shadowed imports, and other patterns that may indicate bugs. Lint diagnostics are also surfaced through the LSP server as editor warnings.
