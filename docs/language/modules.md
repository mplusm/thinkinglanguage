# Modules

ThinkingLanguage has a module system for organizing code into reusable units with controlled visibility.

## Imports with `use`

Import items using dot-path syntax:

```tl
use math.sqrt
use collections.{HashMap, HashSet}
```

Grouped imports with braces allow pulling in multiple items from the same module.

## Visibility with `pub`

By default, items are private to their module. Use `pub` to make them accessible from outside:

```tl
pub fn public_function() {
    // visible to other modules
}

pub struct PublicStruct {
    name: string,
    value: int,
}

fn private_helper() {
    // only accessible within this module
}
```

## Module Declarations with `mod`

Declare submodules with `mod`:

```tl
mod utils
```

This loads either `utils.tl` or `utils/mod.tl` relative to the current file.

## Directory Modules

For larger modules, create a directory with a `mod.tl` entry point:

```
myproject/
  src/
    main.tl
    mymod/
      mod.tl       // module entry point
      helpers.tl
      types.tl
```

Inside `mymod/mod.tl`, declare submodules:

```tl
pub mod helpers
pub mod types
```

## Project Manifest: tl.toml

Every TL project has a `tl.toml` manifest file at the root:

```toml
[package]
name = "myproject"
version = "0.1.0"
entry = "src/main.tl"

[dependencies]
some_lib = { version = "1.0" }
local_dep = { path = "../local" }
git_dep = { git = "https://github.com/user/repo", branch = "main" }
```

### Fields

- `name` -- the package name
- `version` -- semantic version string
- `entry` -- the main entry point file

### Dependency Sources

- **Registry**: `{ version = "1.0" }` -- pulls from the TL package registry
- **Local path**: `{ path = "../local" }` -- references a local directory
- **Git**: `{ git = "https://...", branch = "main" }` -- clones from a git repository

## CLI Commands

### `tl init`

Create a new project scaffold:

```sh
tl init myproject
```

This generates a directory with a `tl.toml` manifest and a starter `src/main.tl` file.

### `tl build`

Resolve dependencies and run the project entry point:

```sh
tl build
```

This reads `tl.toml`, fetches any missing dependencies, and executes the file specified in the `entry` field.
