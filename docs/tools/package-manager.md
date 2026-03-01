# Package Manager

TL includes a built-in package manager for managing project dependencies, building projects, and publishing packages to a registry.

## Project Manifest: tl.toml

Every TL project is defined by a `tl.toml` file at the project root.

```toml
[package]
name = "myproject"
version = "0.1.0"
entry = "src/main.tl"

[dependencies]
utils = { version = "1.0" }
local_lib = { path = "../lib" }
remote = { git = "https://github.com/user/repo", branch = "main" }
```

### Package Section

| Field | Description |
|-------|-------------|
| `name` | Package name |
| `version` | Semantic version string |
| `entry` | Entry point source file |

### Dependencies Section

Each dependency can be specified using one of the following sources:

- **Registry version**: `{ version = "1.0" }`
- **Local path**: `{ path = "../lib" }`
- **Git repository**: `{ git = "https://github.com/user/repo", branch = "main" }`

## Project Structure

A standard TL project follows this layout:

```
myproject/
  tl.toml
  src/
    main.tl
  tests/
    test_main.tl
```

Initialize a new project with this structure using:

```
tl init myproject
```

## Commands

### tl init

Create a new project with a `tl.toml` scaffold and directory structure.

```
tl init myproject
cd myproject
```

### tl add

Add a dependency to the project.

```
tl add utils --version "1.0"
tl add mylib --path "../lib"
tl add remote --git "https://github.com/user/repo" --branch "main"
```

### tl remove

Remove a dependency from the project.

```
tl remove utils
```

### tl install

Install all dependencies declared in `tl.toml`. This resolves dependencies and generates the `tl.lock` lock file.

```
tl install
```

### tl update

Update dependencies to their latest compatible versions. Shows version diffs for each changed package.

```
tl update          # Update all dependencies
tl update utils    # Update a specific dependency
tl update --dry-run    # Preview changes without modifying tl.lock
```

Output shows exactly what changed:

```
Updating all dependencies...
  + newpkg v1.0.0 (new)
  utils: 1.0.0 -> 1.2.0
  - oldpkg v2.0.0 (removed)
  3 package(s) changed (1 added, 1 updated, 1 removed).
Installed 2 package(s).
```

The `--dry-run` flag previews what would change without actually modifying `tl.lock` or downloading packages.

### tl outdated

Show which dependencies have newer versions available.

```
tl outdated
```

Output (requires `registry` feature):

```
Package              Current      Latest Matching    Latest Available
-------              -------      ---------------    ----------------
utils                1.0.0        1.3.0              2.0.0
helpers              2.1.0        2.1.0              2.1.0  (up to date)
mylib                1.0.0        (path)             (path)
```

- **Latest Matching** — newest version satisfying your `tl.toml` version requirement
- **Latest Available** — newest version in the registry (may be outside your requirement)
- Git and path dependencies show their source type instead of version columns

### tl build

Build the current project.

```
tl build
tl build --backend vm
tl build --strict
```

### tl publish

Publish the current package to the registry.

```
tl publish
```

### tl search

Search the package registry for packages.

```
tl search "json parser"
```

## Lock File: tl.lock

The `tl.lock` file pins exact dependency versions for reproducible builds. Each entry records:

- `name` — package name
- `version` — resolved version
- `source` — source descriptor (e.g., `path+/home/user/lib`, `git+url#rev`, `registry+url@ver`)
- `direct` — whether this is a direct dependency (vs transitive)
- `dependencies` — names of packages this package depends on

Old lock files without `direct` and `dependencies` fields are read correctly (defaults: `direct=true`, `dependencies=[]`).

## Dependency Resolution

The package manager supports four dependency sources:

1. **Version** — resolved from the package registry
2. **Git** — cloned from a git repository, optionally pinned to a branch
3. **Path** — local filesystem path, useful for monorepo setups
4. **Registry** — resolved from the TL package registry server

### Transitive Dependencies

When a package is installed, the resolver reads its `tl.toml` and recursively resolves any dependencies it declares. This uses BFS traversal:

1. Direct dependencies from your `tl.toml` are queued
2. Each dependency is fetched and its own `tl.toml` is read
3. Sub-dependencies are queued (marked as transitive)
4. Cycle detection prevents infinite loops
5. Diamond dependencies (A→B,C both→D) resolve D only once

Transitive dependencies appear in `tl.lock` with `direct = false`.

### Conflict Detection

When two packages require incompatible versions of the same dependency, the resolver reports an error:

```
Version conflicts detected:
  shared required by pkg-a (^1.0) and pkg-b (^2.0), resolved to 1.5.0
```

Conflicts are detected after resolution by checking that the resolved version satisfies all requesters' version requirements.

## Registry

The TL package registry is provided by the `tl-registry` crate (requires the `registry` feature).

- **Server**: axum-based HTTP server running on port 3333
- **Storage**: filesystem-based at `~/.tl/registry/`
- **Publishing**: packages are uploaded as base64-encoded JSON tarballs via `tl publish`
- **Search**: `tl search <query>` queries the registry server
- **Download**: dependencies are downloaded automatically during `tl install`

### Running the Registry Server

The registry server is a separate binary from the `tl-registry` crate. It listens on port 3333 and provides the following API endpoints:

- `POST /publish` -- publish a package
- `GET /search?q=<query>` -- search packages
- `GET /download/<name>/<version>` -- download a package

### Registry Client

The `tl-package` crate includes a registry client that communicates with the registry server via reqwest (blocking HTTP). This client is used by the CLI commands `tl publish`, `tl search`, and `tl install`.
