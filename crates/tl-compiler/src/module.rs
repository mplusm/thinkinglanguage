// ThinkingLanguage — Module Resolution Engine
// Licensed under MIT OR Apache-2.0
//
// Maps dot-paths to files, handles mod.tl directories,
// caches modules, and detects circular imports.
// Shared by both VM and interpreter.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tl_errors::{RuntimeError, TlError};

/// Metadata about an exported item from a module.
#[derive(Debug, Clone)]
pub struct ExportedItem {
    pub name: String,
    pub is_public: bool,
}

/// The exports from a loaded module.
#[derive(Debug, Clone)]
pub struct ModuleExports {
    pub items: HashMap<String, ExportedItem>,
    pub file_path: PathBuf,
}

/// Result of resolving a dot-path.
#[derive(Debug, Clone)]
pub struct ResolvedModule {
    /// Absolute file path to the module source
    pub file_path: PathBuf,
    /// If the last segment is an item within the module (not a file)
    pub item_name: Option<String>,
}

/// Module resolver — maps dot-paths to files, detects circulars.
pub struct ModuleResolver {
    /// Project root (where tl.toml lives, or the directory of the entry file)
    root: PathBuf,
    /// File currently being processed (for relative resolution)
    current_file: Option<PathBuf>,
    /// Cache: canonical path → exports
    module_cache: HashMap<PathBuf, ModuleExports>,
    /// Files currently being imported (circular detection)
    importing: HashSet<PathBuf>,
}

impl ModuleResolver {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            current_file: None,
            module_cache: HashMap::new(),
            importing: HashSet::new(),
        }
    }

    pub fn set_current_file(&mut self, path: Option<PathBuf>) {
        self.current_file = path;
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a dot-path (from `use` statement) to a file path.
    ///
    /// Given segments like `["data", "transforms", "clean"]`:
    /// 1. Try `<base>/data/transforms/clean.tl` (file module)
    /// 2. Try `<base>/data/transforms/clean/mod.tl` (directory module)
    /// 3. Try `<base>/data/transforms.tl` with item "clean" (item within file)
    /// 4. Try `<base>/data/transforms/mod.tl` with item "clean" (item within dir module)
    pub fn resolve_path(&self, segments: &[String]) -> Result<ResolvedModule, TlError> {
        let base = self.base_dir();

        if segments.is_empty() {
            return Err(module_err("Empty module path".to_string()));
        }

        // Build the full path from all segments
        let rel_path: PathBuf = segments.iter().collect();

        // 1. Try as file module: segments.tl
        let file_path = base.join(&rel_path).with_extension("tl");
        if file_path.exists() {
            return Ok(ResolvedModule {
                file_path,
                item_name: None,
            });
        }

        // 2. Try as directory module: segments/mod.tl
        let dir_path = base.join(&rel_path).join("mod.tl");
        if dir_path.exists() {
            return Ok(ResolvedModule {
                file_path: dir_path,
                item_name: None,
            });
        }

        // 3. If more than one segment, try parent as module, last as item
        if segments.len() > 1 {
            let (parent_segs, item_name) = segments.split_at(segments.len() - 1);
            let parent_path: PathBuf = parent_segs.iter().collect();

            // Try parent.tl with last segment as item
            let parent_file = base.join(&parent_path).with_extension("tl");
            if parent_file.exists() {
                return Ok(ResolvedModule {
                    file_path: parent_file,
                    item_name: Some(item_name[0].clone()),
                });
            }

            // Try parent/mod.tl with last segment as item
            let parent_dir = base.join(&parent_path).join("mod.tl");
            if parent_dir.exists() {
                return Ok(ResolvedModule {
                    file_path: parent_dir,
                    item_name: Some(item_name[0].clone()),
                });
            }
        }

        Err(module_err(format!(
            "Module not found: `{}`. Searched in: {}",
            segments.join("."),
            base.display()
        )))
    }

    /// Resolve a prefix path for group/wildcard imports.
    /// Returns the file path for the module containing the group items.
    pub fn resolve_prefix(&self, segments: &[String]) -> Result<PathBuf, TlError> {
        let base = self.base_dir();

        if segments.is_empty() {
            return Err(module_err("Empty module path".to_string()));
        }

        let rel_path: PathBuf = segments.iter().collect();

        // Try as file module
        let file_path = base.join(&rel_path).with_extension("tl");
        if file_path.exists() {
            return Ok(file_path);
        }

        // Try as directory module
        let dir_path = base.join(&rel_path).join("mod.tl");
        if dir_path.exists() {
            return Ok(dir_path);
        }

        Err(module_err(format!(
            "Module not found: `{}`",
            segments.join(".")
        )))
    }

    /// Check for circular dependency. Returns Err if circular.
    pub fn begin_import(&mut self, path: &Path) -> Result<(), TlError> {
        let canonical = self.canonicalize(path);
        if self.importing.contains(&canonical) {
            return Err(module_err(format!(
                "Circular import detected: {}",
                canonical.display()
            )));
        }
        self.importing.insert(canonical);
        Ok(())
    }

    /// Mark import as complete.
    pub fn end_import(&mut self, path: &Path) {
        let canonical = self.canonicalize(path);
        self.importing.remove(&canonical);
    }

    /// Check if a module is cached.
    pub fn get_cached(&self, path: &Path) -> Option<&ModuleExports> {
        let canonical = self.canonicalize(path);
        self.module_cache.get(&canonical)
    }

    /// Cache a module's exports.
    pub fn cache_module(&mut self, path: &Path, exports: ModuleExports) {
        let canonical = self.canonicalize(path);
        self.module_cache.insert(canonical, exports);
    }

    fn base_dir(&self) -> PathBuf {
        if let Some(ref current) = self.current_file {
            current
                .parent()
                .unwrap_or(Path::new("."))
                .to_path_buf()
        } else {
            self.root.clone()
        }
    }

    fn canonicalize(&self, path: &Path) -> PathBuf {
        path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
    }

    /// Resolve a module path against package roots.
    /// Returns a ResolvedModule if the first segment matches a package name.
    pub fn resolve_package_path(
        &self,
        segments: &[String],
        package_roots: &HashMap<String, PathBuf>,
    ) -> Option<ResolvedModule> {
        if segments.is_empty() {
            return None;
        }

        let pkg_name = &segments[0];
        let pkg_name_hyphen = pkg_name.replace('_', "-");
        let pkg_root = package_roots.get(pkg_name.as_str())
            .or_else(|| package_roots.get(&pkg_name_hyphen))?;

        let remaining = &segments[1..];

        if remaining.is_empty() {
            // Import the package entry point
            let src = pkg_root.join("src");
            for entry in &["lib.tl", "mod.tl", "main.tl"] {
                let p = src.join(entry);
                if p.exists() {
                    return Some(ResolvedModule { file_path: p, item_name: None });
                }
            }
            for entry in &["mod.tl", "lib.tl"] {
                let p = pkg_root.join(entry);
                if p.exists() {
                    return Some(ResolvedModule { file_path: p, item_name: None });
                }
            }
            return None;
        }

        let rel: PathBuf = remaining.iter().collect();
        let src = pkg_root.join("src");

        // Try src/<rel>.tl
        let file_path = src.join(&rel).with_extension("tl");
        if file_path.exists() {
            return Some(ResolvedModule { file_path, item_name: None });
        }

        // Try src/<rel>/mod.tl
        let dir_path = src.join(&rel).join("mod.tl");
        if dir_path.exists() {
            return Some(ResolvedModule { file_path: dir_path, item_name: None });
        }

        // Try <root>/<rel>.tl
        let file_path = pkg_root.join(&rel).with_extension("tl");
        if file_path.exists() {
            return Some(ResolvedModule { file_path, item_name: None });
        }

        // Parent fallback for item within module
        if remaining.len() > 1 {
            let parent: PathBuf = remaining[..remaining.len() - 1].iter().collect();
            let item = remaining.last().unwrap().clone();
            let parent_file = src.join(&parent).with_extension("tl");
            if parent_file.exists() {
                return Some(ResolvedModule { file_path: parent_file, item_name: Some(item) });
            }
        }

        None
    }
}

fn module_err(message: String) -> TlError {
    TlError::Runtime(RuntimeError {
        message,
        span: None,
        stack_trace: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_test_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();

        // Create test module files
        let src = dir.path();
        fs::write(src.join("math.tl"), "pub fn add(a, b) { a + b }").unwrap();
        fs::create_dir_all(src.join("data")).unwrap();
        fs::write(src.join("data/transforms.tl"), "pub fn clean(x) { x }").unwrap();
        fs::create_dir_all(src.join("utils")).unwrap();
        fs::write(src.join("utils/mod.tl"), "pub fn helper() { 1 }").unwrap();
        fs::create_dir_all(src.join("nested/deep")).unwrap();
        fs::write(src.join("nested/deep/mod.tl"), "pub fn deep_fn() { 42 }").unwrap();

        dir
    }

    #[test]
    fn test_resolve_file_module() {
        let dir = setup_test_dir();
        let resolver = ModuleResolver::new(dir.path().to_path_buf());

        let result = resolver.resolve_path(&["math".into()]).unwrap();
        assert_eq!(result.file_path, dir.path().join("math.tl"));
        assert!(result.item_name.is_none());
    }

    #[test]
    fn test_resolve_nested_file_module() {
        let dir = setup_test_dir();
        let resolver = ModuleResolver::new(dir.path().to_path_buf());

        let result = resolver.resolve_path(&["data".into(), "transforms".into()]).unwrap();
        assert_eq!(result.file_path, dir.path().join("data/transforms.tl"));
        assert!(result.item_name.is_none());
    }

    #[test]
    fn test_resolve_directory_module() {
        let dir = setup_test_dir();
        let resolver = ModuleResolver::new(dir.path().to_path_buf());

        let result = resolver.resolve_path(&["utils".into()]).unwrap();
        assert_eq!(result.file_path, dir.path().join("utils/mod.tl"));
        assert!(result.item_name.is_none());
    }

    #[test]
    fn test_resolve_item_within_module() {
        let dir = setup_test_dir();
        let resolver = ModuleResolver::new(dir.path().to_path_buf());

        // "math.add" → math.tl file with item "add"
        let result = resolver.resolve_path(&["math".into(), "add".into()]).unwrap();
        assert_eq!(result.file_path, dir.path().join("math.tl"));
        assert_eq!(result.item_name, Some("add".into()));
    }

    #[test]
    fn test_circular_detection() {
        let dir = setup_test_dir();
        let mut resolver = ModuleResolver::new(dir.path().to_path_buf());

        let path = dir.path().join("math.tl");
        resolver.begin_import(&path).unwrap();
        let result = resolver.begin_import(&path);
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("Circular import"));
    }

    #[test]
    fn test_module_not_found() {
        let dir = setup_test_dir();
        let resolver = ModuleResolver::new(dir.path().to_path_buf());

        let result = resolver.resolve_path(&["nonexistent".into()]);
        assert!(result.is_err());
    }
}
