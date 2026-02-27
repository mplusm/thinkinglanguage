use std::path::{Path, PathBuf};

/// Manages the local package cache at ~/.tl/packages/.
#[derive(Debug, Clone)]
pub struct PackageCache {
    root: PathBuf,
}

impl PackageCache {
    /// Create a cache at the default location (~/.tl/packages/).
    pub fn default_location() -> Result<Self, String> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| "Could not determine home directory".to_string())?;
        Ok(PackageCache {
            root: PathBuf::from(home).join(".tl").join("packages"),
        })
    }

    /// Create a cache at a custom root path.
    pub fn new(root: PathBuf) -> Self {
        PackageCache { root }
    }

    /// Get the root directory of the cache.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the directory for a specific package version: root/<name>/<version>/
    pub fn package_dir(&self, name: &str, version: &str) -> PathBuf {
        self.root.join(name).join(version)
    }

    /// Check if a package version is already cached.
    pub fn is_cached(&self, name: &str, version: &str) -> bool {
        self.package_dir(name, version).exists()
    }

    /// List all cached versions of a package.
    pub fn list_versions(&self, name: &str) -> Vec<String> {
        let pkg_dir = self.root.join(name);
        if !pkg_dir.is_dir() {
            return Vec::new();
        }
        let mut versions = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&pkg_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        versions.push(name.to_string());
                    }
                }
            }
        }
        versions.sort();
        versions
    }

    /// Remove a cached package version.
    pub fn remove(&self, name: &str, version: &str) -> Result<(), String> {
        let dir = self.package_dir(name, version);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .map_err(|e| format!("Failed to remove cached package: {e}"))?;
        }
        // Clean up empty parent directory
        let pkg_dir = self.root.join(name);
        if pkg_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&pkg_dir) {
                if entries.count() == 0 {
                    let _ = std::fs::remove_dir(&pkg_dir);
                }
            }
        }
        Ok(())
    }

    /// Find the source root for a cached package (where TL source files live).
    /// Looks for: src/ directory, or the package root itself.
    pub fn source_root(&self, name: &str, version: &str) -> Option<PathBuf> {
        let dir = self.package_dir(name, version);
        if !dir.exists() {
            return None;
        }
        // If there's a src/ directory, that's the source root
        let src_dir = dir.join("src");
        if src_dir.is_dir() {
            return Some(src_dir);
        }
        // Otherwise the package root itself
        Some(dir)
    }

    /// Ensure the cache directory exists.
    pub fn ensure_dir(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.root)
            .map_err(|e| format!("Failed to create cache directory: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn cache_dir_layout() {
        let dir = TempDir::new().unwrap();
        let cache = PackageCache::new(dir.path().to_path_buf());

        let pkg_dir = cache.package_dir("mylib", "1.0.0");
        assert!(pkg_dir.ends_with("mylib/1.0.0"));
    }

    #[test]
    fn cache_is_cached() {
        let dir = TempDir::new().unwrap();
        let cache = PackageCache::new(dir.path().to_path_buf());

        assert!(!cache.is_cached("mylib", "1.0.0"));

        let pkg_dir = cache.package_dir("mylib", "1.0.0");
        std::fs::create_dir_all(&pkg_dir).unwrap();
        assert!(cache.is_cached("mylib", "1.0.0"));
    }

    #[test]
    fn cache_list_versions() {
        let dir = TempDir::new().unwrap();
        let cache = PackageCache::new(dir.path().to_path_buf());

        assert!(cache.list_versions("mylib").is_empty());

        std::fs::create_dir_all(cache.package_dir("mylib", "1.0.0")).unwrap();
        std::fs::create_dir_all(cache.package_dir("mylib", "2.0.0")).unwrap();

        let versions = cache.list_versions("mylib");
        assert_eq!(versions, vec!["1.0.0", "2.0.0"]);
    }

    #[test]
    fn cache_remove() {
        let dir = TempDir::new().unwrap();
        let cache = PackageCache::new(dir.path().to_path_buf());

        std::fs::create_dir_all(cache.package_dir("mylib", "1.0.0")).unwrap();
        assert!(cache.is_cached("mylib", "1.0.0"));

        cache.remove("mylib", "1.0.0").unwrap();
        assert!(!cache.is_cached("mylib", "1.0.0"));
    }

    #[test]
    fn cache_source_root() {
        let dir = TempDir::new().unwrap();
        let cache = PackageCache::new(dir.path().to_path_buf());

        // Not cached
        assert!(cache.source_root("mylib", "1.0.0").is_none());

        // With src/ dir
        let pkg_dir = cache.package_dir("mylib", "1.0.0");
        std::fs::create_dir_all(pkg_dir.join("src")).unwrap();
        let root = cache.source_root("mylib", "1.0.0").unwrap();
        assert!(root.ends_with("src"));

        // Without src/ dir
        let cache2 = PackageCache::new(dir.path().to_path_buf());
        let pkg_dir2 = cache2.package_dir("nolib", "1.0.0");
        std::fs::create_dir_all(&pkg_dir2).unwrap();
        let root2 = cache2.source_root("nolib", "1.0.0").unwrap();
        assert!(root2.ends_with("1.0.0"));
    }
}
