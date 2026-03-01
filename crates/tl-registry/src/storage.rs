// ThinkingLanguage — Registry Storage
// Licensed under MIT OR Apache-2.0
//
// Filesystem-based package storage for the registry server.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// Metadata for a single published version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntry {
    pub version: String,
    pub sha256: String,
    pub published: DateTime<Utc>,
    pub description: Option<String>,
}

/// Metadata for a package (all versions).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageMetadata {
    pub name: String,
    pub versions: Vec<VersionEntry>,
}

/// Search result returned by search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub name: String,
    pub latest_version: String,
    pub description: Option<String>,
}

/// Filesystem-based registry storage.
///
/// Layout:
/// ```text
/// root/
///   packages/
///     <name>/
///       index.json          -- PackageMetadata
///       <version>.tar.gz    -- package tarball
/// ```
pub struct RegistryStorage {
    root: PathBuf,
}

impl RegistryStorage {
    /// Create a new storage rooted at the given directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Default location: ~/.tl/registry/
    pub fn default_location() -> Result<Self, String> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| "Cannot determine home directory".to_string())?;
        let root = PathBuf::from(home).join(".tl").join("registry");
        std::fs::create_dir_all(&root)
            .map_err(|e| format!("Cannot create registry directory: {e}"))?;
        Ok(Self { root })
    }

    fn packages_dir(&self) -> PathBuf {
        self.root.join("packages")
    }

    fn package_dir(&self, name: &str) -> PathBuf {
        self.packages_dir().join(name)
    }

    fn index_path(&self, name: &str) -> PathBuf {
        self.package_dir(name).join("index.json")
    }

    fn tarball_path(&self, name: &str, version: &str) -> PathBuf {
        self.package_dir(name).join(format!("{version}.tar.gz"))
    }

    /// Load package metadata, or None if not published.
    pub fn load_metadata(&self, name: &str) -> Result<Option<PackageMetadata>, String> {
        let path = self.index_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read index for '{name}': {e}"))?;
        let meta: PackageMetadata = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse index for '{name}': {e}"))?;
        Ok(Some(meta))
    }

    /// Save package metadata.
    fn save_metadata(&self, meta: &PackageMetadata) -> Result<(), String> {
        let dir = self.package_dir(&meta.name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("Failed to create package dir: {e}"))?;
        let content = serde_json::to_string_pretty(meta)
            .map_err(|e| format!("Failed to serialize index: {e}"))?;
        std::fs::write(self.index_path(&meta.name), content)
            .map_err(|e| format!("Failed to write index: {e}"))?;
        Ok(())
    }

    /// Publish a package version. Returns the SHA-256 hash.
    pub fn publish(
        &self,
        name: &str,
        version: &str,
        description: Option<&str>,
        tarball: &[u8],
    ) -> Result<String, String> {
        // Compute hash
        let mut hasher = Sha256::new();
        hasher.update(tarball);
        let sha256 = format!("{:x}", hasher.finalize());

        // Load or create metadata
        let mut meta = self.load_metadata(name)?.unwrap_or_else(|| PackageMetadata {
            name: name.to_string(),
            versions: Vec::new(),
        });

        // Check for duplicate version
        if meta.versions.iter().any(|v| v.version == version) {
            return Err(format!(
                "Version {version} of '{name}' is already published"
            ));
        }

        // Write tarball
        let dir = self.package_dir(name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("Failed to create package dir: {e}"))?;
        std::fs::write(self.tarball_path(name, version), tarball)
            .map_err(|e| format!("Failed to write tarball: {e}"))?;

        // Update metadata
        meta.versions.push(VersionEntry {
            version: version.to_string(),
            sha256: sha256.clone(),
            published: Utc::now(),
            description: description.map(|s| s.to_string()),
        });

        self.save_metadata(&meta)?;
        Ok(sha256)
    }

    /// Download a package tarball.
    pub fn download(&self, name: &str, version: &str) -> Result<Vec<u8>, String> {
        let path = self.tarball_path(name, version);
        if !path.exists() {
            return Err(format!(
                "Package '{name}' version '{version}' not found"
            ));
        }
        std::fs::read(&path)
            .map_err(|e| format!("Failed to read tarball: {e}"))
    }

    /// Search packages by query (matches name or description).
    pub fn search(&self, query: &str) -> Result<Vec<SearchResult>, String> {
        let pkgs_dir = self.packages_dir();
        if !pkgs_dir.exists() {
            return Ok(Vec::new());
        }

        let query_lower = query.to_lowercase();
        let mut results = Vec::new();

        let entries = std::fs::read_dir(&pkgs_dir)
            .map_err(|e| format!("Failed to read packages dir: {e}"))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {e}"))?;
            if !entry.path().is_dir() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            if let Ok(Some(meta)) = self.load_metadata(&name) {
                let name_matches = name.to_lowercase().contains(&query_lower);
                let desc_matches = meta
                    .versions
                    .last()
                    .and_then(|v| v.description.as_ref())
                    .is_some_and(|d| d.to_lowercase().contains(&query_lower));

                if name_matches || desc_matches {
                    let latest = meta.versions.last().unwrap();
                    results.push(SearchResult {
                        name: meta.name.clone(),
                        latest_version: latest.version.clone(),
                        description: latest.description.clone(),
                    });
                }
            }
        }

        Ok(results)
    }

    /// List all packages.
    pub fn list_all(&self) -> Result<Vec<SearchResult>, String> {
        let pkgs_dir = self.packages_dir();
        if !pkgs_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let entries = std::fs::read_dir(&pkgs_dir)
            .map_err(|e| format!("Failed to read packages dir: {e}"))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read entry: {e}"))?;
            if !entry.path().is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if let Ok(Some(meta)) = self.load_metadata(&name) {
                if let Some(latest) = meta.versions.last() {
                    results.push(SearchResult {
                        name: meta.name.clone(),
                        latest_version: latest.version.clone(),
                        description: latest.description.clone(),
                    });
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage() -> (TempDir, RegistryStorage) {
        let tmp = TempDir::new().unwrap();
        let storage = RegistryStorage::new(tmp.path());
        (tmp, storage)
    }

    #[test]
    fn publish_and_retrieve() {
        let (_tmp, storage) = test_storage();
        let tarball = b"fake-tarball-data";

        let hash = storage
            .publish("mylib", "1.0.0", Some("A test library"), tarball)
            .unwrap();
        assert!(!hash.is_empty());

        // Retrieve metadata
        let meta = storage.load_metadata("mylib").unwrap().unwrap();
        assert_eq!(meta.name, "mylib");
        assert_eq!(meta.versions.len(), 1);
        assert_eq!(meta.versions[0].version, "1.0.0");
        assert_eq!(meta.versions[0].description.as_deref(), Some("A test library"));

        // Download tarball
        let data = storage.download("mylib", "1.0.0").unwrap();
        assert_eq!(data, tarball);
    }

    #[test]
    fn duplicate_version_rejected() {
        let (_tmp, storage) = test_storage();
        storage
            .publish("pkg", "1.0.0", None, b"data1")
            .unwrap();
        let result = storage.publish("pkg", "1.0.0", None, b"data2");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already published"));
    }

    #[test]
    fn search_by_name() {
        let (_tmp, storage) = test_storage();
        storage
            .publish("data-utils", "0.1.0", Some("Utilities for data"), b"d1")
            .unwrap();
        storage
            .publish("web-server", "2.0.0", Some("HTTP server"), b"d2")
            .unwrap();

        let results = storage.search("data").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "data-utils");
    }

    #[test]
    fn search_by_description() {
        let (_tmp, storage) = test_storage();
        storage
            .publish("mylib", "0.5.0", Some("Machine learning tools"), b"d1")
            .unwrap();

        let results = storage.search("machine").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "mylib");
    }

    #[test]
    fn download_nonexistent() {
        let (_tmp, storage) = test_storage();
        let result = storage.download("nope", "1.0.0");
        assert!(result.is_err());
    }

    #[test]
    fn multiple_versions() {
        let (_tmp, storage) = test_storage();
        storage.publish("pkg", "1.0.0", None, b"v1").unwrap();
        storage.publish("pkg", "2.0.0", None, b"v2").unwrap();

        let meta = storage.load_metadata("pkg").unwrap().unwrap();
        assert_eq!(meta.versions.len(), 2);

        let v1 = storage.download("pkg", "1.0.0").unwrap();
        assert_eq!(v1, b"v1");
        let v2 = storage.download("pkg", "2.0.0").unwrap();
        assert_eq!(v2, b"v2");
    }
}
