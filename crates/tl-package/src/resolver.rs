use crate::cache::PackageCache;
use crate::fetch::fetch_dependency;
use crate::lockfile::{LockFile, LockedPackage};
use crate::manifest::{DependencySpec, DepSourceKind, Manifest};
use std::path::{Path, PathBuf};

/// Resolve all dependencies from a manifest, fetching as needed, and produce a lock file.
pub fn resolve_and_install(
    project_root: &Path,
    manifest: &Manifest,
    cache: &PackageCache,
) -> Result<LockFile, String> {
    cache.ensure_dir()?;

    let lock_path = project_root.join("tl.lock");
    let mut lock = LockFile::load(&lock_path)?;

    let mut new_packages: Vec<LockedPackage> = Vec::new();

    for (name, spec) in &manifest.dependencies {
        // Check if already locked and cached
        if let Some(locked) = lock.find(name) {
            if spec_matches_locked(spec, locked) && is_available(name, locked, cache) {
                new_packages.push(locked.clone());
                continue;
            }
        }

        // Fetch the dependency
        let result = fetch_dependency(name, spec, project_root, cache)?;

        new_packages.push(LockedPackage {
            name: result.name,
            version: result.version,
            source: result.source_desc,
        });
    }

    // Update lock file
    lock.packages = new_packages;
    lock.save(&lock_path)?;

    Ok(lock)
}

/// Check if a dependency spec is compatible with what's locked.
pub fn spec_matches_locked(spec: &DependencySpec, locked: &LockedPackage) -> bool {
    match spec.source_kind() {
        DepSourceKind::Path => locked.is_path(),
        DepSourceKind::Git => {
            if !locked.is_git() {
                return false;
            }
            // Check URL matches
            if let DependencySpec::Detailed(d) = spec {
                if let (Some(spec_url), Some(locked_url)) = (d.git.as_deref(), locked.git_url()) {
                    return spec_url == locked_url;
                }
            }
            false
        }
        DepSourceKind::Registry => {
            // Registry deps match if version requirement is satisfied
            if let DependencySpec::Simple(req_str) = spec {
                if let Ok(req) = crate::version::VersionReq::parse(req_str) {
                    if let Ok(ver) = crate::version::Version::parse(&locked.version) {
                        return req.matches(&ver);
                    }
                }
            }
            false
        }
    }
}

/// Check if a locked package is available (cached or path exists).
fn is_available(name: &str, locked: &LockedPackage, cache: &PackageCache) -> bool {
    if locked.is_path() {
        // For path deps, check the source directory still exists
        if let Some(path) = locked.path_value() {
            return Path::new(path).exists();
        }
        false
    } else {
        cache.is_cached(name, &locked.version)
    }
}

/// Find the source directory for an installed package.
/// Checks path deps first, then cache.
pub fn find_package_source(
    name: &str,
    project_root: &Path,
    cache: &PackageCache,
) -> Option<PathBuf> {
    let lock_path = project_root.join("tl.lock");
    let lock = LockFile::load(&lock_path).ok()?;
    let locked = lock.find(name)?;

    if locked.is_path() {
        let path = locked.path_value()?;
        let abs = PathBuf::from(path);
        if abs.exists() {
            return Some(abs);
        }
        return None;
    }

    // Git/registry: look in cache
    // We want the package root (containing tl.toml), not just src/
    if cache.is_cached(name, &locked.version) {
        Some(cache.package_dir(name, &locked.version))
    } else {
        None
    }
}

/// Build a map of package name -> source root for all installed packages.
pub fn build_package_roots(
    project_root: &Path,
    cache: &PackageCache,
) -> std::collections::HashMap<String, PathBuf> {
    let mut roots = std::collections::HashMap::new();
    let lock_path = project_root.join("tl.lock");
    if let Ok(lock) = LockFile::load(&lock_path) {
        for pkg in &lock.packages {
            if let Some(path) = find_single_package_source(&pkg, cache) {
                roots.insert(pkg.name.clone(), path);
            }
        }
    }
    roots
}

fn find_single_package_source(locked: &LockedPackage, cache: &PackageCache) -> Option<PathBuf> {
    if locked.is_path() {
        let path = locked.path_value()?;
        let abs = PathBuf::from(path);
        if abs.exists() {
            return Some(abs);
        }
        return None;
    }
    Some(cache.package_dir(&locked.name, &locked.version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{DetailedDep, ProjectConfig};
    use tempfile::TempDir;

    fn make_test_package(dir: &Path, name: &str, version: &str) {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(
            dir.join("tl.toml"),
            format!("[project]\nname = \"{name}\"\nversion = \"{version}\"\n"),
        )
        .unwrap();
        std::fs::write(dir.join("src/lib.tl"), "pub fn greet() { print(\"hi\") }\n").unwrap();
    }

    #[test]
    fn install_with_path_dep() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        let lib = tmp.path().join("mylib");
        std::fs::create_dir_all(&project).unwrap();
        make_test_package(&lib, "mylib", "1.0.0");

        let manifest = Manifest {
            project: ProjectConfig {
                name: "test".into(),
                version: "0.1.0".into(),
                edition: None,
                authors: None,
                description: None,
                entry: None,
            },
            dependencies: {
                let mut deps = std::collections::BTreeMap::new();
                deps.insert(
                    "mylib".into(),
                    DependencySpec::Detailed(DetailedDep {
                        version: None,
                        git: None,
                        branch: None,
                        tag: None,
                        rev: None,
                        path: Some(lib.to_string_lossy().into()),
                    }),
                );
                deps
            },
        };

        let cache = PackageCache::new(tmp.path().join("cache"));
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert_eq!(lock.packages.len(), 1);
        assert_eq!(lock.packages[0].name, "mylib");
        assert_eq!(lock.packages[0].version, "1.0.0");
        assert!(lock.packages[0].is_path());

        // Lock file should exist on disk
        assert!(project.join("tl.lock").exists());
    }

    #[test]
    fn install_empty_deps() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();

        let manifest = Manifest {
            project: ProjectConfig {
                name: "test".into(),
                version: "0.1.0".into(),
                edition: None,
                authors: None,
                description: None,
                entry: None,
            },
            dependencies: std::collections::BTreeMap::new(),
        };

        let cache = PackageCache::new(tmp.path().join("cache"));
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert!(lock.packages.is_empty());
    }

    #[test]
    fn lock_reuse() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        let lib = tmp.path().join("mylib");
        std::fs::create_dir_all(&project).unwrap();
        make_test_package(&lib, "mylib", "1.0.0");

        let manifest = Manifest {
            project: ProjectConfig {
                name: "test".into(),
                version: "0.1.0".into(),
                edition: None,
                authors: None,
                description: None,
                entry: None,
            },
            dependencies: {
                let mut deps = std::collections::BTreeMap::new();
                deps.insert(
                    "mylib".into(),
                    DependencySpec::Detailed(DetailedDep {
                        version: None,
                        git: None,
                        branch: None,
                        tag: None,
                        rev: None,
                        path: Some(lib.to_string_lossy().into()),
                    }),
                );
                deps
            },
        };

        let cache = PackageCache::new(tmp.path().join("cache"));

        // First install
        let lock1 = resolve_and_install(&project, &manifest, &cache).unwrap();
        // Second install should reuse lock
        let lock2 = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert_eq!(lock1.packages, lock2.packages);
    }

    #[test]
    fn spec_matches_locked_path() {
        let locked = LockedPackage {
            name: "mylib".into(),
            version: "1.0.0".into(),
            source: LockedPackage::path_source("/tmp/mylib"),
        };
        let spec = DependencySpec::Detailed(DetailedDep {
            version: None, git: None, branch: None, tag: None, rev: None,
            path: Some("/tmp/mylib".into()),
        });
        assert!(spec_matches_locked(&spec, &locked));
    }

    #[test]
    fn spec_matches_locked_git() {
        let locked = LockedPackage {
            name: "remote".into(),
            version: "2.0.0".into(),
            source: LockedPackage::git_source("https://github.com/user/remote.git", "abc123"),
        };
        let spec = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: Some("https://github.com/user/remote.git".into()),
            branch: Some("main".into()),
            tag: None, rev: None, path: None,
        });
        assert!(spec_matches_locked(&spec, &locked));
    }
}
