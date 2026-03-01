use crate::cache::PackageCache;
use crate::fetch::{fetch_dependency, read_package_manifest};
use crate::lockfile::{LockFile, LockedPackage};
use crate::manifest::{DepSourceKind, DependencySpec, Manifest};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

/// Describes a single dependency change during resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepChange {
    Added { version: String },
    Updated { from: String, to: String },
    Unchanged { version: String },
    Removed { version: String },
}

/// A report of all changes produced by a resolve operation.
#[derive(Debug, Clone, Default)]
pub struct ResolveReport {
    pub changes: Vec<(String, DepChange)>,
}

impl ResolveReport {
    pub fn added_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|(_, c)| matches!(c, DepChange::Added { .. }))
            .count()
    }
    pub fn updated_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|(_, c)| matches!(c, DepChange::Updated { .. }))
            .count()
    }
    pub fn removed_count(&self) -> usize {
        self.changes
            .iter()
            .filter(|(_, c)| matches!(c, DepChange::Removed { .. }))
            .count()
    }
    pub fn has_changes(&self) -> bool {
        self.changes
            .iter()
            .any(|(_, c)| !matches!(c, DepChange::Unchanged { .. }))
    }
}

/// A version conflict between two requesters of the same package.
#[derive(Debug, Clone)]
pub struct VersionConflict {
    pub package: String,
    pub requester_a: String,
    pub requirement_a: String,
    pub requester_b: String,
    pub requirement_b: String,
    pub resolved_version: Option<String>,
}

/// Build a report by diffing an old lock file against a new set of packages.
pub fn build_report(old_lock: &LockFile, new_packages: &[LockedPackage]) -> ResolveReport {
    let mut changes = Vec::new();

    // Check new packages against old
    for pkg in new_packages {
        if let Some(old) = old_lock.find(&pkg.name) {
            if old.version == pkg.version {
                changes.push((
                    pkg.name.clone(),
                    DepChange::Unchanged {
                        version: pkg.version.clone(),
                    },
                ));
            } else {
                changes.push((
                    pkg.name.clone(),
                    DepChange::Updated {
                        from: old.version.clone(),
                        to: pkg.version.clone(),
                    },
                ));
            }
        } else {
            changes.push((
                pkg.name.clone(),
                DepChange::Added {
                    version: pkg.version.clone(),
                },
            ));
        }
    }

    // Check for removed packages
    let new_names: HashSet<&str> = new_packages.iter().map(|p| p.name.as_str()).collect();
    for old_pkg in &old_lock.packages {
        if !new_names.contains(old_pkg.name.as_str()) {
            changes.push((
                old_pkg.name.clone(),
                DepChange::Removed {
                    version: old_pkg.version.clone(),
                },
            ));
        }
    }

    ResolveReport { changes }
}

/// Detect version conflicts in the requirements map.
/// `requirements` maps package name -> list of (requester_name, version_req_string).
pub fn detect_conflicts(
    requirements: &BTreeMap<String, Vec<(String, String)>>,
    resolved: &BTreeMap<String, String>,
) -> Vec<VersionConflict> {
    let mut conflicts = Vec::new();

    for (pkg_name, requesters) in requirements {
        if requesters.len() < 2 {
            continue;
        }
        let resolved_version = resolved.get(pkg_name).cloned();
        let resolved_ver = resolved_version
            .as_deref()
            .and_then(|v| crate::version::Version::parse(v).ok());

        if let Some(ref ver) = resolved_ver {
            // Find requesters whose requirement is NOT satisfied by the resolved version
            let unsatisfied: Vec<usize> = (0..requesters.len())
                .filter(|&i| {
                    crate::version::VersionReq::parse(&requesters[i].1)
                        .is_ok_and(|req| !req.matches(ver))
                })
                .collect();
            let satisfied: Vec<usize> = (0..requesters.len())
                .filter(|&i| {
                    crate::version::VersionReq::parse(&requesters[i].1)
                        .is_ok_and(|req| req.matches(ver))
                })
                .collect();

            // If some are unsatisfied, pair each unsatisfied with a satisfied one (or another unsatisfied)
            for &u in &unsatisfied {
                let other = if !satisfied.is_empty() {
                    satisfied[0]
                } else {
                    // All unsatisfied — pair with first different one
                    *unsatisfied.iter().find(|&&x| x != u).unwrap_or(&u)
                };
                if other != u {
                    conflicts.push(VersionConflict {
                        package: pkg_name.clone(),
                        requester_a: requesters[u].0.clone(),
                        requirement_a: requesters[u].1.clone(),
                        requester_b: requesters[other].0.clone(),
                        requirement_b: requesters[other].1.clone(),
                        resolved_version: resolved_version.clone(),
                    });
                    break; // One conflict per package is enough
                }
            }
        } else {
            // No resolved version — flag that we can't resolve
            conflicts.push(VersionConflict {
                package: pkg_name.clone(),
                requester_a: requesters[0].0.clone(),
                requirement_a: requesters[0].1.clone(),
                requester_b: requesters[1].0.clone(),
                requirement_b: requesters[1].1.clone(),
                resolved_version: None,
            });
        }
    }

    conflicts
}

/// Resolve all dependencies with transitive resolution and produce a report.
pub fn resolve_and_install_with_report(
    project_root: &Path,
    manifest: &Manifest,
    cache: &PackageCache,
) -> Result<(LockFile, ResolveReport), String> {
    cache.ensure_dir()?;

    let lock_path = project_root.join("tl.lock");
    let old_lock = LockFile::load(&lock_path)?;

    let new_packages = resolve_packages(project_root, manifest, &old_lock, cache)?;

    let report = build_report(&old_lock, &new_packages);

    let lock = LockFile {
        packages: new_packages,
    };
    lock.save(&lock_path)?;

    Ok((lock, report))
}

/// Core resolution logic with BFS transitive dependency resolution.
fn resolve_packages(
    project_root: &Path,
    manifest: &Manifest,
    old_lock: &LockFile,
    cache: &PackageCache,
) -> Result<Vec<LockedPackage>, String> {
    let mut resolved: Vec<LockedPackage> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    // Track requirements: package -> [(requester, version_req)]
    let mut requirements: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

    // BFS queue: (name, spec, is_direct, requester)
    let mut queue: VecDeque<(String, DependencySpec, bool, String)> = VecDeque::new();

    // Seed with direct dependencies
    for (name, spec) in &manifest.dependencies {
        queue.push_back((
            name.clone(),
            spec.clone(),
            true,
            manifest.project.name.clone(),
        ));
        // Track version requirement
        if let DependencySpec::Simple(req) = spec {
            requirements
                .entry(name.clone())
                .or_default()
                .push((manifest.project.name.clone(), req.clone()));
        } else if let DependencySpec::Detailed(d) = spec
            && let Some(ref v) = d.version
        {
            requirements
                .entry(name.clone())
                .or_default()
                .push((manifest.project.name.clone(), v.clone()));
        }
    }

    while let Some((name, spec, is_direct, _requester)) = queue.pop_front() {
        if visited.contains(&name) {
            continue;
        }
        visited.insert(name.clone());

        // Check if already locked and cached
        if let Some(locked) = old_lock.find(&name)
            && spec_matches_locked(&spec, locked)
            && is_available(&name, locked, cache)
        {
            let mut pkg = locked.clone();
            pkg.direct = is_direct;
            resolved.push(pkg);

            // Discover transitive deps from cached package
            discover_transitive_deps(&name, locked, cache, &mut queue, &mut requirements);
            continue;
        }

        // Fetch the dependency
        let result = fetch_dependency(&name, &spec, project_root, cache)?;

        let mut locked_pkg = LockedPackage::new(
            result.name.clone(),
            result.version.clone(),
            result.source_desc,
        );
        locked_pkg.direct = is_direct;

        // Discover transitive deps from the freshly fetched package
        let dep_dir = &result.cache_path;
        if let Some(dep_manifest) = read_package_manifest(dep_dir) {
            let mut dep_names = Vec::new();
            for (dep_name, dep_spec) in &dep_manifest.dependencies {
                dep_names.push(dep_name.clone());
                if !visited.contains(dep_name) {
                    // Track version requirement
                    if let DependencySpec::Simple(req) = dep_spec {
                        requirements
                            .entry(dep_name.clone())
                            .or_default()
                            .push((name.clone(), req.clone()));
                    } else if let DependencySpec::Detailed(d) = dep_spec
                        && let Some(ref v) = d.version
                    {
                        requirements
                            .entry(dep_name.clone())
                            .or_default()
                            .push((name.clone(), v.clone()));
                    }
                    queue.push_back((dep_name.clone(), dep_spec.clone(), false, name.clone()));
                }
            }
            locked_pkg.dependencies = dep_names;
        }

        resolved.push(locked_pkg);
    }

    // Check for version conflicts
    let resolved_versions: BTreeMap<String, String> = resolved
        .iter()
        .map(|p| (p.name.clone(), p.version.clone()))
        .collect();
    let conflicts = detect_conflicts(&requirements, &resolved_versions);
    if !conflicts.is_empty() {
        let mut msg = String::from("Version conflicts detected:\n");
        for c in &conflicts {
            msg.push_str(&format!(
                "  {} required by {} ({}) and {} ({})",
                c.package, c.requester_a, c.requirement_a, c.requester_b, c.requirement_b,
            ));
            if let Some(ref v) = c.resolved_version {
                msg.push_str(&format!(", resolved to {v}"));
            }
            msg.push('\n');
        }
        return Err(msg);
    }

    Ok(resolved)
}

/// Discover transitive dependencies from a locked (and cached) package.
fn discover_transitive_deps(
    name: &str,
    locked: &LockedPackage,
    cache: &PackageCache,
    queue: &mut VecDeque<(String, DependencySpec, bool, String)>,
    requirements: &mut BTreeMap<String, Vec<(String, String)>>,
) {
    let dir = if locked.is_path() {
        locked.path_value().map(PathBuf::from)
    } else {
        Some(cache.package_dir(&locked.name, &locked.version))
    };

    if let Some(dir) = dir
        && let Some(dep_manifest) = read_package_manifest(&dir)
    {
        for (dep_name, dep_spec) in &dep_manifest.dependencies {
            if let DependencySpec::Simple(req) = dep_spec {
                requirements
                    .entry(dep_name.clone())
                    .or_default()
                    .push((name.to_string(), req.clone()));
            } else if let DependencySpec::Detailed(d) = dep_spec
                && let Some(ref v) = d.version
            {
                requirements
                    .entry(dep_name.clone())
                    .or_default()
                    .push((name.to_string(), v.clone()));
            }
            queue.push_back((dep_name.clone(), dep_spec.clone(), false, name.to_string()));
        }
    }
}

/// Preview what would change without modifying tl.lock.
pub fn resolve_dry_run(
    project_root: &Path,
    manifest: &Manifest,
    cache: &PackageCache,
) -> Result<ResolveReport, String> {
    let lock_path = project_root.join("tl.lock");
    let old_lock = LockFile::load(&lock_path)?;

    // Simulate resolution — for registry deps, we'd query for latest matching version.
    // For path/git deps, we read what they would resolve to.
    // We reuse resolve_packages but don't save the lock file.
    cache.ensure_dir()?;
    let new_packages = resolve_packages(project_root, manifest, &old_lock, cache)?;
    Ok(build_report(&old_lock, &new_packages))
}

/// Resolve all dependencies from a manifest, fetching as needed, and produce a lock file.
pub fn resolve_and_install(
    project_root: &Path,
    manifest: &Manifest,
    cache: &PackageCache,
) -> Result<LockFile, String> {
    let (lock, _report) = resolve_and_install_with_report(project_root, manifest, cache)?;
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
            if let DependencySpec::Detailed(d) = spec
                && let (Some(spec_url), Some(locked_url)) = (d.git.as_deref(), locked.git_url())
            {
                return spec_url == locked_url;
            }
            false
        }
        DepSourceKind::Registry => {
            // Registry deps match if version requirement is satisfied
            if let DependencySpec::Simple(req_str) = spec
                && let Ok(req) = crate::version::VersionReq::parse(req_str)
                && let Ok(ver) = crate::version::Version::parse(&locked.version)
            {
                return req.matches(&ver);
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
            if let Some(path) = find_single_package_source(pkg, cache) {
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

    fn make_test_package_with_deps(dir: &Path, name: &str, version: &str, deps: &[(&str, &str)]) {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        let mut toml =
            format!("[project]\nname = \"{name}\"\nversion = \"{version}\"\n\n[dependencies]\n");
        for (dep_name, dep_path) in deps {
            toml.push_str(&format!("{dep_name} = {{ path = \"{dep_path}\" }}\n"));
        }
        std::fs::write(dir.join("tl.toml"), toml).unwrap();
        std::fs::write(dir.join("src/lib.tl"), "pub fn greet() { print(\"hi\") }\n").unwrap();
    }

    fn test_manifest_with_path_dep(name: &str, path: &Path) -> Manifest {
        Manifest {
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
                    name.into(),
                    DependencySpec::Detailed(DetailedDep {
                        version: None,
                        git: None,
                        branch: None,
                        tag: None,
                        rev: None,
                        path: Some(path.to_string_lossy().into()),
                    }),
                );
                deps
            },
        }
    }

    // --- Original tests ---

    #[test]
    fn install_with_path_dep() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        let lib = tmp.path().join("mylib");
        std::fs::create_dir_all(&project).unwrap();
        make_test_package(&lib, "mylib", "1.0.0");

        let manifest = test_manifest_with_path_dep("mylib", &lib);
        let cache = PackageCache::new(tmp.path().join("cache"));
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert_eq!(lock.packages.len(), 1);
        assert_eq!(lock.packages[0].name, "mylib");
        assert_eq!(lock.packages[0].version, "1.0.0");
        assert!(lock.packages[0].is_path());
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

        let manifest = test_manifest_with_path_dep("mylib", &lib);
        let cache = PackageCache::new(tmp.path().join("cache"));

        let lock1 = resolve_and_install(&project, &manifest, &cache).unwrap();
        let lock2 = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert_eq!(lock1.packages, lock2.packages);
    }

    #[test]
    fn spec_matches_locked_path() {
        let locked = LockedPackage::new("mylib", "1.0.0", LockedPackage::path_source("/tmp/mylib"));
        let spec = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: None,
            branch: None,
            tag: None,
            rev: None,
            path: Some("/tmp/mylib".into()),
        });
        assert!(spec_matches_locked(&spec, &locked));
    }

    #[test]
    fn spec_matches_locked_git() {
        let locked = LockedPackage::new(
            "remote",
            "2.0.0",
            LockedPackage::git_source("https://github.com/user/remote.git", "abc123"),
        );
        let spec = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: Some("https://github.com/user/remote.git".into()),
            branch: Some("main".into()),
            tag: None,
            rev: None,
            path: None,
        });
        assert!(spec_matches_locked(&spec, &locked));
    }

    // --- ResolveReport tests ---

    #[test]
    fn test_resolve_report_added() {
        let old_lock = LockFile::default();
        let new = vec![LockedPackage::new("newpkg", "1.0.0", "path+/new".into())];
        let report = build_report(&old_lock, &new);
        assert_eq!(report.changes.len(), 1);
        assert!(matches!(&report.changes[0].1, DepChange::Added { version } if version == "1.0.0"));
        assert_eq!(report.added_count(), 1);
        assert!(report.has_changes());
    }

    #[test]
    fn test_resolve_report_updated() {
        let old_lock = LockFile {
            packages: vec![LockedPackage::new("pkg", "1.0.0", "path+/p".into())],
        };
        let new = vec![LockedPackage::new("pkg", "1.2.0", "path+/p".into())];
        let report = build_report(&old_lock, &new);
        assert_eq!(report.changes.len(), 1);
        assert!(
            matches!(&report.changes[0].1, DepChange::Updated { from, to } if from == "1.0.0" && to == "1.2.0")
        );
        assert_eq!(report.updated_count(), 1);
    }

    #[test]
    fn test_resolve_report_unchanged() {
        let old_lock = LockFile {
            packages: vec![LockedPackage::new("pkg", "1.0.0", "path+/p".into())],
        };
        let new = vec![LockedPackage::new("pkg", "1.0.0", "path+/p".into())];
        let report = build_report(&old_lock, &new);
        assert_eq!(report.changes.len(), 1);
        assert!(
            matches!(&report.changes[0].1, DepChange::Unchanged { version } if version == "1.0.0")
        );
        assert!(!report.has_changes());
    }

    #[test]
    fn test_resolve_report_removed() {
        let old_lock = LockFile {
            packages: vec![LockedPackage::new("oldpkg", "2.0.0", "path+/old".into())],
        };
        let new: Vec<LockedPackage> = vec![];
        let report = build_report(&old_lock, &new);
        assert_eq!(report.changes.len(), 1);
        assert!(
            matches!(&report.changes[0].1, DepChange::Removed { version } if version == "2.0.0")
        );
        assert_eq!(report.removed_count(), 1);
    }

    // --- Transitive resolution tests ---

    #[test]
    fn test_transitive_resolution() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();

        // Create sub-dep (no deps of its own)
        let sub_dep = tmp.path().join("sub-dep");
        make_test_package(&sub_dep, "sub-dep", "0.5.0");

        // Create lib that depends on sub-dep
        let lib = tmp.path().join("mylib");
        make_test_package_with_deps(
            &lib,
            "mylib",
            "1.0.0",
            &[("sub-dep", &sub_dep.to_string_lossy())],
        );

        let manifest = test_manifest_with_path_dep("mylib", &lib);
        let cache = PackageCache::new(tmp.path().join("cache"));
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();

        // Should have both mylib (direct) and sub-dep (transitive)
        assert_eq!(lock.packages.len(), 2);
        let mylib = lock.packages.iter().find(|p| p.name == "mylib").unwrap();
        let subdep = lock.packages.iter().find(|p| p.name == "sub-dep").unwrap();
        assert!(mylib.direct);
        assert!(!subdep.direct);
        assert_eq!(mylib.dependencies, vec!["sub-dep".to_string()]);
    }

    #[test]
    fn test_transitive_no_cycles() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();

        // Create A that depends on B, and B that depends on A (circular)
        let a_dir = tmp.path().join("a");
        let b_dir = tmp.path().join("b");

        // Create B first (depends on A)
        make_test_package_with_deps(&b_dir, "b", "1.0.0", &[("a", &a_dir.to_string_lossy())]);

        // Create A (depends on B)
        make_test_package_with_deps(&a_dir, "a", "1.0.0", &[("b", &b_dir.to_string_lossy())]);

        let manifest = test_manifest_with_path_dep("a", &a_dir);
        let cache = PackageCache::new(tmp.path().join("cache"));
        // Should not loop infinitely
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();
        assert_eq!(lock.packages.len(), 2);
    }

    #[test]
    fn test_transitive_diamond() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();

        // D has no deps
        let d_dir = tmp.path().join("d");
        make_test_package(&d_dir, "d", "1.0.0");

        // B depends on D
        let b_dir = tmp.path().join("b");
        make_test_package_with_deps(&b_dir, "b", "1.0.0", &[("d", &d_dir.to_string_lossy())]);

        // C depends on D
        let c_dir = tmp.path().join("c");
        make_test_package_with_deps(&c_dir, "c", "1.0.0", &[("d", &d_dir.to_string_lossy())]);

        // Project depends on B and C (both depend on D)
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
                    "b".into(),
                    DependencySpec::Detailed(DetailedDep {
                        version: None,
                        git: None,
                        branch: None,
                        tag: None,
                        rev: None,
                        path: Some(b_dir.to_string_lossy().into()),
                    }),
                );
                deps.insert(
                    "c".into(),
                    DependencySpec::Detailed(DetailedDep {
                        version: None,
                        git: None,
                        branch: None,
                        tag: None,
                        rev: None,
                        path: Some(c_dir.to_string_lossy().into()),
                    }),
                );
                deps
            },
        };

        let cache = PackageCache::new(tmp.path().join("cache"));
        let lock = resolve_and_install(&project, &manifest, &cache).unwrap();

        // Should have B, C, D (D resolved once)
        assert_eq!(lock.packages.len(), 3);
        let d_count = lock.packages.iter().filter(|p| p.name == "d").count();
        assert_eq!(d_count, 1, "D should appear exactly once");
        let d = lock.packages.iter().find(|p| p.name == "d").unwrap();
        assert!(!d.direct, "D should be transitive");
    }

    // --- Conflict detection tests ---

    #[test]
    fn test_conflict_detection() {
        let mut requirements: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        requirements.insert(
            "shared".into(),
            vec![
                ("pkg-a".into(), "^1.0".into()),
                ("pkg-b".into(), "^2.0".into()),
            ],
        );
        // Resolved to 1.5.0, which satisfies ^1.0 but not ^2.0
        let mut resolved = BTreeMap::new();
        resolved.insert("shared".into(), "1.5.0".into());

        let conflicts = detect_conflicts(&requirements, &resolved);
        assert!(!conflicts.is_empty(), "should detect version conflict");
        assert_eq!(conflicts[0].package, "shared");
    }

    #[test]
    fn test_conflict_compatible() {
        let mut requirements: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        requirements.insert(
            "shared".into(),
            vec![
                ("pkg-a".into(), "^1.0".into()),
                ("pkg-b".into(), "^1.2".into()),
            ],
        );
        // Resolved to 1.5.0, which satisfies both ^1.0 and ^1.2
        let mut resolved = BTreeMap::new();
        resolved.insert("shared".into(), "1.5.0".into());

        let conflicts = detect_conflicts(&requirements, &resolved);
        assert!(
            conflicts.is_empty(),
            "no conflict expected for compatible requirements"
        );
    }

    // --- resolve_and_install_with_report test ---

    #[test]
    fn test_install_with_report() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("project");
        let lib = tmp.path().join("mylib");
        std::fs::create_dir_all(&project).unwrap();
        make_test_package(&lib, "mylib", "1.0.0");

        let manifest = test_manifest_with_path_dep("mylib", &lib);
        let cache = PackageCache::new(tmp.path().join("cache"));

        let (lock, report) = resolve_and_install_with_report(&project, &manifest, &cache).unwrap();
        assert_eq!(lock.packages.len(), 1);
        // First install — everything is "Added"
        assert_eq!(report.added_count(), 1);
        assert!(report.has_changes());
    }
}
