use crate::lockfile::LockFile;
use crate::manifest::{DepSourceKind, DependencySpec, Manifest};

/// Information about one outdated dependency.
#[derive(Debug, Clone)]
pub struct OutdatedInfo {
    pub name: String,
    pub current: String,
    /// Latest version matching the manifest's version requirement.
    pub latest_matching: Option<String>,
    /// Latest version available in the registry (may be outside the requirement).
    pub latest_available: Option<String>,
    /// Source kind (for git/path deps, version columns don't apply).
    pub source_kind: DepSourceKind,
}

impl OutdatedInfo {
    pub fn is_up_to_date(&self) -> bool {
        match (&self.latest_matching, &self.latest_available) {
            (Some(matching), _) => matching == &self.current,
            _ => true,
        }
    }
}

/// Check which dependencies are outdated.
/// Accepts a version lookup closure for testability without a live registry.
///
/// `version_lookup(name)` should return a list of available version strings.
pub fn check_outdated_with_versions(
    manifest: &Manifest,
    lock: &LockFile,
    version_lookup: &dyn Fn(&str) -> Result<Vec<String>, String>,
) -> Result<Vec<OutdatedInfo>, String> {
    let mut results = Vec::new();

    for (name, spec) in &manifest.dependencies {
        let source_kind = spec.source_kind();

        // Find current locked version
        let current = match lock.find(name) {
            Some(locked) => locked.version.clone(),
            None => continue, // not installed yet
        };

        if source_kind != DepSourceKind::Registry {
            // For git/path deps, we can't query for newer versions
            results.push(OutdatedInfo {
                name: name.clone(),
                current,
                latest_matching: None,
                latest_available: None,
                source_kind,
            });
            continue;
        }

        // Get the version requirement from the spec
        let version_req_str = match spec {
            DependencySpec::Simple(req) => req.clone(),
            DependencySpec::Detailed(d) => d.version.clone().unwrap_or_else(|| "*".into()),
        };

        // Query available versions
        match version_lookup(name) {
            Ok(versions) => {
                let req = crate::version::VersionReq::parse(&version_req_str)?;

                // Find latest matching version
                let mut matching_versions: Vec<crate::version::Version> = versions
                    .iter()
                    .filter_map(|v| crate::version::Version::parse(v).ok())
                    .filter(|v| req.matches(v))
                    .collect();
                matching_versions.sort();
                let latest_matching = matching_versions.last().map(|v| v.to_string());

                // Find latest available version (any)
                let mut all_versions: Vec<crate::version::Version> = versions
                    .iter()
                    .filter_map(|v| crate::version::Version::parse(v).ok())
                    .collect();
                all_versions.sort();
                let latest_available = all_versions.last().map(|v| v.to_string());

                results.push(OutdatedInfo {
                    name: name.clone(),
                    current,
                    latest_matching,
                    latest_available,
                    source_kind,
                });
            }
            Err(_) => {
                // Registry unreachable — still report with no version info
                results.push(OutdatedInfo {
                    name: name.clone(),
                    current,
                    latest_matching: None,
                    latest_available: None,
                    source_kind,
                });
            }
        }
    }

    Ok(results)
}

/// Check outdated dependencies using the package registry.
#[cfg(feature = "registry")]
pub fn check_outdated(manifest: &Manifest, lock: &LockFile) -> Result<Vec<OutdatedInfo>, String> {
    check_outdated_with_versions(manifest, lock, &|name| {
        let info = crate::registry_client::get_package_info(name)?;
        Ok(info.versions.iter().map(|v| v.version.clone()).collect())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lockfile::LockedPackage;
    use crate::manifest::{DetailedDep, ProjectConfig};
    use std::collections::BTreeMap;

    fn test_manifest(deps: Vec<(&str, DependencySpec)>) -> Manifest {
        let mut dependencies = BTreeMap::new();
        for (name, spec) in deps {
            dependencies.insert(name.to_string(), spec);
        }
        Manifest {
            project: ProjectConfig {
                name: "test".into(),
                version: "0.1.0".into(),
                edition: None,
                authors: None,
                description: None,
                entry: None,
            },
            dependencies,
        }
    }

    #[test]
    fn test_outdated_newer_available() {
        let manifest = test_manifest(vec![("utils", DependencySpec::Simple("^1.0".into()))]);
        let lock = LockFile {
            packages: vec![LockedPackage::new(
                "utils",
                "1.0.0",
                "registry+http://localhost@1.0.0".into(),
            )],
        };

        let results = check_outdated_with_versions(&manifest, &lock, &|_name| {
            Ok(vec!["1.0.0".into(), "1.3.0".into(), "2.0.0".into()])
        })
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "utils");
        assert_eq!(results[0].current, "1.0.0");
        assert_eq!(results[0].latest_matching, Some("1.3.0".into()));
        assert_eq!(results[0].latest_available, Some("2.0.0".into()));
        assert!(!results[0].is_up_to_date());
    }

    #[test]
    fn test_outdated_up_to_date() {
        let manifest = test_manifest(vec![("helpers", DependencySpec::Simple("^2.0".into()))]);
        let lock = LockFile {
            packages: vec![LockedPackage::new(
                "helpers",
                "2.1.0",
                "registry+http://localhost@2.1.0".into(),
            )],
        };

        let results = check_outdated_with_versions(&manifest, &lock, &|_name| {
            Ok(vec!["2.0.0".into(), "2.1.0".into()])
        })
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].is_up_to_date());
    }

    #[test]
    fn test_outdated_git_dep() {
        let manifest = test_manifest(vec![(
            "mylib",
            DependencySpec::Detailed(DetailedDep {
                version: None,
                git: Some("https://github.com/user/mylib.git".into()),
                branch: None,
                tag: None,
                rev: None,
                path: None,
            }),
        )]);
        let lock = LockFile {
            packages: vec![LockedPackage::new(
                "mylib",
                "1.0.0",
                LockedPackage::git_source("https://github.com/user/mylib.git", "abc123"),
            )],
        };

        let results = check_outdated_with_versions(&manifest, &lock, &|_| {
            panic!("should not query registry for git deps");
        })
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source_kind, DepSourceKind::Git);
        assert!(results[0].is_up_to_date()); // git deps always "up to date" by this metric
    }
}
