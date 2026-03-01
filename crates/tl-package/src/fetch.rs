use crate::cache::PackageCache;
use crate::manifest::{DependencySpec, DetailedDep, Manifest};
use std::path::PathBuf;
use std::process::Command;

/// Result of fetching a dependency.
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub name: String,
    pub version: String,
    pub source_desc: String,
    pub cache_path: PathBuf,
}

/// Fetch a dependency into the cache based on its spec.
pub fn fetch_dependency(
    name: &str,
    spec: &DependencySpec,
    project_root: &std::path::Path,
    cache: &PackageCache,
) -> Result<FetchResult, String> {
    match spec {
        DependencySpec::Simple(version_req) => fetch_registry(name, version_req, cache),
        DependencySpec::Detailed(d) => {
            if d.git.is_some() {
                fetch_git(name, d, cache)
            } else if d.path.is_some() {
                fetch_path(name, d, project_root, cache)
            } else if d.version.is_some() {
                fetch_registry(name, d.version.as_deref().unwrap(), cache)
            } else {
                Err(format!(
                    "Dependency '{name}' has no source specified. \
                     Use `path = \"..\"` for local or `git = \"url\"` for remote."
                ))
            }
        }
    }
}

/// Fetch from a git repository.
fn fetch_git(name: &str, dep: &DetailedDep, cache: &PackageCache) -> Result<FetchResult, String> {
    let url = dep.git.as_deref().unwrap();

    // Create a temporary clone directory
    let tmp_dir = std::env::temp_dir().join(format!("tl-fetch-{name}-{}", std::process::id()));
    if tmp_dir.exists() {
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    // Build git clone command
    let mut cmd = Command::new("git");
    cmd.arg("clone").arg("--depth").arg("1");

    if let Some(ref branch) = dep.branch {
        cmd.arg("--branch").arg(branch);
    } else if let Some(ref tag) = dep.tag {
        cmd.arg("--branch").arg(tag);
    }

    cmd.arg(url).arg(&tmp_dir);

    let output = cmd.output().map_err(|e| {
        format!("Failed to run git clone for '{name}': {e}. Is git installed?")
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = std::fs::remove_dir_all(&tmp_dir);
        return Err(format!("Git clone failed for '{name}': {stderr}"));
    }

    // If a specific rev is requested, check it out
    if let Some(ref rev) = dep.rev {
        let checkout = Command::new("git")
            .arg("-C")
            .arg(&tmp_dir)
            .arg("checkout")
            .arg(rev)
            .output()
            .map_err(|e| format!("Failed to checkout rev '{rev}': {e}"))?;

        if !checkout.status.success() {
            let stderr = String::from_utf8_lossy(&checkout.stderr);
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(format!("Git checkout failed for '{name}' rev '{rev}': {stderr}"));
        }
    }

    // Get the current rev hash
    let rev_output = Command::new("git")
        .arg("-C")
        .arg(&tmp_dir)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|e| format!("Failed to get git rev: {e}"))?;

    let rev = String::from_utf8_lossy(&rev_output.stdout).trim().to_string();

    // Read tl.toml from the cloned repo to get version
    let version = read_package_version(&tmp_dir, name)?;

    // Copy to cache
    let cache_dir = cache.package_dir(name, &version);
    if cache_dir.exists() {
        let _ = std::fs::remove_dir_all(&cache_dir);
    }
    std::fs::create_dir_all(cache_dir.parent().unwrap())
        .map_err(|e| format!("Failed to create cache dir: {e}"))?;
    copy_dir_recursive(&tmp_dir, &cache_dir)?;

    // Clean up .git directory in cache
    let git_dir = cache_dir.join(".git");
    if git_dir.exists() {
        let _ = std::fs::remove_dir_all(&git_dir);
    }

    // Clean up tmp
    let _ = std::fs::remove_dir_all(&tmp_dir);

    let source_desc = crate::lockfile::LockedPackage::git_source(url, &rev);

    Ok(FetchResult {
        name: name.to_string(),
        version,
        source_desc,
        cache_path: cache_dir,
    })
}

/// Fetch from a local path.
fn fetch_path(
    name: &str,
    dep: &DetailedDep,
    project_root: &std::path::Path,
    cache: &PackageCache,
) -> Result<FetchResult, String> {
    let raw_path = dep.path.as_deref().unwrap();
    let abs_path = if std::path::Path::new(raw_path).is_absolute() {
        PathBuf::from(raw_path)
    } else {
        project_root.join(raw_path)
    };

    let canonical = abs_path.canonicalize().map_err(|e| {
        format!("Path dependency '{name}' at '{}' not found: {e}", abs_path.display())
    })?;

    // Validate tl.toml exists
    let manifest_path = canonical.join("tl.toml");
    if !manifest_path.exists() {
        return Err(format!(
            "Path dependency '{name}' at '{}' has no tl.toml",
            canonical.display()
        ));
    }

    let version = read_package_version(&canonical, name)?;
    let source_desc = crate::lockfile::LockedPackage::path_source(&canonical.to_string_lossy());

    // For path deps, we store a symlink or direct reference in cache
    let cache_dir = cache.package_dir(name, &version);
    if cache_dir.exists() {
        let _ = std::fs::remove_dir_all(&cache_dir);
    }
    std::fs::create_dir_all(cache_dir.parent().unwrap())
        .map_err(|e| format!("Failed to create cache dir: {e}"))?;

    // Create symlink for path deps (so changes are reflected immediately)
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&canonical, &cache_dir)
            .map_err(|e| format!("Failed to symlink path dependency: {e}"))?;
    }
    #[cfg(not(unix))]
    {
        copy_dir_recursive(&canonical, &cache_dir)?;
    }

    Ok(FetchResult {
        name: name.to_string(),
        version,
        source_desc,
        cache_path: cache_dir,
    })
}

/// Registry fetch — downloads from the package registry when the `registry` feature is enabled.
fn fetch_registry(name: &str, version_req: &str, cache: &PackageCache) -> Result<FetchResult, String> {
    #[cfg(feature = "registry")]
    {
        fetch_registry_impl(name, version_req, cache)
    }
    #[cfg(not(feature = "registry"))]
    {
        let _ = cache;
        Err(format!(
            "Package registry is not yet available.\n\
             Cannot fetch '{name}' version '{version_req}' from registry.\n\
             \n\
             Use one of these alternatives:\n\
             - Git dependency:  tl add {name} --git https://github.com/user/{name}.git\n\
             - Path dependency: tl add {name} --path ../path/to/{name}"
        ))
    }
}

#[cfg(feature = "registry")]
fn fetch_registry_impl(name: &str, version_req: &str, cache: &PackageCache) -> Result<FetchResult, String> {
    use crate::version::VersionReq;

    // Get package info from registry
    let info = crate::registry_client::get_package_info(name)?;

    // Find the best matching version
    let req = VersionReq::parse(version_req)?;
    let matching = info
        .versions
        .iter()
        .filter(|v| {
            crate::version::Version::parse(&v.version)
                .is_ok_and(|ver| req.matches(&ver))
        })
        .last(); // latest matching version

    let version_entry = matching.ok_or_else(|| {
        format!(
            "No version of '{name}' matches requirement '{version_req}'"
        )
    })?;

    let version = &version_entry.version;

    // Download tarball
    let tarball = crate::registry_client::download_package(name, version)?;

    // Verify hash
    {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&tarball);
        let hash = format!("{:x}", hasher.finalize());
        if hash != version_entry.sha256 {
            return Err(format!(
                "SHA-256 mismatch for '{name}' v{version}: expected {}, got {hash}",
                version_entry.sha256
            ));
        }
    }

    // Extract to cache
    let cache_dir = cache.package_dir(name, version);
    if cache_dir.exists() {
        let _ = std::fs::remove_dir_all(&cache_dir);
    }
    std::fs::create_dir_all(&cache_dir)
        .map_err(|e| format!("Failed to create cache dir: {e}"))?;

    {
        use flate2::read::GzDecoder;
        use tar::Archive;
        let decoder = GzDecoder::new(tarball.as_slice());
        let mut archive = Archive::new(decoder);
        archive
            .unpack(&cache_dir)
            .map_err(|e| format!("Failed to extract package: {e}"))?;
    }

    let source_desc = format!("registry+{}@{version}", crate::registry_client::registry_url());

    Ok(FetchResult {
        name: name.to_string(),
        version: version.clone(),
        source_desc,
        cache_path: cache_dir,
    })
}

/// Read the version from a package's tl.toml.
fn read_package_version(dir: &std::path::Path, name: &str) -> Result<String, String> {
    let manifest_path = dir.join("tl.toml");
    if !manifest_path.exists() {
        return Ok("0.0.0".to_string());
    }
    let content = std::fs::read_to_string(&manifest_path)
        .map_err(|e| format!("Failed to read tl.toml for '{name}': {e}"))?;
    let manifest: Manifest = toml::from_str(&content)
        .map_err(|e| format!("Failed to parse tl.toml for '{name}': {e}"))?;
    Ok(manifest.project.version)
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(dst)
        .map_err(|e| format!("Failed to create dir '{}': {e}", dst.display()))?;

    for entry in std::fs::read_dir(src)
        .map_err(|e| format!("Failed to read dir '{}': {e}", src.display()))?
    {
        let entry = entry.map_err(|e| format!("Failed to read entry: {e}"))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path).map_err(|e| {
                format!(
                    "Failed to copy '{}' to '{}': {e}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_test_package(dir: &std::path::Path, name: &str, version: &str) {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(
            dir.join("tl.toml"),
            format!(
                "[project]\nname = \"{name}\"\nversion = \"{version}\"\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join("src/lib.tl"), "pub fn hello() { print(\"hello\") }\n").unwrap();
    }

    #[test]
    fn fetch_path_valid() {
        let tmp = TempDir::new().unwrap();
        let project_root = tmp.path().join("project");
        let lib_dir = tmp.path().join("mylib");
        std::fs::create_dir_all(&project_root).unwrap();
        make_test_package(&lib_dir, "mylib", "1.0.0");

        let cache = PackageCache::new(tmp.path().join("cache"));
        cache.ensure_dir().unwrap();

        let spec = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: None,
            branch: None,
            tag: None,
            rev: None,
            path: Some(lib_dir.to_string_lossy().into()),
        });

        let result = fetch_dependency("mylib", &spec, &project_root, &cache).unwrap();
        assert_eq!(result.name, "mylib");
        assert_eq!(result.version, "1.0.0");
        assert!(result.source_desc.starts_with("path+"));
    }

    #[test]
    fn fetch_path_invalid() {
        let tmp = TempDir::new().unwrap();
        let cache = PackageCache::new(tmp.path().join("cache"));

        let spec = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: None,
            branch: None,
            tag: None,
            rev: None,
            path: Some("/nonexistent/path".into()),
        });

        let result = fetch_dependency("missing", &spec, tmp.path(), &cache);
        assert!(result.is_err());
    }

    #[test]
    fn fetch_registry_error() {
        let tmp = TempDir::new().unwrap();
        let cache = PackageCache::new(tmp.path().join("cache"));

        let spec = DependencySpec::Simple("1.0".into());
        let result = fetch_dependency("somepkg", &spec, tmp.path(), &cache);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("registry is not yet available"));
        assert!(err.contains("--git"));
        assert!(err.contains("--path"));
    }

    #[test]
    fn fetch_result_format() {
        let result = FetchResult {
            name: "test".into(),
            version: "1.0.0".into(),
            source_desc: "path+/tmp/test".into(),
            cache_path: PathBuf::from("/cache/test/1.0.0"),
        };
        assert_eq!(result.name, "test");
        assert_eq!(result.version, "1.0.0");
    }

    #[test]
    fn read_version_from_manifest() {
        let tmp = TempDir::new().unwrap();
        make_test_package(tmp.path(), "mypkg", "2.3.4");
        let version = read_package_version(tmp.path(), "mypkg").unwrap();
        assert_eq!(version, "2.3.4");
    }
}
