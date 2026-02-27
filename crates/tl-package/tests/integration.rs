use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;
use tl_package::*;

fn make_package(dir: &std::path::Path, name: &str, version: &str, code: &str) {
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(
        dir.join("tl.toml"),
        format!("[project]\nname = \"{name}\"\nversion = \"{version}\"\n\n[dependencies]\n"),
    )
    .unwrap();
    std::fs::write(dir.join("src/lib.tl"), code).unwrap();
}

fn make_project(dir: &std::path::Path, name: &str, deps: &str, main_code: &str) {
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(
        dir.join("tl.toml"),
        format!(
            "[project]\nname = \"{name}\"\nversion = \"0.1.0\"\n\n[dependencies]\n{deps}"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("src/main.tl"), main_code).unwrap();
}

#[test]
fn full_workflow_path_dep() {
    let tmp = TempDir::new().unwrap();

    // Create a library package
    make_package(
        &tmp.path().join("mylib"),
        "mylib",
        "1.0.0",
        "pub fn greet(name) { \"Hello, \" + name + \"!\" }\npub fn double(x) { x * 2 }\n",
    );

    // Create a project that depends on it
    let project = tmp.path().join("project");
    make_project(
        &project,
        "myapp",
        &format!("mylib = {{ path = \"{}\" }}\n", tmp.path().join("mylib").display()),
        "use mylib\nprint(greet(\"world\"))\nprint(double(21))\n",
    );

    // Resolve and install
    let cache = PackageCache::new(tmp.path().join("cache"));
    let manifest = Manifest::load(&project.join("tl.toml")).unwrap();
    let lock = resolve_and_install(&project, &manifest, &cache).unwrap();

    assert_eq!(lock.packages.len(), 1);
    assert_eq!(lock.packages[0].name, "mylib");
    assert_eq!(lock.packages[0].version, "1.0.0");

    // Lock file should exist
    assert!(project.join("tl.lock").exists());

    // Package source should be findable
    let source = find_package_source("mylib", &project, &cache);
    assert!(source.is_some());
}

#[test]
fn version_constraint_matching() {
    // Test that version requirements work correctly
    let req = VersionReq::parse("^1.0").unwrap();
    assert!(req.matches(&Version::parse("1.0.0").unwrap()));
    assert!(req.matches(&Version::parse("1.5.2").unwrap()));
    assert!(!req.matches(&Version::parse("2.0.0").unwrap()));
    assert!(!req.matches(&Version::parse("0.9.0").unwrap()));

    let req2 = VersionReq::parse(">=1.0, <2.0").unwrap();
    assert!(req2.matches(&Version::parse("1.0.0").unwrap()));
    assert!(req2.matches(&Version::parse("1.99.99").unwrap()));
    assert!(!req2.matches(&Version::parse("2.0.0").unwrap()));
}

#[test]
fn lock_file_stability() {
    let tmp = TempDir::new().unwrap();

    make_package(&tmp.path().join("dep"), "dep", "0.5.0", "pub fn x() { 1 }\n");

    let project = tmp.path().join("project");
    make_project(
        &project,
        "app",
        &format!("dep = {{ path = \"{}\" }}\n", tmp.path().join("dep").display()),
        "use dep\nprint(x())\n",
    );

    let cache = PackageCache::new(tmp.path().join("cache"));
    let manifest = Manifest::load(&project.join("tl.toml")).unwrap();

    // First install
    let lock1 = resolve_and_install(&project, &manifest, &cache).unwrap();
    let content1 = std::fs::read_to_string(project.join("tl.lock")).unwrap();

    // Second install (should be idempotent)
    let lock2 = resolve_and_install(&project, &manifest, &cache).unwrap();
    let content2 = std::fs::read_to_string(project.join("tl.lock")).unwrap();

    assert_eq!(lock1.packages.len(), lock2.packages.len());
    assert_eq!(content1, content2);
}

#[test]
fn name_mapping_hyphen_underscore() {
    let tmp = TempDir::new().unwrap();

    // Package name uses hyphens
    make_package(
        &tmp.path().join("my-utils"),
        "my-utils",
        "1.0.0",
        "pub fn helper() { 42 }\n",
    );

    let project = tmp.path().join("project");
    make_project(
        &project,
        "app",
        &format!("my-utils = {{ path = \"{}\" }}\n", tmp.path().join("my-utils").display()),
        "use my_utils\nprint(helper())\n",
    );

    let cache = PackageCache::new(tmp.path().join("cache"));
    let manifest = Manifest::load(&project.join("tl.toml")).unwrap();
    let lock = resolve_and_install(&project, &manifest, &cache).unwrap();

    assert_eq!(lock.packages.len(), 1);
    assert_eq!(lock.packages[0].name, "my-utils");

    // Build package roots and check the mapping
    let roots = resolver::build_package_roots(&project, &cache);
    // The root should be accessible via the hyphenated name
    assert!(roots.contains_key("my-utils"));
}

#[test]
fn error_quality_missing_package() {
    let tmp = TempDir::new().unwrap();

    let project = tmp.path().join("project");
    make_project(&project, "app", "missing = \"1.0\"\n", "use missing\n");

    let cache = PackageCache::new(tmp.path().join("cache"));
    let manifest = Manifest::load(&project.join("tl.toml")).unwrap();

    let result = resolve_and_install(&project, &manifest, &cache);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("registry is not yet available"));
    assert!(err.contains("--git"));
    assert!(err.contains("--path"));
}

#[test]
fn nested_package_modules() {
    let tmp = TempDir::new().unwrap();

    // Create package with nested modules
    let pkg = tmp.path().join("mathlib");
    std::fs::create_dir_all(pkg.join("src")).unwrap();
    std::fs::write(
        pkg.join("tl.toml"),
        "[project]\nname = \"mathlib\"\nversion = \"2.0.0\"\n\n[dependencies]\n",
    )
    .unwrap();
    std::fs::write(pkg.join("src/lib.tl"), "pub fn version() { \"2.0.0\" }\n").unwrap();
    std::fs::write(pkg.join("src/algebra.tl"), "pub fn square(x) { x * x }\n").unwrap();

    let project = tmp.path().join("project");
    make_project(
        &project,
        "app",
        &format!("mathlib = {{ path = \"{}\" }}\n", pkg.display()),
        "use mathlib\nuse mathlib.algebra\nprint(version())\nprint(square(5))\n",
    );

    let cache = PackageCache::new(tmp.path().join("cache"));
    let manifest = Manifest::load(&project.join("tl.toml")).unwrap();
    let lock = resolve_and_install(&project, &manifest, &cache).unwrap();

    assert_eq!(lock.packages.len(), 1);
    assert_eq!(lock.packages[0].name, "mathlib");
    assert_eq!(lock.packages[0].version, "2.0.0");

    // Verify nested module resolution
    let roots = resolver::build_package_roots(&project, &cache);
    let root = roots.get("mathlib").unwrap();
    assert!(root.join("src/algebra.tl").exists() || root.join("src").join("algebra.tl").exists());
}
