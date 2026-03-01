use std::path::Path;
use std::process;

use tl_package::{
    Manifest, PackageCache, resolve_and_install,
};

/// `tl add <pkg>` — add a dependency to tl.toml and install it.
pub fn cmd_add(
    name: &str,
    version: Option<&str>,
    git: Option<&str>,
    branch: Option<&str>,
    path: Option<&str>,
) {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
        eprintln!("No tl.toml found. Run 'tl init <name>' to create a project.");
        process::exit(1);
    });

    // Read the manifest with toml_edit to preserve formatting
    let content = std::fs::read_to_string(&manifest_path).unwrap_or_else(|e| {
        eprintln!("Cannot read tl.toml: {e}");
        process::exit(1);
    });

    let mut doc = content.parse::<toml_edit::DocumentMut>().unwrap_or_else(|e| {
        eprintln!("Cannot parse tl.toml: {e}");
        process::exit(1);
    });

    // Ensure [dependencies] table exists
    if doc.get("dependencies").is_none() {
        doc["dependencies"] = toml_edit::Item::Table(toml_edit::Table::new());
    }

    // Build the dependency value
    if let Some(p) = path {
        let mut inline = toml_edit::InlineTable::new();
        inline.insert("path", toml_edit::Value::from(p));
        doc["dependencies"][name] = toml_edit::value(toml_edit::Value::InlineTable(inline));
    } else if let Some(url) = git {
        let mut inline = toml_edit::InlineTable::new();
        inline.insert("git", toml_edit::Value::from(url));
        if let Some(b) = branch {
            inline.insert("branch", toml_edit::Value::from(b));
        }
        if let Some(v) = version {
            inline.insert("version", toml_edit::Value::from(v));
        }
        doc["dependencies"][name] = toml_edit::value(toml_edit::Value::InlineTable(inline));
    } else if let Some(v) = version {
        doc["dependencies"][name] = toml_edit::value(v);
    } else {
        eprintln!("Please specify --path, --git, or --version for dependency '{name}'.");
        eprintln!("Examples:");
        eprintln!("  tl add {name} --path ../path/to/{name}");
        eprintln!("  tl add {name} --git https://github.com/user/{name}.git");
        process::exit(1);
    }

    // Write back
    if let Err(e) = std::fs::write(&manifest_path, doc.to_string()) {
        eprintln!("Cannot write tl.toml: {e}");
        process::exit(1);
    }

    println!("Added dependency '{name}' to tl.toml");

    // Run install
    let project_root = manifest_path.parent().unwrap();
    run_install_for(project_root);
}

/// `tl remove <pkg>` — remove a dependency from tl.toml.
pub fn cmd_remove(name: &str) {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
        eprintln!("No tl.toml found. Run 'tl init <name>' to create a project.");
        process::exit(1);
    });

    let content = std::fs::read_to_string(&manifest_path).unwrap_or_else(|e| {
        eprintln!("Cannot read tl.toml: {e}");
        process::exit(1);
    });

    let mut doc = content.parse::<toml_edit::DocumentMut>().unwrap_or_else(|e| {
        eprintln!("Cannot parse tl.toml: {e}");
        process::exit(1);
    });

    if let Some(deps) = doc.get_mut("dependencies") {
        if let Some(table) = deps.as_table_mut() {
            if table.remove(name).is_some() {
                if let Err(e) = std::fs::write(&manifest_path, doc.to_string()) {
                    eprintln!("Cannot write tl.toml: {e}");
                    process::exit(1);
                }
                println!("Removed dependency '{name}' from tl.toml");

                // Regenerate lock file
                let project_root = manifest_path.parent().unwrap();
                run_install_for(project_root);
                return;
            }
        }
    }

    eprintln!("Dependency '{name}' not found in tl.toml");
    process::exit(1);
}

/// `tl install` — install all dependencies from tl.toml.
pub fn cmd_install() {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
        eprintln!("No tl.toml found. Run 'tl init <name>' to create a project.");
        process::exit(1);
    });

    let project_root = manifest_path.parent().unwrap();
    run_install_for(project_root);
}

/// `tl update [pkg]` — update one or all dependencies.
pub fn cmd_update(pkg: Option<&str>) {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
        eprintln!("No tl.toml found.");
        process::exit(1);
    });

    let project_root = manifest_path.parent().unwrap();
    let lock_path = project_root.join("tl.lock");

    // Remove relevant lock entries to force re-fetch
    if lock_path.exists() {
        if let Some(name) = pkg {
            // Remove specific package from lock
            if let Ok(mut lock) = tl_package::LockFile::load(&lock_path) {
                lock.remove(name);
                let _ = lock.save(&lock_path);
            }
            println!("Updating '{name}'...");
        } else {
            // Remove entire lock file
            let _ = std::fs::remove_file(&lock_path);
            println!("Updating all dependencies...");
        }
    }

    run_install_for(project_root);
}

/// `tl publish` — publish package to the registry.
pub fn cmd_publish() {
    #[cfg(feature = "registry")]
    {
        let cwd = std::env::current_dir().unwrap_or_else(|e| {
            eprintln!("Cannot determine current directory: {e}");
            process::exit(1);
        });

        let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
            eprintln!("No tl.toml found. Run 'tl init <name>' to create a project.");
            process::exit(1);
        });

        let project_root = manifest_path.parent().unwrap();
        match tl_package::registry_client::publish_package(project_root) {
            Ok(resp) => {
                println!("Published {} v{}", resp.name, resp.version);
                println!("  sha256: {}", resp.sha256);
            }
            Err(e) => {
                eprintln!("Publish failed: {e}");
                process::exit(1);
            }
        }
    }
    #[cfg(not(feature = "registry"))]
    {
        println!("Package registry is not yet available.");
        println!();
        println!("To share packages, use git repositories:");
        println!("  1. Push your project to GitHub/GitLab");
        println!("  2. Others can add it with:");
        println!("     tl add <name> --git https://github.com/user/repo.git");
    }
}

/// `tl search <query>` — search the package registry.
pub fn cmd_search(query: &str) {
    #[cfg(feature = "registry")]
    {
        match tl_package::registry_client::search_packages(query) {
            Ok(results) => {
                if results.is_empty() {
                    println!("No packages found matching '{query}'");
                } else {
                    for r in &results {
                        let desc = r.description.as_deref().unwrap_or("");
                        println!("  {} v{} — {}", r.name, r.latest_version, desc);
                    }
                    println!("Found {} package(s).", results.len());
                }
            }
            Err(e) => {
                eprintln!("Search failed: {e}");
                process::exit(1);
            }
        }
    }
    #[cfg(not(feature = "registry"))]
    {
        println!("Package registry is not yet available.");
        println!();
        println!("To find TL packages, search GitHub:");
        println!("  https://github.com/topics/thinkinglanguage");
        println!();
        println!("To use a package you found:");
        println!("  tl add <name> --git <url>");
        println!("  tl add <name> --path <local-path>");
        let _ = query;
    }
}

/// Helper: run resolve_and_install for a project root.
fn run_install_for(project_root: &Path) {
    let manifest = match Manifest::load(&project_root.join("tl.toml")) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let cache = match PackageCache::default_location() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    match resolve_and_install(project_root, &manifest, &cache) {
        Ok(lock) => {
            if lock.packages.is_empty() {
                println!("No dependencies to install.");
            } else {
                for pkg in &lock.packages {
                    println!("  {} v{}", pkg.name, pkg.version);
                }
                println!("Installed {} package(s).", lock.packages.len());
            }
        }
        Err(e) => {
            eprintln!("Install failed: {e}");
            process::exit(1);
        }
    }
}
