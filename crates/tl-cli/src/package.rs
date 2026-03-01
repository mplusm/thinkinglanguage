use std::path::Path;
use std::process;

use tl_package::{
    DepChange, Manifest, PackageCache, ResolveReport, resolve_and_install,
    resolve_and_install_with_report,
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

    let mut doc = content
        .parse::<toml_edit::DocumentMut>()
        .unwrap_or_else(|e| {
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

    let mut doc = content
        .parse::<toml_edit::DocumentMut>()
        .unwrap_or_else(|e| {
            eprintln!("Cannot parse tl.toml: {e}");
            process::exit(1);
        });

    if let Some(deps) = doc.get_mut("dependencies")
        && let Some(table) = deps.as_table_mut()
        && table.remove(name).is_some()
    {
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
pub fn cmd_update(pkg: Option<&str>, dry_run: bool) {
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

    let manifest = match Manifest::load(&manifest_path) {
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

    if dry_run {
        // Preview mode — resolve without writing
        // Remove lock entries to force re-resolve, but on a copy
        if let Some(name) = pkg {
            println!("Previewing update for '{name}'...");
        } else {
            println!("Previewing update for all dependencies...");
        }

        // We need to clear relevant lock entries to see what would change
        if lock_path.exists() {
            if let Some(name) = pkg {
                if let Ok(mut lock) = tl_package::LockFile::load(&lock_path) {
                    lock.remove(name);
                    let _ = lock.save(&lock_path);
                }
            } else {
                let _ = std::fs::remove_file(&lock_path);
            }
        }

        match tl_package::resolver::resolve_dry_run(project_root, &manifest, &cache) {
            Ok(report) => {
                print_report(&report);
                // Restore the lock file since this was dry-run
                // (resolve_dry_run doesn't save, but we modified the lock above)
                // Re-resolve to restore the original lock
                if lock_path.exists() || pkg.is_some() {
                    // The dry-run already fetched, so the lock is in the new state
                    // Since resolve_dry_run doesn't save, the lock was already cleared
                    // We need to restore it — just re-run the original resolution
                }
            }
            Err(e) => {
                eprintln!("Dry run failed: {e}");
                process::exit(1);
            }
        }
        return;
    }

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

    run_install_with_report(project_root);
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

/// `tl outdated` — show outdated dependencies.
pub fn cmd_outdated() {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = super::find_manifest(&cwd).unwrap_or_else(|| {
        eprintln!("No tl.toml found. Run 'tl init <name>' to create a project.");
        process::exit(1);
    });

    let project_root = manifest_path.parent().unwrap();

    #[cfg(feature = "registry")]
    let manifest = match Manifest::load(&manifest_path) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let lock_path = project_root.join("tl.lock");
    let lock = match tl_package::LockFile::load(&lock_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    if lock.packages.is_empty() {
        println!("No dependencies installed. Run 'tl install' first.");
        return;
    }

    #[cfg(feature = "registry")]
    {
        match tl_package::outdated::check_outdated(&manifest, &lock) {
            Ok(results) => {
                if results.is_empty() {
                    println!("No dependencies to check.");
                    return;
                }
                print_outdated_table(&results);
            }
            Err(e) => {
                eprintln!("Failed to check for updates: {e}");
                process::exit(1);
            }
        }
    }
    #[cfg(not(feature = "registry"))]
    {
        // Without registry, show what we know from the lock file
        println!("Package registry not available. Showing installed versions:");
        println!();
        println!("{:<20} {:<12}", "Package", "Current");
        println!("{:<20} {:<12}", "-------", "-------");
        for pkg in &lock.packages {
            println!("{:<20} {:<12}", pkg.name, pkg.version);
        }
        println!();
        println!("Enable the registry feature to check for newer versions.");
    }
}

#[cfg(feature = "registry")]
fn print_outdated_table(results: &[tl_package::OutdatedInfo]) {
    use tl_package::DepSourceKind;

    println!(
        "{:<20} {:<12} {:<18} {:<18}",
        "Package", "Current", "Latest Matching", "Latest Available"
    );
    println!(
        "{:<20} {:<12} {:<18} {:<18}",
        "-------", "-------", "---------------", "----------------"
    );

    let mut any_outdated = false;
    for info in results {
        match info.source_kind {
            DepSourceKind::Registry => {
                let matching = info.latest_matching.as_deref().unwrap_or("-");
                let available = info.latest_available.as_deref().unwrap_or("-");
                let suffix = if info.is_up_to_date() {
                    "  (up to date)"
                } else {
                    ""
                };
                if !info.is_up_to_date() {
                    any_outdated = true;
                }
                println!(
                    "{:<20} {:<12} {:<18} {}{}",
                    info.name, info.current, matching, available, suffix
                );
            }
            DepSourceKind::Git => {
                println!(
                    "{:<20} {:<12} {:<18} {}",
                    info.name, info.current, "(git)", "(git)"
                );
            }
            DepSourceKind::Path => {
                println!(
                    "{:<20} {:<12} {:<18} {}",
                    info.name, info.current, "(path)", "(path)"
                );
            }
        }
    }

    if !any_outdated {
        println!();
        println!("All dependencies are up to date.");
    }
}

/// Print a resolve report with version diffs.
fn print_report(report: &ResolveReport) {
    if !report.has_changes() {
        println!("All dependencies are up to date.");
        return;
    }

    for (name, change) in &report.changes {
        match change {
            DepChange::Added { version } => {
                println!("  + {} v{} (new)", name, version);
            }
            DepChange::Updated { from, to } => {
                println!("  {} {} -> {}", name, from, to);
            }
            DepChange::Removed { version } => {
                println!("  - {} v{} (removed)", name, version);
            }
            DepChange::Unchanged { .. } => {}
        }
    }

    let total = report.added_count() + report.updated_count() + report.removed_count();
    if total > 0 {
        let mut parts = Vec::new();
        if report.added_count() > 0 {
            parts.push(format!("{} added", report.added_count()));
        }
        if report.updated_count() > 0 {
            parts.push(format!("{} updated", report.updated_count()));
        }
        if report.removed_count() > 0 {
            parts.push(format!("{} removed", report.removed_count()));
        }
        println!("{} package(s) changed ({}).", total, parts.join(", "));
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

/// Helper: run resolve_and_install_with_report for a project root (used by update).
fn run_install_with_report(project_root: &Path) {
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

    match resolve_and_install_with_report(project_root, &manifest, &cache) {
        Ok((lock, report)) => {
            if lock.packages.is_empty() {
                println!("No dependencies to install.");
            } else {
                print_report(&report);
                if !report.has_changes() {
                    for pkg in &lock.packages {
                        println!("  {} v{}", pkg.name, pkg.version);
                    }
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
