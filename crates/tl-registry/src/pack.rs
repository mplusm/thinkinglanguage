// ThinkingLanguage — Package Packing
// Licensed under MIT OR Apache-2.0
//
// Create publishable .tar.gz from a project directory.

use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::path::Path;
use tar::Builder;

/// Create a gzipped tar archive from a project directory.
/// Includes tl.toml, src/, and common project files.
/// Returns (tarball_bytes, sha256_hex).
pub fn create_package_tarball(project_root: &Path) -> Result<(Vec<u8>, String), String> {
    let manifest_path = project_root.join("tl.toml");
    if !manifest_path.exists() {
        return Err("No tl.toml found in project root".to_string());
    }

    let buf = Vec::new();
    let encoder = GzEncoder::new(buf, Compression::default());
    let mut archive = Builder::new(encoder);

    // Add tl.toml
    archive
        .append_path_with_name(&manifest_path, "tl.toml")
        .map_err(|e| format!("Failed to add tl.toml: {e}"))?;

    // Add src/ directory if it exists
    let src_dir = project_root.join("src");
    if src_dir.exists() {
        archive
            .append_dir_all("src", &src_dir)
            .map_err(|e| format!("Failed to add src/: {e}"))?;
    }

    // Add lib.tl if it exists at root
    let lib_tl = project_root.join("lib.tl");
    if lib_tl.exists() {
        archive
            .append_path_with_name(&lib_tl, "lib.tl")
            .map_err(|e| format!("Failed to add lib.tl: {e}"))?;
    }

    let encoder = archive
        .into_inner()
        .map_err(|e| format!("Failed to finalize archive: {e}"))?;
    let tarball = encoder
        .finish()
        .map_err(|e| format!("Failed to compress: {e}"))?;

    let mut hasher = Sha256::new();
    hasher.update(&tarball);
    let sha256 = format!("{:x}", hasher.finalize());

    Ok((tarball, sha256))
}

/// Extract a tarball into a destination directory.
pub fn extract_tarball(tarball: &[u8], dest: &Path) -> Result<(), String> {
    use flate2::read::GzDecoder;
    use tar::Archive;

    std::fs::create_dir_all(dest)
        .map_err(|e| format!("Failed to create destination: {e}"))?;

    let decoder = GzDecoder::new(tarball);
    let mut archive = Archive::new(decoder);
    archive
        .unpack(dest)
        .map_err(|e| format!("Failed to extract tarball: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn pack_and_unpack() {
        let tmp = TempDir::new().unwrap();
        let project = tmp.path().join("mylib");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::write(
            project.join("tl.toml"),
            "[project]\nname = \"mylib\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        std::fs::write(project.join("src/lib.tl"), "pub fn hello() { print(\"hi\") }\n").unwrap();

        let (tarball, hash) = create_package_tarball(&project).unwrap();
        assert!(!tarball.is_empty());
        assert!(!hash.is_empty());

        // Extract and verify
        let dest = tmp.path().join("extracted");
        extract_tarball(&tarball, &dest).unwrap();
        assert!(dest.join("tl.toml").exists());
        assert!(dest.join("src/lib.tl").exists());
    }

    #[test]
    fn pack_no_manifest() {
        let tmp = TempDir::new().unwrap();
        let result = create_package_tarball(tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No tl.toml"));
    }
}
