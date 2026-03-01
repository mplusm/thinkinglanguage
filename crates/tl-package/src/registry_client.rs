// ThinkingLanguage — Registry Client
// Licensed under MIT OR Apache-2.0
//
// HTTP client for interacting with the TL package registry.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Default registry URL, can be overridden via TL_REGISTRY_URL env var.
pub fn registry_url() -> String {
    std::env::var("TL_REGISTRY_URL").unwrap_or_else(|_| "http://localhost:3333".to_string())
}

/// Package version info from the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub sha256: String,
    pub description: Option<String>,
}

/// Package metadata from the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub versions: Vec<VersionInfo>,
}

/// Search result.
#[derive(Debug, Clone, Deserialize)]
pub struct SearchResult {
    pub name: String,
    pub latest_version: String,
    pub description: Option<String>,
}

/// Publish response.
#[derive(Debug, Clone, Deserialize)]
pub struct PublishResponse {
    pub name: String,
    pub version: String,
    pub sha256: String,
}

/// Publish a package to the registry.
pub fn publish_package(project_root: &Path) -> Result<PublishResponse, String> {
    // Read manifest
    let manifest_path = project_root.join("tl.toml");
    let content =
        std::fs::read_to_string(&manifest_path).map_err(|e| format!("Cannot read tl.toml: {e}"))?;
    let manifest: crate::Manifest =
        toml::from_str(&content).map_err(|e| format!("Cannot parse tl.toml: {e}"))?;

    let name = &manifest.project.name;
    let version = &manifest.project.version;
    let description = manifest.project.description.as_deref();

    // Create tarball
    let tarball = create_tarball(project_root)?;

    // Base64 encode
    use base64::Engine;
    let tarball_b64 = base64::engine::general_purpose::STANDARD.encode(&tarball);

    // POST to registry
    let url = format!("{}/api/v1/packages", registry_url());

    #[derive(Serialize)]
    struct PublishRequest {
        name: String,
        version: String,
        description: Option<String>,
        tarball: String,
    }

    let req = PublishRequest {
        name: name.clone(),
        version: version.clone(),
        description: description.map(|s| s.to_string()),
        tarball: tarball_b64,
    };

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(&url)
        .json(&req)
        .send()
        .map_err(|e| format!("Failed to connect to registry at {}: {e}", registry_url()))?;

    if resp.status().is_success() {
        resp.json::<PublishResponse>()
            .map_err(|e| format!("Failed to parse publish response: {e}"))
    } else {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        Err(format!("Publish failed ({status}): {body}"))
    }
}

/// Search the registry for packages.
pub fn search_packages(query: &str) -> Result<Vec<SearchResult>, String> {
    let url = format!("{}/api/v1/search?q={}", registry_url(), query);

    let resp =
        reqwest::blocking::get(&url).map_err(|e| format!("Failed to connect to registry: {e}"))?;

    if resp.status().is_success() {
        resp.json::<Vec<SearchResult>>()
            .map_err(|e| format!("Failed to parse search response: {e}"))
    } else {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        Err(format!("Search failed ({status}): {body}"))
    }
}

/// Get package info from the registry.
pub fn get_package_info(name: &str) -> Result<PackageInfo, String> {
    let url = format!("{}/api/v1/packages/{}", registry_url(), name);

    let resp =
        reqwest::blocking::get(&url).map_err(|e| format!("Failed to connect to registry: {e}"))?;

    if resp.status().is_success() {
        resp.json::<PackageInfo>()
            .map_err(|e| format!("Failed to parse package info: {e}"))
    } else {
        Err(format!("Package '{name}' not found in registry"))
    }
}

/// Download a package tarball from the registry.
pub fn download_package(name: &str, version: &str) -> Result<Vec<u8>, String> {
    let url = format!(
        "{}/api/v1/packages/{}/{}/download",
        registry_url(),
        name,
        version
    );

    let resp =
        reqwest::blocking::get(&url).map_err(|e| format!("Failed to connect to registry: {e}"))?;

    if resp.status().is_success() {
        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|e| format!("Failed to download package: {e}"))
    } else {
        Err(format!(
            "Package '{name}' version '{version}' not found in registry"
        ))
    }
}

/// Create a gzipped tar archive from a project directory.
fn create_tarball(project_root: &Path) -> Result<Vec<u8>, String> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tar::Builder;

    let buf = Vec::new();
    let encoder = GzEncoder::new(buf, Compression::default());
    let mut archive = Builder::new(encoder);

    let manifest_path = project_root.join("tl.toml");
    archive
        .append_path_with_name(&manifest_path, "tl.toml")
        .map_err(|e| format!("Failed to add tl.toml: {e}"))?;

    let src_dir = project_root.join("src");
    if src_dir.exists() {
        archive
            .append_dir_all("src", &src_dir)
            .map_err(|e| format!("Failed to add src/: {e}"))?;
    }

    let lib_tl = project_root.join("lib.tl");
    if lib_tl.exists() {
        archive
            .append_path_with_name(&lib_tl, "lib.tl")
            .map_err(|e| format!("Failed to add lib.tl: {e}"))?;
    }

    let encoder = archive
        .into_inner()
        .map_err(|e| format!("Failed to finalize archive: {e}"))?;
    encoder
        .finish()
        .map_err(|e| format!("Failed to compress: {e}"))
}
