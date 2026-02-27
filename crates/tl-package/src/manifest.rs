use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level manifest parsed from tl.toml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub project: ProjectConfig,
    #[serde(default)]
    pub dependencies: BTreeMap<String, DependencySpec>,
}

/// Project metadata in [project] table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
}

/// A dependency specification — either a simple version string or detailed config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DependencySpec {
    /// Simple version string: `pkg = "1.0"`
    Simple(String),
    /// Detailed specification with git/path/version fields.
    Detailed(DetailedDep),
}

/// Detailed dependency with optional git, path, or version source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedDep {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// The kind of source a dependency comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepSourceKind {
    Registry,
    Git,
    Path,
}

impl DependencySpec {
    /// Determine what kind of source this dependency uses.
    pub fn source_kind(&self) -> DepSourceKind {
        match self {
            DependencySpec::Simple(_) => DepSourceKind::Registry,
            DependencySpec::Detailed(d) => {
                if d.git.is_some() {
                    DepSourceKind::Git
                } else if d.path.is_some() {
                    DepSourceKind::Path
                } else {
                    DepSourceKind::Registry
                }
            }
        }
    }
}

impl Manifest {
    /// Parse a manifest from TOML string.
    pub fn from_toml(s: &str) -> Result<Self, String> {
        toml::from_str(s).map_err(|e| format!("Failed to parse tl.toml: {e}"))
    }

    /// Load manifest from a file path.
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
        Self::from_toml(&content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_manifest_simple_deps() {
        let toml = r#"
[project]
name = "myapp"
version = "0.1.0"

[dependencies]
utils = "1.0"
helpers = "^2.0"
"#;
        let m = Manifest::from_toml(toml).unwrap();
        assert_eq!(m.project.name, "myapp");
        assert_eq!(m.dependencies.len(), 2);
        assert!(matches!(&m.dependencies["utils"], DependencySpec::Simple(v) if v == "1.0"));
    }

    #[test]
    fn parse_manifest_detailed_deps() {
        let toml = r#"
[project]
name = "myapp"
version = "0.1.0"

[dependencies]
mylib = { path = "../mylib" }
remote = { git = "https://github.com/user/remote.git", branch = "main" }
versioned = { version = "1.2", git = "https://github.com/user/versioned.git", tag = "v1.2.0" }
"#;
        let m = Manifest::from_toml(toml).unwrap();
        assert_eq!(m.dependencies.len(), 3);

        match &m.dependencies["mylib"] {
            DependencySpec::Detailed(d) => {
                assert_eq!(d.path.as_deref(), Some("../mylib"));
                assert!(d.git.is_none());
            }
            _ => panic!("expected Detailed"),
        }

        match &m.dependencies["remote"] {
            DependencySpec::Detailed(d) => {
                assert!(d.git.is_some());
                assert_eq!(d.branch.as_deref(), Some("main"));
            }
            _ => panic!("expected Detailed"),
        }
    }

    #[test]
    fn parse_manifest_no_deps() {
        let toml = r#"
[project]
name = "legacy"
version = "0.1.0"
"#;
        let m = Manifest::from_toml(toml).unwrap();
        assert!(m.dependencies.is_empty());
    }

    #[test]
    fn source_kind_detection() {
        assert_eq!(DependencySpec::Simple("1.0".into()).source_kind(), DepSourceKind::Registry);

        let git_dep = DependencySpec::Detailed(DetailedDep {
            version: None,
            git: Some("https://github.com/user/repo.git".into()),
            branch: None, tag: None, rev: None, path: None,
        });
        assert_eq!(git_dep.source_kind(), DepSourceKind::Git);

        let path_dep = DependencySpec::Detailed(DetailedDep {
            version: None, git: None, branch: None, tag: None, rev: None,
            path: Some("../local".into()),
        });
        assert_eq!(path_dep.source_kind(), DepSourceKind::Path);
    }

    #[test]
    fn manifest_with_optional_fields() {
        let toml = r#"
[project]
name = "full"
version = "1.0.0"
edition = "2024"
authors = ["Alice", "Bob"]
description = "A complete project"
entry = "src/app.tl"
"#;
        let m = Manifest::from_toml(toml).unwrap();
        assert_eq!(m.project.edition.as_deref(), Some("2024"));
        assert_eq!(m.project.authors.as_ref().unwrap().len(), 2);
        assert_eq!(m.project.entry.as_deref(), Some("src/app.tl"));
    }
}
