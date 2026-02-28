// ThinkingLanguage — Security Policy
// Licensed under MIT OR Apache-2.0
//
// Phase 23: Connector permissions, path restrictions, sandbox mode.

use std::collections::HashSet;

/// Security policy controlling access to files, network, and connectors.
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    pub allowed_connectors: HashSet<String>,
    pub denied_paths: Vec<String>,
    pub allow_network: bool,
    pub allow_file_read: bool,
    pub allow_file_write: bool,
    pub sandbox_mode: bool,
}

impl SecurityPolicy {
    pub fn permissive() -> Self {
        SecurityPolicy {
            allowed_connectors: HashSet::new(),
            denied_paths: Vec::new(),
            allow_network: true,
            allow_file_read: true,
            allow_file_write: true,
            sandbox_mode: false,
        }
    }

    pub fn sandbox() -> Self {
        SecurityPolicy {
            allowed_connectors: HashSet::new(),
            denied_paths: Vec::new(),
            allow_network: false,
            allow_file_read: true,
            allow_file_write: false,
            sandbox_mode: true,
        }
    }

    /// Check if a permission is allowed.
    pub fn check(&self, permission: &str) -> bool {
        if !self.sandbox_mode {
            return true;
        }
        match permission {
            "network" => self.allow_network,
            "file_read" => self.allow_file_read,
            "file_write" => self.allow_file_write,
            p if p.starts_with("connector:") => {
                let conn_type = &p["connector:".len()..];
                self.allowed_connectors.is_empty() || self.allowed_connectors.contains(conn_type)
            }
            _ => true,
        }
    }

    /// Check if a file path is allowed.
    pub fn check_path(&self, path: &str) -> bool {
        if !self.sandbox_mode {
            return true;
        }
        !self.denied_paths.iter().any(|denied| path.starts_with(denied))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permissive_allows_all() {
        let policy = SecurityPolicy::permissive();
        assert!(policy.check("network"));
        assert!(policy.check("file_read"));
        assert!(policy.check("file_write"));
        assert!(policy.check("connector:postgres"));
    }

    #[test]
    fn test_sandbox_restricts() {
        let policy = SecurityPolicy::sandbox();
        assert!(!policy.check("network"));
        assert!(policy.check("file_read"));
        assert!(!policy.check("file_write"));
    }

    #[test]
    fn test_connector_whitelist() {
        let mut policy = SecurityPolicy::sandbox();
        policy.allowed_connectors.insert("postgres".to_string());
        assert!(policy.check("connector:postgres"));
        assert!(!policy.check("connector:mysql"));
    }

    #[test]
    fn test_denied_paths() {
        let mut policy = SecurityPolicy::sandbox();
        policy.denied_paths.push("/etc/".to_string());
        assert!(!policy.check_path("/etc/passwd"));
        assert!(policy.check_path("/home/user/file.txt"));
    }
}
