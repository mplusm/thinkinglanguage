// ThinkingLanguage — Security Policy
// Licensed under Apache-2.0
//
// Phase 23: Connector permissions, path restrictions, sandbox mode.
// Phase C3: Moved from tl-compiler to tl-errors for shared access.

use std::collections::HashSet;

/// Security policy controlling access to files, network, connectors, and subprocesses.
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    pub allowed_connectors: HashSet<String>,
    pub denied_paths: Vec<String>,
    pub allow_network: bool,
    pub allow_file_read: bool,
    pub allow_file_write: bool,
    pub sandbox_mode: bool,
    pub allow_subprocess: bool,
    pub allowed_commands: Vec<String>,
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
            allow_subprocess: true,
            allowed_commands: vec![],
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
            allow_subprocess: false,
            allowed_commands: vec![],
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
            "python" => false,
            "env_write" => false,
            "subprocess" => self.allow_subprocess,
            p if p.starts_with("command:") => {
                let cmd = &p["command:".len()..];
                self.check_command(cmd)
            }
            p if p.starts_with("connector:") => {
                let conn_type = &p["connector:".len()..];
                self.allowed_connectors.is_empty() || self.allowed_connectors.contains(conn_type)
            }
            _ => true,
        }
    }

    /// Check if a specific command is allowed for subprocess execution.
    pub fn check_command(&self, command: &str) -> bool {
        if !self.allow_subprocess {
            return false;
        }
        if self.allowed_commands.is_empty() {
            return true;
        }
        self.allowed_commands.iter().any(|c| c == command)
    }

    /// Check if a file path is allowed.
    pub fn check_path(&self, path: &str) -> bool {
        if !self.sandbox_mode {
            return true;
        }
        !self
            .denied_paths
            .iter()
            .any(|denied| path.starts_with(denied))
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

    #[test]
    fn test_sandbox_denies_subprocess() {
        let policy = SecurityPolicy::sandbox();
        assert!(!policy.check("subprocess"));
        assert!(!policy.check_command("npx"));
        assert!(!policy.check("command:npx"));
    }

    #[test]
    fn test_permissive_allows_subprocess() {
        let policy = SecurityPolicy::permissive();
        assert!(policy.check("subprocess"));
        assert!(policy.check_command("npx"));
        assert!(policy.check("command:npx"));
    }

    #[test]
    fn test_command_whitelist() {
        let mut policy = SecurityPolicy::sandbox();
        policy.allow_subprocess = true;
        policy.allowed_commands = vec!["npx".to_string(), "node".to_string()];
        assert!(policy.check_command("npx"));
        assert!(policy.check_command("node"));
        assert!(!policy.check_command("bash"));
        assert!(policy.check("command:npx"));
        assert!(!policy.check("command:bash"));
    }

    #[test]
    fn test_empty_whitelist_allows_all() {
        let mut policy = SecurityPolicy::sandbox();
        policy.allow_subprocess = true;
        // allowed_commands is empty by default
        assert!(policy.check_command("npx"));
        assert!(policy.check_command("anything"));
        assert!(policy.check("command:whatever"));
    }
}
