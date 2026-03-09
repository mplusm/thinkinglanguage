// ThinkingLanguage — SFTP/SCP Connector
// Licensed under MIT OR Apache-2.0
//
// Uses ssh2 (libssh2 bindings) for SFTP and SCP file transfers.
// Supports password and key-based authentication.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use ssh2::Session;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use crate::engine::DataEngine;

/// Parse SFTP config from JSON or key=value format.
/// Supported fields: host, port (default 22), user/username, password, key_path, passphrase
/// JSON: `{"host":"example.com","user":"deploy","key_path":"~/.ssh/id_rsa"}`
/// KV:   `host=example.com user=deploy key_path=~/.ssh/id_rsa`
fn parse_sftp_config(config_str: &str) -> Result<SftpConfig, String> {
    // Try JSON first
    if config_str.trim_start().starts_with('{') {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(config_str) {
            return Ok(SftpConfig {
                host: json["host"].as_str().unwrap_or("").to_string(),
                port: json["port"].as_u64().unwrap_or(22) as u16,
                user: json["user"]
                    .as_str()
                    .or_else(|| json["username"].as_str())
                    .unwrap_or("")
                    .to_string(),
                password: json["password"].as_str().map(|s| s.to_string()),
                key_path: json["key_path"]
                    .as_str()
                    .or_else(|| json["key"].as_str())
                    .or_else(|| json["identity_file"].as_str())
                    .map(|s| expand_tilde(s)),
                passphrase: json["passphrase"].as_str().map(|s| s.to_string()),
            });
        }
    }

    // Key=value format
    let mut config = SftpConfig {
        host: String::new(),
        port: 22,
        user: String::new(),
        password: None,
        key_path: None,
        passphrase: None,
    };

    for part in config_str.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key.to_lowercase().as_str() {
                "host" | "server" => config.host = value.to_string(),
                "port" => {
                    config.port = value
                        .parse::<u16>()
                        .map_err(|_| "Invalid port".to_string())?;
                }
                "user" | "username" => config.user = value.to_string(),
                "password" => config.password = Some(value.to_string()),
                "key_path" | "key" | "identity_file" => {
                    config.key_path = Some(expand_tilde(value));
                }
                "passphrase" => config.passphrase = Some(value.to_string()),
                _ => {}
            }
        }
    }

    if config.host.is_empty() {
        return Err("SFTP config missing 'host'".to_string());
    }
    if config.user.is_empty() {
        // Try current system user
        config.user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_default();
    }

    Ok(config)
}

struct SftpConfig {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    key_path: Option<String>,
    passphrase: Option<String>,
}

/// Expand ~ to home directory
fn expand_tilde(path: &str) -> String {
    if path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}{}", home, &path[1..]);
        }
    }
    path.to_string()
}

/// Create an authenticated SSH session.
fn create_session(config: &SftpConfig) -> Result<Session, String> {
    let addr = format!("{}:{}", config.host, config.port);
    let tcp = TcpStream::connect(&addr)
        .map_err(|e| format!("SFTP TCP connection to {addr} failed: {e}"))?;

    let mut session = Session::new().map_err(|e| format!("SSH session creation failed: {e}"))?;
    session.set_tcp_stream(tcp);
    session
        .handshake()
        .map_err(|e| format!("SSH handshake failed: {e}"))?;

    // Try key-based auth first, then password
    if let Some(ref key_path) = config.key_path {
        let key = Path::new(key_path);
        if key.exists() {
            session
                .userauth_pubkey_file(&config.user, None, key, config.passphrase.as_deref())
                .map_err(|e| format!("SSH key auth failed: {e}"))?;
        } else {
            return Err(format!("SSH key file not found: {key_path}"));
        }
    } else if let Some(ref password) = config.password {
        session
            .userauth_password(&config.user, password)
            .map_err(|e| format!("SSH password auth failed: {e}"))?;
    } else {
        // Try agent-based auth
        let mut agent = session
            .agent()
            .map_err(|e| format!("SSH agent init failed: {e}"))?;
        agent
            .connect()
            .map_err(|e| format!("SSH agent connect failed: {e}"))?;
        agent
            .list_identities()
            .map_err(|e| format!("SSH agent list identities failed: {e}"))?;

        let mut authed = false;
        for identity in agent.identities().unwrap_or_default() {
            if agent.userauth(&config.user, &identity).is_ok() {
                authed = true;
                break;
            }
        }
        if !authed {
            return Err(
                "SSH auth failed: no password, key, or agent identity provided".to_string(),
            );
        }
    }

    if !session.authenticated() {
        return Err("SSH authentication failed".to_string());
    }

    Ok(session)
}

impl DataEngine {
    /// Download a file from a remote server via SFTP.
    /// Returns the local path as a string.
    pub fn sftp_download(
        &self,
        config_str: &str,
        remote_path: &str,
        local_path: &str,
    ) -> Result<String, String> {
        let config = parse_sftp_config(config_str)?;
        let session = create_session(&config)?;
        let sftp = session
            .sftp()
            .map_err(|e| format!("SFTP subsystem init failed: {e}"))?;

        let mut remote_file = sftp
            .open(Path::new(remote_path))
            .map_err(|e| format!("SFTP open remote file '{remote_path}' failed: {e}"))?;

        let mut contents = Vec::new();
        remote_file
            .read_to_end(&mut contents)
            .map_err(|e| format!("SFTP read failed: {e}"))?;

        let local = Path::new(local_path);
        if let Some(parent) = local.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Create local directory failed: {e}"))?;
        }
        std::fs::write(local_path, &contents)
            .map_err(|e| format!("Write local file failed: {e}"))?;

        Ok(local_path.to_string())
    }

    /// Upload a local file to a remote server via SFTP.
    /// Returns the remote path as a string.
    pub fn sftp_upload(
        &self,
        config_str: &str,
        local_path: &str,
        remote_path: &str,
    ) -> Result<String, String> {
        let config = parse_sftp_config(config_str)?;
        let session = create_session(&config)?;
        let sftp = session
            .sftp()
            .map_err(|e| format!("SFTP subsystem init failed: {e}"))?;

        let contents = std::fs::read(local_path)
            .map_err(|e| format!("Read local file '{local_path}' failed: {e}"))?;

        let mut remote_file = sftp
            .create(Path::new(remote_path))
            .map_err(|e| format!("SFTP create remote file '{remote_path}' failed: {e}"))?;

        remote_file
            .write_all(&contents)
            .map_err(|e| format!("SFTP write failed: {e}"))?;

        Ok(remote_path.to_string())
    }

    /// List files in a remote directory via SFTP.
    /// Returns a table with columns: name, size, type, modified.
    pub fn sftp_list(
        &self,
        config_str: &str,
        remote_path: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let config = parse_sftp_config(config_str)?;
        let session = create_session(&config)?;
        let sftp = session
            .sftp()
            .map_err(|e| format!("SFTP subsystem init failed: {e}"))?;

        let entries = sftp
            .readdir(Path::new(remote_path))
            .map_err(|e| format!("SFTP readdir '{remote_path}' failed: {e}"))?;

        let mut names: Vec<Option<String>> = Vec::new();
        let mut sizes: Vec<Option<i64>> = Vec::new();
        let mut types: Vec<Option<String>> = Vec::new();
        let mut modified: Vec<Option<i64>> = Vec::new();

        for (path, stat) in &entries {
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string_lossy().to_string());
            names.push(Some(name));
            sizes.push(Some(stat.size.unwrap_or(0) as i64));
            let file_type = if stat.is_dir() {
                "directory"
            } else if stat.is_file() {
                "file"
            } else {
                "other"
            };
            types.push(Some(file_type.to_string()));
            modified.push(Some(stat.mtime.unwrap_or(0) as i64));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("modified", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(sizes)),
                Arc::new(StringArray::from(types)),
                Arc::new(Int64Array::from(modified)),
            ],
        )
        .map_err(|e| format!("Arrow RecordBatch creation error: {e}"))?;

        let table_name = "__sftp_list";
        let _ = self.ctx.deregister_table(table_name);
        self.register_batches(table_name, schema, vec![batch])?;

        self.rt
            .block_on(self.ctx.table(table_name))
            .map_err(|e| format!("Table reference error: {e}"))
    }

    /// Read a CSV file directly from SFTP into a DataFusion DataFrame.
    /// Downloads to a temp file, reads it, then cleans up.
    pub fn sftp_read_csv(
        &self,
        config_str: &str,
        remote_path: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let tmp_dir = std::env::temp_dir();
        let file_name = Path::new(remote_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "sftp_download.csv".to_string());
        let local_path = tmp_dir.join(format!("_tl_sftp_{}", file_name));
        let local_str = local_path.to_string_lossy().to_string();

        self.sftp_download(config_str, remote_path, &local_str)?;
        let result = self.read_csv(&local_str);
        let _ = std::fs::remove_file(&local_path);
        result
    }

    /// Read a Parquet file directly from SFTP into a DataFusion DataFrame.
    /// Downloads to a temp file, reads it, then cleans up.
    pub fn sftp_read_parquet(
        &self,
        config_str: &str,
        remote_path: &str,
    ) -> Result<datafusion::prelude::DataFrame, String> {
        let tmp_dir = std::env::temp_dir();
        let file_name = Path::new(remote_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "sftp_download.parquet".to_string());
        let local_path = tmp_dir.join(format!("_tl_sftp_{}", file_name));
        let local_str = local_path.to_string_lossy().to_string();

        self.sftp_download(config_str, remote_path, &local_str)?;
        let result = self.read_parquet(&local_str);
        let _ = std::fs::remove_file(&local_path);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sftp_config_json() {
        let config = parse_sftp_config(
            r#"{"host":"example.com","user":"deploy","port":2222,"key_path":"~/.ssh/id_rsa"}"#,
        )
        .unwrap();
        assert_eq!(config.host, "example.com");
        assert_eq!(config.user, "deploy");
        assert_eq!(config.port, 2222);
        assert!(config.key_path.is_some());
    }

    #[test]
    fn test_parse_sftp_config_kv() {
        let config =
            parse_sftp_config("host=example.com user=deploy port=2222 key_path=~/.ssh/id_rsa")
                .unwrap();
        assert_eq!(config.host, "example.com");
        assert_eq!(config.user, "deploy");
        assert_eq!(config.port, 2222);
        assert!(config.key_path.is_some());
    }

    #[test]
    fn test_parse_sftp_config_missing_host() {
        let result = parse_sftp_config("user=deploy");
        assert!(result.is_err());
    }

    #[test]
    #[ignore] // Requires an SFTP server
    fn test_sftp_list() {
        let engine = DataEngine::new();
        let df = engine
            .sftp_list("host=localhost user=testuser password=testpass", "/tmp")
            .unwrap();
        let batches = engine.collect(df).unwrap();
        assert!(!batches.is_empty());
    }
}
