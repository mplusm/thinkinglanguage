//! MCP Client — blocking wrapper around rmcp's async MCP client.
//!
//! [`McpClient`] connects to MCP servers as subprocesses over stdio,
//! performing the 3-step handshake (initialize -> response -> initialized)
//! automatically via rmcp's [`ServiceExt::serve()`].
//!
//! All public methods are blocking — they use an internal tokio runtime
//! with [`Runtime::block_on()`] to bridge async rmcp calls into sync TL land.

use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, Implementation,
    ServerInfo, Tool,
};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::TokioChildProcess;
use rmcp::{ClientHandler, ServiceExt};
use tl_errors::security::SecurityPolicy;

use crate::error::McpError;

// ---------------------------------------------------------------------------
// Client handler (minimal — we just identify ourselves)
// ---------------------------------------------------------------------------

/// Minimal MCP client handler for TL.
///
/// Returns default/error for all server-initiated requests (sampling,
/// elicitation, roots). Only provides client identification via `get_info()`.
#[derive(Debug, Clone)]
pub struct TlClientHandler;

impl ClientHandler for TlClientHandler {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("tl", env!("CARGO_PKG_VERSION"))
                .with_title("ThinkingLanguage MCP Client"),
        )
    }
}

// ---------------------------------------------------------------------------
// McpClient
// ---------------------------------------------------------------------------

/// A blocking MCP client that connects to servers over stdio subprocess.
///
/// Wraps rmcp's async [`RunningService`] with a tokio runtime so all
/// operations can be called from synchronous TL code.
///
/// # Example (conceptual — requires a real MCP server binary)
/// ```ignore
/// let client = McpClient::connect("npx", &["-y".into(), "@modelcontextprotocol/server-filesystem".into(), "/tmp".into()], None)?;
/// let tools = client.list_tools()?;
/// println!("Available tools: {}", tools.len());
/// ```
pub struct McpClient {
    /// Shared tokio runtime for async operations.
    runtime: Arc<tokio::runtime::Runtime>,
    /// The running rmcp service (handles message routing internally).
    service: Option<RunningService<RoleClient, TlClientHandler>>,
    /// Cached server info from the handshake.
    server_info: Option<ServerInfo>,
}

impl std::fmt::Debug for McpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpClient")
            .field("connected", &self.is_connected())
            .field("server_info", &self.server_info)
            .finish()
    }
}

impl McpClient {
    /// Connect to an MCP server by spawning a subprocess.
    ///
    /// 1. Checks [`SecurityPolicy`] (if provided) — denies if subprocess
    ///    execution or the specific command is blocked.
    /// 2. Spawns the command as a child process with stdio piped.
    /// 3. Performs the MCP 3-step handshake via rmcp's `ServiceExt::serve()`.
    /// 4. Caches the server info from the handshake response.
    ///
    /// # Arguments
    /// * `command` — The executable to spawn (e.g. `"npx"`, `"node"`).
    /// * `args` — Arguments to pass to the executable.
    /// * `security_policy` — Optional policy to enforce subprocess restrictions.
    ///
    /// # Errors
    /// * [`McpError::PermissionDenied`] — SecurityPolicy blocked the command.
    /// * [`McpError::ConnectionFailed`] — Could not spawn or handshake.
    /// * [`McpError::RuntimeError`] — Could not create tokio runtime.
    pub fn connect(
        command: &str,
        args: &[String],
        security_policy: Option<&SecurityPolicy>,
    ) -> Result<Self, McpError> {
        // --- Security check ---
        if let Some(policy) = security_policy {
            if !policy.check_command(command) {
                return Err(McpError::PermissionDenied(format!(
                    "Command '{}' is not allowed by security policy",
                    command
                )));
            }
        }

        // --- Create tokio runtime ---
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| McpError::RuntimeError(e.to_string()))?;
        let runtime = Arc::new(runtime);

        // --- Spawn subprocess and perform handshake ---
        let (service, server_info) = runtime.block_on(async {
            // Build the tokio Command
            let mut cmd = tokio::process::Command::new(command);
            cmd.args(args);

            // Spawn via TokioChildProcess (handles piped stdio + framing)
            let transport = TokioChildProcess::new(cmd).map_err(|e| {
                McpError::ConnectionFailed(format!("Failed to spawn '{}': {}", command, e))
            })?;

            // Perform 3-step handshake: initialize -> response -> initialized
            let service = TlClientHandler
                .serve(transport)
                .await
                .map_err(|e| McpError::ConnectionFailed(format!("Handshake failed: {}", e)))?;

            // Cache server info
            let server_info = service.peer().peer_info().cloned();

            Ok::<_, McpError>((service, server_info))
        })?;

        Ok(McpClient {
            runtime,
            service: Some(service),
            server_info,
        })
    }

    /// Connect to an MCP server using an existing tokio runtime.
    ///
    /// Same as [`connect()`](Self::connect) but shares a runtime with the caller
    /// (e.g. the VM's async runtime).
    pub fn connect_with_runtime(
        command: &str,
        args: &[String],
        security_policy: Option<&SecurityPolicy>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, McpError> {
        // --- Security check ---
        if let Some(policy) = security_policy {
            if !policy.check_command(command) {
                return Err(McpError::PermissionDenied(format!(
                    "Command '{}' is not allowed by security policy",
                    command
                )));
            }
        }

        // --- Spawn subprocess and perform handshake ---
        let (service, server_info) = runtime.block_on(async {
            let mut cmd = tokio::process::Command::new(command);
            cmd.args(args);

            let transport = TokioChildProcess::new(cmd).map_err(|e| {
                McpError::ConnectionFailed(format!("Failed to spawn '{}': {}", command, e))
            })?;

            let service = TlClientHandler
                .serve(transport)
                .await
                .map_err(|e| McpError::ConnectionFailed(format!("Handshake failed: {}", e)))?;

            let server_info = service.peer().peer_info().cloned();

            Ok::<_, McpError>((service, server_info))
        })?;

        Ok(McpClient {
            runtime,
            service: Some(service),
            server_info,
        })
    }

    // -----------------------------------------------------------------------
    // Operations
    // -----------------------------------------------------------------------

    /// List all tools exposed by the connected MCP server.
    ///
    /// Uses `list_all_tools()` which automatically handles pagination.
    pub fn list_tools(&self) -> Result<Vec<Tool>, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            service
                .peer()
                .list_all_tools()
                .await
                .map_err(|e| McpError::ProtocolError(e.to_string()))
        })
    }

    /// Call a tool on the connected MCP server.
    ///
    /// # Arguments
    /// * `name` — The tool name (must match one from `list_tools()`).
    /// * `arguments` — JSON value with the tool arguments. Must be a JSON object
    ///   or null/None. Non-object values are rejected.
    ///
    /// # Returns
    /// The [`CallToolResult`] from the server. If the server sets `is_error`,
    /// this method returns `Err(McpError::ToolError)` with the content text.
    pub fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;

        // Convert Value to Option<JsonObject> (rmcp expects Map, not arbitrary Value)
        let args_map = match arguments {
            serde_json::Value::Object(map) => Some(map),
            serde_json::Value::Null => None,
            other => {
                return Err(McpError::ProtocolError(format!(
                    "Tool arguments must be a JSON object, got: {}",
                    other
                )));
            }
        };

        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(map) = args_map {
            params = params.with_arguments(map);
        }

        let result = self.runtime.block_on(async {
            service
                .peer()
                .call_tool(params)
                .await
                .map_err(|e| McpError::ProtocolError(e.to_string()))
        })?;

        // Check is_error flag
        if result.is_error == Some(true) {
            // Extract text content for the error message
            let error_text: String = result
                .content
                .iter()
                .filter_map(|c| c.raw.as_text().map(|t| t.text.as_str()))
                .collect::<Vec<_>>()
                .join("\n");
            return Err(McpError::ToolError(if error_text.is_empty() {
                "Tool returned an error".to_string()
            } else {
                error_text
            }));
        }

        Ok(result)
    }

    /// Ping the connected MCP server.
    ///
    /// Sends a ping request and waits for a response. Useful for health checks.
    pub fn ping(&self) -> Result<(), McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            service
                .peer()
                .send_request(rmcp::model::ClientRequest::PingRequest(
                    rmcp::model::PingRequest {
                        method: Default::default(),
                        extensions: Default::default(),
                    },
                ))
                .await
                .map_err(|e| McpError::ProtocolError(e.to_string()))?;
            Ok(())
        })
    }

    /// Return cached server info from the handshake.
    ///
    /// Contains the server's name, version, capabilities, and protocol version.
    pub fn server_info(&self) -> Option<&ServerInfo> {
        self.server_info.as_ref()
    }

    /// Gracefully disconnect from the MCP server.
    ///
    /// Cancels the rmcp service which triggers transport close and child
    /// process cleanup.
    pub fn disconnect(&mut self) -> Result<(), McpError> {
        if let Some(service) = self.service.take() {
            self.runtime.block_on(async {
                // cancel() consumes the service and triggers graceful shutdown
                let _ = service.cancel().await;
            });
        }
        Ok(())
    }

    /// Check whether the MCP connection is still alive.
    pub fn is_connected(&self) -> bool {
        self.service
            .as_ref()
            .map(|s| !s.is_closed())
            .unwrap_or(false)
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        // Best-effort cleanup: cancel the service if still running.
        // RunningService's own DropGuard will also cancel via CancellationToken,
        // but we do it explicitly to ensure the runtime processes the shutdown.
        if let Some(service) = self.service.take() {
            // We cannot block_on inside Drop if the runtime is being dropped too,
            // so we spawn a fire-and-forget task.
            let rt = self.runtime.clone();
            // Use a separate thread to avoid panic if we're already in an async context.
            std::thread::spawn(move || {
                let _ = rt.block_on(async {
                    let _ = service.cancel().await;
                });
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_error_display() {
        let err = McpError::PermissionDenied("npx not allowed".to_string());
        assert_eq!(err.to_string(), "Permission denied: npx not allowed");

        let err = McpError::ConnectionFailed("spawn failed".to_string());
        assert_eq!(err.to_string(), "Connection failed: spawn failed");

        let err = McpError::ProtocolError("invalid response".to_string());
        assert_eq!(err.to_string(), "Protocol error: invalid response");

        let err = McpError::ToolError("division by zero".to_string());
        assert_eq!(err.to_string(), "Tool error: division by zero");

        let err = McpError::TransportClosed;
        assert_eq!(err.to_string(), "Transport closed");

        let err = McpError::Timeout;
        assert_eq!(err.to_string(), "Timeout");

        let err = McpError::RuntimeError("thread pool exhausted".to_string());
        assert_eq!(err.to_string(), "Runtime error: thread pool exhausted");
    }

    #[test]
    fn test_client_handler_info() {
        let handler = TlClientHandler;
        let info = handler.get_info();

        assert_eq!(info.client_info.name, "tl");
        assert_eq!(info.client_info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            info.client_info.title,
            Some("ThinkingLanguage MCP Client".to_string())
        );
    }

    #[test]
    fn test_security_policy_denies_command() {
        let mut policy = SecurityPolicy::sandbox();
        // sandbox_mode = true, allow_subprocess = false by default
        let result = McpClient::connect("npx", &[], Some(&policy));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, McpError::PermissionDenied(_)));

        // Now with subprocess allowed but command not in whitelist
        policy.allow_subprocess = true;
        policy.allowed_commands = vec!["node".to_string()];
        let result = McpClient::connect("npx", &[], Some(&policy));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, McpError::PermissionDenied(_)));
    }

    #[test]
    fn test_security_policy_allows_command() {
        let mut policy = SecurityPolicy::sandbox();
        policy.allow_subprocess = true;
        policy.allowed_commands = vec!["echo".to_string()];

        // This will fail at connection (echo is not an MCP server) but
        // it should NOT fail at the security check.
        let result = McpClient::connect("echo", &["hello".to_string()], Some(&policy));
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should be a connection error, not permission denied
        assert!(
            matches!(err, McpError::ConnectionFailed(_)),
            "Expected ConnectionFailed, got: {:?}",
            err
        );
    }

    #[test]
    fn test_no_security_policy_allows_anything() {
        // Without a policy, any command is allowed (security check skipped)
        // This will fail at connection (nonexistent binary) but should pass security
        let result = McpClient::connect("__nonexistent_mcp_server__", &[], None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, McpError::ConnectionFailed(_)),
            "Expected ConnectionFailed, got: {:?}",
            err
        );
    }

    #[test]
    fn test_permissive_policy_allows_anything() {
        let policy = SecurityPolicy::permissive();
        let result = McpClient::connect("__nonexistent_mcp_server__", &[], Some(&policy));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, McpError::ConnectionFailed(_)),
            "Expected ConnectionFailed, got: {:?}",
            err
        );
    }
}
