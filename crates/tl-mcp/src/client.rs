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
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo,
    CreateMessageRequestParams, CreateMessageResult, ErrorData, GetPromptRequestParams,
    GetPromptResult, Implementation, ReadResourceRequestParams, ReadResourceResult, Role,
    SamplingCapability, SamplingMessage, SamplingMessageContent, ServerInfo, Tool,
};
use rmcp::service::{RoleClient, RequestContext, RunningService};
use rmcp::transport::TokioChildProcess;
use rmcp::{ClientHandler, ServiceExt};
use tl_errors::security::SecurityPolicy;

use crate::error::McpError;

// ---------------------------------------------------------------------------
// Timeout constants
// ---------------------------------------------------------------------------

/// Timeout for initial MCP handshake (connect / serve).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Timeout for tool calls (may do substantial work).
const TOOL_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Timeout for metadata / lightweight operations (ping, list, read).
const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Sampling types
// ---------------------------------------------------------------------------

/// Request for LLM completion from an MCP server.
///
/// Uses only primitive types so tl-mcp does not depend on tl-ai.
#[derive(Debug, Clone)]
pub struct SamplingRequest {
    /// Conversation messages as (role, content) pairs.
    pub messages: Vec<(String, String)>,
    /// Optional system prompt to guide model behavior.
    pub system_prompt: Option<String>,
    /// Maximum tokens to generate.
    pub max_tokens: u32,
    /// Temperature for controlling randomness (0.0 to 1.0).
    pub temperature: Option<f64>,
    /// Hint for which model to use (e.g. "claude-sonnet-4-20250514").
    pub model_hint: Option<String>,
    /// Sequences that should stop generation.
    pub stop_sequences: Option<Vec<String>>,
}

/// Response from LLM completion.
#[derive(Debug, Clone)]
pub struct SamplingResponse {
    /// The model that produced this response.
    pub model: String,
    /// The generated text content.
    pub content: String,
    /// Reason generation stopped (e.g. "endTurn", "maxTokens").
    pub stop_reason: Option<String>,
}

/// Callback type for handling sampling requests.
///
/// MCP servers can request LLM completions from the client via the
/// `sampling/createMessage` method. This callback bridges that request
/// to whatever LLM backend the host provides (e.g. tl-ai).
pub type SamplingCallback =
    Arc<dyn Fn(SamplingRequest) -> Result<SamplingResponse, String> + Send + Sync>;

// ---------------------------------------------------------------------------
// Client handler
// ---------------------------------------------------------------------------

/// MCP client handler for TL.
///
/// Provides client identification via `get_info()` and optionally handles
/// `sampling/createMessage` requests from the server when a [`SamplingCallback`]
/// is configured.
pub struct TlClientHandler {
    /// Optional callback for handling sampling requests from the server.
    pub(crate) sampling_callback: Option<SamplingCallback>,
}

impl std::fmt::Debug for TlClientHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlClientHandler")
            .field("has_sampling", &self.sampling_callback.is_some())
            .finish()
    }
}

impl TlClientHandler {
    /// Create a new handler with no sampling support.
    pub fn new() -> Self {
        Self {
            sampling_callback: None,
        }
    }

    /// Configure a sampling callback for handling `sampling/createMessage`.
    pub fn with_sampling(mut self, cb: SamplingCallback) -> Self {
        self.sampling_callback = Some(cb);
        self
    }
}

impl Default for TlClientHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientHandler for TlClientHandler {
    fn get_info(&self) -> ClientInfo {
        let mut caps = ClientCapabilities::default();
        if self.sampling_callback.is_some() {
            caps.sampling = Some(SamplingCapability::default());
        }
        ClientInfo::new(
            caps,
            Implementation::new("tl", env!("CARGO_PKG_VERSION"))
                .with_title("ThinkingLanguage MCP Client"),
        )
    }

    fn create_message(
        &self,
        params: CreateMessageRequestParams,
        _context: RequestContext<RoleClient>,
    ) -> impl Future<Output = Result<CreateMessageResult, ErrorData>> + Send + '_ {
        let result = match &self.sampling_callback {
            Some(cb) => {
                // Convert SamplingMessage list to (role, content) pairs
                let messages: Vec<(String, String)> = params
                    .messages
                    .iter()
                    .map(|m| {
                        let role = match m.role {
                            Role::User => "user".to_string(),
                            Role::Assistant => "assistant".to_string(),
                        };
                        // Extract text from content (may be Single or Multiple)
                        let content: String = m
                            .content
                            .iter()
                            .filter_map(|c| c.as_text().map(|t| t.text.as_str()))
                            .collect::<Vec<_>>()
                            .join("");
                        (role, content)
                    })
                    .collect();

                // Extract model hint from model_preferences
                let model_hint = params
                    .model_preferences
                    .as_ref()
                    .and_then(|p| p.hints.as_ref())
                    .and_then(|h| h.first())
                    .and_then(|h| h.name.clone());

                let req = SamplingRequest {
                    messages,
                    system_prompt: params.system_prompt.clone(),
                    max_tokens: params.max_tokens,
                    temperature: params.temperature.map(|t| t as f64),
                    model_hint,
                    stop_sequences: params.stop_sequences.clone(),
                };

                match cb(req) {
                    Ok(resp) => {
                        let mut result = CreateMessageResult::new(
                            SamplingMessage::new(
                                Role::Assistant,
                                SamplingMessageContent::text(resp.content),
                            ),
                            resp.model,
                        );
                        if let Some(reason) = resp.stop_reason {
                            result = result.with_stop_reason(reason);
                        }
                        Ok(result)
                    }
                    Err(e) => Err(ErrorData::internal_error(e, None)),
                }
            }
            None => Err(ErrorData::method_not_found::<
                rmcp::model::CreateMessageRequestMethod,
            >()),
        };
        std::future::ready(result)
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
        Self::connect_with_sampling(command, args, security_policy, None)
    }

    /// Connect to an MCP server by spawning a subprocess, with optional
    /// sampling callback for handling `sampling/createMessage` requests.
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
    /// * `sampling_cb` — Optional callback for LLM sampling requests from the server.
    pub fn connect_with_sampling(
        command: &str,
        args: &[String],
        security_policy: Option<&SecurityPolicy>,
        sampling_cb: Option<SamplingCallback>,
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

        // --- Build handler ---
        let handler = match sampling_cb {
            Some(cb) => TlClientHandler::new().with_sampling(cb),
            None => TlClientHandler::new(),
        };

        // --- Spawn subprocess and perform handshake ---
        let (service, server_info) = runtime.block_on(async {
            // Build the tokio Command
            let mut cmd = tokio::process::Command::new(command);
            cmd.args(args);

            // Spawn via TokioChildProcess (handles piped stdio + framing)
            let transport = TokioChildProcess::new(cmd).map_err(|e| {
                McpError::ConnectionFailed(format!("Failed to spawn '{}': {}", command, e))
            })?;

            // Perform 3-step handshake with timeout
            match tokio::time::timeout(CONNECT_TIMEOUT, handler.serve(transport)).await {
                Ok(Ok(service)) => {
                    let server_info = service.peer().peer_info().cloned();
                    Ok::<_, McpError>((service, server_info))
                }
                Ok(Err(e)) => {
                    Err(McpError::ConnectionFailed(format!("Handshake failed: {}", e)))
                }
                Err(_) => Err(McpError::Timeout),
            }
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
        Self::connect_with_runtime_and_sampling(command, args, security_policy, runtime, None)
    }

    /// Connect to an MCP server using an existing tokio runtime and optional sampling.
    pub fn connect_with_runtime_and_sampling(
        command: &str,
        args: &[String],
        security_policy: Option<&SecurityPolicy>,
        runtime: Arc<tokio::runtime::Runtime>,
        sampling_cb: Option<SamplingCallback>,
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

        // --- Build handler ---
        let handler = match sampling_cb {
            Some(cb) => TlClientHandler::new().with_sampling(cb),
            None => TlClientHandler::new(),
        };

        // --- Spawn subprocess and perform handshake ---
        let (service, server_info) = runtime.block_on(async {
            let mut cmd = tokio::process::Command::new(command);
            cmd.args(args);

            let transport = TokioChildProcess::new(cmd).map_err(|e| {
                McpError::ConnectionFailed(format!("Failed to spawn '{}': {}", command, e))
            })?;

            // Perform 3-step handshake with timeout
            match tokio::time::timeout(CONNECT_TIMEOUT, handler.serve(transport)).await {
                Ok(Ok(service)) => {
                    let server_info = service.peer().peer_info().cloned();
                    Ok::<_, McpError>((service, server_info))
                }
                Ok(Err(e)) => {
                    Err(McpError::ConnectionFailed(format!("Handshake failed: {}", e)))
                }
                Err(_) => Err(McpError::Timeout),
            }
        })?;

        Ok(McpClient {
            runtime,
            service: Some(service),
            server_info,
        })
    }

    /// Connect to a remote MCP server over HTTP (Streamable HTTP transport).
    ///
    /// Creates a new tokio runtime internally. For sharing an existing runtime,
    /// use [`connect_http_with_runtime()`](Self::connect_http_with_runtime).
    ///
    /// # Arguments
    /// * `url` — The HTTP(S) URL of the MCP server endpoint (e.g. `"http://localhost:8080/mcp"`).
    ///
    /// # Errors
    /// * [`McpError::RuntimeError`] — Could not create tokio runtime.
    /// * [`McpError::ConnectionFailed`] — HTTP connection or MCP handshake failed.
    pub fn connect_http(url: &str) -> Result<Self, McpError> {
        Self::connect_http_with_sampling(url, None)
    }

    /// Connect to a remote MCP server over HTTP with optional sampling callback.
    pub fn connect_http_with_sampling(
        url: &str,
        sampling_cb: Option<SamplingCallback>,
    ) -> Result<Self, McpError> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    McpError::RuntimeError(format!("Failed to create runtime: {e}"))
                })?,
        );
        Self::connect_http_with_runtime_and_sampling(url, rt, sampling_cb)
    }

    /// Connect to a remote MCP server over HTTP using an existing tokio runtime.
    ///
    /// # Arguments
    /// * `url` — The HTTP(S) URL of the MCP server endpoint.
    /// * `runtime` — A shared tokio runtime to use for async operations.
    pub fn connect_http_with_runtime(
        url: &str,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, McpError> {
        Self::connect_http_with_runtime_and_sampling(url, runtime, None)
    }

    /// Connect to a remote MCP server over HTTP with runtime and optional sampling.
    pub fn connect_http_with_runtime_and_sampling(
        url: &str,
        runtime: Arc<tokio::runtime::Runtime>,
        sampling_cb: Option<SamplingCallback>,
    ) -> Result<Self, McpError> {
        let url_str = url.to_string();
        let handler = match sampling_cb {
            Some(cb) => TlClientHandler::new().with_sampling(cb),
            None => TlClientHandler::new(),
        };
        let (service, server_info) = runtime.block_on(async {
            use rmcp::transport::StreamableHttpClientTransport;

            let transport = StreamableHttpClientTransport::from_uri(url_str);
            match tokio::time::timeout(CONNECT_TIMEOUT, handler.serve(transport)).await {
                Ok(Ok(service)) => {
                    let info = service.peer_info().cloned();
                    Ok::<_, McpError>((service, info))
                }
                Ok(Err(e)) => {
                    Err(McpError::ConnectionFailed(format!("HTTP connect failed: {e}")))
                }
                Err(_) => Err(McpError::Timeout),
            }
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
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn list_tools(&self) -> Result<Vec<Tool>, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            match tokio::time::timeout(METADATA_TIMEOUT, service.peer().list_all_tools()).await {
                Ok(Ok(tools)) => Ok(tools),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
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
            match tokio::time::timeout(TOOL_CALL_TIMEOUT, service.peer().call_tool(params)).await {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
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
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn ping(&self) -> Result<(), McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            let ping_fut = service.peer().send_request(
                rmcp::model::ClientRequest::PingRequest(rmcp::model::PingRequest {
                    method: Default::default(),
                    extensions: Default::default(),
                }),
            );
            match tokio::time::timeout(METADATA_TIMEOUT, ping_fut).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
        })
    }

    /// List all resources exposed by the connected MCP server.
    ///
    /// Uses `list_all_resources()` which automatically handles pagination.
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn list_resources(&self) -> Result<Vec<rmcp::model::Resource>, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            match tokio::time::timeout(METADATA_TIMEOUT, service.peer().list_all_resources()).await
            {
                Ok(Ok(resources)) => Ok(resources),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
        })
    }

    /// Read a resource by URI.
    ///
    /// Returns the resource contents (text or blob).
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn read_resource(&self, uri: &str) -> Result<ReadResourceResult, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        let params = ReadResourceRequestParams::new(uri);
        self.runtime.block_on(async {
            match tokio::time::timeout(METADATA_TIMEOUT, service.peer().read_resource(params)).await
            {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
        })
    }

    /// List all prompts exposed by the connected MCP server.
    ///
    /// Uses `list_all_prompts()` which automatically handles pagination.
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn list_prompts(&self) -> Result<Vec<rmcp::model::Prompt>, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        self.runtime.block_on(async {
            match tokio::time::timeout(METADATA_TIMEOUT, service.peer().list_all_prompts()).await {
                Ok(Ok(prompts)) => Ok(prompts),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
        })
    }

    /// Get a prompt by name with optional arguments.
    ///
    /// Returns the prompt result containing description and messages.
    /// Times out after [`METADATA_TIMEOUT`] (10 seconds).
    pub fn get_prompt(
        &self,
        name: &str,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<GetPromptResult, McpError> {
        let service = self.service.as_ref().ok_or(McpError::TransportClosed)?;
        let mut params = GetPromptRequestParams::new(name);
        if let Some(args) = arguments {
            params.arguments = Some(args);
        }
        self.runtime.block_on(async {
            match tokio::time::timeout(METADATA_TIMEOUT, service.peer().get_prompt(params)).await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(e)) => Err(McpError::ProtocolError(e.to_string())),
                Err(_) => Err(McpError::Timeout),
            }
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
    fn test_client_handler_info_no_sampling() {
        let handler = TlClientHandler::new();
        let info = handler.get_info();

        assert_eq!(info.client_info.name, "tl");
        assert_eq!(info.client_info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            info.client_info.title,
            Some("ThinkingLanguage MCP Client".to_string())
        );
        // No sampling capability when no callback configured
        assert!(info.capabilities.sampling.is_none());
    }

    #[test]
    fn test_client_handler_info_with_sampling() {
        let cb: SamplingCallback = Arc::new(|_req| {
            Ok(SamplingResponse {
                model: "test".to_string(),
                content: "hello".to_string(),
                stop_reason: None,
            })
        });
        let handler = TlClientHandler::new().with_sampling(cb);
        let info = handler.get_info();

        assert_eq!(info.client_info.name, "tl");
        // Sampling capability advertised when callback is configured
        assert!(info.capabilities.sampling.is_some());
    }

    #[test]
    fn test_sampling_callback_construction() {
        let cb: SamplingCallback = Arc::new(|req| {
            Ok(SamplingResponse {
                model: "test-model".to_string(),
                content: format!(
                    "Echo: {}",
                    req.messages
                        .last()
                        .map(|(_, c)| c.as_str())
                        .unwrap_or("")
                ),
                stop_reason: Some("endTurn".to_string()),
            })
        });
        let handler = TlClientHandler::new().with_sampling(cb);
        assert!(handler.sampling_callback.is_some());
    }

    #[test]
    fn test_no_sampling_callback() {
        let handler = TlClientHandler::new();
        assert!(handler.sampling_callback.is_none());
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
