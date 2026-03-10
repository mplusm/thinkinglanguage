//! Shared error types for MCP client and server operations.

/// Errors that can occur during MCP client or server operations.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// Permission was denied for the requested operation.
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Failed to establish or maintain a connection.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// A protocol-level error occurred (invalid messages, unexpected state, etc.).
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// A tool invocation returned an error result.
    #[error("Tool error: {0}")]
    ToolError(String),

    /// The transport was closed unexpectedly.
    #[error("Transport closed")]
    TransportClosed,

    /// An operation timed out.
    #[error("Timeout")]
    Timeout,

    /// Failed to build or use the tokio runtime.
    #[error("Runtime error: {0}")]
    RuntimeError(String),
}
