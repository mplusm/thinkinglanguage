//! MCP (Model Context Protocol) integration for ThinkingLanguage.
//!
//! Provides client and server MCP support over stdio and HTTP transports.

pub mod client;
pub mod convert;
pub mod error;
pub mod server;

// Re-export key types for convenience
pub use client::{McpClient, SamplingCallback, SamplingRequest, SamplingResponse, TlClientHandler};
pub use error::McpError;

// Re-export rmcp types used by other crates
pub use rmcp;
