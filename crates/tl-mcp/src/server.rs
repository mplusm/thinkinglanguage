//! MCP server implementation for ThinkingLanguage.
//!
//! Provides [`TlServerHandler`] which implements the rmcp [`ServerHandler`] trait,
//! allowing TL to run as an MCP server over stdio. External MCP clients can connect,
//! discover registered tools via `tools/list`, and invoke them via `tools/call`.
//!
//! # Example
//!
//! ```rust,no_run
//! use tl_mcp::server::{TlServerHandler, ToolDef, ToolHandler};
//! use std::sync::Arc;
//! use serde_json::json;
//!
//! let handler = TlServerHandler::builder()
//!     .name("my-tl-server")
//!     .version("1.0.0")
//!     .tool(ToolDef {
//!         name: "echo".to_string(),
//!         description: "Echoes back the input".to_string(),
//!         input_schema: json!({"type": "object", "properties": {"message": {"type": "string"}}}),
//!         handler: Arc::new(|args| Ok(args)),
//!     })
//!     .build();
//!
//! // tl_mcp::server::serve_stdio(handler).unwrap();
//! ```

use std::sync::Arc;

use rmcp::{
    handler::server::ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::ServiceExt,
};

use crate::error::McpError;

// ---------------------------------------------------------------------------
// ToolHandler type & ToolDef struct
// ---------------------------------------------------------------------------

/// Callback type for tool invocation.
///
/// Receives the tool arguments as a JSON value and returns either a JSON result
/// or a string error message.
pub type ToolHandler =
    Arc<dyn Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync>;

/// A tool definition with its handler callback.
pub struct ToolDef {
    /// The tool name (must be unique within a server).
    pub name: String,
    /// Human-readable description of what the tool does.
    pub description: String,
    /// JSON Schema describing the expected input parameters.
    pub input_schema: serde_json::Value,
    /// The callback that executes this tool.
    pub handler: ToolHandler,
}

// ---------------------------------------------------------------------------
// TlServerHandler
// ---------------------------------------------------------------------------

/// MCP server handler that dispatches tool calls to registered TL tool handlers.
///
/// Implements the rmcp [`ServerHandler`] trait. Use [`TlServerHandler::builder()`]
/// to construct an instance with registered tools.
pub struct TlServerHandler {
    tools: Vec<ToolDef>,
    server_info: ServerInfo,
}

impl TlServerHandler {
    /// Create a new builder for constructing a `TlServerHandler`.
    pub fn builder() -> TlServerBuilder {
        TlServerBuilder {
            tools: Vec::new(),
            name: "tl-mcp-server".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    /// Returns the number of registered tools.
    pub fn tool_count(&self) -> usize {
        self.tools.len()
    }

    /// Convert a `ToolDef` into an rmcp `Tool` for protocol responses.
    fn tool_def_to_rmcp(def: &ToolDef) -> Tool {
        // Extract the JSON object from the schema value.
        // If it's not an object, wrap it in one with "type": "object".
        let schema_obj = match &def.input_schema {
            serde_json::Value::Object(map) => map.clone(),
            _ => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "type".to_string(),
                    serde_json::Value::String("object".to_string()),
                );
                map
            }
        };
        Tool::new(def.name.clone(), def.description.clone(), schema_obj)
    }
}

impl ServerHandler for TlServerHandler {
    fn get_info(&self) -> ServerInfo {
        self.server_info.clone()
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        let tools: Vec<Tool> = self.tools.iter().map(Self::tool_def_to_rmcp).collect();
        std::future::ready(Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools,
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        let result = self.dispatch_tool_call(&request);
        std::future::ready(result)
    }
}

impl TlServerHandler {
    /// Dispatch a tool call to the matching registered handler.
    fn dispatch_tool_call(
        &self,
        request: &CallToolRequestParams,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let tool_name = request.name.as_ref();

        // Find the matching tool
        let tool_def = self.tools.iter().find(|t| t.name == tool_name);
        let tool_def = match tool_def {
            Some(t) => t,
            None => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Unknown tool: {tool_name}"
                ))]));
            }
        };

        // Convert the arguments map to a serde_json::Value::Object
        let args = match &request.arguments {
            Some(map) => serde_json::Value::Object(map.clone()),
            None => serde_json::Value::Object(serde_json::Map::new()),
        };

        // Call the handler
        match (tool_def.handler)(args) {
            Ok(result) => {
                let text = match result {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(err) => Ok(CallToolResult::error(vec![Content::text(err)])),
        }
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`TlServerHandler`] with registered tools.
pub struct TlServerBuilder {
    tools: Vec<ToolDef>,
    name: String,
    version: String,
}

impl TlServerBuilder {
    /// Set the server name reported during MCP initialization.
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set the server version reported during MCP initialization.
    pub fn version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self
    }

    /// Register a tool with the server.
    pub fn tool(mut self, def: ToolDef) -> Self {
        self.tools.push(def);
        self
    }

    /// Build the [`TlServerHandler`] with all registered tools.
    pub fn build(self) -> TlServerHandler {
        let capabilities = ServerCapabilities::builder().enable_tools().build();

        let server_info = ServerInfo::new(capabilities)
            .with_server_info(Implementation::new(&self.name, &self.version));

        TlServerHandler {
            tools: self.tools,
            server_info,
        }
    }
}

// ---------------------------------------------------------------------------
// Stdio server entry points
// ---------------------------------------------------------------------------

/// Run the MCP server over stdio, blocking until the client disconnects.
///
/// This creates a new tokio runtime internally. For embedding in an existing
/// runtime, use [`serve_stdio_with_runtime`] instead.
pub fn serve_stdio(handler: TlServerHandler) -> Result<(), McpError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| McpError::ConnectionFailed(format!("Runtime creation failed: {e}")))?;

    rt.block_on(async {
        let transport = rmcp::transport::io::stdio();
        let service = handler
            .serve(transport)
            .await
            .map_err(|e| McpError::ConnectionFailed(e.to_string()))?;

        // Wait until client disconnects or service is cancelled
        service
            .waiting()
            .await
            .map_err(|e| McpError::ProtocolError(e.to_string()))?;

        Ok(())
    })
}

/// Run the MCP server over stdio using an existing tokio runtime.
///
/// Blocks the calling thread until the client disconnects.
pub fn serve_stdio_with_runtime(
    handler: TlServerHandler,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<(), McpError> {
    runtime.block_on(async {
        let transport = rmcp::transport::io::stdio();
        let service = handler
            .serve(transport)
            .await
            .map_err(|e| McpError::ConnectionFailed(e.to_string()))?;

        service
            .waiting()
            .await
            .map_err(|e| McpError::ProtocolError(e.to_string()))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_echo_tool() -> ToolDef {
        ToolDef {
            name: "echo".to_string(),
            description: "Echoes back the input".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": {"type": "string"}
                }
            }),
            handler: Arc::new(|args| {
                let msg = args
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("no message");
                Ok(serde_json::Value::String(msg.to_string()))
            }),
        }
    }

    fn make_add_tool() -> ToolDef {
        ToolDef {
            name: "add".to_string(),
            description: "Adds two numbers".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "a": {"type": "number"},
                    "b": {"type": "number"}
                }
            }),
            handler: Arc::new(|args| {
                let a = args.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let b = args.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                Ok(json!(a + b))
            }),
        }
    }

    #[test]
    fn test_server_builder() {
        let handler = TlServerHandler::builder()
            .name("test-server")
            .version("2.0.0")
            .tool(make_echo_tool())
            .tool(make_add_tool())
            .build();

        assert_eq!(handler.tool_count(), 2);
        assert_eq!(handler.server_info.server_info.name, "test-server");
        assert_eq!(handler.server_info.server_info.version, "2.0.0");
    }

    #[test]
    fn test_server_capabilities() {
        let handler = TlServerHandler::builder()
            .name("cap-test")
            .version("1.0.0")
            .build();

        let info = handler.server_info.clone();
        // Tools capability should be enabled
        assert!(
            info.capabilities.tools.is_some(),
            "Tools capability should be enabled"
        );
    }

    #[test]
    fn test_server_tool_dispatch_success() {
        let handler = TlServerHandler::builder()
            .name("dispatch-test")
            .version("1.0.0")
            .tool(make_echo_tool())
            .tool(make_add_tool())
            .build();

        // Test echo tool
        let mut args_map = serde_json::Map::new();
        args_map.insert(
            "message".to_string(),
            serde_json::Value::String("hello world".to_string()),
        );

        let request = CallToolRequestParams::new("echo").with_arguments(args_map);

        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.content.len(), 1);
        let text = result.content[0].as_text().unwrap();
        assert_eq!(text.text, "hello world");
    }

    #[test]
    fn test_server_tool_dispatch_add() {
        let handler = TlServerHandler::builder()
            .name("dispatch-test")
            .version("1.0.0")
            .tool(make_add_tool())
            .build();

        let mut args_map = serde_json::Map::new();
        args_map.insert("a".to_string(), json!(3.0));
        args_map.insert("b".to_string(), json!(4.0));

        let request = CallToolRequestParams::new("add").with_arguments(args_map);

        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(false));
        let text = result.content[0].as_text().unwrap();
        // json!(a + b) for floats produces "7.0"
        assert!(text.text == "7" || text.text == "7.0", "Expected 7 or 7.0, got: {}", text.text);
    }

    #[test]
    fn test_server_tool_dispatch_unknown_tool() {
        let handler = TlServerHandler::builder()
            .name("dispatch-test")
            .version("1.0.0")
            .tool(make_echo_tool())
            .build();

        let request = CallToolRequestParams::new("nonexistent");

        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap();
        assert!(text.text.contains("Unknown tool"));
    }

    #[test]
    fn test_server_tool_dispatch_handler_error() {
        let handler = TlServerHandler::builder()
            .name("error-test")
            .version("1.0.0")
            .tool(ToolDef {
                name: "fail".to_string(),
                description: "Always fails".to_string(),
                input_schema: json!({"type": "object"}),
                handler: Arc::new(|_| Err("Something went wrong".to_string())),
            })
            .build();

        let request = CallToolRequestParams::new("fail");

        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap();
        assert!(text.text.contains("Something went wrong"));
    }

    #[test]
    fn test_server_tool_dispatch_no_arguments() {
        let handler = TlServerHandler::builder()
            .name("no-args-test")
            .version("1.0.0")
            .tool(make_echo_tool())
            .build();

        // Call with no arguments — handler should still work
        let request = CallToolRequestParams::new("echo");

        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(false));
        let text = result.content[0].as_text().unwrap();
        assert_eq!(text.text, "no message");
    }

    #[tokio::test]
    async fn test_server_list_tools_via_trait() {
        let handler = TlServerHandler::builder()
            .name("list-test")
            .version("1.0.0")
            .tool(make_echo_tool())
            .tool(make_add_tool())
            .build();

        // Create a mock context — we need Peer and RequestContext
        // Since list_tools doesn't actually use the context, we test via
        // the direct method instead of through the full trait dispatch.
        let tools: Vec<Tool> = handler.tools.iter().map(TlServerHandler::tool_def_to_rmcp).collect();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name.as_ref(), "echo");
        assert_eq!(tools[1].name.as_ref(), "add");
        assert!(tools[0].description.as_deref() == Some("Echoes back the input"));
        assert!(tools[1].description.as_deref() == Some("Adds two numbers"));
    }

    #[test]
    fn test_tool_schema_conversion() {
        let def = ToolDef {
            name: "test".to_string(),
            description: "Test tool".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "x": {"type": "integer"}
                },
                "required": ["x"]
            }),
            handler: Arc::new(|_| Ok(json!(null))),
        };

        let tool = TlServerHandler::tool_def_to_rmcp(&def);
        assert_eq!(tool.name.as_ref(), "test");
        let schema = tool.schema_as_json_value();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["x"]["type"] == "integer");
    }

    #[test]
    fn test_empty_server() {
        let handler = TlServerHandler::builder()
            .name("empty")
            .version("0.0.1")
            .build();

        assert_eq!(handler.tool_count(), 0);
        assert!(handler.server_info.capabilities.tools.is_some());
    }
}
