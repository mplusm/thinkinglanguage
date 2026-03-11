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
// Channel-based tool dispatch
// ---------------------------------------------------------------------------

/// A request to call a TL function from the MCP server thread.
///
/// When using channel-based dispatch, each tool invocation produces a
/// `ToolCallRequest` sent over a channel to the main TL execution thread.
/// The receiver processes the call and sends the result back via `response_tx`.
pub struct ToolCallRequest {
    /// Name of the tool being called.
    pub tool_name: String,
    /// JSON arguments passed by the MCP client.
    pub arguments: serde_json::Value,
    /// One-shot channel to send the result back to the MCP server thread.
    pub response_tx: std::sync::mpsc::SyncSender<Result<serde_json::Value, String>>,
}

/// Tool definition for channel-based dispatch (no closure handler).
///
/// Unlike [`ToolDef`], this does not carry a handler callback. Instead,
/// tools registered via [`TlServerBuilder::channel_tools`] send each
/// invocation through a channel for external processing.
pub struct ChannelToolDef {
    /// The tool name (must be unique within a server).
    pub name: String,
    /// Human-readable description of what the tool does.
    pub description: String,
    /// JSON Schema describing the expected input parameters.
    pub input_schema: serde_json::Value,
}

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
///
/// `Clone` is supported because the handler is wrapped in `Arc`.
#[derive(Clone)]
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
    pub(crate) tools: Vec<ToolDef>,
    pub(crate) server_info: ServerInfo,
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
    pub(crate) fn dispatch_tool_call(
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

    /// Register tools that dispatch via channel instead of closures.
    ///
    /// Returns `(self, receiver)` where `receiver` yields [`ToolCallRequest`]s
    /// for each tool invocation. The caller is responsible for reading from the
    /// receiver and sending results back via each request's `response_tx`.
    ///
    /// The channel has a bounded capacity of 32 pending requests.
    pub fn channel_tools(
        mut self,
        tools: Vec<ChannelToolDef>,
    ) -> (Self, std::sync::mpsc::Receiver<ToolCallRequest>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(32);
        for tool in tools {
            let tx = tx.clone();
            let tool_name = tool.name.clone();
            self.tools.push(ToolDef {
                name: tool.name,
                description: tool.description,
                input_schema: tool.input_schema,
                handler: Arc::new(move |args| {
                    let (resp_tx, resp_rx) = std::sync::mpsc::sync_channel(1);
                    tx.send(ToolCallRequest {
                        tool_name: tool_name.clone(),
                        arguments: args,
                        response_tx: resp_tx,
                    })
                    .map_err(|_| "Server dispatch channel closed".to_string())?;
                    resp_rx
                        .recv()
                        .map_err(|_| "Tool response channel closed".to_string())?
                }),
            });
        }
        (self, rx)
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

/// Start the MCP server on a background thread via stdio.
///
/// Returns a join handle. The tool call receiver should have been obtained
/// from [`TlServerBuilder::channel_tools`] before building.
///
/// The background thread creates its own tokio runtime and blocks on
/// [`serve_stdio`] until the client disconnects.
pub fn serve_stdio_background(
    handler: TlServerHandler,
) -> std::thread::JoinHandle<Result<(), McpError>> {
    std::thread::spawn(move || serve_stdio(handler))
}

// ---------------------------------------------------------------------------
// HTTP server entry points
// ---------------------------------------------------------------------------

/// Run the MCP server over HTTP on the given port, blocking until shutdown.
///
/// Creates a new tokio runtime internally. The server listens on `0.0.0.0:{port}`
/// with the MCP endpoint at `/mcp`.
///
/// # Arguments
/// * `handler` — The server handler with registered tools.
/// * `port` — The TCP port to listen on.
///
/// # Errors
/// * [`McpError::RuntimeError`] — Could not create tokio runtime.
/// * [`McpError::ConnectionFailed`] — Could not bind the port.
pub fn serve_http(handler: TlServerHandler, port: u16) -> Result<(), McpError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| McpError::RuntimeError(format!("Failed to create runtime: {e}")))?;
    rt.block_on(serve_http_async(handler, port))
}

/// Run the MCP server over HTTP using an existing tokio runtime.
///
/// Blocks the calling thread until the server shuts down.
pub fn serve_http_with_runtime(
    handler: TlServerHandler,
    port: u16,
    runtime: Arc<tokio::runtime::Runtime>,
) -> Result<(), McpError> {
    runtime.block_on(serve_http_async(handler, port))
}

/// Internal async HTTP server implementation.
async fn serve_http_async(handler: TlServerHandler, port: u16) -> Result<(), McpError> {
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService,
        session::local::LocalSessionManager,
    };

    // Capture the tools and server_info so the factory can create new
    // TlServerHandler instances per session (each session gets its own handler).
    let tools = handler.tools;
    let server_info = handler.server_info;

    let service = StreamableHttpService::new(
        move || {
            Ok(TlServerHandler {
                tools: tools.clone(),
                server_info: server_info.clone(),
            })
        },
        std::sync::Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );

    let app = axum::Router::new().nest_service("/mcp", service);
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| McpError::ConnectionFailed(format!("Failed to bind {addr}: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| McpError::RuntimeError(format!("HTTP server error: {e}")))?;

    Ok(())
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

    // -----------------------------------------------------------------------
    // Channel-based dispatch tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_channel_tool_dispatch() {
        let (builder, rx) = TlServerHandler::builder()
            .name("channel-test")
            .version("1.0.0")
            .channel_tools(vec![ChannelToolDef {
                name: "greet".to_string(),
                description: "Greet someone".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    }
                }),
            }]);
        let handler = builder.build();

        // Simulate a tool call in a background thread
        let handle = std::thread::spawn(move || {
            let request = CallToolRequestParams::new("greet").with_arguments(
                serde_json::Map::from_iter([("name".to_string(), json!("TL"))]),
            );
            handler.dispatch_tool_call(&request)
        });

        // Process the request on the "main" thread
        let req = rx.recv().unwrap();
        assert_eq!(req.tool_name, "greet");
        let name = req.arguments.get("name").unwrap().as_str().unwrap();
        assert_eq!(name, "TL");
        req.response_tx
            .send(Ok(json!(format!("Hello, {name}!"))))
            .unwrap();

        // Check the result
        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.is_error, Some(false));
        let text = result.content[0].as_text().unwrap();
        assert_eq!(text.text, "Hello, TL!");
    }

    #[test]
    fn test_channel_tool_dispatch_error() {
        let (builder, rx) = TlServerHandler::builder()
            .name("channel-err-test")
            .version("1.0.0")
            .channel_tools(vec![ChannelToolDef {
                name: "fail_tool".to_string(),
                description: "A tool that will fail".to_string(),
                input_schema: json!({"type": "object"}),
            }]);
        let handler = builder.build();

        let handle = std::thread::spawn(move || {
            let request = CallToolRequestParams::new("fail_tool");
            handler.dispatch_tool_call(&request)
        });

        // Respond with error
        let req = rx.recv().unwrap();
        assert_eq!(req.tool_name, "fail_tool");
        req.response_tx
            .send(Err("Tool failed intentionally".to_string()))
            .unwrap();

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap();
        assert!(text.text.contains("Tool failed intentionally"));
    }

    #[test]
    fn test_channel_tools_mixed_with_closure_tools() {
        let (builder, rx) = TlServerHandler::builder()
            .name("mixed-test")
            .version("1.0.0")
            .tool(make_echo_tool())
            .channel_tools(vec![ChannelToolDef {
                name: "channel_add".to_string(),
                description: "Add via channel".to_string(),
                input_schema: json!({"type": "object"}),
            }]);
        let handler = builder.build();

        // Handler should have 2 tools total (1 closure + 1 channel)
        assert_eq!(handler.tool_count(), 2);

        // The closure-based echo tool should still work directly
        let echo_req = CallToolRequestParams::new("echo").with_arguments(
            serde_json::Map::from_iter([("message".to_string(), json!("direct"))]),
        );
        let echo_result = handler.dispatch_tool_call(&echo_req).unwrap();
        assert_eq!(echo_result.is_error, Some(false));
        let text = echo_result.content[0].as_text().unwrap();
        assert_eq!(text.text, "direct");

        // The channel-based tool should dispatch via the channel
        let handle = {
            let handler_ref = &handler;
            // We need to move handler into the thread, but we also need rx.
            // Instead, call dispatch from main and process in background.
            let (result_tx, result_rx) =
                std::sync::mpsc::sync_channel::<Result<CallToolResult, rmcp::ErrorData>>(1);
            let handler = TlServerHandler {
                tools: handler_ref.tools.clone(),
                server_info: handler_ref.server_info.clone(),
            };
            let jh = std::thread::spawn(move || {
                let req = CallToolRequestParams::new("channel_add").with_arguments(
                    serde_json::Map::from_iter([
                        ("a".to_string(), json!(10)),
                        ("b".to_string(), json!(20)),
                    ]),
                );
                let r = handler.dispatch_tool_call(&req);
                result_tx.send(r).ok();
            });
            // Process channel request
            let tool_req = rx.recv().unwrap();
            assert_eq!(tool_req.tool_name, "channel_add");
            let a = tool_req.arguments.get("a").and_then(|v| v.as_i64()).unwrap_or(0);
            let b = tool_req.arguments.get("b").and_then(|v| v.as_i64()).unwrap_or(0);
            tool_req
                .response_tx
                .send(Ok(json!(a + b)))
                .unwrap();
            let result = result_rx.recv().unwrap().unwrap();
            assert_eq!(result.is_error, Some(false));
            let text = result.content[0].as_text().unwrap();
            assert_eq!(text.text, "30");
            jh
        };
        handle.join().unwrap();
    }

    #[test]
    fn test_channel_tool_unknown_tool() {
        let (builder, _rx) = TlServerHandler::builder()
            .name("channel-unknown-test")
            .version("1.0.0")
            .channel_tools(vec![ChannelToolDef {
                name: "only_tool".to_string(),
                description: "The only tool".to_string(),
                input_schema: json!({"type": "object"}),
            }]);
        let handler = builder.build();

        // Calling a non-existent tool should not touch the channel
        let request = CallToolRequestParams::new("nonexistent");
        let result = handler.dispatch_tool_call(&request).unwrap();
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap();
        assert!(text.text.contains("Unknown tool"));
    }

    #[test]
    fn test_serve_stdio_background_type() {
        // Just verify the function signature compiles and returns a JoinHandle.
        // We cannot actually run stdio in tests, but we can verify the type.
        let _: fn(TlServerHandler) -> std::thread::JoinHandle<Result<(), McpError>> =
            serve_stdio_background;
    }
}
