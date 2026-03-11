//! Integration tests for MCP client-server communication.
//!
//! These tests spawn the `tl-test-mcp-server` binary as a subprocess,
//! connect via [`McpClient`], and verify the full MCP lifecycle:
//! handshake, tool discovery, tool invocation, error handling, and cleanup.

use std::sync::Arc;
use tl_errors::security::SecurityPolicy;
use tl_mcp::client::McpClient;
use tl_mcp::error::McpError;

/// Returns the path to the compiled `tl-test-mcp-server` binary.
///
/// The test binary is built alongside integration tests by cargo because
/// it's defined as a `[[bin]]` target in the same crate.
fn test_server_path() -> String {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name (e.g., integration-HASH)
    path.pop(); // remove deps/
    path.push("tl-test-mcp-server");
    path.to_string_lossy().to_string()
}

/// Helper: connect to the test server with no security policy.
fn connect_test_server() -> McpClient {
    let path = test_server_path();
    McpClient::connect(&path, &[], None).expect("Failed to connect to test server")
}

// ---------------------------------------------------------------------------
// Test 1: Handshake
// ---------------------------------------------------------------------------

#[test]
fn test_client_server_handshake() {
    let client = connect_test_server();

    // Connection should be alive
    assert!(client.is_connected(), "Client should be connected after handshake");

    // Server info should be populated from the handshake
    let info = client.server_info().expect("server_info should be Some after handshake");
    assert_eq!(info.server_info.name, "test-server");
    assert_eq!(info.server_info.version, "1.0.0");

    // Server should declare tools capability
    assert!(
        info.capabilities.tools.is_some(),
        "Server should declare tools capability"
    );
}

// ---------------------------------------------------------------------------
// Test 2: List tools
// ---------------------------------------------------------------------------

#[test]
fn test_list_tools() {
    let client = connect_test_server();

    let tools = client.list_tools().expect("list_tools should succeed");
    assert_eq!(tools.len(), 2, "Server should expose 2 tools");

    // Check tool names (order may vary, so collect into a set)
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"echo"), "Should have 'echo' tool");
    assert!(names.contains(&"add"), "Should have 'add' tool");

    // Verify descriptions exist
    let echo = tools.iter().find(|t| t.name.as_ref() == "echo").unwrap();
    assert!(
        echo.description
            .as_deref()
            .unwrap_or("")
            .contains("input"),
        "Echo tool should have a description mentioning input"
    );

    let add = tools.iter().find(|t| t.name.as_ref() == "add").unwrap();
    assert!(
        add.description
            .as_deref()
            .unwrap_or("")
            .to_lowercase()
            .contains("add"),
        "Add tool should have a description mentioning add"
    );

    // Verify input schemas have properties
    let echo_schema = echo.schema_as_json_value();
    assert_eq!(echo_schema["type"], "object");
    assert!(
        echo_schema["properties"]["message"].is_object(),
        "Echo schema should have 'message' property"
    );

    let add_schema = add.schema_as_json_value();
    assert_eq!(add_schema["type"], "object");
    assert!(
        add_schema["properties"]["a"].is_object(),
        "Add schema should have 'a' property"
    );
    assert!(
        add_schema["properties"]["b"].is_object(),
        "Add schema should have 'b' property"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Call echo tool
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_echo() {
    let client = connect_test_server();

    let result = client
        .call_tool("echo", serde_json::json!({"message": "hello world"}))
        .expect("call_tool echo should succeed");

    // Result should not be an error
    assert_ne!(
        result.is_error,
        Some(true),
        "Echo result should not be an error"
    );

    // Extract text content
    assert!(!result.content.is_empty(), "Result should have content");
    let text = result.content[0]
        .raw
        .as_text()
        .expect("Content should be text");

    // The server returns json!({"echoed": msg}) which gets serialized to string
    assert!(
        text.text.contains("hello world"),
        "Echo response should contain the input message, got: {}",
        text.text
    );
}

// ---------------------------------------------------------------------------
// Test 4: Call add tool
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_add() {
    let client = connect_test_server();

    let result = client
        .call_tool("add", serde_json::json!({"a": 2, "b": 3}))
        .expect("call_tool add should succeed");

    assert_ne!(
        result.is_error,
        Some(true),
        "Add result should not be an error"
    );

    assert!(!result.content.is_empty(), "Result should have content");
    let text = result.content[0]
        .raw
        .as_text()
        .expect("Content should be text");

    // The server returns json!({"result": a + b}) -> {"result":5} or {"result":5.0}
    assert!(
        text.text.contains("5"),
        "Add response should contain the sum (5), got: {}",
        text.text
    );
}

// ---------------------------------------------------------------------------
// Test 5: Call nonexistent tool
// ---------------------------------------------------------------------------

#[test]
fn test_call_nonexistent_tool() {
    let client = connect_test_server();

    // Our client's call_tool returns Err(ToolError) when is_error is true
    let result = client.call_tool("nonexistent_tool", serde_json::json!({}));

    match result {
        Err(McpError::ToolError(msg)) => {
            assert!(
                msg.contains("Unknown tool"),
                "Error should mention unknown tool, got: {}",
                msg
            );
        }
        Ok(r) => {
            // Some implementations might return Ok with is_error flag
            assert_eq!(
                r.is_error,
                Some(true),
                "Should be marked as error, got: {:?}",
                r
            );
        }
        Err(other) => {
            panic!(
                "Expected ToolError for nonexistent tool, got: {:?}",
                other
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test 6: Ping
// ---------------------------------------------------------------------------

#[test]
fn test_ping() {
    let client = connect_test_server();

    // Ping should succeed without error
    client.ping().expect("ping should succeed");

    // Client should still be connected after ping
    assert!(
        client.is_connected(),
        "Client should still be connected after ping"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Disconnect and cleanup
// ---------------------------------------------------------------------------

#[test]
fn test_disconnect_cleanup() {
    let mut client = connect_test_server();

    // Should be connected initially
    assert!(client.is_connected(), "Should be connected initially");

    // Disconnect
    client
        .disconnect()
        .expect("disconnect should succeed");

    // Should no longer be connected
    assert!(
        !client.is_connected(),
        "Should not be connected after disconnect"
    );

    // Operations on a disconnected client should fail with TransportClosed
    let list_result = client.list_tools();
    assert!(
        list_result.is_err(),
        "Operations after disconnect should fail"
    );
    match list_result.unwrap_err() {
        McpError::TransportClosed => {} // expected
        other => panic!("Expected TransportClosed, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 8: Security policy blocks connection
// ---------------------------------------------------------------------------

#[test]
fn test_security_policy_blocks() {
    let path = test_server_path();
    let policy = SecurityPolicy::sandbox(); // sandbox blocks subprocess execution

    let result = McpClient::connect(&path, &[], Some(&policy));
    assert!(result.is_err(), "Sandbox policy should block connection");

    match result.unwrap_err() {
        McpError::PermissionDenied(msg) => {
            assert!(
                msg.contains("not allowed"),
                "Error should mention command not allowed, got: {}",
                msg
            );
        }
        other => {
            panic!("Expected PermissionDenied, got: {:?}", other);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 9: Multiple tool calls on same connection
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_calls_same_connection() {
    let client = connect_test_server();

    // Call echo multiple times
    for i in 0..3 {
        let msg = format!("message {}", i);
        let result = client
            .call_tool("echo", serde_json::json!({"message": msg}))
            .expect("repeated echo call should succeed");

        let text = result.content[0]
            .raw
            .as_text()
            .expect("Content should be text");
        assert!(
            text.text.contains(&msg),
            "Echo {} should return the input, got: {}",
            i,
            text.text
        );
    }

    // Call add with different values
    let result = client
        .call_tool("add", serde_json::json!({"a": 100, "b": 200}))
        .expect("add call should succeed");
    let text = result.content[0]
        .raw
        .as_text()
        .expect("Content should be text");
    assert!(
        text.text.contains("300"),
        "Add should return 300, got: {}",
        text.text
    );

    // Connection should still be alive
    assert!(client.is_connected(), "Should still be connected after multiple calls");
}

// ---------------------------------------------------------------------------
// Test 10: connect_with_runtime
// ---------------------------------------------------------------------------

#[test]
fn test_connect_with_runtime() {
    let path = test_server_path();
    let runtime = std::sync::Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build runtime"),
    );

    let client = McpClient::connect_with_runtime(&path, &[], None, runtime)
        .expect("connect_with_runtime should succeed");

    assert!(client.is_connected(), "Should be connected");

    let info = client.server_info().expect("Should have server info");
    assert_eq!(info.server_info.name, "test-server");

    let tools = client.list_tools().expect("list_tools should succeed");
    assert_eq!(tools.len(), 2);
}

// ---------------------------------------------------------------------------
// Test 11: Operations on disconnected client return TransportClosed
// ---------------------------------------------------------------------------

#[test]
fn test_call_on_disconnected_client() {
    let mut client = connect_test_server();
    client.disconnect().unwrap();

    // All operations should fail with TransportClosed
    assert!(
        matches!(client.list_tools(), Err(McpError::TransportClosed)),
        "list_tools on disconnected should be TransportClosed"
    );
    assert!(
        matches!(
            client.call_tool("echo", serde_json::json!({})),
            Err(McpError::TransportClosed)
        ),
        "call_tool on disconnected should be TransportClosed"
    );
    assert!(
        matches!(client.ping(), Err(McpError::TransportClosed)),
        "ping on disconnected should be TransportClosed"
    );
    assert!(
        matches!(client.list_resources(), Err(McpError::TransportClosed)),
        "list_resources on disconnected should be TransportClosed"
    );
    assert!(
        matches!(client.list_prompts(), Err(McpError::TransportClosed)),
        "list_prompts on disconnected should be TransportClosed"
    );
    assert!(
        matches!(
            client.read_resource("tl://readme"),
            Err(McpError::TransportClosed)
        ),
        "read_resource on disconnected should be TransportClosed"
    );
    assert!(
        matches!(
            client.get_prompt("greeting", None),
            Err(McpError::TransportClosed)
        ),
        "get_prompt on disconnected should be TransportClosed"
    );
}

// ---------------------------------------------------------------------------
// Test 12: Multiple operations after disconnect (no panics)
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_operations_after_disconnect() {
    let mut client = connect_test_server();
    client.disconnect().unwrap();

    // Multiple calls should all fail cleanly, no panics
    for _ in 0..5 {
        assert!(client.list_tools().is_err());
        assert!(client.call_tool("echo", serde_json::json!({})).is_err());
        assert!(client.ping().is_err());
    }
}

// ---------------------------------------------------------------------------
// Test 13: HTTP client-server round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_http_client_server_roundtrip() {
    use std::sync::Arc;
    use tl_mcp::server::{TlServerHandler, ToolDef};

    // Build a server handler with tools
    let handler = TlServerHandler::builder()
        .name("http-test-server")
        .version("0.1.0")
        .tool(ToolDef {
            name: "greet".to_string(),
            description: "Returns a greeting".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"}
                }
            }),
            handler: Arc::new(|args| {
                let name = args
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("World");
                Ok(serde_json::json!(format!("Hello, {name}!")))
            }),
        })
        .build();

    // Find an available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Start HTTP server in background thread
    let server_handle = std::thread::spawn(move || {
        tl_mcp::server::serve_http(handler, port)
    });

    // Give the server a moment to start
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Connect as HTTP client
    let url = format!("http://127.0.0.1:{port}/mcp");
    let client = McpClient::connect_http(&url)
        .expect("HTTP client should connect");

    // Verify connection
    assert!(client.is_connected(), "HTTP client should be connected");

    let info = client.server_info().expect("Should have server info");
    assert_eq!(info.server_info.name, "http-test-server");

    // List tools
    let tools = client.list_tools().expect("list_tools should succeed");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name.as_ref(), "greet");

    // Call tool
    let result = client
        .call_tool("greet", serde_json::json!({"name": "TL"}))
        .expect("call_tool should succeed");
    assert_ne!(result.is_error, Some(true));
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("Hello, TL!"),
        "Greet should return Hello, TL!, got: {}",
        text.text
    );

    // Cleanup: disconnect (server thread will not stop on its own since
    // serve_http blocks forever, but the thread will be detached when
    // the test exits)
    drop(client);
    drop(server_handle);
}

// ---------------------------------------------------------------------------
// Test 14: Tool call with invalid (non-object) arguments
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_invalid_arguments() {
    let client = connect_test_server();

    // Array argument should be rejected
    let result = client.call_tool("echo", serde_json::json!(["not", "an", "object"]));
    assert!(result.is_err(), "Array arguments should be rejected");
    match result.unwrap_err() {
        McpError::ProtocolError(msg) => {
            assert!(
                msg.contains("JSON object"),
                "Error should mention JSON object, got: {}",
                msg
            );
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // String argument should be rejected
    let result = client.call_tool("echo", serde_json::json!("just a string"));
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        McpError::ProtocolError(_)
    ));

    // Number argument should be rejected
    let result = client.call_tool("echo", serde_json::json!(42));
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        McpError::ProtocolError(_)
    ));
}

// ---------------------------------------------------------------------------
// Test 15: Tool call with null arguments (should be accepted)
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_null_arguments() {
    let client = connect_test_server();

    // Null arguments should be accepted (echo defaults to "no message")
    let result = client
        .call_tool("echo", serde_json::Value::Null)
        .expect("null arguments should be accepted");
    assert_ne!(result.is_error, Some(true));
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("no message"),
        "Echo with null args should default, got: {}",
        text.text
    );
}

// ---------------------------------------------------------------------------
// Test 16: Tool call with empty object arguments
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_empty_object() {
    let client = connect_test_server();

    // Empty object should be accepted (echo defaults to "no message")
    let result = client
        .call_tool("echo", serde_json::json!({}))
        .expect("empty object arguments should be accepted");
    assert_ne!(result.is_error, Some(true));
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("no message"),
        "Echo with empty args should default, got: {}",
        text.text
    );
}

// ---------------------------------------------------------------------------
// Test 17: Tool with Unicode arguments
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_unicode_arguments() {
    let client = connect_test_server();

    // Unicode string
    let result = client
        .call_tool("echo", serde_json::json!({"message": "Hello \u{1F680}\u{2728} World"}))
        .expect("unicode arguments should work");
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("\u{1F680}"),
        "Should preserve unicode, got: {}",
        text.text
    );

    // Empty string argument
    let result = client
        .call_tool("echo", serde_json::json!({"message": ""}))
        .expect("empty string argument should work");
    assert_ne!(result.is_error, Some(true));
}

// ---------------------------------------------------------------------------
// Test 18: Add tool with edge case numbers
// ---------------------------------------------------------------------------

#[test]
fn test_call_tool_add_edge_numbers() {
    let client = connect_test_server();

    // Zero + zero
    let result = client
        .call_tool("add", serde_json::json!({"a": 0, "b": 0}))
        .expect("add 0+0 should work");
    let text = result.content[0].raw.as_text().unwrap();
    assert!(text.text.contains("0"), "0+0 should give 0, got: {}", text.text);

    // Negative numbers
    let result = client
        .call_tool("add", serde_json::json!({"a": -10, "b": -20}))
        .expect("add negatives should work");
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("-30"),
        "(-10)+(-20) should give -30, got: {}",
        text.text
    );

    // Large numbers
    let result = client
        .call_tool("add", serde_json::json!({"a": 999999999, "b": 1}))
        .expect("add large numbers should work");
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("1000000000"),
        "Should give 1000000000, got: {}",
        text.text
    );

    // Float precision
    let result = client
        .call_tool("add", serde_json::json!({"a": 0.1, "b": 0.2}))
        .expect("add floats should work");
    let text = result.content[0].raw.as_text().unwrap();
    assert!(
        text.text.contains("0.3"),
        "0.1+0.2 should contain 0.3, got: {}",
        text.text
    );
}

// ---------------------------------------------------------------------------
// Test 19: Resources listing and reading
// ---------------------------------------------------------------------------

#[test]
fn test_list_and_read_resources() {
    let client = connect_test_server();

    let resources = client.list_resources().expect("list_resources should succeed");
    assert!(!resources.is_empty(), "Test server should have resources");

    // Find the readme resource
    let readme = resources
        .iter()
        .find(|r| r.name.as_str() == "readme")
        .expect("Should have 'readme' resource");
    assert_eq!(readme.uri.as_str(), "tl://readme");

    // Read the resource
    let result = client.read_resource("tl://readme").expect("read_resource should succeed");
    assert!(
        !result.contents.is_empty(),
        "Resource should have contents"
    );
}

// ---------------------------------------------------------------------------
// Test 20: Prompts listing and retrieval
// ---------------------------------------------------------------------------

#[test]
fn test_list_and_get_prompts() {
    let client = connect_test_server();

    let prompts = client.list_prompts().expect("list_prompts should succeed");
    assert!(!prompts.is_empty(), "Test server should have prompts");

    // Find the greeting prompt
    let greeting = prompts
        .iter()
        .find(|p| p.name.as_str() == "greeting")
        .expect("Should have 'greeting' prompt");
    assert!(
        greeting.description.is_some(),
        "Prompt should have description"
    );

    // Get prompt with arguments
    let mut args = serde_json::Map::new();
    args.insert("name".to_string(), serde_json::json!("TL"));
    let result = client
        .get_prompt("greeting", Some(args))
        .expect("get_prompt should succeed");
    assert!(
        !result.messages.is_empty(),
        "Prompt result should have messages"
    );
}

// ---------------------------------------------------------------------------
// Test 21: Read nonexistent resource
// ---------------------------------------------------------------------------

#[test]
fn test_read_nonexistent_resource() {
    let client = connect_test_server();

    let result = client.read_resource("tl://nonexistent");
    assert!(result.is_err(), "Reading nonexistent resource should fail");
}

// ---------------------------------------------------------------------------
// Test 22: Get nonexistent prompt
// ---------------------------------------------------------------------------

#[test]
fn test_get_nonexistent_prompt() {
    let client = connect_test_server();

    let result = client.get_prompt("nonexistent", None);
    assert!(result.is_err(), "Getting nonexistent prompt should fail");
}

// ---------------------------------------------------------------------------
// Test 23: Double disconnect is safe
// ---------------------------------------------------------------------------

#[test]
fn test_double_disconnect() {
    let mut client = connect_test_server();

    client.disconnect().expect("first disconnect should succeed");
    assert!(!client.is_connected());

    // Second disconnect should also succeed (no-op)
    client
        .disconnect()
        .expect("second disconnect should succeed (no-op)");
    assert!(!client.is_connected());
}

// ---------------------------------------------------------------------------
// Test 24: Sampling callback construction
// ---------------------------------------------------------------------------

#[test]
fn test_sampling_callback_unit() {
    use tl_mcp::client::{SamplingCallback, SamplingResponse};

    let cb: SamplingCallback = Arc::new(|req| {
        let last_msg = req
            .messages
            .last()
            .map(|(_, c)| c.clone())
            .unwrap_or_default();
        Ok(SamplingResponse {
            model: "test-model".to_string(),
            content: format!("Echo: {}", last_msg),
            stop_reason: Some("endTurn".to_string()),
        })
    });

    // Invoke the callback directly
    let req = tl_mcp::client::SamplingRequest {
        messages: vec![("user".to_string(), "hello".to_string())],
        system_prompt: None,
        max_tokens: 100,
        temperature: None,
        model_hint: None,
        stop_sequences: None,
    };
    let resp = cb(req).unwrap();
    assert_eq!(resp.model, "test-model");
    assert_eq!(resp.content, "Echo: hello");
    assert_eq!(resp.stop_reason, Some("endTurn".to_string()));
}

// ---------------------------------------------------------------------------
// Test 25: Sampling callback error path
// ---------------------------------------------------------------------------

#[test]
fn test_sampling_callback_error() {
    use tl_mcp::client::{SamplingCallback, SamplingRequest};

    let cb: SamplingCallback = Arc::new(|_req| {
        Err("LLM provider unavailable".to_string())
    });

    let req = SamplingRequest {
        messages: vec![("user".to_string(), "test".to_string())],
        system_prompt: None,
        max_tokens: 100,
        temperature: None,
        model_hint: None,
        stop_sequences: None,
    };
    let result = cb(req);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "LLM provider unavailable");
}

// ---------------------------------------------------------------------------
// Test 26: Server builder with all capability types (roundtrip)
// ---------------------------------------------------------------------------

#[test]
fn test_server_builder_all_capabilities() {
    use tl_mcp::server::{
        PromptArgDef, PromptDef, PromptMessageDef, ResourceDef, TlServerHandler, ToolDef,
    };

    let handler = TlServerHandler::builder()
        .name("full-server")
        .version("2.0.0")
        .tool(ToolDef {
            name: "t1".to_string(),
            description: "Tool 1".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
            handler: Arc::new(|_| Ok(serde_json::json!("ok"))),
        })
        .tool(ToolDef {
            name: "t2".to_string(),
            description: "Tool 2".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
            handler: Arc::new(|_| Ok(serde_json::json!("ok"))),
        })
        .resource(ResourceDef {
            name: "r1".to_string(),
            uri: "tl://r1".to_string(),
            description: Some("Resource 1".to_string()),
            mime_type: Some("text/plain".to_string()),
            content: "resource content".to_string(),
        })
        .prompt(PromptDef {
            name: "p1".to_string(),
            description: Some("Prompt 1".to_string()),
            arguments: vec![PromptArgDef {
                name: "arg1".to_string(),
                description: Some("First arg".to_string()),
                required: true,
            }],
            handler: Arc::new(|_args| {
                Ok(vec![PromptMessageDef {
                    role: "user".to_string(),
                    content: "test".to_string(),
                }])
            }),
        })
        .build();

    // Verify via HTTP roundtrip that all capabilities work
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let _server = std::thread::spawn(move || {
        tl_mcp::server::serve_http(handler, port)
    });
    std::thread::sleep(std::time::Duration::from_millis(500));

    let client = McpClient::connect_http(&format!("http://127.0.0.1:{port}/mcp"))
        .expect("connect should succeed");

    let info = client.server_info().expect("info");
    assert_eq!(info.server_info.name, "full-server");
    assert_eq!(info.server_info.version, "2.0.0");

    let tools = client.list_tools().expect("list_tools");
    assert_eq!(tools.len(), 2);

    let resources = client.list_resources().expect("list_resources");
    assert_eq!(resources.len(), 1);

    let prompts = client.list_prompts().expect("list_prompts");
    assert_eq!(prompts.len(), 1);
}

// ---------------------------------------------------------------------------
// Test 27: McpError Display coverage
// ---------------------------------------------------------------------------

#[test]
fn test_mcp_error_variants() {
    // Ensure all error variants have proper Display
    let errors: Vec<McpError> = vec![
        McpError::PermissionDenied("blocked".into()),
        McpError::ConnectionFailed("refused".into()),
        McpError::ProtocolError("bad message".into()),
        McpError::ToolError("crash".into()),
        McpError::TransportClosed,
        McpError::Timeout,
        McpError::RuntimeError("exhausted".into()),
    ];

    let displays = vec![
        "Permission denied: blocked",
        "Connection failed: refused",
        "Protocol error: bad message",
        "Tool error: crash",
        "Transport closed",
        "Timeout",
        "Runtime error: exhausted",
    ];

    for (err, expected) in errors.iter().zip(displays.iter()) {
        assert_eq!(err.to_string(), *expected);
    }
}

// ---------------------------------------------------------------------------
// Test 28: Security policy edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_security_policy_edge_cases() {
    // Sandbox with no allowed commands
    let policy = SecurityPolicy::sandbox();
    let result = McpClient::connect("anything", &[], Some(&policy));
    assert!(matches!(
        result.unwrap_err(),
        McpError::PermissionDenied(_)
    ));

    // Permissive policy should not block (will fail at connection, not security)
    let policy = SecurityPolicy::permissive();
    let result = McpClient::connect("__nonexistent__", &[], Some(&policy));
    assert!(matches!(
        result.unwrap_err(),
        McpError::ConnectionFailed(_)
    ));
}

// ---------------------------------------------------------------------------
// Test 29: Server builder with empty capabilities (roundtrip)
// ---------------------------------------------------------------------------

#[test]
fn test_server_builder_no_capabilities() {
    use tl_mcp::server::TlServerHandler;

    let handler = TlServerHandler::builder()
        .name("empty-server")
        .version("0.0.1")
        .build();

    // Verify via HTTP roundtrip that empty capabilities work
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let _server = std::thread::spawn(move || {
        tl_mcp::server::serve_http(handler, port)
    });
    std::thread::sleep(std::time::Duration::from_millis(500));

    let client = McpClient::connect_http(&format!("http://127.0.0.1:{port}/mcp"))
        .expect("connect should succeed");

    let info = client.server_info().expect("info");
    assert_eq!(info.server_info.name, "empty-server");

    // Empty server should return empty lists (or error since capabilities not declared)
    // Tools capability may not be declared, so list_tools could fail
    let tools_result = client.list_tools();
    // Either returns empty list or errors (both acceptable for server with no tools declared)
    match tools_result {
        Ok(tools) => assert!(tools.is_empty()),
        Err(_) => {} // Expected: tools capability not declared
    }
}
