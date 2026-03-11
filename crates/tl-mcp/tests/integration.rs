//! Integration tests for MCP client-server communication.
//!
//! These tests spawn the `tl-test-mcp-server` binary as a subprocess,
//! connect via [`McpClient`], and verify the full MCP lifecycle:
//! handshake, tool discovery, tool invocation, error handling, and cleanup.

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
