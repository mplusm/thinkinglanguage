// MCP Builtins Integration Test
// Requires: tl-test-mcp-server binary built

let server_path = "./target/debug/tl-test-mcp-server"

// Test 1: Connect to test server
print("Test 1: mcp_connect")
let client = mcp_connect(server_path)
assert_eq(type_of(client), "mcp_client")
print("  PASS: connected")

// Test 2: Server info
print("Test 2: mcp_server_info")
let info = mcp_server_info(client)
assert_eq(info["name"], "test-server")
assert_eq(info["version"], "1.0.0")
print("  PASS: server info correct")

// Test 3: Ping
print("Test 3: mcp_ping")
let ping_result = mcp_ping(client)
assert_eq(ping_result, true)
print("  PASS: ping succeeded")

// Test 4: List tools
print("Test 4: mcp_list_tools")
let tools = mcp_list_tools(client)
assert_eq(len(tools), 2)
print("  PASS: " + str(len(tools)) + " tools found")

// Test 5: Call echo tool
print("Test 5: mcp_call_tool echo")
let echo_args = map_from("message", "hello from TL")
let echo_result = mcp_call_tool(client, "echo", echo_args)
assert_eq(echo_result["is_error"], false)
print("  PASS: echo returned: " + str(echo_result["content"]))

// Test 6: Call add tool
print("Test 6: mcp_call_tool add")
let add_args = map_from("a", 10, "b", 32)
let add_result = mcp_call_tool(client, "add", add_args)
assert_eq(add_result["is_error"], false)
print("  PASS: add returned: " + str(add_result["content"]))

// Test 7: Disconnect
print("Test 7: mcp_disconnect")
mcp_disconnect(client)
print("  PASS: disconnected")

print("")
print("All MCP builtin tests passed!")
