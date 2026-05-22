// MCP Error Handling — Graceful failure patterns
//
// Demonstrates try/catch with MCP operations, handling
// connection failures, tool errors, and disconnected clients.
//
// Build: cargo build --features mcp
// Run:   tl run examples/mcp_04_error_handling.tl

let server_path = "./target/debug/tl-test-mcp-server"

// --- 1. Handle connection failure ---
print("1. Connection to nonexistent server:")
try {
    let _bad = mcp_connect("./nonexistent-mcp-server")
} catch e {
    print("  Caught: " + str(e))
}

// --- 2. Handle tool errors ---
print("\n2. Calling a nonexistent tool:")
let client = mcp_connect(server_path)
try {
    let _result = mcp_call_tool(client, "this_tool_does_not_exist", map_from())
} catch e {
    print("  Caught: " + str(e))
}

// --- 3. Double disconnect is safe ---
print("\n3. Double disconnect:")
mcp_disconnect(client)
mcp_disconnect(client)
print("  No error (safe no-op)")

// --- 4. Reconnect pattern ---
print("\n4. Reconnect after disconnect:")
let client2 = mcp_connect(server_path)
let reconnected_tools = mcp_list_tools(client2)
print("  Reconnected! Tools: " + str(len(reconnected_tools)))
mcp_disconnect(client2)

print("\nAll error handling examples complete.")
