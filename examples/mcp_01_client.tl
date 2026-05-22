// MCP Client — Connect to an MCP server and use its tools
//
// This example connects to the built-in test server.
// To use a real MCP server, replace the path with your server command.
//
// Build: cargo build --features mcp
// Run:   tl run examples/mcp_01_client.tl

let server_path = "./target/debug/tl-test-mcp-server"

// --- Connect ---
print("Connecting to MCP server...")
let client = mcp_connect(server_path)

let info = mcp_server_info(client)
print("Connected to: " + info["name"] + " v" + info["version"])

// --- Discover tools ---
let tools = mcp_list_tools(client)
print("\nAvailable tools (" + str(len(tools)) + "):")
for tool in tools {
    print("  - " + tool["name"] + ": " + tool["description"])
}

// --- Call tools ---
print("\nCalling echo tool...")
let echo_result = mcp_call_tool(client, "echo", map_from("message", "Hello from ThinkingLanguage!"))
print("  Echo says: " + str(echo_result["content"]))

print("\nCalling add tool...")
let add_result = mcp_call_tool(client, "add", map_from("a", 17, "b", 25))
print("  17 + 25 = " + str(add_result["content"]))

// --- Health check ---
if mcp_ping(client) {
    print("\nServer is alive")
}

// --- Clean up ---
mcp_disconnect(client)
print("Disconnected.")
