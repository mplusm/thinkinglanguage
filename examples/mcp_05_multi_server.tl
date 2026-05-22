// MCP Multi-Server — Connect to multiple servers simultaneously
//
// Demonstrates connecting to the same server binary twice
// (simulating separate MCP servers) and using both.
//
// Build: cargo build --features mcp
// Run:   tl run examples/mcp_05_multi_server.tl

let server_path = "./target/debug/tl-test-mcp-server"

// Connect to two independent server instances
print("Connecting to server A...")
let server_a = mcp_connect(server_path)
print("Connecting to server B...")
let server_b = mcp_connect(server_path)

let info_a = mcp_server_info(server_a)
let info_b = mcp_server_info(server_b)
print("Server A: " + info_a["name"] + " v" + info_a["version"])
print("Server B: " + info_b["name"] + " v" + info_b["version"])

// Use tools from both servers
print("\nServer A — echo:")
let a_result = mcp_call_tool(server_a, "echo", map_from("message", "from A"))
print("  " + str(a_result["content"]))

print("Server B — add:")
let b_result = mcp_call_tool(server_b, "add", map_from("a", 100, "b", 200))
print("  " + str(b_result["content"]))

// Disconnect server A, server B still works
mcp_disconnect(server_a)
print("\nServer A disconnected.")

print("Server B still alive: " + str(mcp_ping(server_b)))
let b_echo = mcp_call_tool(server_b, "echo", map_from("message", "B is still working"))
print("Server B — echo: " + str(b_echo["content"]))

mcp_disconnect(server_b)
print("\nBoth servers disconnected.")
