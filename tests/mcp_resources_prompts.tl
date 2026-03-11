// MCP Resources & Prompts Integration Test
// Requires: tl-test-mcp-server binary built with resources/prompts

let server_path = "./target/debug/tl-test-mcp-server"
let client = mcp_connect(server_path)

// Test 1: List resources
print("Test 1: mcp_list_resources")
let resources = mcp_list_resources(client)
assert_eq(len(resources), 1)
assert_eq(resources[0]["name"], "readme")
assert_eq(resources[0]["uri"], "tl://readme")
print("  PASS: " + str(len(resources)) + " resource(s) found")

// Test 2: Read resource
print("Test 2: mcp_read_resource")
let content = mcp_read_resource(client, "tl://readme")
assert_eq(content["text"], "Hello from ThinkingLanguage MCP!")
print("  PASS: content = " + content["text"])

// Test 3: List prompts
print("Test 3: mcp_list_prompts")
let prompts = mcp_list_prompts(client)
assert_eq(len(prompts), 1)
assert_eq(prompts[0]["name"], "greeting")
print("  PASS: " + str(len(prompts)) + " prompt(s) found")

// Test 4: Get prompt
print("Test 4: mcp_get_prompt")
let prompt_args = map_from("name", "TL")
let result = mcp_get_prompt(client, "greeting", prompt_args)
assert_eq(len(result["messages"]), 1)
assert_eq(result["messages"][0]["role"], "user")
print("  PASS: " + result["messages"][0]["content"])

// Test 5: Original tools still work
print("Test 5: tools still work")
let tools = mcp_list_tools(client)
assert_eq(len(tools), 2)
print("  PASS: " + str(len(tools)) + " tools available")

mcp_disconnect(client)
print("")
print("All MCP resources & prompts tests passed!")
