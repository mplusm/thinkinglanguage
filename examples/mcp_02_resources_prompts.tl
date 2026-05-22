// MCP Resources & Prompts — Browse server capabilities
//
// MCP servers can expose resources (data you can read) and
// prompts (reusable templates) in addition to tools.
//
// Build: cargo build --features mcp
// Run:   tl run examples/mcp_02_resources_prompts.tl

let server_path = "./target/debug/tl-test-mcp-server"
let client = mcp_connect(server_path)
print("Connected to " + mcp_server_info(client)["name"])

// --- Resources ---
print("\n=== Resources ===")
let resources = mcp_list_resources(client)
for r in resources {
    let desc = r["name"]
    print(r["uri"] + " (" + desc + ")")

    // Read the resource content
    let content = mcp_read_resource(client, r["uri"])
    print("  Content: " + content["text"])
}

// --- Prompts ---
print("\n=== Prompts ===")
let prompts = mcp_list_prompts(client)
for p in prompts {
    print(p["name"])

    // Show arguments
    if p["arguments"] != none {
        for arg in p["arguments"] {
            let label = "  param: " + arg["name"]
            if arg["required"] {
                label = label + " [required]"
            }
            print(label)
        }
    }
}

// --- Get a prompt with arguments ---
print("\n=== Get Prompt ===")
let result = mcp_get_prompt(client, "greeting", map_from("name", "World"))
for msg in result["messages"] {
    print(msg["role"] + ": " + msg["content"])
}

mcp_disconnect(client)
