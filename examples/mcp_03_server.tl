// MCP Server — Expose TL functions to external AI tools
//
// This starts TL as an MCP server over stdio. External clients
// like Claude Desktop, Cursor, or MCP Inspector can connect.
//
// Build: cargo build --features mcp
// Run:   tl run examples/mcp_03_server.tl
//
// To use with Claude Desktop, add to claude_desktop_config.json:
//   {
//     "mcpServers": {
//       "tl-tools": {
//         "command": "tl",
//         "args": ["run", "examples/mcp_03_server.tl"]
//       }
//     }
//   }

fn greet(name) {
    "Hello, " + name + "! Welcome to ThinkingLanguage."
}

fn add_numbers(a, b) {
    str(a + b)
}

fn reverse_text(text) {
    let chars = split(text, "")
    let reversed = []
    let i = len(chars) - 1
    while i >= 0 {
        reversed = reversed + [chars[i]]
        i = i - 1
    }
    join(reversed, "")
}

fn word_count(text) {
    let words = split(text, " ")
    str(len(words))
}

// Start the MCP server — this blocks until the client disconnects
mcp_serve([
    {
        name: "greet",
        description: "Greet someone by name",
        handler: greet,
        input_schema: {
            type: "object",
            properties: {
                name: { type: "string", description: "The person's name" }
            },
            required: ["name"]
        }
    },
    {
        name: "add_numbers",
        description: "Add two numbers together",
        handler: add_numbers,
        input_schema: {
            type: "object",
            properties: {
                a: { type: "number", description: "First number" },
                b: { type: "number", description: "Second number" }
            },
            required: ["a", "b"]
        }
    },
    {
        name: "reverse_text",
        description: "Reverse the characters in a string",
        handler: reverse_text,
        input_schema: {
            type: "object",
            properties: {
                text: { type: "string", description: "Text to reverse" }
            },
            required: ["text"]
        }
    },
    {
        name: "word_count",
        description: "Count the number of words in text",
        handler: word_count,
        input_schema: {
            type: "object",
            properties: {
                text: { type: "string", description: "Text to count words in" }
            },
            required: ["text"]
        }
    }
])
