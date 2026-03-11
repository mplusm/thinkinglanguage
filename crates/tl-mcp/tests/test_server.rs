//! Test MCP server binary for integration testing.
//!
//! Exposes two tools:
//! - "echo" -- returns the input message unchanged
//! - "add"  -- adds two numbers and returns the sum
//!
//! Runs over stdio using `serve_stdio()`.

use std::sync::Arc;

use serde_json::json;
use tl_mcp::server::{
    PromptArgDef, PromptDef, PromptMessageDef, ResourceDef, TlServerHandler, ToolDef, serve_stdio,
};

fn main() {
    let handler = TlServerHandler::builder()
        .name("test-server")
        .version("1.0.0")
        .resource(ResourceDef {
            name: "readme".to_string(),
            uri: "tl://readme".to_string(),
            description: Some("Project readme".to_string()),
            mime_type: Some("text/plain".to_string()),
            content: "Hello from ThinkingLanguage MCP!".to_string(),
        })
        .prompt(PromptDef {
            name: "greeting".to_string(),
            description: Some("Generate a greeting".to_string()),
            arguments: vec![PromptArgDef {
                name: "name".to_string(),
                description: Some("Person to greet".to_string()),
                required: true,
            }],
            handler: Arc::new(|args| {
                let name = args.get("name").and_then(|v| v.as_str()).unwrap_or("World");
                Ok(vec![PromptMessageDef {
                    role: "user".to_string(),
                    content: format!("Please greet {name} warmly"),
                }])
            }),
        })
        .tool(ToolDef {
            name: "echo".to_string(),
            description: "Returns the input message unchanged".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" }
                },
                "required": ["message"]
            }),
            handler: Arc::new(|args| {
                let msg = args
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("no message");
                Ok(json!({ "echoed": msg }))
            }),
        })
        .tool(ToolDef {
            name: "add".to_string(),
            description: "Adds two numbers".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "a": { "type": "number" },
                    "b": { "type": "number" }
                },
                "required": ["a", "b"]
            }),
            handler: Arc::new(|args| {
                let a = args.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let b = args.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                Ok(json!({ "result": a + b }))
            }),
        })
        .build();

    serve_stdio(handler).expect("Test MCP server failed");
}
