// ThinkingLanguage — Language Server Protocol Implementation
// Licensed under MIT OR Apache-2.0

pub mod ast_util;
pub mod completion;
pub mod diagnostics;
pub mod doc;
pub mod document;
pub mod format;
pub mod goto_def;
pub mod hover;
pub mod server;
pub mod symbols;

/// Run the LSP server over stdio.
pub fn run_server() -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    server::run_server()
}
