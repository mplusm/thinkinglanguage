// ThinkingLanguage — WASM Browser Execution
// Licensed under MIT OR Apache-2.0
//
// Phase 31: Compiles TL source to bytecode and runs it in the VM,
// exposed via wasm-bindgen for browser-based playgrounds.

use wasm_bindgen::prelude::*;

/// Execute a TL program and return its output (or an error message).
#[wasm_bindgen]
pub fn execute(source: &str) -> Result<String, String> {
    let program = tl_parser::parse(source)
        .map_err(|e| format!("Parse error: {e}"))?;
    let proto = tl_compiler::compile_with_source(&program, source)
        .map_err(|e| format!("Compile error: {e}"))?;
    let mut vm = tl_compiler::Vm::new();
    match vm.execute(&proto) {
        Ok(result) => {
            let mut output = vm.output.join("\n");
            if output.is_empty() {
                let s = format!("{result}");
                if s != "none" {
                    output = s;
                }
            }
            Ok(output)
        }
        Err(e) => {
            let output = vm.output.join("\n");
            if output.is_empty() {
                Err(format!("{e}"))
            } else {
                Err(format!("{output}\n\nError: {e}"))
            }
        }
    }
}

/// Type-check a TL program and return diagnostics.
#[wasm_bindgen]
pub fn check(source: &str) -> String {
    match tl_parser::parse(source) {
        Ok(program) => {
            let config = tl_types::checker::CheckerConfig::default();
            let result = tl_types::checker::check_program(&program, &config);
            if result.errors.is_empty() && result.warnings.is_empty() {
                "OK".into()
            } else {
                let mut lines: Vec<String> = Vec::new();
                for e in &result.errors {
                    lines.push(format!("error: {e}"));
                }
                for w in &result.warnings {
                    lines.push(format!("warning: {w}"));
                }
                lines.join("\n")
            }
        }
        Err(e) => format!("Parse error: {e}"),
    }
}
