// ThinkingLanguage -- CLI Entry Point
// Licensed under MIT OR Apache-2.0
//
// Commands:
//   tl run <file.tl>   -- Execute a .tl source file
//   tl shell            -- Start the interactive REPL

use clap::{Parser, Subcommand};
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Config, Editor, Helper};
use std::borrow::Cow;
use std::fs;
use std::process;

use tl_errors::{report_parser_error, report_runtime_error, report_type_error, report_type_warning, TlError};
use tl_interpreter::Interpreter;
use tl_compiler::{compile, compile_with_source, Vm, VmValue};
use tl_parser::parse;
use tl_types::checker::{CheckerConfig, check_program};

mod deploy;

// ---------------------------------------------------------------------------
// Tab-completion helper
// ---------------------------------------------------------------------------

struct TlHelper {
    completions: Vec<String>,
}

impl TlHelper {
    fn new() -> Self {
        let mut completions: Vec<String> = [
            // Keywords
            "let", "fn", "if", "else", "while", "for", "in", "return", "true", "false", "none",
            "struct", "enum", "impl", "try", "catch", "throw", "import", "test", "break", "continue",
            "and", "or", "not", "mut", "await", "yield", "match", "schema", "pipeline", "stream",
            "source", "sink",
            // Builtin functions
            "print", "println", "len", "str", "int", "float", "abs", "min", "max", "range",
            "push", "type_of", "map", "filter", "reduce", "sum", "any", "all",
            "read_csv", "read_parquet", "write_csv", "write_parquet", "collect", "show",
            "describe", "head", "sqrt", "pow", "floor", "ceil", "round", "sin", "cos", "tan",
            "log", "log2", "log10", "join", "assert", "assert_eq",
            "json_parse", "json_stringify", "map_from", "read_file", "write_file", "append_file",
            "file_exists", "list_dir", "env_get", "env_set", "regex_match", "regex_find",
            "regex_replace", "now", "date_format", "date_parse", "zip", "enumerate", "bool",
            "spawn", "sleep", "channel", "send", "recv", "try_recv", "await_all", "pmap", "timeout",
            "next", "is_generator", "iter", "take", "skip", "gen_collect", "gen_map", "gen_filter",
            "chain", "gen_zip", "gen_enumerate",
            "Ok", "Err", "is_ok", "is_err", "unwrap",
            "set_from", "set_add", "set_remove", "set_contains", "set_union", "set_intersection", "set_difference",
        ].iter().map(|s| String::from(*s)).collect();
        completions.sort();
        TlHelper { completions }
    }
}

impl Completer for TlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..pos]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| i + 1)
            .unwrap_or(0);
        let prefix = &line[start..pos];

        if prefix.is_empty() {
            return Ok((pos, vec![]));
        }

        let matches: Vec<Pair> = self
            .completions
            .iter()
            .filter(|c| c.starts_with(prefix))
            .map(|c| Pair {
                display: c.clone(),
                replacement: c[prefix.len()..].to_string(),
            })
            .collect();

        Ok((start + prefix.len(), matches))
    }
}

impl Hinter for TlHelper {
    type Hint = String;
}

impl Highlighter for TlHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }
}

impl Validator for TlHelper {}

impl Helper for TlHelper {}

// ---------------------------------------------------------------------------
// Multi-line continuation detection
// ---------------------------------------------------------------------------

/// Check if input has unclosed delimiters (brackets, parens, braces).
/// Returns true if more input is needed.
fn needs_continuation(input: &str) -> bool {
    let mut depth_brace = 0i32;
    let mut depth_paren = 0i32;
    let mut depth_bracket = 0i32;
    let mut in_string = false;
    let mut prev_char = '\0';

    for ch in input.chars() {
        if ch == '"' && prev_char != '\\' {
            in_string = !in_string;
        }
        if !in_string {
            match ch {
                '{' => depth_brace += 1,
                '}' => depth_brace -= 1,
                '(' => depth_paren += 1,
                ')' => depth_paren -= 1,
                '[' => depth_bracket += 1,
                ']' => depth_bracket -= 1,
                _ => {}
            }
        }
        prev_char = ch;
    }

    depth_brace > 0 || depth_paren > 0 || depth_bracket > 0
}

/// Compute the history file path (~/.tl_history).
fn history_path() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(|h| std::path::PathBuf::from(h).join(".tl_history"))
        .unwrap_or_else(|_| std::path::PathBuf::from(".tl_history"))
}

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "tl", version, about = "ThinkingLanguage -- Data Engineering & AI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a .tl source file
    Run {
        /// Path to the .tl file
        file: String,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
        /// Dump compiled bytecode instead of executing
        #[arg(long)]
        dump_bytecode: bool,
        /// Skip type checking
        #[arg(long)]
        no_check: bool,
        /// Strict mode: require type annotations on function parameters
        #[arg(long)]
        strict: bool,
    },
    /// Start the interactive REPL
    Shell {
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
    /// Manage the model registry
    Models {
        #[command(subcommand)]
        action: ModelsAction,
    },
    /// Generate deployment artifacts (Dockerfile, K8s manifests)
    Deploy {
        /// Path to the .tl pipeline file
        file: String,
        /// Target: "docker" or "k8s"
        #[arg(long, default_value = "docker")]
        target: String,
        /// Output directory
        #[arg(long, default_value = "./deploy")]
        output: String,
    },
    /// Show data lineage for a pipeline
    Lineage {
        /// Path to the .tl file
        file: String,
        /// Output format: "dot", "json", or "text"
        #[arg(long, default_value = "text")]
        format: String,
    },
    /// Disassemble a .tl file to show bytecode
    Disasm {
        /// Path to the .tl file
        file: String,
    },
    /// Run tests in a .tl file
    Test {
        /// Path to the .tl file (or directory)
        path: String,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
}

#[derive(Subcommand)]
enum ModelsAction {
    /// List all registered models
    List,
    /// Show model metadata
    Info {
        /// Model name
        name: String,
    },
    /// Delete a registered model
    Delete {
        /// Model name
        name: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Run { file, backend, dump_bytecode, no_check, strict }) => run_file(&file, &backend, dump_bytecode, no_check, strict),
        Some(Commands::Shell { backend }) => run_repl(&backend),
        Some(Commands::Models { action }) => run_models(action),
        Some(Commands::Deploy { file, target, output }) => run_deploy(&file, &target, &output),
        Some(Commands::Lineage { file, format }) => run_lineage(&file, &format),
        Some(Commands::Disasm { file }) => run_disasm(&file),
        Some(Commands::Test { path, backend }) => run_tests(&path, &backend),
        None => run_repl("vm"), // Default to REPL with VM backend
    }
}

fn run_file(path: &str, backend: &str, dump_bytecode: bool, no_check: bool, strict: bool) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{path}': {e}");
            process::exit(1);
        }
    };

    let program = match parse(&source) {
        Ok(p) => p,
        Err(TlError::Parser(ref e)) => {
            report_parser_error(&source, path, e);
            process::exit(1);
        }
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    // Type checking pass (unless --no-check)
    if !no_check {
        let config = CheckerConfig { strict };
        let result = check_program(&program, &config);

        for warning in &result.warnings {
            report_type_warning(&source, path, &tl_errors::TypeError {
                message: warning.message.clone(),
                span: warning.span,
                expected: warning.expected.clone(),
                found: warning.found.clone(),
                hint: warning.hint.clone(),
            });
        }

        if result.has_errors() {
            for error in &result.errors {
                report_type_error(&source, path, &tl_errors::TypeError {
                    message: error.message.clone(),
                    span: error.span,
                    expected: error.expected.clone(),
                    found: error.found.clone(),
                    hint: error.hint.clone(),
                });
            }
            process::exit(1);
        }
    }

    if dump_bytecode {
        let proto = match compile_with_source(&program, &source) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Compile error: {e}");
                process::exit(1);
            }
        };
        print!("{}", proto.disassemble());
        return;
    }

    match backend {
        "vm" => {
            let proto = match compile_with_source(&program, &source) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Compile error: {e}");
                    process::exit(1);
                }
            };
            let mut vm = Vm::new();
            if let Err(e) = vm.execute(&proto) {
                match &e {
                    TlError::Runtime(re) => report_runtime_error(&source, path, re),
                    _ => eprintln!("{e}"),
                }
                process::exit(1);
            }
        }
        "interp" => {
            let mut interp = Interpreter::new();
            if let Err(e) = interp.execute(&program) {
                match &e {
                    TlError::Runtime(re) => report_runtime_error(&source, path, re),
                    _ => eprintln!("{e}"),
                }
                process::exit(1);
            }
        }
        _ => {
            eprintln!("Unknown backend: '{backend}'. Use 'vm' or 'interp'.");
            process::exit(1);
        }
    }
}

fn run_disasm(path: &str) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{path}': {e}");
            process::exit(1);
        }
    };

    let program = match parse(&source) {
        Ok(p) => p,
        Err(TlError::Parser(ref e)) => {
            report_parser_error(&source, path, e);
            process::exit(1);
        }
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let proto = match compile_with_source(&program, &source) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Compile error: {e}");
            process::exit(1);
        }
    };

    print!("{}", proto.disassemble());
}

fn run_repl(backend: &str) {
    println!("ThinkingLanguage v0.1.0 -- REPL (backend: {backend})");
    println!("Type expressions or statements. Press Ctrl+D to exit.\n");

    let config = Config::builder()
        .auto_add_history(false)
        .build();
    let mut editor = match Editor::with_config(config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to initialize REPL: {e}");
            process::exit(1);
        }
    };
    editor.set_helper(Some(TlHelper::new()));

    let hist = history_path();
    let _ = editor.load_history(&hist);

    match backend {
        "vm" => run_repl_vm(&mut editor),
        "interp" => run_repl_interp(&mut editor),
        _ => {
            eprintln!("Unknown backend: '{backend}'. Use 'vm' or 'interp'.");
            process::exit(1);
        }
    }

    let _ = editor.save_history(&hist);
}

fn run_repl_vm(editor: &mut Editor<TlHelper, DefaultHistory>) {
    let mut vm = Vm::new();

    loop {
        let readline = editor.readline("tl> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Multi-line continuation: keep reading while delimiters are unclosed
                let mut input = trimmed.to_string();
                while needs_continuation(&input) {
                    match editor.readline("...> ") {
                        Ok(cont) => {
                            input.push('\n');
                            input.push_str(cont.trim());
                        }
                        Err(_) => break,
                    }
                }
                let _ = editor.add_history_entry(&input);

                match parse(&input) {
                    Ok(program) => {
                        // Type check (warnings only in REPL — don't block execution)
                        let check_result = check_program(&program, &CheckerConfig::default());
                        for warning in &check_result.warnings {
                            report_type_warning(&input, "<repl>", &tl_errors::TypeError {
                                message: warning.message.clone(),
                                span: warning.span,
                                expected: warning.expected.clone(),
                                found: warning.found.clone(),
                                hint: warning.hint.clone(),
                            });
                        }
                        for error in &check_result.errors {
                            report_type_error(&input, "<repl>", &tl_errors::TypeError {
                                message: error.message.clone(),
                                span: error.span,
                                expected: error.expected.clone(),
                                found: error.found.clone(),
                                hint: error.hint.clone(),
                            });
                        }

                        let proto = match compile(&program) {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("Compile error: {e}");
                                continue;
                            }
                        };
                        match vm.execute(&proto) {
                            Ok(val) => {
                                if !matches!(val, VmValue::None) {
                                    println!("{val}");
                                }
                            }
                            Err(TlError::Runtime(ref re)) => {
                                report_runtime_error(&input, "<repl>", re);
                            }
                            Err(e) => eprintln!("{e}"),
                        }
                    }
                    Err(TlError::Parser(ref e)) => {
                        report_parser_error(&input, "<repl>", e);
                    }
                    Err(e) => eprintln!("{e}"),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }
}

fn run_repl_interp(editor: &mut Editor<TlHelper, DefaultHistory>) {
    let mut interp = Interpreter::new();

    loop {
        let readline = editor.readline("tl> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Multi-line continuation: keep reading while delimiters are unclosed
                let mut input = trimmed.to_string();
                while needs_continuation(&input) {
                    match editor.readline("...> ") {
                        Ok(cont) => {
                            input.push('\n');
                            input.push_str(cont.trim());
                        }
                        Err(_) => break,
                    }
                }
                let _ = editor.add_history_entry(&input);

                match parse(&input) {
                    Ok(program) => {
                        // Type check (warnings only in REPL — don't block execution)
                        let check_result = check_program(&program, &CheckerConfig::default());
                        for warning in &check_result.warnings {
                            report_type_warning(&input, "<repl>", &tl_errors::TypeError {
                                message: warning.message.clone(),
                                span: warning.span,
                                expected: warning.expected.clone(),
                                found: warning.found.clone(),
                                hint: warning.hint.clone(),
                            });
                        }
                        for error in &check_result.errors {
                            report_type_error(&input, "<repl>", &tl_errors::TypeError {
                                message: error.message.clone(),
                                span: error.span,
                                expected: error.expected.clone(),
                                found: error.found.clone(),
                                hint: error.hint.clone(),
                            });
                        }

                        for stmt in &program.statements {
                            match interp.execute_stmt(stmt) {
                                Ok(val) => {
                                    // Only print non-None values for expression statements
                                    if let tl_ast::StmtKind::Expr(_) = &stmt.kind {
                                        if !matches!(val, tl_interpreter::Value::None) {
                                            println!("{val}");
                                        }
                                    }
                                }
                                Err(TlError::Runtime(ref re)) => {
                                    report_runtime_error(&input, "<repl>", re);
                                }
                                Err(e) => eprintln!("{e}"),
                            }
                        }
                    }
                    Err(TlError::Parser(ref e)) => {
                        report_parser_error(&input, "<repl>", e);
                    }
                    Err(e) => eprintln!("{e}"),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }
}

fn run_models(action: ModelsAction) {
    let registry = tl_ai::ModelRegistry::default_location();
    match action {
        ModelsAction::List => {
            let names = registry.list();
            if names.is_empty() {
                println!("No models registered.");
                println!("Models are stored in ~/.tl/models/");
            } else {
                println!("Registered models:");
                for name in &names {
                    println!("  {name}");
                }
                println!("\n{} model(s) total", names.len());
            }
        }
        ModelsAction::Info { name } => {
            match registry.get(&name) {
                Ok(model) => {
                    println!("Model: {name}");
                    println!("{model}");
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
        ModelsAction::Delete { name } => {
            match registry.delete(&name) {
                Ok(()) => println!("Deleted model '{name}'"),
                Err(e) => {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
    }
}

fn run_deploy(file: &str, target: &str, output: &str) {
    if !std::path::Path::new(file).exists() {
        eprintln!("File not found: {file}");
        process::exit(1);
    }
    if let Err(e) = deploy::write_deploy(file, target, output) {
        eprintln!("Deploy error: {e}");
        process::exit(1);
    }
}

fn run_lineage(file: &str, format: &str) {
    let source = match fs::read_to_string(file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{file}': {e}");
            process::exit(1);
        }
    };

    let program = match parse(&source) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Parse error: {e}");
            process::exit(1);
        }
    };

    // Build lineage by scanning pipeline statements
    let mut tracker = tl_stream::LineageTracker::new();
    for (i, stmt) in program.statements.iter().enumerate() {
        if let tl_ast::StmtKind::Pipeline { name, .. } = &stmt.kind {
            let extract_id = tracker.record(
                &format!("{name}/extract"),
                "Read source data",
                None,
                vec![],
            );
            let transform_id = tracker.record(
                &format!("{name}/transform"),
                "Transform data",
                None,
                vec![extract_id],
            );
            tracker.record(
                &format!("{name}/load"),
                "Write to sink",
                None,
                vec![transform_id],
            );
        } else {
            tracker.record(
                &format!("stmt_{i}"),
                "Execute statement",
                None,
                vec![],
            );
        }
    }

    let output = match format {
        "dot" => tracker.to_dot(),
        "json" => tracker.to_json(),
        "text" => tracker.to_text(),
        other => {
            eprintln!("Unknown format: '{other}'. Use 'dot', 'json', or 'text'.");
            process::exit(1);
        }
    };
    println!("{output}");
}

fn run_tests(path: &str, backend: &str) {
    let mut files = Vec::new();
    let p = std::path::Path::new(path);
    if p.is_dir() {
        // Find all .tl files in directory
        if let Ok(entries) = std::fs::read_dir(p) {
            for entry in entries.flatten() {
                let ep = entry.path();
                if ep.extension().and_then(|e| e.to_str()) == Some("tl") {
                    files.push(ep);
                }
            }
        }
    } else {
        files.push(p.to_path_buf());
    }

    if files.is_empty() {
        eprintln!("No .tl files found at '{path}'");
        process::exit(1);
    }

    let mut total = 0;
    let mut passed = 0;
    let mut failed = 0;
    let mut failures: Vec<(String, String)> = Vec::new();

    for file in &files {
        let source = match fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error reading {}: {e}", file.display());
                continue;
            }
        };

        let program = match parse(&source) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Parse error in {}: {e}", file.display());
                continue;
            }
        };

        // Find test blocks
        for stmt in &program.statements {
            if let tl_ast::StmtKind::Test { name, body } = &stmt.kind {
                total += 1;
                let test_program = tl_ast::Program { statements: body.clone() };

                let result = match backend {
                    "vm" => {
                        let proto = match tl_compiler::compile(&test_program) {
                            Ok(p) => p,
                            Err(e) => {
                                failed += 1;
                                failures.push((name.clone(), format!("Compile error: {e}")));
                                println!("  FAIL  {name}");
                                continue;
                            }
                        };
                        let mut vm = tl_compiler::Vm::new();
                        vm.execute(&proto)
                    }
                    _ => {
                        let mut interp = Interpreter::new();
                        interp.execute(&test_program).map(|_| tl_compiler::VmValue::None)
                    }
                };

                match result {
                    Ok(_) => {
                        passed += 1;
                        println!("  PASS  {name}");
                    }
                    Err(e) => {
                        failed += 1;
                        let msg = match &e {
                            TlError::Runtime(re) => re.message.clone(),
                            other => format!("{other}"),
                        };
                        failures.push((name.clone(), msg.clone()));
                        println!("  FAIL  {name}: {msg}");
                    }
                }
            }
        }
    }

    println!("\n{total} tests, {passed} passed, {failed} failed");
    if !failures.is_empty() {
        process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needs_continuation_balanced() {
        assert!(!needs_continuation("let x = 42"));
        assert!(!needs_continuation("fn add(a, b) { a + b }"));
        assert!(!needs_continuation("let xs = [1, 2, 3]"));
    }

    #[test]
    fn test_needs_continuation_unclosed() {
        assert!(needs_continuation("fn add(a, b) {"));
        assert!(needs_continuation("let x = [1, 2,"));
        assert!(needs_continuation("print("));
    }

    #[test]
    fn test_needs_continuation_strings() {
        // Braces inside strings should not count
        assert!(!needs_continuation(r#"let x = "{hello}""#));
        assert!(!needs_continuation(r#"let x = "(test)""#));
    }

    #[test]
    fn test_needs_continuation_nested() {
        assert!(needs_continuation("fn foo() { if true {"));
        assert!(!needs_continuation("fn foo() { if true { 1 } }"));
    }

    #[test]
    fn test_needs_continuation_empty() {
        assert!(!needs_continuation(""));
    }
}
