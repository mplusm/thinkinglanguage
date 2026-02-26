// ThinkingLanguage — CLI Entry Point
// Licensed under MIT OR Apache-2.0
//
// Commands:
//   tl run <file.tl>   — Execute a .tl source file
//   tl shell            — Start the interactive REPL

use clap::{Parser, Subcommand};
use rustyline::DefaultEditor;
use std::fs;
use std::process;

use tl_errors::{report_parser_error, report_runtime_error, TlError};
use tl_interpreter::Interpreter;
use tl_compiler::{compile, Vm, VmValue};
use tl_parser::parse;

mod deploy;

#[derive(Parser)]
#[command(name = "tl", version, about = "ThinkingLanguage — Data Engineering & AI")]
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
        Some(Commands::Run { file, backend }) => run_file(&file, &backend),
        Some(Commands::Shell { backend }) => run_repl(&backend),
        Some(Commands::Models { action }) => run_models(action),
        Some(Commands::Deploy { file, target, output }) => run_deploy(&file, &target, &output),
        Some(Commands::Lineage { file, format }) => run_lineage(&file, &format),
        Some(Commands::Test { path, backend }) => run_tests(&path, &backend),
        None => run_repl("vm"), // Default to REPL with VM backend
    }
}

fn run_file(path: &str, backend: &str) {
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

    match backend {
        "vm" => {
            let proto = match compile(&program) {
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

fn run_repl(backend: &str) {
    println!("ThinkingLanguage v0.1.0 — REPL (backend: {backend})");
    println!("Type expressions or statements. Press Ctrl+D to exit.\n");

    let mut editor = match DefaultEditor::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to initialize REPL: {e}");
            process::exit(1);
        }
    };

    match backend {
        "vm" => run_repl_vm(&mut editor),
        "interp" => run_repl_interp(&mut editor),
        _ => {
            eprintln!("Unknown backend: '{backend}'. Use 'vm' or 'interp'.");
            process::exit(1);
        }
    }
}

fn run_repl_vm(editor: &mut DefaultEditor) {
    let mut vm = Vm::new();

    loop {
        let readline = editor.readline("tl> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line);

                match parse(line) {
                    Ok(program) => {
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
                                report_runtime_error(line, "<repl>", re);
                            }
                            Err(e) => eprintln!("{e}"),
                        }
                    }
                    Err(TlError::Parser(ref e)) => {
                        report_parser_error(line, "<repl>", e);
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
        if let tl_ast::Stmt::Pipeline { name, .. } = stmt {
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
            if let tl_ast::Stmt::Test { name, body } = stmt {
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

fn run_repl_interp(editor: &mut DefaultEditor) {
    let mut interp = Interpreter::new();

    loop {
        let readline = editor.readline("tl> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line);

                match parse(line) {
                    Ok(program) => {
                        for stmt in &program.statements {
                            match interp.execute_stmt(stmt) {
                                Ok(val) => {
                                    // Only print non-None values for expression statements
                                    if let tl_ast::Stmt::Expr(_) = stmt {
                                        if !matches!(val, tl_interpreter::Value::None) {
                                            println!("{val}");
                                        }
                                    }
                                }
                                Err(TlError::Runtime(ref re)) => {
                                    report_runtime_error(line, "<repl>", re);
                                }
                                Err(e) => eprintln!("{e}"),
                            }
                        }
                    }
                    Err(TlError::Parser(ref e)) => {
                        report_parser_error(line, "<repl>", e);
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
