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
use tl_parser::parse;

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
    },
    /// Start the interactive REPL
    Shell,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Run { file }) => run_file(&file),
        Some(Commands::Shell) => run_repl(),
        None => run_repl(), // Default to REPL if no subcommand
    }
}

fn run_file(path: &str) {
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

    let mut interp = Interpreter::new();
    if let Err(e) = interp.execute(&program) {
        match &e {
            TlError::Runtime(re) => report_runtime_error(&source, path, re),
            _ => eprintln!("{e}"),
        }
        process::exit(1);
    }
}

fn run_repl() {
    println!("ThinkingLanguage v0.1.0 — REPL");
    println!("Type expressions or statements. Press Ctrl+D to exit.\n");

    let mut editor = match DefaultEditor::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to initialize REPL: {e}");
            process::exit(1);
        }
    };

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
