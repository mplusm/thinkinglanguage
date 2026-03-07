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
use serde::Deserialize;
use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use tl_compiler::{Vm, VmValue, compile, compile_with_source};
use tl_errors::{
    TlError, report_parser_error, report_runtime_error, report_type_error, report_type_warning,
};
use tl_interpreter::Interpreter;
use tl_parser::parse;
use tl_types::checker::{CheckerConfig, check_program};

mod deploy;
mod notebook;
#[cfg(feature = "notebook")]
mod notebook_tui;
mod package;

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
            "let",
            "fn",
            "if",
            "else",
            "while",
            "for",
            "in",
            "return",
            "true",
            "false",
            "none",
            "struct",
            "enum",
            "impl",
            "try",
            "catch",
            "throw",
            "import",
            "test",
            "break",
            "continue",
            "and",
            "or",
            "not",
            "mut",
            "await",
            "yield",
            "match",
            "schema",
            "pipeline",
            "stream",
            "source",
            "sink",
            "use",
            "pub",
            "mod",
            "trait",
            "where",
            "type",
            "check",
            "lsp",
            "fmt",
            "lint",
            "add",
            "remove",
            "install",
            "update",
            "publish",
            "search",
            "doc",
            // Builtin functions
            "print",
            "println",
            "len",
            "str",
            "int",
            "float",
            "abs",
            "min",
            "max",
            "range",
            "push",
            "type_of",
            "map",
            "filter",
            "reduce",
            "sum",
            "any",
            "all",
            "read_csv",
            "read_parquet",
            "write_csv",
            "write_parquet",
            "collect",
            "show",
            "describe",
            "head",
            "sqrt",
            "pow",
            "floor",
            "ceil",
            "round",
            "sin",
            "cos",
            "tan",
            "log",
            "log2",
            "log10",
            "join",
            "assert",
            "assert_eq",
            "json_parse",
            "json_stringify",
            "map_from",
            "read_file",
            "write_file",
            "append_file",
            "file_exists",
            "list_dir",
            "env_get",
            "env_set",
            "regex_match",
            "regex_find",
            "regex_replace",
            "now",
            "date_format",
            "date_parse",
            "zip",
            "enumerate",
            "bool",
            "spawn",
            "sleep",
            "channel",
            "send",
            "recv",
            "try_recv",
            "await_all",
            "pmap",
            "timeout",
            "next",
            "is_generator",
            "iter",
            "take",
            "skip",
            "gen_collect",
            "gen_map",
            "gen_filter",
            "chain",
            "gen_zip",
            "gen_enumerate",
            "Ok",
            "Err",
            "is_ok",
            "is_err",
            "unwrap",
            "set_from",
            "set_add",
            "set_remove",
            "set_contains",
            "set_union",
            "set_intersection",
            "set_difference",
            // Phase 15: Data Quality & Connectors
            "fill_null",
            "drop_null",
            "dedup",
            "clamp",
            "data_profile",
            "row_count",
            "null_rate",
            "is_unique",
            "is_email",
            "is_url",
            "is_phone",
            "is_between",
            "levenshtein",
            "soundex",
            "read_mysql",
            "redis_connect",
            "redis_get",
            "redis_set",
            "redis_del",
            "graphql_query",
            "register_s3",
            // Phase 20: Python FFI
            "py_import",
            "py_call",
            "py_eval",
            "py_getattr",
            "py_setattr",
            "py_to_tl",
            // Phase 21: Schema Evolution
            "schema_register",
            "schema_get",
            "schema_latest",
            "schema_history",
            "schema_check",
            "schema_diff",
            "schema_versions",
            "schema_fields",
            "migrate",
            // Phase 22: Advanced Types
            "decimal",
            // Phase 23: Security & Access Control
            "secret_get",
            "secret_set",
            "secret_delete",
            "secret_list",
            "check_permission",
            "mask_email",
            "mask_phone",
            "mask_cc",
            "redact",
            "hash",
            // Phase 24: Async/Await
            "async_read_file",
            "async_write_file",
            "async_http_get",
            "async_http_post",
            "async_sleep",
            "select",
            "async_map",
            "async_filter",
            "race_all",
        ]
        .iter()
        .map(|s| String::from(*s))
        .collect();
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
#[command(
    name = "tl",
    version,
    about = "ThinkingLanguage -- Data Engineering & AI"
)]
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
        /// Backend: "vm" (default), "interp", or "llvm" (requires llvm-backend feature)
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
        /// Enable sandbox mode (restrict file write, network access)
        #[arg(long)]
        sandbox: bool,
        /// Allow specific connector types in sandbox mode (can be repeated)
        #[arg(long = "allow-connector")]
        allow_connectors: Vec<String>,
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
    /// Initialize a new TL project
    Init {
        /// Project name
        name: String,
    },
    /// Type-check a .tl file without executing it
    Check {
        /// Path to the .tl file
        file: String,
        /// Strict mode: require type annotations on function parameters
        #[arg(long)]
        strict: bool,
    },
    /// Start the Language Server Protocol server
    Lsp,
    /// Format .tl source files
    Fmt {
        /// Path to the .tl file
        path: String,
        /// Check formatting without writing (exit 1 if changes needed)
        #[arg(long)]
        check: bool,
    },
    /// Lint a .tl file for style and correctness issues
    Lint {
        /// Path to the .tl file
        path: String,
        /// Strict mode: require type annotations on function parameters
        #[arg(long)]
        strict: bool,
    },
    /// Build and run the current project (requires tl.toml)
    Build {
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
    /// Add a dependency to the project
    Add {
        /// Package name
        name: String,
        /// Version requirement (e.g. "1.0", "^2.0")
        #[arg(long)]
        version: Option<String>,
        /// Git repository URL
        #[arg(long)]
        git: Option<String>,
        /// Git branch (used with --git)
        #[arg(long)]
        branch: Option<String>,
        /// Local path to the package
        #[arg(long)]
        path: Option<String>,
    },
    /// Remove a dependency from the project
    Remove {
        /// Package name
        name: String,
    },
    /// Install all dependencies from tl.toml
    Install,
    /// Update dependencies (all or a specific package)
    Update {
        /// Package name (updates all if omitted)
        name: Option<String>,
        /// Preview changes without modifying tl.lock
        #[arg(long)]
        dry_run: bool,
    },
    /// Show outdated dependencies
    Outdated,
    /// Generate documentation for a .tl file
    Doc {
        /// Path to the .tl file or directory
        path: String,
        /// Output format: "html", "markdown", or "json"
        #[arg(long, default_value = "html")]
        format: String,
        /// Output file path (defaults to stdout)
        #[arg(long, short)]
        output: Option<String>,
        /// Only include public items
        #[arg(long)]
        public_only: bool,
    },
    /// Schema evolution and migration commands
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
    /// Publish a package to the registry (not yet available)
    Publish,
    /// Search for packages in the registry (not yet available)
    Search {
        /// Search query
        query: String,
    },
    /// Open or create an interactive notebook (.tlnb)
    Notebook {
        /// Path to the .tlnb file (created if it doesn't exist)
        file: String,
        /// Export notebook to .tl file instead of opening TUI
        #[arg(long)]
        export: bool,
    },
    /// Compile a .tl file to native object file (requires llvm-backend feature)
    Compile {
        /// Path to the .tl file
        file: String,
        /// Output file path (defaults to <file>.o)
        #[arg(short, long)]
        output: Option<String>,
        /// Dump LLVM IR instead of writing object file
        #[arg(long)]
        emit_ir: bool,
    },
    /// Debug a .tl file with interactive step debugger
    Debug {
        /// Path to the .tl file
        file: String,
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

#[derive(Subcommand)]
enum MigrateAction {
    /// Apply migrations from a .tl source file
    Apply {
        /// Path to the .tl file containing schema and migrate statements
        file: String,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
    /// Check compatibility without applying migrations
    Check {
        /// Path to the .tl file
        file: String,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
    /// Show diff between schema versions
    Diff {
        /// Path to the .tl file
        file: String,
        /// Schema name
        schema: String,
        /// Source version
        v1: i64,
        /// Target version
        v2: i64,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
    /// Show version history for a schema
    History {
        /// Path to the .tl file
        file: String,
        /// Schema name
        schema: String,
        /// Backend: "vm" (default) or "interp"
        #[arg(long, default_value = "vm")]
        backend: String,
    },
}

// ---------------------------------------------------------------------------
// tl.toml manifest
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TlManifest {
    project: ProjectConfig,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ProjectConfig {
    name: String,
    version: String,
    #[allow(dead_code)]
    edition: Option<String>,
    #[allow(dead_code)]
    authors: Option<Vec<String>>,
    #[allow(dead_code)]
    description: Option<String>,
}

/// Find tl.toml by walking up from the given directory.
fn find_manifest(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        let candidate = dir.join("tl.toml");
        if candidate.exists() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Parse a tl.toml file into a TlManifest (legacy, used by tests).
#[allow(dead_code)]
fn parse_manifest(path: &Path) -> Result<TlManifest, String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("Cannot read '{}': {e}", path.display()))?;
    let manifest: TlManifest =
        toml::from_str(&content).map_err(|e| format!("Invalid tl.toml: {e}"))?;
    Ok(manifest)
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Run {
            file,
            backend,
            dump_bytecode,
            no_check,
            strict,
            sandbox,
            allow_connectors,
        }) => run_file(
            &file,
            &backend,
            dump_bytecode,
            no_check,
            strict,
            sandbox,
            &allow_connectors,
        ),
        Some(Commands::Shell { backend }) => run_repl(&backend),
        Some(Commands::Models { action }) => run_models(action),
        Some(Commands::Deploy {
            file,
            target,
            output,
        }) => run_deploy(&file, &target, &output),
        Some(Commands::Lineage { file, format }) => run_lineage(&file, &format),
        Some(Commands::Disasm { file }) => run_disasm(&file),
        Some(Commands::Check { file, strict }) => run_check(&file, strict),
        Some(Commands::Test { path, backend }) => run_tests(&path, &backend),
        Some(Commands::Init { name }) => run_init(&name),
        Some(Commands::Lsp) => run_lsp(),
        Some(Commands::Fmt { path, check }) => run_fmt(&path, check),
        Some(Commands::Lint { path, strict }) => run_lint(&path, strict),
        Some(Commands::Build {
            backend,
            dump_bytecode,
            no_check,
            strict,
        }) => run_build(&backend, dump_bytecode, no_check, strict),
        Some(Commands::Doc {
            path,
            format,
            output,
            public_only,
        }) => run_doc(&path, &format, output.as_deref(), public_only),
        Some(Commands::Add {
            name,
            version,
            git,
            branch,
            path,
        }) => {
            package::cmd_add(
                &name,
                version.as_deref(),
                git.as_deref(),
                branch.as_deref(),
                path.as_deref(),
            );
        }
        Some(Commands::Remove { name }) => package::cmd_remove(&name),
        Some(Commands::Install) => package::cmd_install(),
        Some(Commands::Update { name, dry_run }) => package::cmd_update(name.as_deref(), dry_run),
        Some(Commands::Outdated) => package::cmd_outdated(),
        Some(Commands::Migrate { action }) => run_migrate(action),
        Some(Commands::Notebook { file, export }) => cmd_notebook(&file, export),
        Some(Commands::Publish) => package::cmd_publish(),
        Some(Commands::Search { query }) => package::cmd_search(&query),
        Some(Commands::Compile {
            file,
            output,
            emit_ir,
        }) => run_compile(&file, output.as_deref(), emit_ir),
        Some(Commands::Debug { file }) => run_debug(&file),
        None => run_repl("vm"), // Default to REPL with VM backend
    }
}

fn run_file(
    path: &str,
    backend: &str,
    dump_bytecode: bool,
    no_check: bool,
    strict: bool,
    sandbox: bool,
    allow_connectors: &[String],
) {
    run_file_with_packages(
        path,
        backend,
        dump_bytecode,
        no_check,
        strict,
        None,
        None,
        sandbox,
        allow_connectors,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_file_with_packages(
    path: &str,
    backend: &str,
    dump_bytecode: bool,
    no_check: bool,
    strict: bool,
    package_roots: Option<std::collections::HashMap<String, PathBuf>>,
    project_root: Option<PathBuf>,
    sandbox: bool,
    allow_connectors: &[String],
) {
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
            report_type_warning(
                &source,
                path,
                &tl_errors::TypeError {
                    message: warning.message.clone(),
                    span: warning.span,
                    expected: warning.expected.clone(),
                    found: warning.found.clone(),
                    hint: warning.hint.clone(),
                },
            );
        }

        if result.has_errors() {
            for error in &result.errors {
                report_type_error(
                    &source,
                    path,
                    &tl_errors::TypeError {
                        message: error.message.clone(),
                        span: error.span,
                        expected: error.expected.clone(),
                        found: error.found.clone(),
                        hint: error.hint.clone(),
                    },
                );
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
            vm.file_path = Some(path.to_string());
            if sandbox {
                use tl_compiler::security::SecurityPolicy;
                let mut policy = SecurityPolicy::sandbox();
                for conn in allow_connectors {
                    policy.allowed_connectors.insert(conn.clone());
                }
                vm.security_policy = Some(policy);
            }
            if let Some(ref roots) = package_roots {
                vm.package_roots = roots.clone();
            }
            if let Some(ref root) = project_root {
                vm.project_root = Some(root.clone());
            }
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
            interp.file_path = Some(path.to_string());
            if sandbox {
                use tl_compiler::security::SecurityPolicy;
                let mut policy = SecurityPolicy::sandbox();
                for conn in allow_connectors {
                    policy.allowed_connectors.insert(conn.clone());
                }
                interp.security_policy = Some(policy);
            }
            if let Some(ref roots) = package_roots {
                interp.package_roots = roots.clone();
            }
            if let Some(ref root) = project_root {
                interp.project_root = Some(root.clone());
            }
            if let Err(e) = interp.execute(&program) {
                match &e {
                    TlError::Runtime(re) => report_runtime_error(&source, path, re),
                    _ => eprintln!("{e}"),
                }
                process::exit(1);
            }
        }
        #[cfg(feature = "llvm-backend")]
        "llvm" => {
            if let Err(e) = tl_llvm::aot::compile_and_run(&source, Some(path)) {
                eprintln!("LLVM backend error: {e}");
                process::exit(1);
            }
        }
        _ => {
            let backends = if cfg!(feature = "llvm-backend") {
                "'vm', 'interp', or 'llvm'"
            } else {
                "'vm' or 'interp'"
            };
            eprintln!("Unknown backend: '{backend}'. Use {backends}.");
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

fn run_check(path: &str, strict: bool) {
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

    let config = CheckerConfig { strict };
    let result = check_program(&program, &config);

    for warning in &result.warnings {
        report_type_warning(
            &source,
            path,
            &tl_errors::TypeError {
                message: warning.message.clone(),
                span: warning.span,
                expected: warning.expected.clone(),
                found: warning.found.clone(),
                hint: warning.hint.clone(),
            },
        );
    }

    for error in &result.errors {
        report_type_error(
            &source,
            path,
            &tl_errors::TypeError {
                message: error.message.clone(),
                span: error.span,
                expected: error.expected.clone(),
                found: error.found.clone(),
                hint: error.hint.clone(),
            },
        );
    }

    let warning_count = result.warnings.len();
    let error_count = result.errors.len();

    if error_count > 0 {
        eprintln!("{error_count} error(s), {warning_count} warning(s)");
        process::exit(1);
    } else if warning_count > 0 {
        eprintln!("{warning_count} warning(s)");
    } else {
        eprintln!("No errors or warnings.");
    }
}

fn cmd_notebook(file: &str, export: bool) {
    use std::path::PathBuf;

    let path = PathBuf::from(file);

    // Load or create notebook
    let nb = if path.exists() {
        match notebook::Notebook::load(&path) {
            Ok(nb) => nb,
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
        }
    } else {
        notebook::Notebook::new()
    };

    if export {
        let content = nb.export_tl();
        let tl_path = path.with_extension("tl");
        if let Err(e) = std::fs::write(&tl_path, &content) {
            eprintln!("Cannot write {}: {e}", tl_path.display());
            process::exit(1);
        }
        println!("Exported to {}", tl_path.display());
        return;
    }

    #[cfg(feature = "notebook")]
    {
        let mut app = notebook_tui::NotebookApp::new(nb, path);
        if let Err(e) = app.run() {
            eprintln!("Notebook error: {e}");
            process::exit(1);
        }
    }
    #[cfg(not(feature = "notebook"))]
    {
        eprintln!("Notebook TUI requires the 'notebook' feature.");
        eprintln!("Rebuild with: cargo build --features notebook");
        let _ = nb;
        let _ = path;
        process::exit(1);
    }
}

fn run_repl(backend: &str) {
    println!("ThinkingLanguage v0.1.0 -- REPL (backend: {backend})");
    println!("Type expressions or statements. Press Ctrl+D to exit.\n");

    let config = Config::builder().auto_add_history(false).build();
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
                            report_type_warning(
                                &input,
                                "<repl>",
                                &tl_errors::TypeError {
                                    message: warning.message.clone(),
                                    span: warning.span,
                                    expected: warning.expected.clone(),
                                    found: warning.found.clone(),
                                    hint: warning.hint.clone(),
                                },
                            );
                        }
                        for error in &check_result.errors {
                            report_type_error(
                                &input,
                                "<repl>",
                                &tl_errors::TypeError {
                                    message: error.message.clone(),
                                    span: error.span,
                                    expected: error.expected.clone(),
                                    found: error.found.clone(),
                                    hint: error.hint.clone(),
                                },
                            );
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
                            report_type_warning(
                                &input,
                                "<repl>",
                                &tl_errors::TypeError {
                                    message: warning.message.clone(),
                                    span: warning.span,
                                    expected: warning.expected.clone(),
                                    found: warning.found.clone(),
                                    hint: warning.hint.clone(),
                                },
                            );
                        }
                        for error in &check_result.errors {
                            report_type_error(
                                &input,
                                "<repl>",
                                &tl_errors::TypeError {
                                    message: error.message.clone(),
                                    span: error.span,
                                    expected: error.expected.clone(),
                                    found: error.found.clone(),
                                    hint: error.hint.clone(),
                                },
                            );
                        }

                        for stmt in &program.statements {
                            match interp.execute_stmt(stmt) {
                                Ok(val) => {
                                    // Only print non-None values for expression statements
                                    if let tl_ast::StmtKind::Expr(_) = &stmt.kind
                                        && !matches!(val, tl_interpreter::Value::None)
                                    {
                                        println!("{val}");
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
        ModelsAction::Info { name } => match registry.get(&name) {
            Ok(model) => {
                println!("Model: {name}");
                println!("{model}");
            }
            Err(e) => {
                eprintln!("Error: {e}");
                process::exit(1);
            }
        },
        ModelsAction::Delete { name } => match registry.delete(&name) {
            Ok(()) => println!("Deleted model '{name}'"),
            Err(e) => {
                eprintln!("Error: {e}");
                process::exit(1);
            }
        },
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
            let extract_id =
                tracker.record(&format!("{name}/extract"), "Read source data", None, vec![]);
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
            tracker.record(&format!("stmt_{i}"), "Execute statement", None, vec![]);
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
                let test_program = tl_ast::Program {
                    statements: body.clone(),
                    module_doc: None,
                };

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
                        interp
                            .execute(&test_program)
                            .map(|_| tl_compiler::VmValue::None)
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

fn run_init(name: &str) {
    let project_dir = Path::new(name);
    if project_dir.exists() {
        eprintln!("Directory '{name}' already exists");
        process::exit(1);
    }

    // Create directory structure
    let src_dir = project_dir.join("src");
    if let Err(e) = fs::create_dir_all(&src_dir) {
        eprintln!("Failed to create directory: {e}");
        process::exit(1);
    }

    // Write tl.toml
    let manifest = format!(
        r#"[project]
name = "{name}"
version = "0.1.0"

[dependencies]
"#
    );
    if let Err(e) = fs::write(project_dir.join("tl.toml"), manifest) {
        eprintln!("Failed to write tl.toml: {e}");
        process::exit(1);
    }

    // Write src/main.tl
    let main_tl = format!("print(\"Hello from {name}!\")\n");
    if let Err(e) = fs::write(src_dir.join("main.tl"), main_tl) {
        eprintln!("Failed to write src/main.tl: {e}");
        process::exit(1);
    }

    println!("Created project '{name}'");
    println!("  {name}/tl.toml");
    println!("  {name}/src/main.tl");
    println!("\nRun with: cd {name} && tl build");
}

fn run_lsp() {
    if let Err(e) = tl_lsp::run_server() {
        eprintln!("LSP server error: {e}");
        process::exit(1);
    }
}

fn run_fmt(path: &str, check: bool) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading file '{path}': {e}");
            process::exit(1);
        }
    };

    let formatted = match tl_lsp::format::Formatter::format(&source) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    if check {
        if formatted != source {
            eprintln!("{path}: formatting changes needed");
            process::exit(1);
        }
    } else if formatted != source {
        if let Err(e) = fs::write(path, &formatted) {
            eprintln!("Error writing file '{path}': {e}");
            process::exit(1);
        }
        eprintln!("Formatted {path}");
    } else {
        eprintln!("{path}: already formatted");
    }
}

fn run_lint(path: &str, strict: bool) {
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

    let config = CheckerConfig { strict };
    let result = check_program(&program, &config);

    for warning in &result.warnings {
        report_type_warning(
            &source,
            path,
            &tl_errors::TypeError {
                message: warning.message.clone(),
                span: warning.span,
                expected: warning.expected.clone(),
                found: warning.found.clone(),
                hint: warning.hint.clone(),
            },
        );
    }

    for error in &result.errors {
        report_type_error(
            &source,
            path,
            &tl_errors::TypeError {
                message: error.message.clone(),
                span: error.span,
                expected: error.expected.clone(),
                found: error.found.clone(),
                hint: error.hint.clone(),
            },
        );
    }

    let warning_count = result.warnings.len();
    let error_count = result.errors.len();

    if error_count > 0 {
        eprintln!("{error_count} error(s), {warning_count} warning(s)");
        process::exit(1);
    } else if warning_count > 0 {
        eprintln!("{warning_count} warning(s)");
    } else {
        eprintln!("No lint issues found.");
    }
}

fn run_doc(path: &str, format: &str, output: Option<&str>, public_only: bool) {
    let path_obj = Path::new(path);

    if path_obj.is_dir() {
        // Project-level documentation: walk directory for .tl files
        run_doc_project(path, format, output, public_only);
        return;
    }

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

    let docs = if public_only {
        tl_lsp::doc::extract_public_docs(&program, Some(path))
    } else {
        tl_lsp::doc::extract_docs(&program, Some(path))
    };

    let result = match format {
        "html" => tl_lsp::doc::generate_html(&docs),
        "markdown" | "md" => tl_lsp::doc::generate_markdown(&docs),
        "json" => tl_lsp::doc::generate_json(&docs),
        _ => {
            eprintln!("Unknown format '{format}'. Use 'html', 'markdown', or 'json'.");
            process::exit(1);
        }
    };

    write_doc_output(&result, output);
}

fn run_doc_project(dir: &str, format: &str, output: Option<&str>, public_only: bool) {
    let mut modules = Vec::new();

    fn collect_tl_files(dir: &Path, files: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    collect_tl_files(&path, files);
                } else if path.extension().is_some_and(|e| e == "tl") {
                    files.push(path);
                }
            }
        }
    }

    let mut files = Vec::new();
    collect_tl_files(Path::new(dir), &mut files);
    files.sort();

    for file in &files {
        let source = match fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Warning: cannot read '{}': {e}", file.display());
                continue;
            }
        };
        let program = match parse(&source) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Warning: parse error in '{}': {e}", file.display());
                continue;
            }
        };
        let rel_path = file.strip_prefix(dir).unwrap_or(file);
        let module = if public_only {
            tl_lsp::doc::extract_public_docs(&program, Some(&rel_path.display().to_string()))
        } else {
            tl_lsp::doc::extract_docs(&program, Some(&rel_path.display().to_string()))
        };
        modules.push(module);
    }

    let project = tl_lsp::doc::ProjectDoc { modules };
    let result = match format {
        "html" => tl_lsp::doc::generate_project_html(&project),
        "markdown" | "md" => tl_lsp::doc::generate_project_markdown(&project),
        "json" => tl_lsp::doc::generate_project_json(&project),
        _ => {
            eprintln!("Unknown format '{format}'. Use 'html', 'markdown', or 'json'.");
            process::exit(1);
        }
    };

    write_doc_output(&result, output);
}

fn write_doc_output(content: &str, output: Option<&str>) {
    match output {
        Some(out_path) => {
            if let Err(e) = fs::write(out_path, content) {
                eprintln!("Error writing to '{out_path}': {e}");
                process::exit(1);
            }
            eprintln!("Documentation written to {out_path}");
        }
        None => {
            print!("{content}");
        }
    }
}

fn run_build(backend: &str, dump_bytecode: bool, no_check: bool, strict: bool) {
    let cwd = std::env::current_dir().unwrap_or_else(|e| {
        eprintln!("Cannot determine current directory: {e}");
        process::exit(1);
    });

    let manifest_path = match find_manifest(&cwd) {
        Some(p) => p,
        None => {
            eprintln!("No tl.toml found in current directory or any parent directory.");
            eprintln!("Run 'tl init <name>' to create a new project.");
            process::exit(1);
        }
    };

    // Parse manifest using tl_package::Manifest for full dependency support
    let manifest = match tl_package::Manifest::load(&manifest_path) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let project_root = manifest_path.parent().unwrap();
    let entry = project_root.join("src").join("main.tl");

    if !entry.exists() {
        eprintln!("Entry point not found: {}", entry.display());
        eprintln!(
            "Expected src/main.tl in project '{}'",
            manifest.project.name
        );
        process::exit(1);
    }

    // Resolve and install dependencies
    let package_roots = if !manifest.dependencies.is_empty() {
        let cache = match tl_package::PackageCache::default_location() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Warning: {e}");
                tl_package::PackageCache::new(project_root.join(".tl_cache"))
            }
        };

        match tl_package::resolve_and_install(project_root, &manifest, &cache) {
            Ok(lock) => {
                if !lock.packages.is_empty() {
                    for pkg in &lock.packages {
                        eprintln!("  {} v{}", pkg.name, pkg.version);
                    }
                }
                Some(tl_package::resolver::build_package_roots(
                    project_root,
                    &cache,
                ))
            }
            Err(e) => {
                eprintln!("Warning: dependency resolution failed: {e}");
                None
            }
        }
    } else {
        None
    };

    let entry_str = entry.to_string_lossy().to_string();
    println!(
        "Building {} v{}",
        manifest.project.name, manifest.project.version
    );
    run_file_with_packages(
        &entry_str,
        backend,
        dump_bytecode,
        no_check,
        strict,
        package_roots,
        Some(project_root.to_path_buf()),
        false,
        &[],
    );
}

// ---------------------------------------------------------------------------
// tl compile (LLVM backend)
// ---------------------------------------------------------------------------

fn run_compile(path: &str, output: Option<&str>, emit_ir: bool) {
    #[cfg(not(feature = "llvm-backend"))]
    {
        let _ = (path, output, emit_ir);
        eprintln!("LLVM backend is not enabled. Rebuild with: cargo build --features llvm-backend");
        process::exit(1);
    }

    #[cfg(feature = "llvm-backend")]
    {
        let source = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Cannot read '{path}': {e}");
                process::exit(1);
            }
        };

        if emit_ir {
            match tl_llvm::aot::compile_to_ir(&source) {
                Ok(ir) => println!("{ir}"),
                Err(e) => {
                    eprintln!("LLVM IR generation error: {e}");
                    process::exit(1);
                }
            }
        } else {
            let out_path = match output {
                Some(o) => std::path::PathBuf::from(o),
                None => {
                    let p = std::path::Path::new(path);
                    p.with_extension("o")
                }
            };
            match tl_llvm::aot::compile_to_object(&source, &out_path) {
                Ok(()) => println!("Compiled to {}", out_path.display()),
                Err(e) => {
                    eprintln!("Compilation error: {e}");
                    process::exit(1);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// tl debug — Interactive step debugger
// ---------------------------------------------------------------------------

fn run_debug(path: &str) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Cannot read '{path}': {e}");
            process::exit(1);
        }
    };

    let source_lines: Vec<&str> = source.lines().collect();

    // Parse
    let ast = match parse(&source) {
        Ok(program) => program,
        Err(e) => {
            eprintln!("Parse error: {e}");
            process::exit(1);
        }
    };

    // Compile
    let proto = match compile_with_source(&ast, &source) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Compile error: {e}");
            process::exit(1);
        }
    };

    // Set up VM
    let mut vm = Vm::new();
    vm.file_path = Some(path.to_string());
    vm.debug_load(&proto);

    let mut breakpoints: Vec<u32> = Vec::new();

    println!("TL Debugger — {} ({} lines)", path, source_lines.len());
    println!("Commands: s(tep), n(ext), c(ontinue), b <line>, d <line>, p <var>, l(ist), q(uit)");

    // Show initial position
    let line = vm.debug_current_line();
    if line > 0 && (line as usize) <= source_lines.len() {
        println!("=> {:>4} | {}", line, source_lines[line as usize - 1]);
    }

    let mut rl = rustyline::DefaultEditor::new().unwrap_or_else(|_| {
        eprintln!("Failed to create readline editor");
        process::exit(1);
    });

    loop {
        let readline = rl.readline("debug> ");
        match readline {
            Ok(input) => {
                let input = input.trim();
                if input.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(input);

                let parts: Vec<&str> = input.splitn(2, ' ').collect();
                match parts[0] {
                    "s" | "step" => {
                        // Single instruction step
                        match vm.debug_step() {
                            Ok(Some(val)) => {
                                println!("Program finished with: {val}");
                                break;
                            }
                            Ok(None) => {
                                show_current_line(&vm, &source_lines);
                            }
                            Err(e) => {
                                eprintln!("Runtime error: {e}");
                                break;
                            }
                        }
                    }
                    "n" | "next" => {
                        // Step to next source line
                        match vm.debug_step_line() {
                            Ok(Some(val)) => {
                                println!("Program finished with: {val}");
                                break;
                            }
                            Ok(None) => {
                                show_current_line(&vm, &source_lines);
                            }
                            Err(e) => {
                                eprintln!("Runtime error: {e}");
                                break;
                            }
                        }
                    }
                    "c" | "continue" => match vm.debug_continue(&breakpoints) {
                        Ok(Some(val)) => {
                            println!("Program finished with: {val}");
                            break;
                        }
                        Ok(None) => {
                            let line = vm.debug_current_line();
                            println!("Hit breakpoint at line {line}");
                            show_current_line(&vm, &source_lines);
                        }
                        Err(e) => {
                            eprintln!("Runtime error: {e}");
                            break;
                        }
                    },
                    "b" | "break" => {
                        if let Some(line_str) = parts.get(1) {
                            if let Ok(line) = line_str.parse::<u32>() {
                                if !breakpoints.contains(&line) {
                                    breakpoints.push(line);
                                    println!("Breakpoint set at line {line}");
                                } else {
                                    println!("Breakpoint already exists at line {line}");
                                }
                            } else {
                                println!("Usage: b <line_number>");
                            }
                        } else {
                            println!("Breakpoints: {:?}", breakpoints);
                        }
                    }
                    "d" | "delete" => {
                        if let Some(line_str) = parts.get(1)
                            && let Ok(line) = line_str.parse::<u32>()
                        {
                            breakpoints.retain(|&l| l != line);
                            println!("Breakpoint removed at line {line}");
                        }
                    }
                    "p" | "print" => {
                        if let Some(var_name) = parts.get(1) {
                            if let Some(val) = vm.debug_get_local(var_name) {
                                println!("{var_name} = {val}");
                            } else if let Some(val) = vm.debug_get_global(var_name) {
                                println!("{var_name} = {val}");
                            } else {
                                println!("{var_name} = (not found)");
                            }
                        } else {
                            // Print locals first
                            let locals = vm.debug_locals();
                            if !locals.is_empty() {
                                println!("Locals:");
                                for (name, val) in &locals {
                                    println!("  {name} = {val}");
                                }
                            }
                        }
                    }
                    "l" | "list" => {
                        let current = vm.debug_current_line() as usize;
                        let start = if current > 3 { current - 3 } else { 1 };
                        let end = (current + 4).min(source_lines.len() + 1);
                        for i in start..end {
                            if i <= source_lines.len() {
                                let marker = if i == current { "=>" } else { "  " };
                                let bp = if breakpoints.contains(&(i as u32)) {
                                    "*"
                                } else {
                                    " "
                                };
                                println!("{marker}{bp}{:>4} | {}", i, source_lines[i - 1]);
                            }
                        }
                    }
                    "w" | "where" => {
                        let func = vm.debug_current_function();
                        let line = vm.debug_current_line();
                        let ip = vm.debug_current_ip();
                        println!(
                            "  in {} at line {}, ip={}",
                            if func.is_empty() { "<top>" } else { &func },
                            line,
                            ip
                        );
                    }
                    "q" | "quit" => {
                        println!("Debugger exited.");
                        break;
                    }
                    _ => {
                        println!(
                            "Unknown command: '{}'. Commands: s(tep), n(ext), c(ontinue), b <line>, d <line>, p <var>, l(ist), w(here), q(uit)",
                            parts[0]
                        );
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Eof)
            | Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("Debugger exited.");
                break;
            }
            Err(e) => {
                eprintln!("Input error: {e}");
                break;
            }
        }
    }
}

fn show_current_line(vm: &Vm, source_lines: &[&str]) {
    let line = vm.debug_current_line();
    if line > 0 && (line as usize) <= source_lines.len() {
        println!("=> {:>4} | {}", line, source_lines[line as usize - 1]);
    }
}

// ---------------------------------------------------------------------------
// tl migrate
// ---------------------------------------------------------------------------

fn run_migrate(action: MigrateAction) {
    match action {
        MigrateAction::Apply { file, backend } => {
            println!("Applying migrations from {}...", file);
            run_migrate_file(&file, &backend, false);
        }
        MigrateAction::Check { file, backend } => {
            println!("Checking schema compatibility in {}...", file);
            run_migrate_file(&file, &backend, true);
        }
        MigrateAction::Diff {
            file,
            schema,
            v1,
            v2,
            backend,
        } => {
            run_migrate_diff(&file, &backend, &schema, v1, v2);
        }
        MigrateAction::History {
            file,
            schema,
            backend,
        } => {
            run_migrate_history(&file, &backend, &schema);
        }
    }
}

fn run_migrate_file(path: &str, backend: &str, check_only: bool) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading '{}': {}", path, e);
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
            eprintln!("Error: {e}");
            process::exit(1);
        }
    };

    if backend == "interp" {
        let mut interp = Interpreter::new();
        match interp.execute(&program) {
            Ok(_) => {
                if check_only {
                    println!("All schemas and migrations validated successfully.");
                } else {
                    println!("Migrations applied successfully.");
                }
            }
            Err(e) => {
                match &e {
                    TlError::Runtime(re) => report_runtime_error(&source, path, re),
                    _ => eprintln!("Error: {e}"),
                }
                process::exit(1);
            }
        }
    } else {
        let proto = match compile(&program) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Compilation error: {e}");
                process::exit(1);
            }
        };
        let mut vm = Vm::new();
        match vm.execute(&proto) {
            Ok(_) => {
                if check_only {
                    println!("All schemas and migrations validated successfully.");
                } else {
                    println!("Migrations applied successfully.");
                }
            }
            Err(e) => {
                match &e {
                    TlError::Runtime(re) => report_runtime_error(&source, path, re),
                    _ => eprintln!("Error: {e}"),
                }
                process::exit(1);
            }
        }
    }
}

fn run_migrate_diff(path: &str, backend: &str, schema_name: &str, v1: i64, v2: i64) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading '{}': {}", path, e);
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
            eprintln!("Error: {e}");
            process::exit(1);
        }
    };

    if backend == "interp" {
        let mut interp = Interpreter::new();
        if let Err(e) = interp.execute(&program) {
            match &e {
                TlError::Runtime(re) => report_runtime_error(&source, path, re),
                _ => eprintln!("Error: {e}"),
            }
            process::exit(1);
        }
        let diffs = interp.schema_registry.diff(schema_name, v1, v2);
        if diffs.is_empty() {
            println!("No differences between {} v{} and v{}", schema_name, v1, v2);
        } else {
            println!("Schema `{}` diff (v{} -> v{}):", schema_name, v1, v2);
            for d in &diffs {
                println!("  - {}", d);
            }
        }
    } else {
        let proto = match compile(&program) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Compilation error: {e}");
                process::exit(1);
            }
        };
        let mut vm = Vm::new();
        if let Err(e) = vm.execute(&proto) {
            match &e {
                TlError::Runtime(re) => report_runtime_error(&source, path, re),
                _ => eprintln!("Error: {e}"),
            }
            process::exit(1);
        }
        let diffs = vm.schema_registry.diff(schema_name, v1, v2);
        if diffs.is_empty() {
            println!("No differences between {} v{} and v{}", schema_name, v1, v2);
        } else {
            println!("Schema `{}` diff (v{} -> v{}):", schema_name, v1, v2);
            for d in &diffs {
                println!("  - {}", d);
            }
        }
    }
}

fn run_migrate_history(path: &str, backend: &str, schema_name: &str) {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading '{}': {}", path, e);
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
            eprintln!("Error: {e}");
            process::exit(1);
        }
    };

    if backend == "interp" {
        let mut interp = Interpreter::new();
        if let Err(e) = interp.execute(&program) {
            match &e {
                TlError::Runtime(re) => report_runtime_error(&source, path, re),
                _ => eprintln!("Error: {e}"),
            }
            process::exit(1);
        }
        let versions = interp.schema_registry.versions(schema_name);
        if versions.is_empty() {
            println!("No versions found for schema `{}`", schema_name);
        } else {
            println!("Schema `{}` version history:", schema_name);
            for v in &versions {
                println!("  v{}", v);
            }
        }
    } else {
        let proto = match compile(&program) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Compilation error: {e}");
                process::exit(1);
            }
        };
        let mut vm = Vm::new();
        if let Err(e) = vm.execute(&proto) {
            match &e {
                TlError::Runtime(re) => report_runtime_error(&source, path, re),
                _ => eprintln!("Error: {e}"),
            }
            process::exit(1);
        }
        let versions = vm.schema_registry.versions(schema_name);
        if versions.is_empty() {
            println!("No versions found for schema `{}`", schema_name);
        } else {
            println!("Schema `{}` version history:", schema_name);
            for v in &versions {
                println!("  v{}", v);
            }
        }
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

    // -- Step 6: tl.toml, tl init, tl build tests --

    #[test]
    fn test_parse_manifest_basic() {
        let dir = tempfile::tempdir().unwrap();
        let toml_path = dir.path().join("tl.toml");
        fs::write(
            &toml_path,
            r#"
[project]
name = "myapp"
version = "1.2.3"
"#,
        )
        .unwrap();
        let m = parse_manifest(&toml_path).unwrap();
        assert_eq!(m.project.name, "myapp");
        assert_eq!(m.project.version, "1.2.3");
        assert!(m.project.edition.is_none());
        assert!(m.project.authors.is_none());
    }

    #[test]
    fn test_parse_manifest_all_fields() {
        let dir = tempfile::tempdir().unwrap();
        let toml_path = dir.path().join("tl.toml");
        fs::write(
            &toml_path,
            r#"
[project]
name = "myapp"
version = "0.1.0"
edition = "2024"
authors = ["Alice", "Bob"]
description = "A great project"
"#,
        )
        .unwrap();
        let m = parse_manifest(&toml_path).unwrap();
        assert_eq!(m.project.name, "myapp");
        assert_eq!(m.project.edition.as_deref(), Some("2024"));
        assert_eq!(m.project.authors.as_ref().unwrap().len(), 2);
        assert_eq!(m.project.description.as_deref(), Some("A great project"));
    }

    #[test]
    fn test_parse_manifest_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let toml_path = dir.path().join("tl.toml");
        fs::write(&toml_path, "not valid toml [[[").unwrap();
        assert!(parse_manifest(&toml_path).is_err());
    }

    #[test]
    fn test_find_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("a").join("b").join("c");
        fs::create_dir_all(&sub).unwrap();
        fs::write(
            dir.path().join("tl.toml"),
            "[project]\nname = \"x\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        let found = find_manifest(&sub).unwrap();
        assert_eq!(found, dir.path().join("tl.toml"));
    }

    #[test]
    fn test_find_manifest_not_found() {
        let dir = tempfile::tempdir().unwrap();
        assert!(find_manifest(dir.path()).is_none());
    }

    // -- Step 7: Type checker handles use/pub/mod --

    #[test]
    fn test_type_checker_use_pub_mod() {
        // Type checker should not error on use, pub, or mod statements
        let src = "use data.transforms\npub fn foo() { 42 }\nmod quality";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(
            !result.has_errors(),
            "Type checker should pass through use/pub/mod"
        );
    }

    #[test]
    fn test_type_checker_pub_struct_enum() {
        let src = "pub struct Point { x: int, y: int }\npub enum Color { Red, Blue }";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(
            !result.has_errors(),
            "Type checker should handle pub struct/enum"
        );
    }

    // -- Phase 13 Step 5: tl check subcommand tests --

    #[test]
    fn test_check_valid_file_no_errors() {
        let src = "let x: int = 42\nprint(x)";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(!result.has_errors());
        assert_eq!(result.warnings.len(), 0);
    }

    #[test]
    fn test_check_strict_unannotated_params() {
        let src = "fn add(a, b) { a + b }\nprint(add(1, 2))";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig { strict: true };
        let result = tl_types::checker::check_program(&program, &config);
        assert!(
            result.has_errors(),
            "Strict mode should flag unannotated params"
        );
    }

    #[test]
    fn test_check_non_strict_unannotated_ok() {
        let src = "fn add(a, b) { a + b }\nprint(add(1, 2))";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(
            !result.has_errors(),
            "Non-strict mode should allow unannotated params"
        );
    }

    #[test]
    fn test_check_unused_variable_warning() {
        let src = "let x = 42";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(!result.has_errors());
        assert!(
            result.warnings.len() > 0,
            "Unused variable should produce warning"
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.message.contains("Unused variable"))
        );
    }

    #[test]
    fn test_check_unreachable_code_warning() {
        let src = "fn foo() {\n  return 1\n  print(2)\n}\nprint(foo())";
        let program = tl_parser::parse(src).unwrap();
        let config = tl_types::checker::CheckerConfig::default();
        let result = tl_types::checker::check_program(&program, &config);
        assert!(!result.has_errors());
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.message.contains("Unreachable")),
            "Should warn about unreachable code after return"
        );
    }

    // -- Phase 16: Package manager tests --

    #[test]
    fn test_init_includes_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        let project_name = "test_pkg_init";
        let project_dir = dir.path().join(project_name);

        // Manually create the init structure (like run_init does)
        let src_dir = project_dir.join("src");
        fs::create_dir_all(&src_dir).unwrap();
        let manifest = format!(
            "[project]\nname = \"{project_name}\"\nversion = \"0.1.0\"\n\n[dependencies]\n"
        );
        fs::write(project_dir.join("tl.toml"), &manifest).unwrap();
        fs::write(
            src_dir.join("main.tl"),
            format!("print(\"Hello from {project_name}!\")\n"),
        )
        .unwrap();

        // Verify the manifest has [dependencies] section
        let content = fs::read_to_string(project_dir.join("tl.toml")).unwrap();
        assert!(
            content.contains("[dependencies]"),
            "tl init should include [dependencies] section"
        );

        // Verify it parses correctly with tl_package::Manifest
        let m = tl_package::Manifest::from_toml(&content).unwrap();
        assert_eq!(m.project.name, project_name);
        assert!(m.dependencies.is_empty());
    }

    #[test]
    fn test_manifest_backward_compat() {
        // Old-style manifest without [dependencies] should still work
        let toml = r#"
[project]
name = "legacy"
version = "0.1.0"
"#;
        let m = tl_package::Manifest::from_toml(toml).unwrap();
        assert_eq!(m.project.name, "legacy");
        assert!(m.dependencies.is_empty());
    }

    #[test]
    fn test_manifest_with_deps() {
        let toml = r#"
[project]
name = "myapp"
version = "0.1.0"

[dependencies]
mylib = { path = "../mylib" }
remote = { git = "https://github.com/user/remote.git", branch = "main" }
"#;
        let m = tl_package::Manifest::from_toml(toml).unwrap();
        assert_eq!(m.dependencies.len(), 2);
        assert!(m.dependencies.contains_key("mylib"));
        assert!(m.dependencies.contains_key("remote"));
    }

    #[test]
    fn test_tab_completion_includes_package_commands() {
        let helper = TlHelper::new();
        // Check that package management keywords are in completions
        assert!(helper.completions.contains(&"add".to_string()));
        assert!(helper.completions.contains(&"remove".to_string()));
        assert!(helper.completions.contains(&"install".to_string()));
        assert!(helper.completions.contains(&"update".to_string()));
        assert!(helper.completions.contains(&"publish".to_string()));
        assert!(helper.completions.contains(&"search".to_string()));
    }
}
