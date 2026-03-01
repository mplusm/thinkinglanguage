// ThinkingLanguage — LSP Document State
// Manages open documents, re-parsing and re-checking on changes.

use lsp_types::{Diagnostic, Uri};
use std::collections::HashMap;
use tl_ast::Program;
use tl_types::checker::{CheckResult, CheckerConfig, check_program};

use crate::diagnostics::build_diagnostics;

pub struct DocumentData {
    pub source: String,
    pub version: i32,
    pub ast: Option<Program>,
    pub check_result: Option<CheckResult>,
}

pub struct ServerState {
    pub documents: HashMap<Uri, DocumentData>,
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerState {
    pub fn new() -> Self {
        ServerState {
            documents: HashMap::new(),
        }
    }

    /// Open or update a document. Returns diagnostics to publish.
    pub fn update_document(&mut self, uri: Uri, source: String, version: i32) -> Vec<Diagnostic> {
        let (ast, parse_errors) = match tl_parser::parse(&source) {
            Ok(program) => (Some(program), vec![]),
            Err(tl_errors::TlError::Parser(e)) => (None, vec![e]),
            Err(_) => (None, vec![]),
        };

        let check_result = ast.as_ref().map(|program| {
            let config = CheckerConfig::default();
            check_program(program, &config)
        });

        let type_errors = check_result
            .as_ref()
            .map(|r| r.errors.as_slice())
            .unwrap_or(&[]);
        let type_warnings = check_result
            .as_ref()
            .map(|r| r.warnings.as_slice())
            .unwrap_or(&[]);

        let diagnostics = build_diagnostics(&source, &parse_errors, type_errors, type_warnings);

        self.documents.insert(
            uri,
            DocumentData {
                source,
                version,
                ast,
                check_result,
            },
        );

        diagnostics
    }

    pub fn close_document(&mut self, uri: &Uri) {
        self.documents.remove(uri);
    }

    pub fn get_document(&self, uri: &Uri) -> Option<&DocumentData> {
        self.documents.get(uri)
    }
}
