// ThinkingLanguage — LSP Server Main Loop
// Uses lsp-server (sync) for stdio communication.

use lsp_server::{Connection, Message, Notification, Request, RequestId, Response};
use lsp_types::notification::{
    DidChangeTextDocument, DidCloseTextDocument, DidOpenTextDocument, Notification as _,
    PublishDiagnostics,
};
use lsp_types::request::{
    Completion, DocumentSymbolRequest, Formatting, GotoDefinition, HoverRequest, References,
    Rename, Request as LspRequest,
};
use lsp_types::{
    CompletionOptions, DocumentSymbolResponse, InitializeParams, OneOf, PublishDiagnosticsParams,
    ServerCapabilities, TextDocumentSyncCapability, TextDocumentSyncKind,
};

use crate::completion;
use crate::document::ServerState;
use crate::format;
use crate::goto_def;
use crate::hover;
use crate::symbols;

pub fn run_server() -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let (connection, io_threads) = Connection::stdio();

    let capabilities = ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Kind(TextDocumentSyncKind::FULL)),
        completion_provider: Some(CompletionOptions {
            trigger_characters: Some(vec![".".to_string()]),
            ..Default::default()
        }),
        hover_provider: Some(lsp_types::HoverProviderCapability::Simple(true)),
        definition_provider: Some(OneOf::Left(true)),
        document_symbol_provider: Some(OneOf::Left(true)),
        document_formatting_provider: Some(OneOf::Left(true)),
        references_provider: Some(OneOf::Left(true)),
        rename_provider: Some(OneOf::Left(true)),
        ..Default::default()
    };

    let server_capabilities = serde_json::to_value(capabilities)?;
    let init_params = connection.initialize(server_capabilities)?;
    let _params: InitializeParams = serde_json::from_value(init_params)?;

    main_loop(&connection)?;

    io_threads.join()?;
    Ok(())
}

fn main_loop(connection: &Connection) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let mut state = ServerState::new();

    for msg in &connection.receiver {
        match msg {
            Message::Request(req) => {
                if connection.handle_shutdown(&req)? {
                    return Ok(());
                }
                handle_request(&mut state, connection, req)?;
            }
            Message::Notification(not) => {
                handle_notification(&mut state, connection, not)?;
            }
            Message::Response(_) => {}
        }
    }
    Ok(())
}

fn handle_request(
    state: &mut ServerState,
    connection: &Connection,
    req: Request,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let id = req.id.clone();

    if let Some((_, params)) = cast_request::<Completion>(&req) {
        let doc = state.get_document(&params.text_document_position.text_document.uri);
        let items = if let Some(doc) = doc {
            completion::provide_completions(
                &doc.source,
                doc.ast.as_ref(),
                doc.check_result.as_ref(),
                params.text_document_position.position,
            )
        } else {
            completion::provide_completions("", None, None, params.text_document_position.position)
        };
        let result = lsp_types::CompletionResponse::Array(items);
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<HoverRequest>(&req) {
        let doc = state.get_document(&params.text_document_position_params.text_document.uri);
        let result = if let Some(doc) = doc {
            hover::provide_hover(
                &doc.source,
                doc.ast.as_ref(),
                doc.check_result.as_ref(),
                params.text_document_position_params.position,
            )
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<GotoDefinition>(&req) {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .clone();
        let doc = state.get_document(&uri);
        let result = if let Some(doc) = doc {
            goto_def::provide_goto_definition(
                &doc.source,
                doc.ast.as_ref(),
                params.text_document_position_params.position,
                &uri,
            )
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<DocumentSymbolRequest>(&req) {
        let doc = state.get_document(&params.text_document.uri);
        let result = if let Some(doc) = doc {
            let syms = symbols::provide_document_symbols(&doc.source, doc.ast.as_ref());
            Some(DocumentSymbolResponse::Flat(syms))
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<Formatting>(&req) {
        let doc = state.get_document(&params.text_document.uri);
        let result = if let Some(doc) = doc {
            format::provide_formatting(&doc.source)
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<References>(&req) {
        let uri = params.text_document_position.text_document.uri.clone();
        let doc = state.get_document(&uri);
        let result: Option<Vec<lsp_types::Location>> = if let Some(doc) = doc {
            let offset = crate::ast_util::position_to_offset(
                &doc.source,
                params.text_document_position.position.line,
                params.text_document_position.position.character,
            );
            if let Some((name, _, _)) = crate::ast_util::find_ident_at_offset(&doc.source, offset) {
                let refs = crate::ast_util::find_all_references(&doc.source, &name);
                if refs.is_empty() {
                    None
                } else {
                    Some(
                        refs.iter()
                            .map(|&(start, _end)| lsp_types::Location {
                                uri: uri.clone(),
                                range: crate::diagnostics::span_to_range(
                                    &doc.source,
                                    tl_errors::Span { start, end: _end },
                                ),
                            })
                            .collect(),
                    )
                }
            } else {
                None
            }
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    } else if let Some((_, params)) = cast_request::<Rename>(&req) {
        let uri = params.text_document_position.text_document.uri.clone();
        let doc = state.get_document(&uri);
        let result: Option<lsp_types::WorkspaceEdit> = if let Some(doc) = doc {
            let offset = crate::ast_util::position_to_offset(
                &doc.source,
                params.text_document_position.position.line,
                params.text_document_position.position.character,
            );
            if let Some((name, _, _)) = crate::ast_util::find_ident_at_offset(&doc.source, offset) {
                let refs = crate::ast_util::find_all_references(&doc.source, &name);
                if refs.is_empty() {
                    None
                } else {
                    let edits: Vec<lsp_types::TextEdit> = refs
                        .iter()
                        .map(|&(start, end)| lsp_types::TextEdit {
                            range: crate::diagnostics::span_to_range(
                                &doc.source,
                                tl_errors::Span { start, end },
                            ),
                            new_text: params.new_name.clone(),
                        })
                        .collect();
                    #[allow(clippy::mutable_key_type)]
                    let mut changes = std::collections::HashMap::new();
                    changes.insert(uri, edits);
                    Some(lsp_types::WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    })
                }
            } else {
                None
            }
        } else {
            None
        };
        send_response(connection, id, serde_json::to_value(result)?)?;
    }
    Ok(())
}

fn handle_notification(
    state: &mut ServerState,
    connection: &Connection,
    not: Notification,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    if not.method == DidOpenTextDocument::METHOD {
        let params: lsp_types::DidOpenTextDocumentParams = serde_json::from_value(not.params)?;
        let uri = params.text_document.uri.clone();
        let diagnostics = state.update_document(
            params.text_document.uri,
            params.text_document.text,
            params.text_document.version,
        );
        publish_diagnostics(connection, uri, diagnostics)?;
    } else if not.method == DidChangeTextDocument::METHOD {
        let params: lsp_types::DidChangeTextDocumentParams = serde_json::from_value(not.params)?;
        let uri = params.text_document.uri.clone();
        // Full sync: use the last content change
        if let Some(change) = params.content_changes.into_iter().last() {
            let diagnostics = state.update_document(
                params.text_document.uri,
                change.text,
                params.text_document.version,
            );
            publish_diagnostics(connection, uri, diagnostics)?;
        }
    } else if not.method == DidCloseTextDocument::METHOD {
        let params: lsp_types::DidCloseTextDocumentParams = serde_json::from_value(not.params)?;
        state.close_document(&params.text_document.uri);
    }
    Ok(())
}

fn cast_request<R: LspRequest>(req: &Request) -> Option<(RequestId, R::Params)>
where
    R::Params: serde::de::DeserializeOwned,
{
    if req.method == R::METHOD {
        let params: R::Params = serde_json::from_value(req.params.clone()).ok()?;
        Some((req.id.clone(), params))
    } else {
        None
    }
}

fn send_response(
    connection: &Connection,
    id: RequestId,
    result: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let resp = Response::new_ok(id, result);
    connection.sender.send(Message::Response(resp))?;
    Ok(())
}

fn publish_diagnostics(
    connection: &Connection,
    uri: lsp_types::Uri,
    diagnostics: Vec<lsp_types::Diagnostic>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let params = PublishDiagnosticsParams {
        uri,
        diagnostics,
        version: None,
    };
    let not = Notification::new(PublishDiagnostics::METHOD.to_string(), params);
    connection.sender.send(Message::Notification(not))?;
    Ok(())
}
