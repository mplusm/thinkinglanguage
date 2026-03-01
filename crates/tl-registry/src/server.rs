// ThinkingLanguage — Registry HTTP Server
// Licensed under MIT OR Apache-2.0
//
// Axum-based HTTP server for the package registry.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::RegistryStorage;

/// Shared server state.
pub struct AppState {
    pub storage: Mutex<RegistryStorage>,
}

/// Request body for publishing a package.
#[derive(Deserialize)]
pub struct PublishRequest {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    /// Base64-encoded tarball
    pub tarball: String,
}

/// Response after publishing.
#[derive(Serialize)]
pub struct PublishResponse {
    pub name: String,
    pub version: String,
    pub sha256: String,
}

/// Search query parameters.
#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
}

/// Build the Axum router.
pub fn build_router(storage: RegistryStorage) -> Router {
    let state = Arc::new(AppState {
        storage: Mutex::new(storage),
    });

    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/packages", post(publish_package))
        .route("/api/v1/packages/{name}", get(get_package_info))
        .route(
            "/api/v1/packages/{name}/{version}/download",
            get(download_package),
        )
        .route("/api/v1/search", get(search_packages))
        .with_state(state)
}

/// Health check endpoint.
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Publish a new package version.
async fn publish_package(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PublishRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    use base64::Engine;
    let tarball = base64::engine::general_purpose::STANDARD
        .decode(&req.tarball)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Invalid base64 tarball: {e}"),
            )
        })?;

    let storage = state.storage.lock().await;
    let sha256 = storage
        .publish(&req.name, &req.version, req.description.as_deref(), &tarball)
        .map_err(|e| (StatusCode::CONFLICT, e))?;

    Ok((
        StatusCode::CREATED,
        Json(PublishResponse {
            name: req.name,
            version: req.version,
            sha256,
        }),
    ))
}

/// Get package metadata.
async fn get_package_info(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let storage = state.storage.lock().await;
    match storage.load_metadata(&name) {
        Ok(Some(meta)) => Ok(Json(meta)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            format!("Package '{name}' not found"),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

/// Download a package tarball.
async fn download_package(
    State(state): State<Arc<AppState>>,
    Path((name, version)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let storage = state.storage.lock().await;
    match storage.download(&name, &version) {
        Ok(data) => Ok((
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "application/gzip",
            )],
            data,
        )),
        Err(e) => Err((StatusCode::NOT_FOUND, e)),
    }
}

/// Search for packages.
async fn search_packages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let storage = state.storage.lock().await;
    let results = match &params.q {
        Some(q) if !q.is_empty() => storage.search(q),
        _ => storage.list_all(),
    }
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(results))
}
