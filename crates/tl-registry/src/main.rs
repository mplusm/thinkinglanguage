// ThinkingLanguage — Registry Server Binary
// Licensed under MIT OR Apache-2.0

use tl_registry::server::build_router;
use tl_registry::storage::RegistryStorage;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("TL_REGISTRY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3333);

    let storage = RegistryStorage::default_location().unwrap_or_else(|e| {
        eprintln!("Failed to initialize storage: {e}");
        std::process::exit(1);
    });

    let app = build_router(storage);
    let addr = format!("0.0.0.0:{port}");
    println!("TL Package Registry listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {addr}: {e}");
            std::process::exit(1);
        });

    axum::serve(listener, app).await.unwrap_or_else(|e| {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    });
}
