//! Deerborn server library.
//!
//! Keeps app-construction (router, handlers, shared state) separate from the
//! binary entrypoint so later tasks can add modules and integration tests
//! cleanly.

pub mod auth;
pub mod config;
pub mod crypto;
pub mod db;
pub mod epics;
pub mod error;
pub mod git;
pub mod hub;
pub mod projects;
pub mod ws;

use std::sync::Arc;

use std::path::Path;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

pub use config::{Config, ConfigError};
pub use crypto::MasterKey;
pub use db::{Db, DbError};
pub use error::{AppError, AppResult};
pub use hub::Hub;

/// Initialise the global `tracing` subscriber. Idempotent; safe to skip in tests.
/// Honours `RUST_LOG`, defaulting to `info`.
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,deerborn_server=debug"));
    // `try_init` returns Err if a subscriber is already set — ignore it.
    let _ = fmt().with_env_filter(filter).try_init();
}

/// Shared application state handed to handlers and middleware.
///
/// `Clone` is cheap: everything inside is reference-counted.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    /// Topic pub/sub broadcaster for live WebSocket subscriptions. Server-side
    /// code publishes events via `state.hub.publish(topic, type, payload)`.
    pub hub: Arc<Hub>,
    /// AES-256 key (derived from `DEERBORN_MASTER_KEY`) used to encrypt/decrypt
    /// per-project PATs. Never serialised or logged.
    pub crypto: Arc<MasterKey>,
}

impl AppState {
    /// Construct shared state from a resolved [`Config`] and open [`Db`].
    ///
    /// The master key is derived here; `config.master_key` is guaranteed
    /// non-empty by config loading, so derivation cannot fail. Boot code should
    /// nevertheless call [`MasterKey::derive`] first to fail fast (see `main`).
    pub fn new(config: Config, db: Db) -> AppState {
        let crypto = MasterKey::derive(&config.master_key)
            .expect("master key material validated non-empty at config load");
        AppState {
            config: Arc::new(config),
            db,
            hub: Arc::new(Hub::new()),
            crypto: Arc::new(crypto),
        }
    }
}

/// Build the application router.
///
/// `/health` is public; every other API route sits behind the bearer-token
/// layer. Any request that matches **no** API route falls through to the SPA
/// static handler (the built Vite assets), so the HTML/JS load without auth and
/// the user can then enter their token — auth is enforced on the API calls the
/// SPA makes, not on serving the static shell.
pub fn app(state: AppState) -> Router {
    // `/health` is public; `/ws` authenticates the handshake in-handler (the
    // header-only bearer middleware would reject browser WS handshakes, which
    // carry the token in the query string instead).
    let public = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::ws_handler));

    let protected = Router::new()
        .route("/whoami", get(whoami))
        .route(
            "/projects",
            get(projects::list_projects).post(projects::create_project),
        )
        .route(
            "/projects/:id",
            get(projects::get_project)
                .patch(projects::update_project)
                .delete(projects::delete_project),
        )
        .route(
            "/projects/:id/refresh",
            axum::routing::post(projects::refresh_project),
        )
        .route(
            "/projects/:id/epics",
            get(epics::list_epics).post(epics::create_epic),
        )
        .route("/epics/:id", get(epics::get_epic))
        .route(
            "/epics/:id/messages",
            axum::routing::post(epics::post_message),
        )
        .route("/epics/:id/transcript", get(epics::get_transcript))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ));

    let mut router = public.merge(protected);

    // Serve the built SPA (and its client-side-routing fallback) for everything
    // the API routes above don't claim. Degrade gracefully if it isn't built.
    if let Some(spa) = spa_service(&state.config.static_dir) {
        router = router.fallback_service(spa);
    }

    router.layer(TraceLayer::new_for_http()).with_state(state)
}

/// Build the static-file service for the built SPA at `dir`, or `None` if `dir`
/// doesn't exist (dev without a client build). `ServeDir` serves real asset
/// files; any unknown path (a client-side route like `/projects/123`) falls
/// back to `index.html` so the Vue router can take over — an SPA fallback.
///
/// Returning `None` (rather than crashing) lets `cargo run` still serve the API
/// when the client hasn't been built; a warning tells the operator how to fix it.
fn spa_service(static_dir: &str) -> Option<ServeDir<ServeFile>> {
    let dir = Path::new(static_dir);
    let index = dir.join("index.html");
    if !index.is_file() {
        tracing::warn!(
            static_dir = %static_dir,
            "no built SPA found (missing {}); serving API only — run `npm run build` in ./client",
            index.display()
        );
        return None;
    }
    tracing::info!(static_dir = %static_dir, "serving built SPA with client-side-routing fallback");
    Some(ServeDir::new(dir).fallback(ServeFile::new(index)))
}

/// Liveness probe. Public — returns `200 OK` with a small JSON body.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Authenticated token check. Useful for the client's token-entry screen.
async fn whoami() -> Json<Value> {
    Json(json!({ "status": "authenticated" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    const TOKEN: &str = "s3cret-token";

    async fn test_app() -> Router {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        app(AppState::new(Config::for_test(TOKEN), db))
    }

    /// Build an app whose SPA static dir is a freshly-created temp dir holding a
    /// sentinel `index.html`, so the static/SPA-fallback path is exercised.
    async fn test_app_with_spa(marker: &str) -> (Router, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("deerborn-spa-test-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), marker).unwrap();
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test(TOKEN);
        config.static_dir = dir.to_string_lossy().into_owned();
        (app(AppState::new(config, db)), dir)
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn health_is_public_and_returns_200_ok() {
        let response = test_app().await
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn protected_route_without_token_is_401() {
        let response = test_app().await
            .oneshot(Request::builder().uri("/whoami").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_wrong_token_is_401() {
        let response = test_app().await
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .header(AUTHORIZATION, "Bearer not-the-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_correct_token_is_200() {
        let response = test_app().await
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "status": "authenticated" }));
    }

    #[test]
    fn default_bind_is_well_formed() {
        assert!(config::DEFAULT_BIND.parse::<std::net::SocketAddr>().is_ok());
    }

    #[tokio::test]
    async fn spa_served_at_root_when_built() {
        let marker = "<!doctype html><title>deerborn-spa-marker</title>";
        let (app, dir) = test_app_with_spa(marker).await;
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, marker);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn unknown_client_route_falls_back_to_index_html() {
        let marker = "<!doctype html><title>deerborn-spa-marker</title>";
        let (app, dir) = test_app_with_spa(marker).await;
        // A client-side-routing path (not an API route, not a real file) must
        // return index.html so the Vue router can take over.
        let response = app
            .oneshot(Request::builder().uri("/foo/bar").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, marker);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn api_routes_win_over_spa_fallback() {
        let (app, dir) = test_app_with_spa("spa").await;
        // `/projects` is a real API route: it must still enforce auth (401),
        // never be shadowed by the static/SPA fallback.
        let response = app
            .oneshot(Request::builder().uri("/projects").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["error"]["code"], "unauthorized");
        std::fs::remove_dir_all(dir).ok();
    }
}
