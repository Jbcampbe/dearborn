//! Deerborn server library.
//!
//! Keeps app-construction (router, handlers, shared state) separate from the
//! binary entrypoint so later tasks can add modules and integration tests
//! cleanly.

pub mod auth;
pub mod config;
pub mod crypto;
pub mod db;
pub mod error;
pub mod hub;
pub mod projects;
pub mod ws;

use std::sync::Arc;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::trace::TraceLayer;

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
/// `/health` is public; every other route sits behind the bearer-token layer.
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
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ));

    public
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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
}
