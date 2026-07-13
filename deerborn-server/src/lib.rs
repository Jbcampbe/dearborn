//! Deerborn server library.
//!
//! Keeps app-construction (router, handlers, shared state) separate from the
//! binary entrypoint so later tasks can add modules and integration tests
//! cleanly.

pub mod auth;
pub mod config;
pub mod db;

use std::sync::Arc;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};

pub use config::{Config, ConfigError};
pub use db::{Db, DbError};

/// Shared application state handed to handlers and middleware.
///
/// `Clone` is cheap: everything inside is reference-counted.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
}

impl AppState {
    /// Construct shared state from a resolved [`Config`] and open [`Db`].
    pub fn new(config: Config, db: Db) -> AppState {
        AppState {
            config: Arc::new(config),
            db,
        }
    }
}

/// Build the application router.
///
/// `/health` is public; every other route sits behind the bearer-token layer.
pub fn app(state: AppState) -> Router {
    let public = Router::new().route("/health", get(health));

    let protected = Router::new()
        .route("/whoami", get(whoami))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ));

    public.merge(protected).with_state(state)
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
