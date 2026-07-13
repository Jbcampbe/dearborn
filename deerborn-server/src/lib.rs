//! Deerborn server library.
//!
//! Keeps the app-construction logic (router, handlers) separate from the binary
//! entrypoint so later tasks can add modules and integration tests cleanly.

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};

/// Default bind address when `DEERBORN_BIND` is unset.
/// Full config handling lands in T-002; this is intentionally minimal.
pub const DEFAULT_BIND: &str = "127.0.0.1:8787";

/// Resolve the bind address from the environment, falling back to [`DEFAULT_BIND`].
pub fn bind_addr() -> String {
    std::env::var("DEERBORN_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string())
}

/// Build the application router. Later tasks extend this (auth, REST, WS, SPA).
pub fn app() -> Router {
    Router::new().route("/health", get(health))
}

/// Liveness probe. Returns `200 OK` with a small JSON body.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    #[tokio::test]
    async fn health_returns_200_ok() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body, json!({ "status": "ok" }));
    }

    #[test]
    fn bind_addr_defaults_when_unset() {
        // Only assert the default constant is well-formed; env is process-global.
        assert!(DEFAULT_BIND.parse::<std::net::SocketAddr>().is_ok());
    }
}
