//! Application error type and the JSON error envelope.
//!
//! Handlers return [`AppResult<T>`] and `?`-propagate. Every error renders as a
//! consistent envelope (see `CONVENTIONS.md`):
//!
//! ```json
//! { "error": { "code": "not_found", "message": "project abc not found" } }
//! ```
//!
//! Internal failures (database errors, unexpected conditions) are logged in full
//! via `tracing` but reported to clients as a generic `internal` error so we
//! never leak implementation detail.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

use crate::DbError;

/// Convenience alias for handler return types.
pub type AppResult<T> = Result<T, AppError>;

/// The single error type all handlers surface.
#[derive(Debug, Error)]
pub enum AppError {
    /// 400 — the request was malformed or failed validation.
    #[error("{0}")]
    BadRequest(String),
    /// 401 — missing or invalid credentials.
    #[error("unauthorized")]
    Unauthorized,
    /// 404 — the addressed resource does not exist.
    #[error("{0}")]
    NotFound(String),
    /// 409 — the request conflicts with current state (e.g. a cycle, a dup).
    #[error("{0}")]
    Conflict(String),
    /// 500 — an unexpected internal failure (message not shown to clients).
    #[error("{0}")]
    Internal(String),
    /// 500 — a database error (logged in full, reported generically).
    #[error(transparent)]
    Db(#[from] DbError),
}

/// Let handlers `?`-propagate raw `libsql` query errors (from `query`/`execute`/
/// `Row::get`) straight into the generic-500 `Db` path, same as a [`DbError`].
impl From<libsql::Error> for AppError {
    fn from(err: libsql::Error) -> Self {
        AppError::Db(DbError::Libsql(err))
    }
}

impl AppError {
    /// The HTTP status and stable machine-readable error code for this error.
    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            AppError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            AppError::Internal(_) | AppError::Db(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal")
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();

        // Client-facing message: precise for 4xx, generic for 5xx (and log the
        // real cause so it is never lost).
        let message = if status.is_server_error() {
            tracing::error!(error.code = code, error.detail = %self, "request failed");
            "internal server error".to_string()
        } else {
            self.to_string()
        };

        let body = Json(json!({
            "error": { "code": code, "message": message }
        }));

        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::{routing::get, Router};
    use serde_json::Value;
    use tower::ServiceExt; // for `oneshot`

    async fn body_json(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn maps_variants_to_status_and_code() {
        assert_eq!(
            AppError::BadRequest("x".into()).status_and_code(),
            (StatusCode::BAD_REQUEST, "bad_request")
        );
        assert_eq!(
            AppError::NotFound("x".into()).status_and_code(),
            (StatusCode::NOT_FOUND, "not_found")
        );
        assert_eq!(
            AppError::Conflict("x".into()).status_and_code(),
            (StatusCode::CONFLICT, "conflict")
        );
        assert_eq!(
            AppError::Unauthorized.status_and_code(),
            (StatusCode::UNAUTHORIZED, "unauthorized")
        );
        assert_eq!(
            AppError::Internal("x".into()).status_and_code(),
            (StatusCode::INTERNAL_SERVER_ERROR, "internal")
        );
    }

    #[tokio::test]
    async fn client_error_renders_full_envelope() {
        let err = AppError::NotFound("project abc not found".into());
        let response = err.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            body_json(response).await,
            json!({ "error": { "code": "not_found", "message": "project abc not found" } })
        );
    }

    #[tokio::test]
    async fn internal_error_hides_detail() {
        let response = AppError::Internal("db password was 'hunter2'".into()).into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body_json(response).await,
            json!({ "error": { "code": "internal", "message": "internal server error" } })
        );
    }

    #[tokio::test]
    async fn deliberately_failing_handler_returns_error_shape() {
        // A handler that `?`-propagates an AppError produces the envelope.
        async fn boom() -> AppResult<&'static str> {
            Err(AppError::BadRequest("nope".into()))
        }

        let response = Router::new()
            .route("/boom", get(boom))
            .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_json(response).await,
            json!({ "error": { "code": "bad_request", "message": "nope" } })
        );
    }
}
