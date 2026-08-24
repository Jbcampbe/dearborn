//! Session store and the public `/auth/*` surface (technical plan §4, §5).
//!
//! A logged-in device holds **two** credentials, and the split is the whole of
//! "revocation is eventual, bounded by the access-token lifetime":
//!
//! - an **access token** — self-contained and signed by [`crate::auth::AuthKey`],
//!   verified with no database read at all, so the hot path stays one HMAC;
//! - a **refresh token** — 256 bits of `OsRng`, opaque, whose SHA-256 digest is
//!   a `session` row. The plaintext is returned to the caller **once**, at
//!   issuance, and is never stored: the column holds only the digest, so a leaked
//!   database file yields no usable session.
//!
//! [`refresh`] is therefore the single choke point that re-reads `active` and
//! the current `role` from the user row. Deactivation, an admin password reset,
//! and a demotion all take effect *there* — at most one access-token lifetime
//! after the fact.
//!
//! ## No reaper task
//!
//! Expired `session` rows are pruned **opportunistically on refresh**
//! ([`prune_expired`]), the same implicit-expiry pattern the executor's leases
//! already use. Nothing in the system depends on an expired row being gone
//! promptly — [`resolve_refresh`] rejects one by `expires_at` whether or not the
//! sweep has reached it yet.
//!
//! ## Refresh tokens do not rotate
//!
//! A session's refresh token is fixed for its life, with an absolute expiry at
//! issuance + TTL. Rotation-on-use is better hygiene in a hostile setting, but
//! two browser tabs refreshing concurrently would then race, and the loser would
//! be logged out. For a self-hosted instance on `127.0.0.1` shared by a handful
//! of trusted engineers, the simpler design is the right trade.
//!
//! ## Indistinguishable login failures
//!
//! [`login`] answers an unknown username, a wrong password, and a deactivated
//! account with the **same bytes** ([`AppError::InvalidCredentials`]). It also
//! makes the three cost the same: an unknown username is verified against a
//! fixed dummy Argon2 hash ([`dummy_hash`]) and a found-but-inactive user's
//! password is verified *before* the `active` check, so response **time** does
//! not distinguish them either.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{rand_core::RngCore, OsRng};
use axum::{extract::State, http::StatusCode, Json};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use libsql::params;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::{AuthKey, Claims};
use crate::config::AuthConfig;
use crate::db::Db;
use crate::users::{self, Role, User};
use crate::{AppError, AppResult, AppState};

/// Bytes of `OsRng` behind every refresh token (256 bits), base64url-encoded
/// for transport. Far beyond guessing, which is why the stored digest can be a
/// fast SHA-256 rather than an Argon2 hash — stretching adds cost and no
/// security to a value with this much entropy.
const REFRESH_TOKEN_BYTES: usize = 32;

/// The password [`dummy_hash`] hashes. Its value is irrelevant — it exists only
/// so an unknown username still pays for one Argon2 verification.
const DUMMY_PASSWORD: &str = "dearborn/no-such-user/timing-equalizer";

// ---- Model ------------------------------------------------------------------

/// One logged-in device: a `session` row, without the refresh-token digest.
///
/// Like [`User`] and its `password_hash`, the secret-bearing column simply is
/// not a field here, so nothing downstream can leak it by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// ULID; the `sid` claim carried by every access token this session mints.
    pub id: String,
    pub user_id: String,
    pub created_at: i64,
    /// Absolute expiry, unix ms. Fixed at issuance — refresh does not extend it.
    pub expires_at: i64,
    pub last_used_at: i64,
}

/// The columns projected into a [`Session`]. Note the absence of
/// `refresh_token_hash`.
const SESSION_COLUMNS: &str = "id, user_id, created_at, expires_at, last_used_at";

fn row_to_session(row: &libsql::Row) -> AppResult<Session> {
    Ok(Session {
        id: row.get(0)?,
        user_id: row.get(1)?,
        created_at: row.get(2)?,
        expires_at: row.get(3)?,
        last_used_at: row.get(4)?,
    })
}

/// Everything a successful [`issue`] produces. The two plaintext tokens exist
/// only in this struct and the response built from it — neither is ever written
/// to the database.
#[derive(Debug, Clone)]
pub struct IssuedSession {
    /// The `session` row that was created.
    pub session: Session,
    /// The signed access token (`v1.<payload>.<mac>`).
    pub access_token: String,
    /// The access token's expiry, unix ms.
    pub expires_at: i64,
    /// The opaque refresh token, in the clear. Returned to the caller **once**.
    pub refresh_token: String,
    /// The refresh token's absolute expiry, unix ms.
    pub refresh_expires_at: i64,
}

// ---- Store ------------------------------------------------------------------

/// Mint a session for `user`: a new `session` row, a signed access token, and a
/// fresh opaque refresh token.
///
/// The refresh token is generated here and **only its SHA-256 hex digest is
/// stored**; the plaintext is returned in [`IssuedSession`] and never persisted.
///
/// Takes the full [`AuthConfig`] rather than a single duration because a session
/// has two lifetimes, and pairing them in one argument keeps a caller from
/// passing the access TTL where the refresh TTL belongs.
pub async fn issue(
    db: &Db,
    key: &AuthKey,
    user: &User,
    ttl: &AuthConfig,
) -> AppResult<IssuedSession> {
    let now = now_ms();
    let id = ulid::Ulid::new().to_string();
    let refresh_token = generate_refresh_token();
    let refresh_expires_at = now + secs_to_ms(ttl.refresh_ttl_secs);
    let expires_at = now + secs_to_ms(ttl.access_ttl_secs);

    db.conn()
        .execute(
            "INSERT INTO session \
                 (id, user_id, refresh_token_hash, created_at, expires_at, last_used_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id.clone(),
                user.id.clone(),
                digest_hex(&refresh_token),
                now,
                refresh_expires_at,
                now,
            ],
        )
        .await?;

    let access_token = key.mint(&Claims {
        sub: user.id.clone(),
        sid: id.clone(),
        role: user.role,
        exp: expires_at,
    });

    Ok(IssuedSession {
        session: Session {
            id,
            user_id: user.id.clone(),
            created_at: now,
            expires_at: refresh_expires_at,
            last_used_at: now,
        },
        access_token,
        expires_at,
        refresh_token,
        refresh_expires_at,
    })
}

/// Look up the live session a refresh token names, or `None` if there is none.
///
/// `None` covers **unknown**, **expired**, and **revoked** identically — the
/// caller has no business telling those apart, and neither does the client.
///
/// Expired rows are swept here ([`prune_expired`]) before the lookup, which is
/// the whole of the "no reaper task" story. The sweep is an optimisation, not
/// the check: the `expires_at` predicate below would reject an expired row on
/// its own if the sweep had never run.
pub async fn resolve_refresh(db: &Db, refresh_token: &str) -> AppResult<Option<Session>> {
    prune_expired(db).await?;

    let now = now_ms();
    let sql = format!(
        "SELECT {SESSION_COLUMNS} FROM session \
         WHERE refresh_token_hash = ?1 AND expires_at > ?2"
    );
    let mut rows = db
        .conn()
        .query(&sql, params![digest_hex(refresh_token), now])
        .await?;
    rows.next()
        .await?
        .map(|row| row_to_session(&row))
        .transpose()
}

/// Record that a session was just used, so an idle-session view has something
/// to render later. Best-effort in the sense that it never gates the refresh.
pub async fn touch(db: &Db, sid: &str) -> AppResult<()> {
    db.conn()
        .execute(
            "UPDATE session SET last_used_at = ?2 WHERE id = ?1",
            params![sid, now_ms()],
        )
        .await?;
    Ok(())
}

/// End one session. Idempotent: revoking an already-gone session is a clean
/// no-op, which is what makes a double `POST /auth/logout` harmless.
pub async fn revoke(db: &Db, sid: &str) -> AppResult<()> {
    db.conn()
        .execute("DELETE FROM session WHERE id = ?1", params![sid])
        .await?;
    Ok(())
}

/// End **every** session a user holds — the admin password-reset and
/// deactivation hammer. Returns how many were ended.
pub async fn revoke_all_for_user(db: &Db, user_id: &str) -> AppResult<u64> {
    Ok(db
        .conn()
        .execute("DELETE FROM session WHERE user_id = ?1", params![user_id])
        .await?)
}

/// End every session a user holds **except** `sid` — the self-service
/// password-change form, which logs the other devices out without logging the
/// person changing their password out of the tab they are sitting in.
pub async fn revoke_all_for_user_except(db: &Db, user_id: &str, sid: &str) -> AppResult<u64> {
    Ok(db
        .conn()
        .execute(
            "DELETE FROM session WHERE user_id = ?1 AND id <> ?2",
            params![user_id, sid],
        )
        .await?)
}

/// Delete every session whose absolute expiry has passed. Called from
/// [`resolve_refresh`]; there is no background reaper.
pub async fn prune_expired(db: &Db) -> AppResult<u64> {
    Ok(db
        .conn()
        .execute("DELETE FROM session WHERE expires_at <= ?1", params![now_ms()])
        .await?)
}

// ---- Wire types -------------------------------------------------------------

/// What a successful setup or login returns: both credentials, both expiries,
/// and the user they belong to.
#[derive(Debug, Serialize)]
pub struct SessionEnvelope {
    pub access_token: String,
    pub expires_at: i64,
    pub refresh_token: String,
    pub refresh_expires_at: i64,
    /// The authenticated user. Carries no `password_hash` — [`User`] has no
    /// such field to serialize.
    pub user: User,
}

impl SessionEnvelope {
    fn new(issued: IssuedSession, user: User) -> SessionEnvelope {
        SessionEnvelope {
            access_token: issued.access_token,
            expires_at: issued.expires_at,
            refresh_token: issued.refresh_token,
            refresh_expires_at: issued.refresh_expires_at,
            user,
        }
    }
}

/// `GET /auth/status` — whether this instance is still unclaimed.
#[derive(Debug, Serialize)]
pub struct AuthStatus {
    pub setup_required: bool,
}

/// `POST /auth/setup` body.
#[derive(Debug, Deserialize)]
pub struct SetupRequest {
    pub username: String,
    pub display_name: String,
    pub password: String,
}

/// `POST /auth/login` body.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// `POST /auth/refresh` body.
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// `POST /auth/refresh` response. Deliberately **not** a full
/// [`SessionEnvelope`]: refresh tokens do not rotate, so echoing the one the
/// caller just presented would only widen its exposure for no benefit.
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub access_token: String,
    pub expires_at: i64,
    pub user: User,
}

// ---- Handlers ---------------------------------------------------------------

/// `GET /auth/status` — the SPA's cheap boot probe: create-admin form or login
/// form? Public and unauthenticated by necessity.
pub async fn auth_status(State(state): State<AppState>) -> AppResult<Json<AuthStatus>> {
    Ok(Json(AuthStatus {
        setup_required: !state.instance_claimed().await?,
    }))
}

/// `POST /auth/setup` — claim an unclaimed instance, creating the first user as
/// an `admin` and logging them straight in (`201` with a session envelope).
///
/// `409` once any user exists; `400` if the password is short of the policy.
///
/// The path is unauthenticated by necessity, and the product accepts the
/// consequence: whoever reaches a freshly started instance first claims it.
/// Dearborn binds to `127.0.0.1` by default, and operators exposing it more
/// broadly are expected to claim it immediately.
pub async fn setup(
    State(state): State<AppState>,
    Json(req): Json<SetupRequest>,
) -> AppResult<(StatusCode, Json<SessionEnvelope>)> {
    if state.instance_claimed().await? {
        return Err(already_claimed());
    }

    // The first user is an admin by construction — the role is not in the
    // request body, so there is no way to claim an instance as a non-admin and
    // lock it out of its own user management.
    let user = users::create(
        &state.db,
        &req.username,
        &req.display_name,
        &req.password,
        Role::Admin,
        state.config.argon2_fast,
    )
    .await
    // A duplicate username here means a concurrent setup won the race between
    // the claim check above and this insert. Report it as the same conflict a
    // second setup call gets, rather than as "username taken" — from the
    // caller's point of view the instance is simply already claimed.
    .map_err(|err| match err {
        AppError::Conflict(_) => already_claimed(),
        other => other,
    })?;
    state.claimed.store(true, Ordering::Relaxed);

    let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth).await?;
    Ok((
        StatusCode::CREATED,
        Json(SessionEnvelope::new(issued, user)),
    ))
}

/// `POST /auth/login` — username + password, `200` with a session envelope.
///
/// Every failure mode answers [`AppError::InvalidCredentials`] — one status,
/// one code, one message. See the module doc for why the *timing* matches too.
pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> AppResult<Json<SessionEnvelope>> {
    if !state.instance_claimed().await? {
        return Err(AppError::SetupRequired);
    }

    let found = users::get_by_username(&state.db, req.username.trim()).await?;
    // Verify against the real hash when there is one and a fixed dummy when
    // there is not, so both branches do the same Argon2 work. Note this runs
    // *before* the `active` check, so a deactivated account costs the same as a
    // live one too.
    let phc = match &found {
        Some(user) => users::password_hash_of(&state.db, &user.id).await?,
        None => None,
    };
    let phc = match phc {
        Some(hash) => hash,
        None => dummy_hash(state.config.argon2_fast).await?.to_string(),
    };
    let password_ok = users::verify_password(&req.password, &phc).await?;

    let user = match found {
        Some(user) if password_ok && user.active => user,
        _ => return Err(AppError::InvalidCredentials),
    };

    let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth).await?;
    Ok(Json(SessionEnvelope::new(issued, user)))
}

/// `POST /auth/refresh` — trade a refresh token for a new access token.
///
/// **The** freshness checkpoint: the user row is re-read here, so the minted
/// token carries the user's *current* role and a deactivated user is turned
/// away. `401` for an unknown, expired, or revoked refresh token, and for a
/// user who is no longer active.
pub async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> AppResult<Json<RefreshResponse>> {
    let session = resolve_refresh(&state.db, &req.refresh_token)
        .await?
        .ok_or(AppError::Unauthorized)?;

    // Re-read the row rather than trusting anything minted earlier: this is
    // where deactivation, a password reset, and a demotion all land.
    let user = users::get(&state.db, &session.user_id)
        .await?
        .ok_or(AppError::Unauthorized)?;
    if !user.active {
        // A deactivated user's sessions are dead weight — drop this one on the
        // way out so the row does not sit until its absolute expiry.
        revoke(&state.db, &session.id).await?;
        return Err(AppError::Unauthorized);
    }

    let expires_at = now_ms() + secs_to_ms(state.config.auth.access_ttl_secs);
    let access_token = state.auth_key.mint(&Claims {
        sub: user.id.clone(),
        sid: session.id.clone(),
        role: user.role,
        exp: expires_at,
    });
    touch(&state.db, &session.id).await?;

    Ok(Json(RefreshResponse {
        access_token,
        expires_at,
        user,
    }))
}

// ---- Helpers ----------------------------------------------------------------

/// The conflict a second `POST /auth/setup` gets. One message, one place.
fn already_claimed() -> AppError {
    AppError::Conflict("this instance has already been set up".to_string())
}

/// A fresh opaque refresh token: [`REFRESH_TOKEN_BYTES`] of `OsRng`, encoded
/// unpadded base64url so it is safe in a JSON body and a header alike.
///
/// `OsRng` arrives via `aes-gcm`'s re-exported `aead::OsRng`, already in the
/// tree for nonce generation — no new `rand` dependency.
fn generate_refresh_token() -> String {
    let mut bytes = [0u8; REFRESH_TOKEN_BYTES];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// SHA-256 hex digest of a refresh token — the only form of it that is ever
/// written down.
fn digest_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The fixed hash an unknown username is verified against, so "no such user"
/// costs the same as "wrong password".
///
/// Computed once per process **per cost setting** and cached: the production
/// and fast parameters produce different work, and a test suite must not pay
/// production Argon2 for the privilege of a constant.
async fn dummy_hash(fast: bool) -> AppResult<&'static str> {
    static PRODUCTION: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    static FAST: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

    let cell = if fast { &FAST } else { &PRODUCTION };
    cell.get_or_try_init(|| users::hash_password(DUMMY_PASSWORD, fast))
        .await
        .map(String::as_str)
}

/// Seconds → milliseconds, saturating. The `*_at` columns are unix ms; a TTL
/// large enough to overflow is a misconfiguration that should clamp rather than
/// wrap into the past.
fn secs_to_ms(secs: u64) -> i64 {
    i64::try_from(secs.saturating_mul(1_000)).unwrap_or(i64::MAX)
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- Test fixtures ----------------------------------------------------------

/// Fixtures the rest of the epic's tests authenticate with.
///
/// Unconditionally `pub` (not `#[cfg(test)]`) for the same reason
/// [`crate::users::testing`] is: an integration test in `tests/` compiles as
/// its own crate and never sees anything gated behind *this* crate's
/// `#[cfg(test)]`.
pub mod testing {
    use super::*;

    /// Log `user` in without going through HTTP, returning a bearer-ready
    /// access token — the replacement for the epic's old `const TOKEN`.
    ///
    /// Issues a real session, so the returned token carries a real `sid` and
    /// the matching row exists: a test can log out, revoke, or refresh from it
    /// exactly as a browser would.
    ///
    /// Panics on failure — a fixture that cannot log in has nothing useful to
    /// report to the test that called it.
    pub async fn login_as(state: &AppState, user: &User) -> String {
        issue(&state.db, &state.auth_key, user, &state.config.auth)
            .await
            .unwrap_or_else(|err| panic!("login_as({}): {err}", user.username))
            .access_token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::testing::{seed_user, SEED_PASSWORD};
    use crate::{app, Config};
    use axum::body::Body;
    use axum::http::Request;
    use axum::Router;
    use base64::Engine as _;
    use serde_json::{json, Value};
    use tower::ServiceExt; // for `oneshot`

    async fn test_state() -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        AppState::new(Config::for_test("unused"), db)
    }

    /// A state backed by a real file on disk, plus the temp directory holding
    /// it — the only way to rebuild an `AppState` over the *same* data and
    /// actually prove a session survives a restart (an in-memory database
    /// would not). The directory (not just the `.db`) is what gets removed, so
    /// libSQL's `-wal`/`-shm` siblings go with it.
    async fn file_backed_state() -> (AppState, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dearborn-sessions-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let state = state_at(&dir).await;
        (state, dir)
    }

    async fn state_at(dir: &std::path::Path) -> AppState {
        let db = Db::connect(&dir.join("dearborn.db").to_string_lossy())
            .await
            .unwrap();
        db.run_migrations().await.unwrap();
        AppState::new(Config::for_test("unused"), db)
    }

    fn post(uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    async fn body_bytes(response: axum::response::Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        serde_json::from_slice(&body_bytes(response).await).unwrap()
    }

    /// `POST /auth/login`, returning the raw response so a test can assert on
    /// its exact bytes.
    async fn login_response(router: Router, username: &str, password: &str) -> Vec<u8> {
        let response = router
            .oneshot(post(
                "/auth/login",
                json!({ "username": username, "password": password }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        body_bytes(response).await
    }

    /// How many `session` rows exist right now.
    async fn session_count(state: &AppState) -> i64 {
        let mut rows = state
            .db
            .conn()
            .query("SELECT COUNT(*) FROM session", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// The stored digest column for a session, read raw.
    async fn stored_digest(state: &AppState, sid: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT refresh_token_hash FROM session WHERE id = ?1",
                params![sid],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    // ---- GET /auth/status (AC1/AC3) -----------------------------------------

    #[tokio::test]
    async fn status_is_setup_required_on_an_empty_db_and_false_once_a_user_exists() {
        let state = test_state().await;

        let response = app(state.clone()).oneshot(get("/auth/status")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "setup_required": true }));

        seed_user(&state, "josiah", Role::Admin, true).await;

        let response = app(state.clone()).oneshot(get("/auth/status")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "setup_required": false }));
    }

    #[tokio::test]
    async fn status_is_public_and_needs_no_credentials() {
        // No Authorization header anywhere in these calls — the SPA has to be
        // able to ask this before it has anything to present.
        let state = test_state().await;
        let response = app(state).oneshot(get("/auth/status")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn claim_latch_is_monotonic_and_stops_counting_users() {
        let state = test_state().await;
        assert!(!state.instance_claimed().await.unwrap());
        assert!(
            !state.claimed.load(Ordering::Relaxed),
            "an unclaimed instance must not latch"
        );

        seed_user(&state, "josiah", Role::Admin, true).await;
        assert!(state.instance_claimed().await.unwrap());
        assert!(state.claimed.load(Ordering::Relaxed), "the latch is set");

        // Once latched it never asks the database again — deleting every row
        // out from under it (which the API itself can never do) leaves the
        // instance claimed.
        state
            .db
            .conn()
            .execute("DELETE FROM user", ())
            .await
            .unwrap();
        assert!(state.instance_claimed().await.unwrap());
    }

    // ---- POST /auth/setup (AC2/AC3) -----------------------------------------

    #[tokio::test]
    async fn setup_claims_an_empty_instance_as_admin_and_returns_a_working_token() {
        let state = test_state().await;
        let response = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "josiah",
                    "display_name": "Josiah Campbell",
                    "password": "a-long-enough-password",
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_json(response).await;
        assert_eq!(body["user"]["username"], "josiah");
        assert_eq!(body["user"]["display_name"], "Josiah Campbell");
        assert_eq!(body["user"]["role"], "admin", "the first user is an admin");
        assert_eq!(body["user"]["active"], true);

        // Exactly one user exists, and it is that one.
        let users = users::list(&state.db).await.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].role, Role::Admin);

        // The returned access token verifies, and its claims name that user
        // and a session row that really exists.
        let claims = state
            .auth_key
            .verify(body["access_token"].as_str().unwrap())
            .expect("the returned access token must verify");
        assert_eq!(claims.sub, users[0].id);
        assert_eq!(claims.role, Role::Admin);
        assert_eq!(claims.exp, body["expires_at"].as_i64().unwrap());
        assert_eq!(session_count(&state).await, 1);
        assert_eq!(stored_digest(&state, &claims.sid).await.len(), 64);

        // ...and the credentials it set work at the front door.
        assert!(body["refresh_token"].as_str().unwrap().len() >= 43);
        assert!(body["refresh_expires_at"].as_i64().unwrap() > body["expires_at"].as_i64().unwrap());
    }

    #[tokio::test]
    async fn a_second_setup_is_a_409_that_creates_nothing() {
        let state = test_state().await;
        let claim = json!({
            "username": "josiah",
            "display_name": "Josiah",
            "password": "a-long-enough-password",
        });
        let first = app(state.clone())
            .oneshot(post("/auth/setup", claim.clone()))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);

        // A different username, so what is refused is the *claim*, not a
        // duplicate identifier.
        let second = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "impostor",
                    "display_name": "Impostor",
                    "password": "a-long-enough-password",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(second).await["error"]["code"], "conflict");

        assert_eq!(users::list(&state.db).await.unwrap().len(), 1);
        assert_eq!(session_count(&state).await, 1, "no session was minted");
        assert!(state.claimed.load(Ordering::Relaxed), "the latch stays set");
    }

    #[tokio::test]
    async fn setup_enforces_the_password_policy_and_leaves_the_instance_unclaimed() {
        // AC17, setup path: 11 characters is short by one...
        let state = test_state().await;
        let response = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "josiah",
                    "display_name": "Josiah",
                    "password": "elevenchars",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_json(response).await["error"]["message"],
            "password must be at least 12 characters"
        );
        assert!(users::list(&state.db).await.unwrap().is_empty());
        assert!(!state.instance_claimed().await.unwrap(), "still unclaimed");

        // ...and a 12-character all-lowercase one, with no digits or symbols,
        // is accepted: the product forbids composition rules.
        let response = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "josiah",
                    "display_name": "Josiah",
                    "password": "twelvecharss",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn setup_validates_the_username_too() {
        let state = test_state().await;
        let response = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "  ",
                    "display_name": "Josiah",
                    "password": "a-long-enough-password",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!state.instance_claimed().await.unwrap());
    }

    // ---- POST /auth/login (AC4) ---------------------------------------------

    #[tokio::test]
    async fn correct_credentials_log_in() {
        let state = test_state().await;
        let user = seed_user(&state, "Josiah", Role::User, true).await;

        let response = app(state.clone())
            .oneshot(post(
                "/auth/login",
                // Lower-case: the username column is NOCASE.
                json!({ "username": "josiah", "password": SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_json(response).await;
        assert_eq!(body["user"]["id"], user.id);
        assert_eq!(body["user"]["role"], "user");
        let claims = state
            .auth_key
            .verify(body["access_token"].as_str().unwrap())
            .unwrap();
        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.role, Role::User);
        assert_eq!(session_count(&state).await, 1);
    }

    #[tokio::test]
    async fn wrong_password_unknown_username_and_deactivated_are_byte_identical() {
        let state = test_state().await;
        seed_user(&state, "josiah", Role::Admin, true).await;
        seed_user(&state, "benched", Role::User, false).await;

        let wrong_password = login_response(app(state.clone()), "josiah", "not-my-password").await;
        let unknown_user = login_response(app(state.clone()), "nobody", SEED_PASSWORD).await;
        let deactivated = login_response(app(state.clone()), "benched", SEED_PASSWORD).await;

        assert_eq!(wrong_password, unknown_user, "byte-identical bodies");
        assert_eq!(wrong_password, deactivated, "byte-identical bodies");
        assert_eq!(
            serde_json::from_slice::<Value>(&wrong_password).unwrap(),
            json!({
                "error": {
                    "code": "invalid_credentials",
                    "message": "invalid username or password",
                }
            })
        );
        assert_eq!(session_count(&state).await, 0, "no session was minted");
    }

    #[tokio::test]
    async fn a_deactivated_user_who_is_reactivated_can_log_in_again() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let user = seed_user(&state, "benched", Role::User, false).await;

        users::set_active(&state.db, &user.id, true, Some(&admin.id))
            .await
            .unwrap();
        let response = app(state.clone())
            .oneshot(post(
                "/auth/login",
                json!({ "username": "benched", "password": SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn login_on_an_unclaimed_instance_says_setup_required() {
        let state = test_state().await;
        let response = app(state)
            .oneshot(post(
                "/auth/login",
                json!({ "username": "josiah", "password": SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            body_json(response).await["error"]["code"],
            "setup_required",
            "an unauthenticated client branches on the code, not the status"
        );
    }

    // ---- POST /auth/refresh (AC7) -------------------------------------------

    #[tokio::test]
    async fn refresh_survives_a_rebuilt_appstate_over_the_same_db_file() {
        let (state, dir) = file_backed_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        drop(state);

        // A brand-new AppState over the same file — a server restart, which is
        // the only thing that actually proves the session is durable. (The
        // signing key is derived from the same master material, so tokens
        // minted before the restart still verify after it.)
        let restarted = state_at(&dir).await;
        let response = app(restarted.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": issued.refresh_token }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["user"]["id"], user.id);
        let claims = restarted
            .auth_key
            .verify(body["access_token"].as_str().unwrap())
            .expect("the new access token must verify");
        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.sid, issued.session.id, "the same session");
        assert_ne!(
            body["access_token"].as_str().unwrap(),
            issued.access_token,
            "a *new* access token"
        );
        // The refresh token is not echoed back — it does not rotate, so there
        // is nothing to return.
        assert!(body.get("refresh_token").is_none());

        drop(restarted);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refresh_rejects_unknown_expired_and_revoked_tokens() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;

        // Unknown.
        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": "no-such-token" }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Expired: back-date the row's absolute expiry.
        let expired = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        state
            .db
            .conn()
            .execute(
                "UPDATE session SET expires_at = ?2 WHERE id = ?1",
                params![expired.session.id.clone(), now_ms() - 1_000],
            )
            .await
            .unwrap();
        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": expired.refresh_token }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Revoked.
        let revoked = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        revoke(&state.db, &revoked.session.id).await.unwrap();
        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": revoked.refresh_token }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_rejects_a_user_who_has_since_been_deactivated() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();

        users::set_active(&state.db, &user.id, false, Some(&admin.id))
            .await
            .unwrap();

        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": issued.refresh_token.clone() }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        // The dead session is dropped rather than left to age out.
        assert!(resolve_refresh(&state.db, &issued.refresh_token)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn refresh_picks_up_a_promotion_because_it_re_reads_the_role() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        assert_eq!(
            state.auth_key.verify(&issued.access_token).unwrap().role,
            Role::User,
            "the token minted at login carries the role of the time"
        );

        users::update(&state.db, &user.id, None, Some(Role::Admin))
            .await
            .unwrap();

        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": issued.refresh_token }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["user"]["role"], "admin");
        assert_eq!(
            state
                .auth_key
                .verify(body["access_token"].as_str().unwrap())
                .unwrap()
                .role,
            Role::Admin,
            "the refreshed token carries the new role"
        );
    }

    #[tokio::test]
    async fn refresh_prunes_expired_session_rows() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let live = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        let stale = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        state
            .db
            .conn()
            .execute(
                "UPDATE session SET expires_at = ?2 WHERE id = ?1",
                params![stale.session.id.clone(), now_ms() - 1],
            )
            .await
            .unwrap();
        assert_eq!(session_count(&state).await, 2);

        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": live.refresh_token }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            session_count(&state).await,
            1,
            "the expired row was swept on the way through"
        );
    }

    #[tokio::test]
    async fn refresh_updates_last_used_but_never_extends_the_absolute_expiry() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();

        // A refresh token does not rotate and its expiry is absolute — a
        // session cannot be kept alive forever by refreshing.
        let response = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": issued.refresh_token.clone() }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let after = resolve_refresh(&state.db, &issued.refresh_token)
            .await
            .unwrap()
            .expect("still live");
        assert_eq!(after.expires_at, issued.session.expires_at);
        assert!(after.last_used_at >= issued.session.last_used_at);
    }

    // ---- Store ---------------------------------------------------------------

    #[tokio::test]
    async fn the_refresh_token_is_only_ever_stored_as_a_sha256_digest() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();

        let stored = stored_digest(&state, &issued.session.id).await;
        assert_eq!(stored.len(), 64, "a full SHA-256 hex digest");
        assert!(stored.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(stored, issued.refresh_token);
        assert_eq!(stored, digest_hex(&issued.refresh_token));

        // No text column anywhere in the row holds the plaintext.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT id, user_id, refresh_token_hash FROM session WHERE id = ?1",
                params![issued.session.id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        for i in 0..3 {
            assert_ne!(
                row.get::<String>(i).unwrap(),
                issued.refresh_token,
                "column {i} leaks the token"
            );
        }
    }

    #[tokio::test]
    async fn every_issued_refresh_token_is_distinct_and_full_entropy() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;

        let mut seen = std::collections::HashSet::new();
        for _ in 0..16 {
            let issued = issue(&state.db, &state.auth_key, &user, &state.config.auth)
                .await
                .unwrap();
            // 32 bytes, unpadded base64url → 43 characters.
            assert_eq!(issued.refresh_token.len(), 43);
            assert!(
                URL_SAFE_NO_PAD
                    .decode(&issued.refresh_token)
                    .unwrap()
                    .len()
                    == REFRESH_TOKEN_BYTES
            );
            assert!(seen.insert(issued.refresh_token), "tokens must not repeat");
        }
        assert_eq!(session_count(&state).await, 16);
    }

    #[tokio::test]
    async fn revoke_is_idempotent_and_scoped_to_one_session() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let a = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        let b = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();

        revoke(&state.db, &a.session.id).await.unwrap();
        assert!(resolve_refresh(&state.db, &a.refresh_token)
            .await
            .unwrap()
            .is_none());
        assert!(resolve_refresh(&state.db, &b.refresh_token)
            .await
            .unwrap()
            .is_some());

        // Revoking again (or revoking nothing) is a clean no-op.
        revoke(&state.db, &a.session.id).await.unwrap();
        revoke(&state.db, "no-such-session").await.unwrap();
        assert_eq!(session_count(&state).await, 1);
    }

    #[tokio::test]
    async fn revoke_all_for_user_spares_other_users() {
        let state = test_state().await;
        let josiah = seed_user(&state, "josiah", Role::User, true).await;
        let other = seed_user(&state, "other", Role::User, true).await;
        for _ in 0..3 {
            issue(&state.db, &state.auth_key, &josiah, &state.config.auth)
                .await
                .unwrap();
        }
        let survivor = issue(&state.db, &state.auth_key, &other, &state.config.auth)
            .await
            .unwrap();

        assert_eq!(revoke_all_for_user(&state.db, &josiah.id).await.unwrap(), 3);
        assert_eq!(session_count(&state).await, 1);
        assert!(resolve_refresh(&state.db, &survivor.refresh_token)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn revoke_all_for_user_except_keeps_the_calling_session_alive() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let current = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        let elsewhere = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();

        assert_eq!(
            revoke_all_for_user_except(&state.db, &user.id, &current.session.id)
                .await
                .unwrap(),
            1
        );
        assert!(resolve_refresh(&state.db, &current.refresh_token)
            .await
            .unwrap()
            .is_some());
        assert!(resolve_refresh(&state.db, &elsewhere.refresh_token)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn prune_expired_removes_only_lapsed_rows() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let live = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        let stale = issue(&state.db, &state.auth_key, &user, &state.config.auth)
            .await
            .unwrap();
        state
            .db
            .conn()
            .execute(
                "UPDATE session SET expires_at = ?2 WHERE id = ?1",
                params![stale.session.id.clone(), now_ms() - 1],
            )
            .await
            .unwrap();

        assert_eq!(prune_expired(&state.db).await.unwrap(), 1);
        assert_eq!(prune_expired(&state.db).await.unwrap(), 0, "idempotent");
        assert!(resolve_refresh(&state.db, &live.refresh_token)
            .await
            .unwrap()
            .is_some());
    }

    // ---- Serialization -------------------------------------------------------

    #[tokio::test]
    async fn no_serialized_payload_anywhere_carries_a_password_hash() {
        let state = test_state().await;
        let setup = app(state.clone())
            .oneshot(post(
                "/auth/setup",
                json!({
                    "username": "josiah",
                    "display_name": "Josiah",
                    "password": "a-long-enough-password",
                }),
            ))
            .await
            .unwrap();
        let envelope = String::from_utf8(body_bytes(setup).await).unwrap();

        let login = app(state.clone())
            .oneshot(post(
                "/auth/login",
                json!({ "username": "josiah", "password": "a-long-enough-password" }),
            ))
            .await
            .unwrap();
        let login_body = String::from_utf8(body_bytes(login).await).unwrap();

        let refresh_token = serde_json::from_str::<Value>(&login_body).unwrap()["refresh_token"]
            .as_str()
            .unwrap()
            .to_string();
        let refreshed = app(state.clone())
            .oneshot(post(
                "/auth/refresh",
                json!({ "refresh_token": refresh_token }),
            ))
            .await
            .unwrap();
        let refreshed_body = String::from_utf8(body_bytes(refreshed).await).unwrap();

        for body in [&envelope, &login_body, &refreshed_body] {
            assert!(!body.contains("password_hash"), "leaked in {body}");
            assert!(!body.contains("$argon2id$"), "leaked in {body}");
        }
    }

    // ---- Fixtures ------------------------------------------------------------

    #[tokio::test]
    async fn login_as_returns_a_bearer_ready_token_backed_by_a_real_session() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::Admin, true).await;

        let token = testing::login_as(&state, &user).await;
        let claims = state.auth_key.verify(&token).expect("a valid token");
        assert_eq!(claims.sub, user.id);
        assert_eq!(claims.role, Role::Admin);
        assert!(claims.exp > now_ms());
        // The `sid` names a session row that really exists, so a test can
        // revoke or refresh from it exactly as a browser would.
        assert_eq!(stored_digest(&state, &claims.sid).await.len(), 64);
    }

    // ---- Regression: the existing bearer layer is untouched -------------------

    #[tokio::test]
    async fn the_auth_routes_are_public_and_the_old_bearer_layer_still_guards_the_rest() {
        let state = test_state().await;
        seed_user(&state, "josiah", Role::Admin, true).await;

        // Every /auth route answers without an Authorization header.
        for request in [
            get("/auth/status"),
            post("/auth/login", json!({ "username": "x", "password": "y" })),
            post("/auth/refresh", json!({ "refresh_token": "x" })),
        ] {
            let status = app(state.clone()).oneshot(request).await.unwrap().status();
            assert_ne!(status, StatusCode::NOT_FOUND, "the route must be mounted");
        }

        // ...while a protected route still rejects an unauthenticated caller.
        let response = app(state.clone()).oneshot(get("/projects")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["error"]["code"], "unauthorized");
    }
}
