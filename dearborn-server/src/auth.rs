//! Request authentication for the multi-user access-token model, plus the
//! self-contained signed access-token format it is built on.
//!
//! [`require_auth`] replaces the old shared-static-token bearer layer as
//! the `route_layer` over the protected router: it reads the `Authorization:
//! Bearer` header, verifies the presented token with the instance's [`AuthKey`]
//! (stateless — one HMAC, zero database reads), inserts the resulting [`Claims`]
//! into the request extensions, and lets the request through. Public routes
//! (notably `GET /health`) bypass it entirely.
//!
//! On failure the layer returns `401`. When the token is absent or invalid
//! **and** the instance has no users yet (unclaimed), it returns
//! [`AppError::SetupRequired`] instead — still a `401`, but with the stable
//! `setup_required` code the SPA branches on to show a create-admin form rather
//! than a login form.
//!
//! ## Access-token format (no JWT dependency)
//!
//! ```text
//! v1.<b64url(payload)>.<b64url(HMAC-SHA256(key, "v1." + b64url(payload)))>
//! ```
//!
//! Base64url, unpadded. The payload is [`Claims`]. Verification splits on `.`,
//! checks the `v1` tag, recomputes the MAC over the signing input, compares it
//! to the presented MAC **in constant time**, then parses the payload and
//! finally checks `exp`. A bad signature and an expired token are distinct
//! errors so callers can tell them apart.
//!
//! No third party ever consumes these tokens, so JWT interop buys nothing — a
//! hand-rolled codec matches the precedent already set by `crypto.rs`.
//!
//! ## Signing key
//!
//! Derived from the existing `DEARBORN_MASTER_KEY` material with domain
//! separation: `SHA-256("dearborn/auth-token/v1" || master_key_bytes)`. That
//! makes it distinct from the AES key `crypto.rs` derives from the same
//! material, and stable across restarts.

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequestParts, State},
    http::{header::AUTHORIZATION, request::Parts, Request},
    middleware::Next,
    response::Response,
};

use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{users::Role, AppError, AppState};

/// Version tag embedded in every token and covered by the MAC. Bump on any
/// format change; old tokens then fail the tag check before anything else.
const TOKEN_VERSION: &str = "v1";

/// Domain-separation prefix for deriving the HMAC signing key from the master
/// key material. Distinct from `crypto.rs`'s bare `SHA-256(material)` so the
/// two derivations of one secret never share bytes.
const AUTH_KEY_DOMAIN: &str = "dearborn/auth-token/v1";

// ---- Claims -----------------------------------------------------------------

/// The authenticated identity carried inside an access token.
///
/// `sub`/`sid` are ULID strings (`user.id` / `session.id`). `exp` is unix ms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// User id (ULID).
    pub sub: String,
    /// Session id (ULID) — lets logout revoke this session from the token alone.
    pub sid: String,
    /// Role at mint time. Stale by design until refresh (eventual revocation).
    pub role: Role,
    /// Expiry, unix milliseconds.
    pub exp: i64,
}

// ---- Errors -----------------------------------------------------------------

/// Why a token failed verification. Signature and expiry failures are distinct
/// variants so callers can report "expired" differently from "tampered".
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// Not three dot-separated parts, or undecodable base64url/JSON.
    #[error("malformed token")]
    Malformed,
    /// The version tag is present but not `v1`.
    #[error("unsupported token version")]
    Version,
    /// The MAC does not match — wrong key or tampered bytes.
    #[error("bad signature")]
    BadSignature,
    /// The signature was valid but `exp` is already in the past.
    #[error("token expired")]
    Expired,
}

// ---- AuthKey ----------------------------------------------------------------

/// HMAC-SHA256 signing key for access tokens, derived from the
/// `DEARBORN_MASTER_KEY` material with domain separation (see module doc).
///
/// Deliberately does **not** derive `Debug`/`Serialize`: like [`crate::MasterKey`],
/// the key bytes must never be logged or serialised.
#[derive(Clone)]
pub struct AuthKey([u8; 32]);

impl std::fmt::Debug for AuthKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthKey(<redacted>)")
    }
}

impl AuthKey {
    /// Derive the signing key from raw `DEARBORN_MASTER_KEY` material via
    /// domain-separated SHA-256. Deterministic, so the same material yields the
    /// same key across restarts (sessions survive a server restart).
    ///
    /// Fails only on empty material, mirroring [`crate::MasterKey::derive`]'s
    /// boot-time validation.
    pub fn derive(material: &str) -> Result<AuthKey, crate::CryptoError> {
        if material.is_empty() {
            return Err(crate::CryptoError::EmptyKeyMaterial);
        }
        let mut hasher = Sha256::new();
        hasher.update(AUTH_KEY_DOMAIN.as_bytes());
        hasher.update(material.as_bytes());
        let digest = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&digest);
        Ok(AuthKey(key))
    }

    /// The raw 32 key bytes. Test-only — for asserting that this signing key
    /// differs from the AES key `crypto.rs` derives from the same master
    /// material, and that derivation is deterministic.
    #[cfg(test)]
    pub(crate) fn raw_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn mac(&self, signing_input: &[u8]) -> [u8; 32] {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.0)
            .expect("HMAC accepts any key length");
        mac.update(signing_input);
        mac.finalize().into_bytes().into()
    }

    /// Mint a signed token for `claims`:
    /// `v1.<b64url(payload)>.<b64url(HMAC-SHA256(key, "v1." + b64url(payload)))>`.
    pub fn mint(&self, claims: &Claims) -> String {
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(claims).expect("Claims serialize cannot fail"),
        );
        let signing_input = format!("{TOKEN_VERSION}.{payload}");
        let signature = URL_SAFE_NO_PAD.encode(self.mac(signing_input.as_bytes()));
        format!("{signing_input}.{signature}")
    }

    /// Verify a token and return its [`Claims`]. Order of checks: structure →
    /// version tag → constant-time MAC comparison → payload parse → expiry.
    pub fn verify(&self, token: &str) -> Result<Claims, TokenError> {
        // Exactly three dot-separated segments. `splitn` would happily accept
        // extra dots inside the signature, so count strictly.
        let mut parts = token.split('.');
        let (version, payload, signature) = match (parts.next(), parts.next(), parts.next()) {
            (Some(v), Some(p), Some(s)) if parts.next().is_none() => (v, p, s),
            _ => return Err(TokenError::Malformed),
        };
        if version != TOKEN_VERSION {
            return Err(TokenError::Version);
        }

        // Recompute the MAC over the full signing input and compare in constant
        // time — never `==` on the raw bytes. The length check first is fine:
        // the token's own length is public.
        let expected = self.mac(format!("{version}.{payload}").as_bytes());
        let presented = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| TokenError::Malformed)?;
        let macs_match =
            presented.len() == expected.len() && bool::from(presented.ct_eq(&expected));
        if !macs_match {
            return Err(TokenError::BadSignature);
        }

        let claims: Claims = serde_json::from_slice(
            &URL_SAFE_NO_PAD.decode(payload).map_err(|_| TokenError::Malformed)?,
        )
        .map_err(|_| TokenError::Malformed)?;

        if claims.exp <= now_ms() {
            return Err(TokenError::Expired);
        }
        Ok(claims)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// Authenticate a request by verifying its `Authorization: Bearer <token>`
/// against the instance's signing key, then inserting the verified [`Claims`]
/// into the request extensions for handlers to read via [`CurrentUser`].
///
/// Verification is stateless: one HMAC check and an `exp` comparison, no
/// database query — that is what keeps the hot path cheap. Revocation of a live
/// token therefore does not happen mid-flight; it lands at the next refresh
/// (`POST /auth/refresh` re-reads the user row).
///
/// On any failure returns `401` ([`AppError::Unauthorized`]), **except** when
/// the instance is unclaimed — no `user` row exists yet — in which case it
/// returns [`AppError::SetupRequired`]: same status, but the SPA can tell from
/// the stable code that it should render the create-admin screen instead of the
/// login screen.
pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let presented = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token);

    let claims = match presented {
        Some(token) => state.auth_key.verify(token).ok(),
        None => None,
    };
    let Some(claims) = claims else {
        return Err(unauthorized_or_setup(&state).await);
    };

    request.extensions_mut().insert(claims);
    Ok(next.run(request).await)
}

/// The 401 an unauthenticated caller gets: `setup_required` while the instance
/// has no users at all (so a fresh boot steers the browser to the claim form),
/// plain `unauthorized` once anyone has signed up.
async fn unauthorized_or_setup(state: &AppState) -> AppError {
    if state.instance_claimed().await.unwrap_or(true) {
        AppError::Unauthorized
    } else {
        AppError::SetupRequired
    }
}

/// Extractor for the authenticated identity on protected routes.
///
/// Populated from the claims the [`require_auth`] middleware already verified
/// and inserted into the request extensions — **zero database reads**. Like the
/// token itself, `role` reflects mint time; ordinary routes deliberately trust
/// it until refresh (eventual revocation). Any route taking `CurrentUser` is
/// thereby gated behind a valid access token.
#[derive(Debug, Clone)]
pub struct CurrentUser(pub Claims);

#[async_trait]
impl FromRequestParts<AppState> for CurrentUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, _state: &AppState) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Claims>()
            .cloned()
            .map(CurrentUser)
            .ok_or(AppError::Unauthorized)
    }
}

/// Extractor for admin-only routes.
///
/// Like [`CurrentUser`], but additionally re-reads the user row and confirms
/// `active = 1 AND role = 'admin'` *right now* — not at token mint time. This
/// closes the one genuinely dangerous staleness window: a demoted or
/// deactivated admin holding a still-valid access token cannot use it to call
/// user-management routes, even though ordinary protected routes still accept
/// that token (eventual-revocation by design).
///
/// Returns [`AppError::Forbidden`] — never `404` — for any caller that is
/// authenticated but is not currently an active admin, including formerly-admin
/// users whose role or active flag has changed since the token was minted.
#[derive(Debug, Clone)]
pub struct AdminUser(pub crate::users::User);

#[async_trait]
impl FromRequestParts<AppState> for AdminUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let claims = parts
            .extensions
            .get::<Claims>()
            .cloned()
            .ok_or(AppError::Unauthorized)?;

        // Re-read the user row to confirm the *current* active + role, not the
        // stale claims values. A 403 — not 401 — is the right response here:
        // the token itself is valid; the caller lacks the required privilege.
        let user = crate::users::get(&state.db, &claims.sub)
            .await?
            .ok_or_else(|| AppError::Forbidden("forbidden".to_string()))?;

        if !user.active || user.role != Role::Admin {
            return Err(AppError::Forbidden("forbidden".to_string()));
        }

        Ok(AdminUser(user))
    }
}

/// Extract the token from an `Authorization` header value, if it is a Bearer
/// credential. The scheme is matched case-insensitively per RFC 7235.
///
/// Shared with the WebSocket handshake (`ws.rs`), which also accepts a bearer
/// token from the `Authorization` header.
pub(crate) fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        let token = token.trim();
        (!token.is_empty()).then_some(token)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::MasterKey;

    const MATERIAL: &str = "test-master-key-material";

    fn test_claims() -> Claims {
        Claims {
            sub: "01JD2Q7XK3V9M4N8P6R2T5W9YA".to_string(),
            sid: "01JD2Q8BZ4W0N5P9Q3S3U6X0ZB".to_string(),
            role: Role::Admin,
            exp: now_ms() + 60_000, // one minute out
        }
    }

    #[test]
    fn parses_bearer_case_insensitively() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer abc"), Some("abc"));
        assert_eq!(bearer_token("BEARER  abc "), Some("abc"));
    }

    #[test]
    fn rejects_non_bearer_or_empty() {
        assert_eq!(bearer_token("Basic abc"), None);
        assert_eq!(bearer_token("Bearer "), None);
        assert_eq!(bearer_token("abc"), None);
    }

    // ---- Access-token codec ----

    fn key() -> AuthKey {
        AuthKey::derive(MATERIAL).unwrap()
    }

    #[test]
    fn mint_verify_round_trips_all_four_claims() {
        let claims = test_claims();
        let token = key().mint(&claims);
        assert_eq!(key().verify(&token).unwrap(), claims);
    }

    #[test]
    fn token_shape_is_v1_payload_signature_unpadded_base64url() {
        let token = key().mint(&test_claims());
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "v1");
        // Unpadded base64url alphabet only (no `+`, `/`, or `=`).
        for segment in &parts[1..] {
            assert!(!segment.contains(['+', '/', '=']));
            URL_SAFE_NO_PAD.decode(segment).unwrap();
        }
        // The payload decodes back to the four expected keys.
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        for field in ["sub", "sid", "role", "exp"] {
            assert!(payload.get(field).is_some(), "missing {field}");
        }
    }

    #[test]
    fn flipped_signature_byte_is_rejected() {
        let claims = test_claims();
        let token = key().mint(&claims);
        // Flip one character of the final (signature) segment. The FIRST
        // character is used deliberately: any base64url alphabet character is
        // decodable there, whereas flipping the last one can set non-zero
        // trailing bits in a short final group, which the strict unpadded
        // decoder rejects as undecodable before the MAC is ever compared.
        let (head, sig) = token.rsplit_once('.').unwrap();
        let mut sig: Vec<char> = sig.chars().collect();
        sig[0] = if sig[0] == 'A' { 'B' } else { 'A' };
        let tampered = format!("{head}.{}", sig.iter().collect::<String>());
        assert_eq!(key().verify(&tampered), Err(TokenError::BadSignature));
        // ...and the untouched token still verifies.
        let good = key().mint(&claims);
        assert!(key().verify(&good).is_ok());
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let token = key().mint(&test_claims());
        // Mint a different payload under the same key and graft its payload
        // segment onto the original signature.
        let other = key().mint(&Claims { role: Role::User, ..test_claims() });
        let spliced = format!("{}.{}", other.rsplit_once('.').unwrap().0, token.rsplit_once('.').unwrap().1);
        assert_eq!(key().verify(&spliced), Err(TokenError::BadSignature));
    }

    #[test]
    fn token_from_different_key_is_rejected() {
        let claims = test_claims();
        let token = AuthKey::derive("other-instance-material").unwrap().mint(&claims);
        assert_eq!(key().verify(&token), Err(TokenError::BadSignature));
    }

    #[test]
    fn mangled_version_tags_are_rejected() {
        let key = key();
        let token = key.mint(&test_claims());
        let (_, rest) = token.split_once('.').unwrap();

        // Wrong version tag — even a validly signed one.
        let v2 = format!("v2.{rest}");
        let v2_signed = {
            let payload = v2.split_once('.').unwrap().1;
            let sig = URL_SAFE_NO_PAD.encode(key.mac(format!("v2.{payload}").as_bytes()));
            format!("v2.{payload}.{sig}")
        };
        for bad in [v2, v2_signed, format!("abc.{rest}"), format!("a.b.{rest}"), "".to_string(), ".".to_string(), "a.b".to_string()] {
            let err = key.verify(&bad).unwrap_err();
            assert_ne!(err, TokenError::Expired);
            assert!(matches!(err, TokenError::Malformed | TokenError::Version));
        }
    }

    #[test]
    fn expired_token_fails_as_expired_not_bad_signature() {
        let key = key();
        let claims = Claims { exp: now_ms() - 1_000, ..test_claims() };
        let token = key.mint(&claims);
        // The signature is fine; the failure must be distinctly expiry.
        assert_eq!(key.verify(&token), Err(TokenError::Expired));
    }

    #[test]
    fn signing_key_differs_from_aes_key_derived_from_same_material() {
        let material = MATERIAL;
        let aes = MasterKey::derive(material).unwrap();
        let auth = AuthKey::derive(material).unwrap();
        assert_ne!(auth.raw_bytes(), aes.raw_bytes());
    }

    #[test]
    fn derivation_is_deterministic_across_constructions() {
        let a = AuthKey::derive(MATERIAL).unwrap();
        let b = AuthKey::derive(MATERIAL).unwrap();
        assert_eq!(a.raw_bytes(), b.raw_bytes());
        // A token minted before a restart still verifies after it.
        let token = a.mint(&test_claims());
        assert!(b.verify(&token).is_ok());
    }

    #[test]
    fn empty_master_material_is_rejected_and_key_debug_redacted() {
        assert!(matches!(
            AuthKey::derive(""),
            Err(crate::CryptoError::EmptyKeyMaterial)
        ));
        assert_eq!(format!("{:?}", key()), "AuthKey(<redacted>)");
    }
}
