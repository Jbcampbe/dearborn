//! User store and password policy (technical plan §4).
//!
//! Backs the `user` table added by migration `0007_users.sql`. This module is
//! the **only** way anything in the server reads or writes a user row, and it
//! deliberately carries no HTTP surface of its own — the `/auth/*` and
//! `/users` handlers are later work and sit on top of these functions.
//!
//! ## Passwords
//!
//! Stored only as an Argon2id PHC string (`$argon2id$v=19$m=…,t=…,p=…$salt$hash`)
//! in `user.password_hash`, never recoverable. The algorithm and its cost
//! parameters travel *inside* each hash, so raising the cost later needs no
//! migration and no rehash-on-read machinery: an old hash keeps verifying under
//! its own parameters.
//!
//! Both hashing and verification run inside [`tokio::task::spawn_blocking`].
//! Production parameters burn ~40–60 ms of CPU by design, which would otherwise
//! stall a runtime worker for the whole of that.
//! [`crate::Config::argon2_fast`] is the internal test seam that drops the cost
//! (see [`hash_password`]).
//!
//! The policy is a **12-character minimum and nothing else** — no forced
//! symbols, digits, or case mixing. It lives in exactly one function,
//! [`validate_password`], which every write path calls, so "enforced
//! identically at setup, admin create, admin reset, and self-change" stays true
//! by construction rather than by four handlers remembering to agree.
//!
//! ## Lockout guards
//!
//! The product forbids locking an instance out of its own user management: the
//! last active admin can be neither deactivated nor demoted, and no admin may
//! deactivate themselves. Those checks are **store-level**
//! ([`ensure_can_deactivate`], [`ensure_can_demote`]) and are invoked from
//! inside [`update`] and [`set_active`] rather than from the handlers, so a
//! future call site physically cannot forget them.
//!
//! ## Rows are never deleted
//!
//! There is no `delete`. Deactivation flips `active` to 0 and nothing else, so
//! future authorship references keep resolving to a real row.

use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::{Algorithm, Argon2, Params, Version};
use libsql::params;
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::{AppError, AppResult, AppState};

/// Minimum password length, in **characters** (not bytes). The product's only
/// composition rule.
pub const MIN_PASSWORD_CHARS: usize = 12;

/// Maximum username length, in characters.
pub const MAX_USERNAME_CHARS: usize = 64;

/// Maximum display-name length, in characters. Generous — this is a
/// human-facing label, not an identifier.
pub const MAX_DISPLAY_NAME_CHARS: usize = 128;

// ---- Model ------------------------------------------------------------------

/// A user's role. The two roles differ in exactly one thing: user management.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Admin,
    User,
}

impl Role {
    /// The stored string, matching the `role` column's CHECK vocabulary.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::User => "user",
        }
    }

    /// Parse a stored/inbound role string; `None` for anything else.
    pub fn parse(value: &str) -> Option<Role> {
        match value {
            "admin" => Some(Role::Admin),
            "user" => Some(Role::User),
            _ => None,
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A user as the rest of the server sees it.
///
/// Conspicuously **without** `password_hash`: the field simply does not exist
/// on this struct, so no serialization of a `User` can ever leak a hash — the
/// same discipline `projects.rs` applies to `pat_encrypted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub role: Role,
    pub active: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The columns projected into a [`User`]. Note the absence of `password_hash`.
const USER_COLUMNS: &str =
    "id, username, display_name, role, active, created_at, updated_at";

fn row_to_user(row: &libsql::Row) -> AppResult<User> {
    let role_raw: String = row.get(3)?;
    let role = Role::parse(&role_raw).ok_or_else(|| {
        // The column has a CHECK constraint, so this is corrupted server state
        // rather than bad input — internal, never leaked.
        AppError::Internal(format!("user row carries unknown role `{role_raw}`"))
    })?;
    Ok(User {
        id: row.get(0)?,
        username: row.get(1)?,
        display_name: row.get(2)?,
        role,
        active: row.get::<i64>(4)? != 0,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

// ---- Validation -------------------------------------------------------------

/// The password policy, in one place: **at least 12 characters, no composition
/// rules**. Every write path (setup, admin create, admin reset, self-change)
/// calls this and nothing else, which is what keeps the four entry points from
/// drifting apart.
///
/// Counted in `chars()`, not bytes: a 12-emoji password is 12 characters and is
/// accepted, while a 11-character ASCII one is not — length here means what a
/// person typing it would count.
pub fn validate_password(password: &str) -> AppResult<()> {
    if password.chars().count() < MIN_PASSWORD_CHARS {
        return Err(AppError::BadRequest(
            "password must be at least 12 characters".to_string(),
        ));
    }
    Ok(())
}

/// Validate a login identifier and return its canonical (trimmed) form.
///
/// Non-empty, at most [`MAX_USERNAME_CHARS`] characters, and containing no
/// whitespace at all — an interior space in a login id is a support ticket
/// waiting to happen, and the surrounding kind is silently stripped rather than
/// rejected. Case is *not* normalized here: the column's `COLLATE NOCASE`
/// already makes lookups and the unique index case-insensitive, so the user's
/// own capitalization is preserved for display.
pub fn validate_username(username: &str) -> AppResult<String> {
    let trimmed = username.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(
            "username must not be empty".to_string(),
        ));
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(AppError::BadRequest(
            "username must not contain whitespace".to_string(),
        ));
    }
    if trimmed.chars().count() > MAX_USERNAME_CHARS {
        return Err(AppError::BadRequest(format!(
            "username must be at most {MAX_USERNAME_CHARS} characters"
        )));
    }
    Ok(trimmed.to_string())
}

/// Validate a display name and return its trimmed form: non-empty, at most
/// [`MAX_DISPLAY_NAME_CHARS`] characters. Whitespace *inside* is fine — unlike
/// a username, this is a human name.
pub fn validate_display_name(display_name: &str) -> AppResult<String> {
    let trimmed = display_name.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(
            "display_name must not be empty".to_string(),
        ));
    }
    if trimmed.chars().count() > MAX_DISPLAY_NAME_CHARS {
        return Err(AppError::BadRequest(format!(
            "display_name must be at most {MAX_DISPLAY_NAME_CHARS} characters"
        )));
    }
    Ok(trimmed.to_string())
}

// ---- Password hashing -------------------------------------------------------

/// Argon2id cost parameters.
///
/// `fast` selects the cheapest legal configuration (m=8 KiB, t=1, p=1) for the
/// test seam; production uses the `argon2` crate's own OWASP-tracking defaults
/// (m=19456 KiB, t=2, p=1). Both are recorded in the PHC string each produces,
/// so hashes written under one verify fine under the other.
fn argon2_params(fast: bool) -> Params {
    if !fast {
        return Params::default();
    }
    // The cheapest legal configuration: `m_cost` must be at least `8 * p_cost`.
    // Constant, so construction cannot fail.
    Params::new(8, 1, 1, None).expect("m=8,t=1,p=1 is within Argon2's legal bounds")
}

/// Hash `password` as an Argon2id PHC string.
///
/// Runs on the blocking pool: even the fast parameters do real work, and the
/// production ones burn ~40–60 ms, which must never sit on a runtime worker.
pub async fn hash_password(password: &str, fast: bool) -> AppResult<String> {
    let password = password.to_string();
    tokio::task::spawn_blocking(move || -> AppResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let hasher = Argon2::new(Algorithm::Argon2id, Version::V0x13, argon2_params(fast));
        hasher
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            // The message is an Argon2 internal, never a client mistake.
            .map_err(|err| AppError::Internal(format!("password hashing failed: {err}")))
    })
    .await
    .map_err(|err| AppError::Internal(format!("password hashing task failed: {err}")))?
}

/// Verify `password` against a stored PHC string.
///
/// `Ok(false)` is a *wrong password*; `Err` means the stored hash could not be
/// parsed at all (corrupted row), which is a server problem, not a failed
/// login. Callers must keep the two apart so a corrupt row never silently reads
/// as "bad credentials".
///
/// The cost parameters come from the hash itself, so this is correct for hashes
/// written under either [`argon2_params`] setting.
pub async fn verify_password(password: &str, phc: &str) -> AppResult<bool> {
    let password = password.to_string();
    let phc = phc.to_string();
    tokio::task::spawn_blocking(move || -> AppResult<bool> {
        let parsed = PasswordHash::new(&phc)
            .map_err(|err| AppError::Internal(format!("stored password hash is invalid: {err}")))?;
        // Any verification error (including a genuine mismatch) is a failed
        // login; only *parsing* the stored hash can be an internal fault.
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    })
    .await
    .map_err(|err| AppError::Internal(format!("password verification task failed: {err}")))?
}

// ---- Store ------------------------------------------------------------------

/// Create a user. `201`-shaped: validates the username, display name, and
/// password policy, hashes the password, and inserts.
///
/// A username already taken (case-insensitively) is a **`409 Conflict`**, not a
/// raw database error. The check happens twice on purpose: once up front so the
/// message can name the username, and once as a mapping of the unique-index
/// violation, which is the only thing that closes the race between the two.
pub async fn create(
    db: &Db,
    username: &str,
    display_name: &str,
    password: &str,
    role: Role,
    argon2_fast: bool,
) -> AppResult<User> {
    let username = validate_username(username)?;
    let display_name = validate_display_name(display_name)?;
    validate_password(password)?;

    if get_by_username(db, &username).await?.is_some() {
        return Err(duplicate_username(&username));
    }

    let password_hash = hash_password(password, argon2_fast).await?;
    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    db.conn()
        .execute(
            "INSERT INTO user \
                 (id, username, display_name, password_hash, role, active, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7)",
            params![
                id.clone(),
                username.clone(),
                display_name,
                password_hash,
                role.as_str(),
                now,
                now,
            ],
        )
        .await
        .map_err(|err| map_insert_error(err, &username))?;

    get(db, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("user {id} vanished after insert")))
}

/// Every user, active and inactive alike, ordered by username.
///
/// Deactivated users are deliberately included: the admin screen has to show
/// them as inactive, and a row that vanished from the list would look deleted.
pub async fn list(db: &Db) -> AppResult<Vec<User>> {
    let sql = format!("SELECT {USER_COLUMNS} FROM user ORDER BY username");
    let mut rows = db.conn().query(&sql, ()).await?;
    let mut users = Vec::new();
    while let Some(row) = rows.next().await? {
        users.push(row_to_user(&row)?);
    }
    Ok(users)
}

/// Fetch one user by id; `None` when no such row exists.
pub async fn get(db: &Db, id: &str) -> AppResult<Option<User>> {
    let sql = format!("SELECT {USER_COLUMNS} FROM user WHERE id = ?1");
    let mut rows = db.conn().query(&sql, params![id]).await?;
    rows.next().await?.map(|row| row_to_user(&row)).transpose()
}

/// Fetch one user by login identifier; `None` when no such row exists.
///
/// Case-insensitive: the comparison inherits the column's `COLLATE NOCASE`, so
/// creating `Josiah` and fetching `josiah` returns the same row without this
/// query saying anything about case at all.
pub async fn get_by_username(db: &Db, username: &str) -> AppResult<Option<User>> {
    let sql = format!("SELECT {USER_COLUMNS} FROM user WHERE username = ?1");
    let mut rows = db.conn().query(&sql, params![username]).await?;
    rows.next().await?.map(|row| row_to_user(&row)).transpose()
}

/// Read a user's stored PHC hash — the only path that ever touches the column,
/// and the reason [`User`] can afford not to carry it. `None` when no such row.
pub async fn password_hash_of(db: &Db, id: &str) -> AppResult<Option<String>> {
    let mut rows = db
        .conn()
        .query("SELECT password_hash FROM user WHERE id = ?1", params![id])
        .await?;
    Ok(match rows.next().await? {
        Some(row) => Some(row.get::<String>(0)?),
        None => None,
    })
}

/// Update a user's display name and/or role. `None` for a field leaves it
/// untouched. `404` when the user does not exist.
///
/// Demoting the **last active admin** is refused here, in the store, so no
/// caller can route around the guard (see [`ensure_can_demote`]).
pub async fn update(
    db: &Db,
    id: &str,
    display_name: Option<&str>,
    role: Option<Role>,
) -> AppResult<User> {
    let current = get(db, id).await?.ok_or_else(|| not_found(id))?;

    if let Some(role) = role {
        ensure_can_demote(db, &current, role).await?;
    }

    let display_name = match display_name {
        Some(value) => Some(validate_display_name(value)?),
        None => None,
    };
    if display_name.is_none() && role.is_none() {
        return Ok(current);
    }

    db.conn()
        .execute(
            "UPDATE user SET \
                 display_name = COALESCE(?2, display_name), \
                 role         = COALESCE(?3, role), \
                 updated_at   = ?4 \
             WHERE id = ?1",
            params![
                id,
                display_name,
                role.map(|r| r.as_str().to_string()),
                now_ms(),
            ],
        )
        .await?;

    get(db, id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("user {id} vanished during update")))
}

/// Replace a user's password. Enforces the same [`validate_password`] policy as
/// every other write path. `404` when the user does not exist.
///
/// Revoking that user's sessions is the caller's job (a later task owns the
/// session store); this function owns only the hash.
pub async fn set_password(db: &Db, id: &str, password: &str, argon2_fast: bool) -> AppResult<()> {
    validate_password(password)?;
    if get(db, id).await?.is_none() {
        return Err(not_found(id));
    }

    let password_hash = hash_password(password, argon2_fast).await?;
    db.conn()
        .execute(
            "UPDATE user SET password_hash = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, password_hash, now_ms()],
        )
        .await?;
    Ok(())
}

/// Activate or deactivate a user. `404` when the user does not exist.
///
/// This is the **only** form of removal there is: the row is never deleted, so
/// future authorship references keep resolving. `actor_id` is whoever is making
/// the change — passed so the self-deactivation guard can fire; `None` for
/// system-initiated changes with no acting user.
///
/// Both lockout guards run here, in the store (see [`ensure_can_deactivate`]).
pub async fn set_active(
    db: &Db,
    id: &str,
    active: bool,
    actor_id: Option<&str>,
) -> AppResult<User> {
    let current = get(db, id).await?.ok_or_else(|| not_found(id))?;

    if !active {
        ensure_can_deactivate(db, &current, actor_id).await?;
    }
    if current.active == active {
        return Ok(current);
    }

    let flag: i64 = if active { 1 } else { 0 };
    db.conn()
        .execute(
            "UPDATE user SET active = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, flag, now_ms()],
        )
        .await?;

    get(db, id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("user {id} vanished during set_active")))
}

// ---- Lockout guards ---------------------------------------------------------

/// How many users are both `active` and `role = 'admin'` right now.
///
/// The quantity both lockout guards are about: while it is 1, that one admin is
/// the instance's only route back into user management.
pub async fn count_active_admins(db: &Db) -> AppResult<i64> {
    let mut rows = db
        .conn()
        .query(
            "SELECT COUNT(*) FROM user WHERE active = 1 AND role = 'admin'",
            (),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| AppError::Internal("COUNT(*) returned no row".to_string()))?;
    Ok(row.get::<i64>(0)?)
}

/// Refuse to deactivate `target` if doing so would lock the instance out of its
/// own user management, or if the caller is deactivating themselves.
///
/// Two separate `409`s:
/// - `actor_id == target.id` → "you cannot deactivate your own account". Checked
///   first, because it is the more specific explanation of what the caller just
///   tried to do — an admin clicking the wrong row's deactivate button wants to
///   hear that, not a lecture about admin counts.
/// - `target` is the last active admin → "cannot deactivate the last active
///   admin".
///
/// Called from [`set_active`], not from handlers, so it cannot be skipped.
pub async fn ensure_can_deactivate(
    db: &Db,
    target: &User,
    actor_id: Option<&str>,
) -> AppResult<()> {
    if actor_id == Some(target.id.as_str()) {
        return Err(AppError::Conflict(
            "you cannot deactivate your own account".to_string(),
        ));
    }
    if target.active && target.role == Role::Admin && count_active_admins(db).await? <= 1 {
        return Err(AppError::Conflict(
            "cannot deactivate the last active admin".to_string(),
        ));
    }
    Ok(())
}

/// Refuse to demote `target` to `new_role` if that would leave the instance
/// with no active admin — `409` "cannot demote the last active admin".
///
/// Only a genuine demotion trips this: promoting, or re-writing the role a user
/// already has, is always allowed. Called from [`update`], not from handlers.
pub async fn ensure_can_demote(db: &Db, target: &User, new_role: Role) -> AppResult<()> {
    let demoting = target.role == Role::Admin && new_role != Role::Admin;
    if demoting && target.active && count_active_admins(db).await? <= 1 {
        return Err(AppError::Conflict(
            "cannot demote the last active admin".to_string(),
        ));
    }
    Ok(())
}

// ---- Helpers ----------------------------------------------------------------

/// The standard not-found error for a user id.
fn not_found(id: &str) -> AppError {
    AppError::NotFound(format!("user {id} not found"))
}

/// The standard duplicate-username conflict.
fn duplicate_username(username: &str) -> AppError {
    AppError::Conflict(format!("username `{username}` is already taken"))
}

/// Map an insert failure to a client-meaningful error. A unique-index violation
/// on `username` is a `409`, never a generic 500 with a SQL string in the log —
/// it is an ordinary thing for two admins to do at once, and the only case the
/// up-front existence check in [`create`] can lose a race to.
fn map_insert_error(err: libsql::Error, username: &str) -> AppError {
    if err.to_string().contains("UNIQUE constraint failed") {
        duplicate_username(username)
    } else {
        AppError::from(err)
    }
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- Test fixtures ----------------------------------------------------------

/// Fixtures the rest of the epic's tests seed users with.
///
/// Unconditionally `pub` (not `#[cfg(test)]`) for the same reason
/// [`crate::git_host::testing`] is: an integration test in `tests/` compiles as
/// its own crate and never sees anything gated behind *this* crate's
/// `#[cfg(test)]`.
pub mod testing {
    use super::*;

    /// The password [`seed_user`] gives every user it creates. Long enough to
    /// pass [`validate_password`]; exported so a login test can present it.
    pub const SEED_PASSWORD: &str = "seed-password-1234";

    /// Seed one user directly into `state`'s database, bypassing HTTP.
    ///
    /// The `active` flag is written with raw SQL rather than through
    /// [`set_active`] on purpose: the lockout guards are *policy on the API*,
    /// and a fixture needs to be able to construct states the API would refuse
    /// to reach (an inactive admin, say) in order to test what happens next.
    ///
    /// Panics on failure — a fixture that cannot seed has nothing useful to
    /// report to the test that called it.
    pub async fn seed_user(state: &AppState, username: &str, role: Role, active: bool) -> User {
        let user = create(
            &state.db,
            username,
            username,
            SEED_PASSWORD,
            role,
            state.config.argon2_fast,
        )
        .await
        .unwrap_or_else(|err| panic!("seed_user({username}): {err}"));

        if active {
            return user;
        }
        state
            .db
            .conn()
            .execute(
                "UPDATE user SET active = 0 WHERE id = ?1",
                params![user.id.clone()],
            )
            .await
            .unwrap_or_else(|err| panic!("seed_user({username}): deactivate: {err}"));
        get(&state.db, &user.id)
            .await
            .unwrap_or_else(|err| panic!("seed_user({username}): reread: {err}"))
            .expect("seeded user row exists")
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{seed_user, SEED_PASSWORD};
    use super::*;
    use crate::Config;

    async fn test_state() -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        AppState::new(Config::for_test(), db)
    }

    /// Assert an error is a `409 Conflict` carrying exactly `message`.
    fn assert_conflict(err: AppError, message: &str) {
        match err {
            AppError::Conflict(actual) => assert_eq!(actual, message),
            other => panic!("expected a 409 Conflict, got {other:?}"),
        }
    }

    // ---- Password policy ----------------------------------------------------

    #[test]
    fn password_policy_is_twelve_characters_and_nothing_else() {
        // 11 characters is short by one.
        assert!(validate_password("elevenchars").is_err());
        // 12 all-lowercase characters, no digits or symbols, is fine: the
        // product forbids composition rules.
        assert!(validate_password("twelvecharss").is_ok());
        // Counted in characters, not bytes: 12 emoji is a 12-character
        // password (and 48 bytes).
        let emoji = "🔒".repeat(12);
        assert_eq!(emoji.len(), 48, "the byte count would over-count");
        assert!(validate_password(&emoji).is_ok());
        // ...and 11 emoji is still short.
        assert!(validate_password(&"🔒".repeat(11)).is_err());
    }

    #[test]
    fn short_password_is_a_bad_request_with_the_documented_message() {
        match validate_password("short").unwrap_err() {
            AppError::BadRequest(message) => {
                assert_eq!(message, "password must be at least 12 characters");
            }
            other => panic!("expected a 400 BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn username_policy_trims_and_rejects_empty_long_and_whitespace() {
        assert_eq!(validate_username("  josiah  ").unwrap(), "josiah");
        // Case is preserved — NOCASE handles matching, not normalization.
        assert_eq!(validate_username("Josiah").unwrap(), "Josiah");
        assert!(validate_username("").is_err());
        assert!(validate_username("   ").is_err());
        assert!(validate_username("two words").is_err());
        assert!(validate_username("tab\there").is_err());
        assert!(validate_username(&"a".repeat(MAX_USERNAME_CHARS)).is_ok());
        assert!(validate_username(&"a".repeat(MAX_USERNAME_CHARS + 1)).is_err());
    }

    #[test]
    fn display_name_policy_allows_interior_spaces_but_not_emptiness() {
        assert_eq!(validate_display_name("  Josiah C  ").unwrap(), "Josiah C");
        assert!(validate_display_name("   ").is_err());
        assert!(validate_display_name(&"a".repeat(MAX_DISPLAY_NAME_CHARS + 1)).is_err());
    }

    // ---- Hashing ------------------------------------------------------------

    #[tokio::test]
    async fn hashes_are_argon2id_phc_strings_that_round_trip() {
        let hash = hash_password("correct horse battery", true).await.unwrap();
        assert!(
            hash.starts_with("$argon2id$"),
            "expected an argon2id PHC string, got {hash}"
        );
        assert!(verify_password("correct horse battery", &hash).await.unwrap());
        assert!(!verify_password("correct horse batterz", &hash).await.unwrap());
        assert!(!verify_password("", &hash).await.unwrap());
    }

    #[tokio::test]
    async fn each_hash_gets_a_fresh_salt() {
        let a = hash_password("same password here", true).await.unwrap();
        let b = hash_password("same password here", true).await.unwrap();
        assert_ne!(a, b, "identical passwords must not produce identical hashes");
        // Both still verify — the salt travels inside the PHC string.
        assert!(verify_password("same password here", &a).await.unwrap());
        assert!(verify_password("same password here", &b).await.unwrap());
    }

    #[tokio::test]
    async fn cost_parameters_travel_with_the_hash() {
        // The fast seam is a *cost* change, not a format change: the parameters
        // are recorded in the hash, so verification never needs to know which
        // setting produced it.
        let fast = hash_password("parameters travel", true).await.unwrap();
        assert!(fast.contains("m=8,t=1,p=1"), "unexpected params in {fast}");
        assert!(verify_password("parameters travel", &fast).await.unwrap());
    }

    #[tokio::test]
    async fn a_corrupt_stored_hash_is_an_internal_error_not_a_failed_login() {
        let err = verify_password("anything at all", "not-a-phc-string")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Internal(_)), "got {err:?}");
    }

    // ---- Store CRUD ---------------------------------------------------------

    #[tokio::test]
    async fn create_get_and_get_by_username_round_trip() {
        let state = test_state().await;
        let created = create(
            &state.db,
            "  Josiah  ",
            "  Josiah Campbell  ",
            "a-long-enough-password",
            Role::Admin,
            true,
        )
        .await
        .unwrap();

        assert_eq!(created.username, "Josiah", "trimmed, case preserved");
        assert_eq!(created.display_name, "Josiah Campbell");
        assert_eq!(created.role, Role::Admin);
        assert!(created.active, "new users are active");
        assert_eq!(created.created_at, created.updated_at);

        assert_eq!(get(&state.db, &created.id).await.unwrap(), Some(created.clone()));
        assert_eq!(get(&state.db, "no-such-id").await.unwrap(), None);

        // Case-insensitive lookup: created as `Josiah`, found as `josiah`.
        for probe in ["josiah", "Josiah", "JOSIAH"] {
            assert_eq!(
                get_by_username(&state.db, probe).await.unwrap(),
                Some(created.clone()),
                "lookup by `{probe}` must find the row"
            );
        }
        assert_eq!(get_by_username(&state.db, "nobody").await.unwrap(), None);
    }

    #[tokio::test]
    async fn created_password_verifies_against_the_stored_hash() {
        let state = test_state().await;
        let user = create(
            &state.db,
            "josiah",
            "Josiah",
            "a-long-enough-password",
            Role::Admin,
            true,
        )
        .await
        .unwrap();

        let stored = password_hash_of(&state.db, &user.id).await.unwrap().unwrap();
        assert!(stored.starts_with("$argon2id$"));
        assert!(verify_password("a-long-enough-password", &stored)
            .await
            .unwrap());
        assert!(!verify_password("some-other-password", &stored).await.unwrap());
    }

    #[tokio::test]
    async fn duplicate_username_is_a_409_not_a_raw_db_error() {
        let state = test_state().await;
        create(
            &state.db,
            "josiah",
            "Josiah",
            "a-long-enough-password",
            Role::Admin,
            true,
        )
        .await
        .unwrap();

        // Exact duplicate.
        let err = create(
            &state.db,
            "josiah",
            "Impostor",
            "a-long-enough-password",
            Role::User,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");

        // ...and a case-variant duplicate, since the index is NOCASE.
        let err = create(
            &state.db,
            "JOSIAH",
            "Impostor",
            "a-long-enough-password",
            Role::User,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");

        assert_eq!(list(&state.db).await.unwrap().len(), 1, "nothing was created");
    }

    #[tokio::test]
    async fn create_enforces_the_password_policy() {
        let state = test_state().await;
        let err = create(
            &state.db,
            "josiah",
            "Josiah",
            "elevenchars",
            Role::Admin,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)), "got {err:?}");
        assert!(list(&state.db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_returns_active_and_inactive_users_by_username() {
        let state = test_state().await;
        seed_user(&state, "zoe", Role::Admin, true).await;
        seed_user(&state, "adam", Role::User, true).await;
        let inactive = seed_user(&state, "mallory", Role::User, false).await;

        let usernames: Vec<String> = list(&state.db)
            .await
            .unwrap()
            .into_iter()
            .map(|u| u.username)
            .collect();
        assert_eq!(usernames, vec!["adam", "mallory", "zoe"]);
        assert!(!inactive.active, "a deactivated user still lists");
    }

    #[tokio::test]
    async fn update_changes_display_name_and_role_and_leaves_absent_fields_alone() {
        let state = test_state().await;
        // A second admin so the demote guard is not what is being tested here.
        seed_user(&state, "admin", Role::Admin, true).await;
        let user = seed_user(&state, "josiah", Role::User, true).await;

        // Display name only.
        let renamed = update(&state.db, &user.id, Some("  Josiah C  "), None)
            .await
            .unwrap();
        assert_eq!(renamed.display_name, "Josiah C");
        assert_eq!(renamed.role, Role::User, "role untouched");
        assert!(renamed.updated_at >= user.updated_at);

        // Role only (a promotion).
        let promoted = update(&state.db, &user.id, None, Some(Role::Admin))
            .await
            .unwrap();
        assert_eq!(promoted.role, Role::Admin);
        assert_eq!(promoted.display_name, "Josiah C", "display name untouched");

        // Neither: a no-op that still returns the row.
        assert_eq!(
            update(&state.db, &user.id, None, None).await.unwrap(),
            promoted
        );

        // An empty display name is rejected.
        assert!(update(&state.db, &user.id, Some("   "), None).await.is_err());

        // Unknown id is a 404.
        let err = update(&state.db, "no-such-id", Some("X"), None)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn set_password_replaces_the_hash_and_enforces_the_policy() {
        let state = test_state().await;
        let user = seed_user(&state, "josiah", Role::User, true).await;
        let before = password_hash_of(&state.db, &user.id).await.unwrap().unwrap();
        assert!(verify_password(SEED_PASSWORD, &before).await.unwrap());

        set_password(&state.db, &user.id, "brand-new-password", true)
            .await
            .unwrap();

        let after = password_hash_of(&state.db, &user.id).await.unwrap().unwrap();
        assert_ne!(after, before);
        assert!(verify_password("brand-new-password", &after).await.unwrap());
        assert!(
            !verify_password(SEED_PASSWORD, &after).await.unwrap(),
            "the old password must stop working"
        );

        // Same 12-character policy as every other write path.
        let err = set_password(&state.db, &user.id, "elevenchars", true)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)), "got {err:?}");
        // ...and the stored hash is unchanged by the rejected attempt.
        assert_eq!(
            password_hash_of(&state.db, &user.id).await.unwrap().unwrap(),
            after
        );

        // Unknown id is a 404.
        let err = set_password(&state.db, "no-such-id", "brand-new-password", true)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn set_active_toggles_the_flag_and_never_deletes_the_row() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let user = seed_user(&state, "josiah", Role::User, true).await;

        let deactivated = set_active(&state.db, &user.id, false, Some(&admin.id))
            .await
            .unwrap();
        assert!(!deactivated.active);

        // The row survives — deactivation is a flag, never a delete.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT active FROM user WHERE id = ?1",
                params![user.id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("the row still exists");
        assert_eq!(row.get::<i64>(0).unwrap(), 0);
        assert_eq!(list(&state.db).await.unwrap().len(), 2);

        // Reactivating restores it; a redundant call is a clean no-op.
        assert!(set_active(&state.db, &user.id, true, Some(&admin.id))
            .await
            .unwrap()
            .active);
        assert!(set_active(&state.db, &user.id, true, Some(&admin.id))
            .await
            .unwrap()
            .active);

        let err = set_active(&state.db, "no-such-id", false, Some(&admin.id))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn no_store_method_ever_deletes_a_user_row() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let user = seed_user(&state, "josiah", Role::User, true).await;

        set_active(&state.db, &user.id, false, Some(&admin.id))
            .await
            .unwrap();
        update(&state.db, &user.id, Some("Gone"), Some(Role::User))
            .await
            .unwrap();
        set_password(&state.db, &user.id, "another-password", true)
            .await
            .unwrap();

        let mut rows = state
            .db
            .conn()
            .query("SELECT COUNT(*) FROM user", ())
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 2, "every row written is still there");
        assert!(get(&state.db, &user.id).await.unwrap().is_some());
    }

    // ---- Lockout guards -----------------------------------------------------

    #[tokio::test]
    async fn count_active_admins_counts_only_active_admins() {
        let state = test_state().await;
        assert_eq!(count_active_admins(&state.db).await.unwrap(), 0);

        seed_user(&state, "admin1", Role::Admin, true).await;
        assert_eq!(count_active_admins(&state.db).await.unwrap(), 1);

        seed_user(&state, "admin2", Role::Admin, true).await;
        assert_eq!(count_active_admins(&state.db).await.unwrap(), 2);

        // An *inactive* admin does not count.
        seed_user(&state, "admin3", Role::Admin, false).await;
        // Neither does an active non-admin.
        seed_user(&state, "regular", Role::User, true).await;
        assert_eq!(count_active_admins(&state.db).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn deactivating_the_last_active_admin_is_a_409() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let other = seed_user(&state, "regular", Role::User, true).await;

        // `other` is the actor, so the self-deactivation guard is not what
        // fires here — the last-active-admin one is.
        let err = set_active(&state.db, &admin.id, false, Some(&other.id))
            .await
            .unwrap_err();
        assert_conflict(err, "cannot deactivate the last active admin");
        assert!(
            get(&state.db, &admin.id).await.unwrap().unwrap().active,
            "the refused change must not have been applied"
        );

        // With a second active admin the same call succeeds.
        seed_user(&state, "admin2", Role::Admin, true).await;
        assert!(!set_active(&state.db, &admin.id, false, Some(&other.id))
            .await
            .unwrap()
            .active);
    }

    #[tokio::test]
    async fn demoting_the_last_active_admin_is_a_409() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;

        let err = update(&state.db, &admin.id, None, Some(Role::User))
            .await
            .unwrap_err();
        assert_conflict(err, "cannot demote the last active admin");
        assert_eq!(
            get(&state.db, &admin.id).await.unwrap().unwrap().role,
            Role::Admin,
            "the refused change must not have been applied"
        );

        // Re-writing the role it already has is not a demotion.
        assert_eq!(
            update(&state.db, &admin.id, None, Some(Role::Admin))
                .await
                .unwrap()
                .role,
            Role::Admin
        );

        // With a second active admin the demotion goes through.
        seed_user(&state, "admin2", Role::Admin, true).await;
        assert_eq!(
            update(&state.db, &admin.id, None, Some(Role::User))
                .await
                .unwrap()
                .role,
            Role::User
        );
    }

    #[tokio::test]
    async fn an_inactive_admin_does_not_keep_the_last_active_one_safe() {
        // The guard counts *active* admins, so a deactivated admin row is no
        // substitute for a second live one.
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let benched = seed_user(&state, "benched", Role::Admin, false).await;
        let other = seed_user(&state, "regular", Role::User, true).await;

        let err = set_active(&state.db, &admin.id, false, Some(&other.id))
            .await
            .unwrap_err();
        assert_conflict(err, "cannot deactivate the last active admin");
        // Deactivating the already-inactive one is a harmless no-op.
        assert!(!set_active(&state.db, &benched.id, false, Some(&other.id))
            .await
            .unwrap()
            .active);
    }

    #[tokio::test]
    async fn self_deactivation_is_a_409() {
        let state = test_state().await;
        // Two active admins, so the *only* guard that can fire is the self one.
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        seed_user(&state, "admin2", Role::Admin, true).await;

        let err = set_active(&state.db, &admin.id, false, Some(&admin.id))
            .await
            .unwrap_err();
        assert_conflict(err, "you cannot deactivate your own account");
        assert!(get(&state.db, &admin.id).await.unwrap().unwrap().active);

        // A regular user cannot deactivate themselves either.
        let regular = seed_user(&state, "regular", Role::User, true).await;
        let err = set_active(&state.db, &regular.id, false, Some(&regular.id))
            .await
            .unwrap_err();
        assert_conflict(err, "you cannot deactivate your own account");

        // Reactivating yourself is fine — the guard is about deactivation only.
        set_active(&state.db, &regular.id, true, Some(&regular.id))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn guards_do_not_block_reactivation_or_promotion() {
        let state = test_state().await;
        let admin = seed_user(&state, "admin", Role::Admin, true).await;
        let benched = seed_user(&state, "benched", Role::Admin, false).await;

        assert!(set_active(&state.db, &benched.id, true, Some(&admin.id))
            .await
            .unwrap()
            .active);
        let regular = seed_user(&state, "regular", Role::User, true).await;
        assert_eq!(
            update(&state.db, &regular.id, None, Some(Role::Admin))
                .await
                .unwrap()
                .role,
            Role::Admin
        );
    }

    // ---- Fixtures -----------------------------------------------------------

    #[tokio::test]
    async fn seed_user_produces_a_loginable_user_in_either_active_state() {
        let state = test_state().await;
        let active = seed_user(&state, "josiah", Role::Admin, true).await;
        assert_eq!(active.username, "josiah");
        assert_eq!(active.role, Role::Admin);
        assert!(active.active);

        let hash = password_hash_of(&state.db, &active.id).await.unwrap().unwrap();
        assert!(verify_password(SEED_PASSWORD, &hash).await.unwrap());

        // The inactive form bypasses the guards a real API call would hit.
        let inactive = seed_user(&state, "benched", Role::Admin, false).await;
        assert!(!inactive.active);
    }

    #[test]
    fn role_round_trips_through_its_stored_string() {
        for role in [Role::Admin, Role::User] {
            assert_eq!(Role::parse(role.as_str()), Some(role));
        }
        assert_eq!(Role::parse("superuser"), None);
        assert_eq!(
            serde_json::to_value(Role::Admin).unwrap(),
            serde_json::json!("admin")
        );
    }

    #[test]
    fn serialized_user_never_carries_a_password_hash() {
        let user = User {
            id: "u1".to_string(),
            username: "josiah".to_string(),
            display_name: "Josiah".to_string(),
            role: Role::Admin,
            active: true,
            created_at: 1,
            updated_at: 2,
        };
        let json = serde_json::to_value(&user).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "u1",
                "username": "josiah",
                "display_name": "Josiah",
                "role": "admin",
                "active": true,
                "created_at": 1,
                "updated_at": 2,
            })
        );
    }
}
