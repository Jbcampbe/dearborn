//! Runtime configuration.
//!
//! Config is loaded from the process environment, with an **optional** config
//! file used as a fallback for any key not present in the environment. Point
//! `DEARBORN_CONFIG` at a `KEY=VALUE` file (`#` comments and blank lines are
//! ignored) to use it. Environment variables always win over the file.
//!
//! The one required value (`DEARBORN_MASTER_KEY`) is validated at load time so
//! the server fails fast at boot rather than at first request. There is no
//! bearer-token setting any more: credentials are per-user access tokens minted
//! by the `/auth/*` routes (see [`crate::auth`] and [`crate::sessions`]).

use std::collections::HashMap;

use thiserror::Error;

/// Default bind address when `DEARBORN_BIND` is unset.
pub const DEFAULT_BIND: &str = "127.0.0.1:8787";
/// Default local libSQL/SQLite database path when `DEARBORN_DB` is unset.
pub const DEFAULT_DB_PATH: &str = "~/.dearborn/dearborn.db";
/// Default per-project clone root when `DEARBORN_CLONE_ROOT` is unset.
pub const DEFAULT_CLONE_ROOT: &str = "~/.dearborn/clones";
/// Default scratch-workspace root when `DEARBORN_SCRATCH_ROOT` is unset.
/// Prototype nodes build their throwaway artifacts under here — deliberately
/// a separate tree from [`DEFAULT_CLONE_ROOT`], because a prototype's scratch
/// workspace is **not** a target-repo clone (wayfinder epic §10).
pub const DEFAULT_SCRATCH_ROOT: &str = "~/.dearborn/scratch";
/// Default directory of built SPA assets when `DEARBORN_STATIC_DIR` is unset.
/// Relative to the process working directory (the workspace root under `cargo
/// run`). If it does not exist the server serves the API only (see `lib::app`).
pub const DEFAULT_STATIC_DIR: &str = "./client/dist";

/// Fully-resolved server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the HTTP server binds to (`DEARBORN_BIND`).
    pub bind: String,
    /// AES-256-GCM key material used to encrypt PATs at rest (`DEARBORN_MASTER_KEY`).
    /// Validated for presence here; consumed by T-102.
    pub master_key: String,
    /// Path to the local libSQL database file (`DEARBORN_DB`).
    pub db_path: String,
    /// Root directory under which per-project clones live (`DEARBORN_CLONE_ROOT`).
    pub clone_root: String,
    /// Root directory under which throwaway agent scratch workspaces live
    /// (`DEARBORN_SCRATCH_ROOT`) — prototype nodes get
    /// `<scratch_root>/prototype/<node_id>/` as their working directory, kept
    /// strictly apart from the project clones (wayfinder epic §10: the
    /// prototype's scratch workspace is never a target-repo clone).
    pub scratch_root: String,
    /// Directory of built Vite SPA assets served at `/` (`DEARBORN_STATIC_DIR`).
    /// When it is absent the server logs a warning and serves the API only.
    pub static_dir: String,
    /// Whether project create/refresh spawns a real `git clone`/`git fetch`
    /// (T-103). Always `true` in production; tests default it `false` so plain
    /// CRUD tests never shell out to git. Not env-configurable — an internal seam.
    pub auto_clone: bool,
    /// Whether password hashing uses deliberately weak Argon2id parameters
    /// (m=8 KiB, t=1, p=1) instead of the production cost. Always `false` in
    /// production; tests default it `true`. Not env-configurable — an internal
    /// seam, exactly like [`auto_clone`](Self::auto_clone).
    ///
    /// Production Argon2id burns ~40–60 ms of CPU per hash by design. That is
    /// the point in production and pure tax in a test suite that seeds a user
    /// per case, so the test config selects the cheapest legal parameters. The
    /// PHC string records whichever parameters produced it, so a hash written
    /// under either setting still verifies under the other. See
    /// [`crate::users::hash_password`].
    pub argon2_fast: bool,
    /// Session-lifetime tuning for the multi-user auth epic. See [`AuthConfig`].
    pub auth: AuthConfig,
    /// Executor worker-pool tuning (Milestone 2 §2.7). See [`ExecutorConfig`].
    pub executor: ExecutorConfig,
}

/// How long the two halves of a session live. Both fields resolve through the
/// same env-then-file path as the rest of [`Config`] and are parsed with the
/// same [`parse_or_warn`] the executor knobs use: a missing, unparseable, or
/// zero value **warns and falls back to the default** rather than failing boot.
///
/// The split is what makes the product's "revocation is eventual, bounded by
/// the access-token lifetime" literally true: the access token is verified
/// offline (no database read), so a deactivation lands at the next refresh, at
/// most `access_ttl_secs` later.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Lifetime of a minted access token, in seconds
    /// (`DEARBORN_ACCESS_TTL_SECS`). Default `86400` (24 h) — long enough that
    /// day-to-day use never re-prompts, short enough to bound how stale a
    /// revoked claim can get. **Rejects `0`**: a 0s access token would expire
    /// before the response carrying it reached the browser.
    pub access_ttl_secs: u64,
    /// Absolute lifetime of a session's refresh token, in seconds
    /// (`DEARBORN_REFRESH_TTL_SECS`). Default `15552000` (180 days), so a
    /// browser left alone for a year re-prompts for a password but one left
    /// alone over a holiday does not. **Rejects `0`** for the same reason.
    pub refresh_ttl_secs: u64,
}

/// Tuning knobs for the executor worker pool (Milestone 2 §2.7). Every field
/// has a same-named environment variable and resolves through the same
/// env-then-file path as the rest of [`Config`] (see [`resolve`]). An invalid
/// or missing value **never fails boot**: it falls back to the default noted
/// below and emits a `tracing::warn!` naming the offending variable — a bad
/// executor knob should degrade the run, not crash the server.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Number of long-lived worker loops started in `main` (D2), each
    /// claiming and driving one epic/task at a time
    /// (`DEARBORN_WORKER_CONCURRENCY`). Default `2`. **Rejects `0`**: a
    /// 0-worker pool would silently accept enqueued work and never run it —
    /// a hang with no error, which is worse than falling back to a default.
    pub worker_concurrency: usize,
    /// How long a claimed lease is valid before it implicitly expires and
    /// becomes re-claimable by another worker (`DEARBORN_LEASE_TTL_SECS`,
    /// D4 — there is no reaper task, expiry is implicit). Default `300`
    /// (5 minutes): long enough to tolerate a missed heartbeat tick or two
    /// without letting a genuinely dead worker squat on work indefinitely.
    /// **Rejects `0`**: a 0s lease would expire the instant it is granted.
    pub lease_ttl_secs: u64,
    /// Interval between heartbeat renewals of a held lease
    /// (`DEARBORN_HEARTBEAT_SECS`). Default `30`, comfortably under
    /// `lease_ttl_secs` so ordinary scheduling jitter never lets a live
    /// worker's lease lapse. **Rejects `0`**: a 0s heartbeat is a busy-loop
    /// against the database.
    pub heartbeat_secs: u64,
    /// Wall-clock ceiling on a single agent stage — implement, review, or fix
    /// (`DEARBORN_AGENT_STAGE_TIMEOUT_SECS`). Default `9000` (2.5 hours):
    /// generous enough for a real Claude Code run on a nontrivial task while
    /// still bounding a stuck or looping agent. **Rejects `0`**: an instant
    /// timeout would fail every stage before it could produce anything —
    /// that is a misconfiguration, not a valid "run forever" or "skip" knob.
    pub agent_stage_timeout_secs: u64,
    /// Wall-clock ceiling on a single `setup_cmd`/`test_cmd` invocation
    /// (`DEARBORN_CMD_TIMEOUT_SECS`, T-520). Default `900` (15 minutes):
    /// enough headroom for a cold dependency install or a slow test suite.
    /// **Rejects `0`** for the same reason as `agent_stage_timeout_secs`.
    pub cmd_timeout_secs: u64,
    /// Maximum attempts the test-driven fix loop takes to turn a red
    /// `test_cmd` green before failing the task
    /// (`DEARBORN_MAX_TEST_FIX_ATTEMPTS`, ralph parity, T-522). Default `3`.
    /// `0` is **accepted** — "fail on first red, no fix loop" is an
    /// aggressive but legitimate configuration, not a broken one.
    pub max_test_fix_attempts: u32,
    /// Maximum rounds of reviewer `NEEDS_CHANGES` → fix the review-convergence
    /// loop takes before giving up (`DEARBORN_MAX_FIX_ROUNDS`, ralph parity,
    /// T-530+). Default `3`. `0` is **accepted** for the same reason as
    /// `max_test_fix_attempts`.
    pub max_fix_rounds: u32,
    /// Extra attempts the verdict parser gets when a review response is
    /// missing a parseable `VERDICT:` line before the round counts as a
    /// contract failure (`DEARBORN_VERDICT_RETRIES`, D9, ralph parity).
    /// Default `1`. `0` is **accepted** — "no re-run, first miss fails" is a
    /// valid strict-contract configuration.
    pub verdict_retries: u32,
    /// Extra attempts `process_one_task` gives the implement stage when its
    /// recorded error text matches a transient provider signal (an HTTP 429
    /// rate limit, an "overloaded"/5xx gateway response — see
    /// `worker::is_transient_provider_error` for the exact match set)
    /// (`DEARBORN_IMPLEMENT_TRANSIENT_RETRIES`). Default `1`: one automatic
    /// retry on top of the original run, so a mid-run rate limit no longer
    /// fails a task whose agent may already have finished (or nearly
    /// finished) its fix. `0` is **accepted** — "fail on first transient,
    /// never re-run" is a legitimate strict configuration, not a broken one.
    pub implement_transient_retries: u32,
    /// Fallback poll interval for workers waiting on `tokio::sync::Notify`
    /// (`DEARBORN_POLL_INTERVAL_MS`) — the backstop in case a notify is
    /// missed. Default `1500`ms. **Rejects `0`**: a 0ms poll is a busy-loop.
    pub poll_interval_ms: u64,
    /// Interval between ticks of the single long-lived review-poller
    /// (`DEARBORN_REVIEW_POLL_INTERVAL_SECS`), which scans `InReview` items
    /// for PR merge/close state and (in later tasks) feedback. Default `60`
    /// seconds. **Rejects `0`**: a 0-second poll is a busy-loop against the
    /// database and the GitHub API.
    pub review_poll_interval_secs: u64,
}

/// Errors that prevent the server from booting with a valid configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required variable was absent from both the environment and the file.
    #[error("required configuration `{0}` is not set (via env or config file)")]
    Missing(&'static str),
    /// A required variable was present but empty.
    #[error("required configuration `{0}` must not be empty")]
    Empty(&'static str),
    /// The `DEARBORN_CONFIG` file was named but could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    ConfigFileRead {
        path: String,
        source: std::io::Error,
    },
}

impl Config {
    /// Resolve configuration from the environment plus an optional config file.
    ///
    /// Fails fast if `DEARBORN_MASTER_KEY` is missing/empty.
    pub fn from_env() -> Result<Config, ConfigError> {
        let file = load_config_file()?;

        let bind = resolve(&file, "DEARBORN_BIND")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_BIND.to_string());
        let master_key = required(&file, "DEARBORN_MASTER_KEY")?;
        let db_path = expand_tilde(
            resolve(&file, "DEARBORN_DB")
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_DB_PATH.to_string()),
        );
        let clone_root = expand_tilde(
            resolve(&file, "DEARBORN_CLONE_ROOT")
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_CLONE_ROOT.to_string()),
        );
        let scratch_root = expand_tilde(
            resolve(&file, "DEARBORN_SCRATCH_ROOT")
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_SCRATCH_ROOT.to_string()),
        );
        let static_dir = resolve(&file, "DEARBORN_STATIC_DIR")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_STATIC_DIR.to_string());
        let auth = auth_from(&resolve_vars(&file, AUTH_VAR_NAMES));
        let executor = executor_from(&resolve_vars(&file, EXECUTOR_VAR_NAMES));

        Ok(Config {
            bind,
            master_key,
            db_path,
            clone_root,
            scratch_root,
            static_dir,
            auto_clone: true,
            argon2_fast: false,
            auth,
            executor,
        })
    }
}

/// Environment variable names for every [`ExecutorConfig`] field, in
/// declaration order.
const EXECUTOR_VAR_NAMES: &[&str] = &[
    "DEARBORN_WORKER_CONCURRENCY",
    "DEARBORN_LEASE_TTL_SECS",
    "DEARBORN_HEARTBEAT_SECS",
    "DEARBORN_AGENT_STAGE_TIMEOUT_SECS",
    "DEARBORN_CMD_TIMEOUT_SECS",
    "DEARBORN_MAX_TEST_FIX_ATTEMPTS",
    "DEARBORN_MAX_FIX_ROUNDS",
    "DEARBORN_VERDICT_RETRIES",
    "DEARBORN_IMPLEMENT_TRANSIENT_RETRIES",
    "DEARBORN_POLL_INTERVAL_MS",
    "DEARBORN_REVIEW_POLL_INTERVAL_SECS",
];

/// Environment variable names for every [`AuthConfig`] field, in declaration
/// order.
const AUTH_VAR_NAMES: &[&str] = &["DEARBORN_ACCESS_TTL_SECS", "DEARBORN_REFRESH_TTL_SECS"];

/// Resolve a group of tuning variables through the same env-then-file path as
/// the rest of `Config` (via [`resolve`]), collecting the results into a plain
/// map. Kept separate from [`executor_from`]/[`auth_from`] so the actual
/// parse-or-default logic is a pure function of a `HashMap`, testable without
/// touching process-global env (see `mod tests`).
fn resolve_vars(file: &HashMap<String, String>, names: &[&str]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for key in names {
        if let Some(v) = resolve(file, key) {
            map.insert((*key).to_string(), v);
        }
    }
    map
}

/// Parse an [`AuthConfig`] out of an already-resolved key/value map (see
/// [`resolve_vars`]). Pure, for the same reason [`executor_from`] is.
///
/// Both TTLs reject `0` and warn-and-default rather than failing boot: a bad
/// session lifetime should degrade to the documented one, not stop an operator
/// from reaching their own instance.
fn auth_from(map: &HashMap<String, String>) -> AuthConfig {
    AuthConfig {
        access_ttl_secs: parse_or_warn(map, "DEARBORN_ACCESS_TTL_SECS", 86_400u64, true),
        refresh_ttl_secs: parse_or_warn(map, "DEARBORN_REFRESH_TTL_SECS", 15_552_000u64, true),
    }
}

/// Parse an [`ExecutorConfig`] out of an already-resolved key/value map (see
/// [`resolve_vars`]). Pure, so it is unit-tested directly with
/// hand-built maps instead of through `from_env`, which would require
/// mutating process-global env under a threaded test runner.
fn executor_from(map: &HashMap<String, String>) -> ExecutorConfig {
    ExecutorConfig {
        worker_concurrency: parse_or_warn(map, "DEARBORN_WORKER_CONCURRENCY", 2usize, true),
        lease_ttl_secs: parse_or_warn(map, "DEARBORN_LEASE_TTL_SECS", 300u64, true),
        heartbeat_secs: parse_or_warn(map, "DEARBORN_HEARTBEAT_SECS", 30u64, true),
        agent_stage_timeout_secs: parse_or_warn(
            map,
            "DEARBORN_AGENT_STAGE_TIMEOUT_SECS",
            9000u64,
            true,
        ),
        cmd_timeout_secs: parse_or_warn(map, "DEARBORN_CMD_TIMEOUT_SECS", 900u64, true),
        max_test_fix_attempts: parse_or_warn(map, "DEARBORN_MAX_TEST_FIX_ATTEMPTS", 3u32, false),
        max_fix_rounds: parse_or_warn(map, "DEARBORN_MAX_FIX_ROUNDS", 3u32, false),
        verdict_retries: parse_or_warn(map, "DEARBORN_VERDICT_RETRIES", 1u32, false),
        implement_transient_retries: parse_or_warn(
            map,
            "DEARBORN_IMPLEMENT_TRANSIENT_RETRIES",
            1u32,
            false,
        ),
        poll_interval_ms: parse_or_warn(map, "DEARBORN_POLL_INTERVAL_MS", 1500u64, true),
        review_poll_interval_secs: parse_or_warn(
            map,
            "DEARBORN_REVIEW_POLL_INTERVAL_SECS",
            60u64,
            true,
        ),
    }
}

/// Parse a single tuning value (executor or auth) out of a resolved map, falling back
/// to `default` (with a `tracing::warn!` naming the variable and the bad
/// value) if the key is absent, empty, unparseable, or — when `reject_zero`
/// is set — zero. Factors the "parse or warn-and-default" behavior so it is
/// written once instead of once per field.
fn parse_or_warn<T>(map: &HashMap<String, String>, key: &str, default: T, reject_zero: bool) -> T
where
    T: std::str::FromStr + PartialEq + From<u8> + std::fmt::Display + Copy,
{
    let Some(raw) = map.get(key).filter(|v| !v.is_empty()) else {
        return default;
    };
    match raw.parse::<T>() {
        Ok(v) if reject_zero && v == T::from(0u8) => {
            tracing::warn!(
                var = key,
                value = %raw,
                default = %default,
                "config: value must be nonzero; using default"
            );
            default
        }
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(
                var = key,
                value = %raw,
                default = %default,
                "config: value is not a valid number; using default"
            );
            default
        }
    }
}

/// Expand a leading `~/` (or bare `~`) to the user's home directory.
///
/// `~` is a shell convention: neither the OS nor libSQL interprets it, so a
/// literal `~/.dearborn/dearborn.db` would be opened *relative to the current
/// directory* inside a directory actually named `~`. Only a leading tilde
/// followed by end-of-string or `/` is expanded; `~user` forms are left as-is.
fn expand_tilde(path: String) -> String {
    let Some(rest) = path.strip_prefix('~') else {
        return path;
    };
    // Only expand a bare `~` or `~/...`; leave `~user` and `~foo/bar` alone.
    if !(rest.is_empty() || rest.starts_with('/')) {
        return path;
    }
    match std::env::var("HOME") {
        Ok(home) if !home.is_empty() => format!("{home}{rest}"),
        _ => path, // no HOME: leave the path untouched
    }
}

/// Look a key up in the environment first, then the config-file map.
fn resolve(file: &HashMap<String, String>, key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) => Some(v),
        Err(_) => file.get(key).cloned(),
    }
}

/// Resolve a required, non-empty value or produce a precise [`ConfigError`].
fn required(file: &HashMap<String, String>, key: &'static str) -> Result<String, ConfigError> {
    match resolve(file, key) {
        None => Err(ConfigError::Missing(key)),
        Some(v) if v.is_empty() => Err(ConfigError::Empty(key)),
        Some(v) => Ok(v),
    }
}

/// Load the optional `KEY=VALUE` config file named by `DEARBORN_CONFIG`.
///
/// Returns an empty map when `DEARBORN_CONFIG` is unset. Blank lines and lines
/// starting with `#` are ignored; the value is everything after the first `=`
/// (surrounding whitespace and one layer of matching quotes are trimmed).
fn load_config_file() -> Result<HashMap<String, String>, ConfigError> {
    let path = match std::env::var("DEARBORN_CONFIG") {
        Ok(p) if !p.is_empty() => p,
        _ => return Ok(HashMap::new()),
    };

    let contents = std::fs::read_to_string(&path)
        .map_err(|source| ConfigError::ConfigFileRead { path, source })?;

    Ok(parse_config_file(&contents))
}

/// Parse `KEY=VALUE` lines into a map. Pure so it can be unit-tested.
fn parse_config_file(contents: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let value = value.trim();
        // Strip one layer of matching surrounding quotes, if present.
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(value);
        map.insert(key.to_string(), value.to_string());
    }
    map
}

impl Config {
    /// Build a config for tests without touching process-global env.
    ///
    /// Unconditionally `pub` (not `#[cfg(test)]`) for the same reason
    /// [`crate::users::testing`] is: an integration test in `tests/` compiles as
    /// its own crate and never sees anything gated behind this crate's
    /// `#[cfg(test)]`.
    pub fn for_test() -> Config {
        Config {
            bind: DEFAULT_BIND.to_string(),
            master_key: "test-master-key".to_string(),
            db_path: ":memory:".to_string(),
            clone_root: DEFAULT_CLONE_ROOT.to_string(),
            // A throwaway location no real scratch work would ever live in —
            // per-node scratch directories are ULID-keyed, so concurrent
            // tests never collide.
            scratch_root: std::env::temp_dir()
                .join("dearborn-test-scratch")
                .to_string_lossy()
                .to_string(),
            static_dir: DEFAULT_STATIC_DIR.to_string(),
            // Plain CRUD tests must not shell out to git; T-103 tests that
            // exercise cloning flip this on explicitly.
            auto_clone: false,
            // Seeding a user per test case must not cost ~50ms of Argon2.
            argon2_fast: true,
            // Production lifetimes: a test that wants an *expired* session
            // writes the row's `expires_at` directly rather than waiting one
            // out, so shortening these here would buy nothing and would make
            // ordinary login/refresh tests race the clock.
            auth: AuthConfig {
                access_ttl_secs: 86_400,
                refresh_ttl_secs: 15_552_000,
            },
            executor: ExecutorConfig {
                // 1 worker + a 10ms poll keep tests deterministic and fast.
                worker_concurrency: 1,
                lease_ttl_secs: 30,
                heartbeat_secs: 5,
                agent_stage_timeout_secs: 10,
                cmd_timeout_secs: 10,
                // Ralph-parity counts stay at production defaults so tests
                // exercise the real loop bounds (T-522, T-530+) — including
                // this one, so implement-retry tests see the real one extra
                // transient attempt rather than an off-by-one test-only bound.
                max_test_fix_attempts: 3,
                max_fix_rounds: 3,
                verdict_retries: 1,
                implement_transient_retries: 1,
                poll_interval_ms: 10,
                review_poll_interval_secs: 60,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ignores_comments_and_blanks() {
        let map = parse_config_file("# a comment\n\nDEARBORN_DB=x.db\n");
        assert_eq!(map.get("DEARBORN_DB"), Some(&"x.db".to_string()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn parse_strips_matching_quotes_and_whitespace() {
        let map = parse_config_file("  DEARBORN_DB = \"./x.db\" \nK='v'\n");
        assert_eq!(map.get("DEARBORN_DB"), Some(&"./x.db".to_string()));
        assert_eq!(map.get("K"), Some(&"v".to_string()));
    }

    #[test]
    fn parse_keeps_equals_in_value() {
        let map = parse_config_file("DEARBORN_MASTER_KEY=aa==bb\n");
        assert_eq!(map.get("DEARBORN_MASTER_KEY"), Some(&"aa==bb".to_string()));
    }

    #[test]
    fn expand_tilde_expands_leading_home() {
        // SAFETY: tests run single-threaded w.r.t. this env var; restore after.
        let old = std::env::var("HOME").ok();
        std::env::set_var("HOME", "/home/tester");

        assert_eq!(
            expand_tilde("~/.dearborn/dearborn.db".into()),
            "/home/tester/.dearborn/dearborn.db"
        );
        assert_eq!(expand_tilde("~".into()), "/home/tester");
        // Non-leading or `~user` forms are left untouched.
        assert_eq!(expand_tilde("/tmp/~x".into()), "/tmp/~x");
        assert_eq!(expand_tilde("~root/db".into()), "~root/db");
        // No/empty HOME leaves the path as-is.
        std::env::set_var("HOME", "");
        assert_eq!(
            expand_tilde("~/.dearborn/dearborn.db".into()),
            "~/.dearborn/dearborn.db"
        );

        match old {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }

    fn map_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn executor_from_parses_every_documented_value() {
        let map = map_of(&[
            ("DEARBORN_WORKER_CONCURRENCY", "5"),
            ("DEARBORN_LEASE_TTL_SECS", "600"),
            ("DEARBORN_HEARTBEAT_SECS", "60"),
            ("DEARBORN_AGENT_STAGE_TIMEOUT_SECS", "3600"),
            ("DEARBORN_CMD_TIMEOUT_SECS", "1200"),
            ("DEARBORN_MAX_TEST_FIX_ATTEMPTS", "5"),
            ("DEARBORN_MAX_FIX_ROUNDS", "4"),
            ("DEARBORN_VERDICT_RETRIES", "2"),
            ("DEARBORN_IMPLEMENT_TRANSIENT_RETRIES", "4"),
            ("DEARBORN_POLL_INTERVAL_MS", "2000"),
            ("DEARBORN_REVIEW_POLL_INTERVAL_SECS", "90"),
        ]);
        let cfg = executor_from(&map);
        assert_eq!(cfg.worker_concurrency, 5);
        assert_eq!(cfg.lease_ttl_secs, 600);
        assert_eq!(cfg.heartbeat_secs, 60);
        assert_eq!(cfg.agent_stage_timeout_secs, 3600);
        assert_eq!(cfg.cmd_timeout_secs, 1200);
        assert_eq!(cfg.max_test_fix_attempts, 5);
        assert_eq!(cfg.max_fix_rounds, 4);
        assert_eq!(cfg.verdict_retries, 2);
        assert_eq!(cfg.implement_transient_retries, 4);
        assert_eq!(cfg.poll_interval_ms, 2000);
        assert_eq!(cfg.review_poll_interval_secs, 90);
    }

    #[test]
    fn executor_from_defaults_when_all_absent() {
        let cfg = executor_from(&HashMap::new());
        assert_eq!(cfg.worker_concurrency, 2);
        assert_eq!(cfg.lease_ttl_secs, 300);
        assert_eq!(cfg.heartbeat_secs, 30);
        assert_eq!(cfg.agent_stage_timeout_secs, 9000);
        assert_eq!(cfg.cmd_timeout_secs, 900);
        assert_eq!(cfg.max_test_fix_attempts, 3);
        assert_eq!(cfg.max_fix_rounds, 3);
        assert_eq!(cfg.verdict_retries, 1);
        assert_eq!(cfg.implement_transient_retries, 1);
        assert_eq!(cfg.poll_interval_ms, 1500);
        assert_eq!(cfg.review_poll_interval_secs, 60);
    }

    #[test]
    fn executor_from_defaults_on_unparseable_values() {
        let map = map_of(&[
            ("DEARBORN_WORKER_CONCURRENCY", "abc"),
            ("DEARBORN_LEASE_TTL_SECS", "abc"),
            ("DEARBORN_HEARTBEAT_SECS", "abc"),
            ("DEARBORN_AGENT_STAGE_TIMEOUT_SECS", "abc"),
            ("DEARBORN_CMD_TIMEOUT_SECS", "abc"),
            ("DEARBORN_MAX_TEST_FIX_ATTEMPTS", "abc"),
            ("DEARBORN_MAX_FIX_ROUNDS", "abc"),
            ("DEARBORN_VERDICT_RETRIES", "abc"),
            ("DEARBORN_IMPLEMENT_TRANSIENT_RETRIES", "abc"),
            ("DEARBORN_POLL_INTERVAL_MS", "abc"),
            ("DEARBORN_REVIEW_POLL_INTERVAL_SECS", "abc"),
        ]);
        let cfg = executor_from(&map);
        let defaults = executor_from(&HashMap::new());
        assert_eq!(cfg.worker_concurrency, defaults.worker_concurrency);
        assert_eq!(cfg.lease_ttl_secs, defaults.lease_ttl_secs);
        assert_eq!(cfg.heartbeat_secs, defaults.heartbeat_secs);
        assert_eq!(
            cfg.agent_stage_timeout_secs,
            defaults.agent_stage_timeout_secs
        );
        assert_eq!(cfg.cmd_timeout_secs, defaults.cmd_timeout_secs);
        assert_eq!(cfg.max_test_fix_attempts, defaults.max_test_fix_attempts);
        assert_eq!(cfg.max_fix_rounds, defaults.max_fix_rounds);
        assert_eq!(cfg.verdict_retries, defaults.verdict_retries);
        assert_eq!(
            cfg.implement_transient_retries,
            defaults.implement_transient_retries
        );
        assert_eq!(cfg.poll_interval_ms, defaults.poll_interval_ms);
        assert_eq!(
            cfg.review_poll_interval_secs,
            defaults.review_poll_interval_secs
        );
    }

    #[test]
    fn executor_from_defaults_on_zero_where_zero_is_invalid() {
        let map = map_of(&[
            ("DEARBORN_WORKER_CONCURRENCY", "0"),
            ("DEARBORN_LEASE_TTL_SECS", "0"),
            ("DEARBORN_HEARTBEAT_SECS", "0"),
            ("DEARBORN_AGENT_STAGE_TIMEOUT_SECS", "0"),
            ("DEARBORN_CMD_TIMEOUT_SECS", "0"),
            ("DEARBORN_POLL_INTERVAL_MS", "0"),
            ("DEARBORN_REVIEW_POLL_INTERVAL_SECS", "0"),
        ]);
        let cfg = executor_from(&map);
        assert_eq!(cfg.worker_concurrency, 2);
        assert_eq!(cfg.lease_ttl_secs, 300);
        assert_eq!(cfg.heartbeat_secs, 30);
        assert_eq!(cfg.agent_stage_timeout_secs, 9000);
        assert_eq!(cfg.cmd_timeout_secs, 900);
        assert_eq!(cfg.poll_interval_ms, 1500);
        assert_eq!(cfg.review_poll_interval_secs, 60);
    }

    #[test]
    fn executor_from_accepts_zero_for_ralph_parity_counts() {
        let map = map_of(&[
            ("DEARBORN_MAX_TEST_FIX_ATTEMPTS", "0"),
            ("DEARBORN_MAX_FIX_ROUNDS", "0"),
            ("DEARBORN_VERDICT_RETRIES", "0"),
            ("DEARBORN_IMPLEMENT_TRANSIENT_RETRIES", "0"),
        ]);
        let cfg = executor_from(&map);
        assert_eq!(cfg.max_test_fix_attempts, 0);
        assert_eq!(cfg.max_fix_rounds, 0);
        assert_eq!(cfg.verdict_retries, 0);
        assert_eq!(cfg.implement_transient_retries, 0);
    }

    #[test]
    fn auth_from_parses_both_ttls_and_defaults_when_absent() {
        let cfg = auth_from(&map_of(&[
            ("DEARBORN_ACCESS_TTL_SECS", "3600"),
            ("DEARBORN_REFRESH_TTL_SECS", "604800"),
        ]));
        assert_eq!(cfg.access_ttl_secs, 3600);
        assert_eq!(cfg.refresh_ttl_secs, 604_800);

        let defaults = auth_from(&HashMap::new());
        assert_eq!(defaults.access_ttl_secs, 86_400, "24 hours");
        assert_eq!(defaults.refresh_ttl_secs, 15_552_000, "180 days");
    }

    #[test]
    fn a_bad_ttl_warns_and_defaults_rather_than_failing_boot() {
        // Unparseable, zero, and empty all degrade to the documented default —
        // a mistyped session lifetime must never stop the server booting.
        for bad in ["abc", "0", "", "-5", "1.5"] {
            let cfg = auth_from(&map_of(&[
                ("DEARBORN_ACCESS_TTL_SECS", bad),
                ("DEARBORN_REFRESH_TTL_SECS", bad),
            ]));
            assert_eq!(cfg.access_ttl_secs, 86_400, "`{bad}` must fall back");
            assert_eq!(cfg.refresh_ttl_secs, 15_552_000, "`{bad}` must fall back");
        }
    }

    #[test]
    fn for_test_yields_fast_executor_values() {
        let cfg = Config::for_test();
        assert_eq!(cfg.executor.worker_concurrency, 1);
        assert_eq!(cfg.executor.poll_interval_ms, 10);
        assert_eq!(cfg.executor.lease_ttl_secs, 30);
        assert_eq!(cfg.executor.heartbeat_secs, 5);
        assert_eq!(cfg.executor.agent_stage_timeout_secs, 10);
        assert_eq!(cfg.executor.cmd_timeout_secs, 10);
        assert_eq!(cfg.executor.review_poll_interval_secs, 60);
        // Ralph-parity counts stay at real defaults even in test config.
        assert_eq!(cfg.executor.max_test_fix_attempts, 3);
        assert_eq!(cfg.executor.max_fix_rounds, 3);
        assert_eq!(cfg.executor.verdict_retries, 1);
        assert_eq!(cfg.executor.implement_transient_retries, 1);
    }

    #[test]
    fn internal_seams_default_off_in_tests_and_are_not_env_configurable() {
        let cfg = Config::for_test();
        // Neither seam is read from the environment — they exist only as
        // fields, so a `DEARBORN_ARGON2_FAST=1` in the environment is as
        // meaningless as a `DEARBORN_AUTO_CLONE=1`.
        assert!(!cfg.auto_clone);
        assert!(
            cfg.argon2_fast,
            "tests must not pay production Argon2 cost per seeded user"
        );
        assert!(!EXECUTOR_VAR_NAMES.contains(&"DEARBORN_ARGON2_FAST"));
    }
}
