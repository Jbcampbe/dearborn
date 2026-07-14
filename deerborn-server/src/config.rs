//! Runtime configuration.
//!
//! Config is loaded from the process environment, with an **optional** config
//! file used as a fallback for any key not present in the environment. Point
//! `DEERBORN_CONFIG` at a `KEY=VALUE` file (`#` comments and blank lines are
//! ignored) to use it. Environment variables always win over the file.
//!
//! Required values (`DEERBORN_TOKEN`, `DEERBORN_MASTER_KEY`) are validated at
//! load time so the server fails fast at boot rather than at first request.

use std::collections::HashMap;

use thiserror::Error;

/// Default bind address when `DEERBORN_BIND` is unset.
pub const DEFAULT_BIND: &str = "127.0.0.1:8787";
/// Default local libSQL/SQLite database path when `DEERBORN_DB` is unset.
pub const DEFAULT_DB_PATH: &str = "./deerborn.db";
/// Default per-project clone root when `DEERBORN_CLONE_ROOT` is unset.
pub const DEFAULT_CLONE_ROOT: &str = "./clones";
/// Default directory of built SPA assets when `DEERBORN_STATIC_DIR` is unset.
/// Relative to the process working directory (the workspace root under `cargo
/// run`). If it does not exist the server serves the API only (see `lib::app`).
pub const DEFAULT_STATIC_DIR: &str = "./client/dist";

/// Fully-resolved server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address the HTTP server binds to (`DEERBORN_BIND`).
    pub bind: String,
    /// Single-user bearer token every non-`/health` route requires (`DEERBORN_TOKEN`).
    pub token: String,
    /// AES-256-GCM key material used to encrypt PATs at rest (`DEERBORN_MASTER_KEY`).
    /// Validated for presence here; consumed by T-102.
    pub master_key: String,
    /// Path to the local libSQL database file (`DEERBORN_DB`).
    pub db_path: String,
    /// Root directory under which per-project clones live (`DEERBORN_CLONE_ROOT`).
    pub clone_root: String,
    /// Directory of built Vite SPA assets served at `/` (`DEERBORN_STATIC_DIR`).
    /// When it is absent the server logs a warning and serves the API only.
    pub static_dir: String,
    /// Whether project create/refresh spawns a real `git clone`/`git fetch`
    /// (T-103). Always `true` in production; tests default it `false` so plain
    /// CRUD tests never shell out to git. Not env-configurable — an internal seam.
    pub auto_clone: bool,
    /// Milliseconds the stub worker (T-403) sleeps between task transitions so a
    /// browser can watch the walk. `0` in tests (hermetic + fast); `600` in
    /// production. Optional env override: `DEERBORN_STUB_WORKER_DELAY_MS`.
    pub stub_worker_delay_ms: u64,
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
    /// The `DEERBORN_CONFIG` file was named but could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    ConfigFileRead {
        path: String,
        source: std::io::Error,
    },
}

impl Config {
    /// Resolve configuration from the environment plus an optional config file.
    ///
    /// Fails fast if `DEERBORN_TOKEN` or `DEERBORN_MASTER_KEY` is missing/empty.
    pub fn from_env() -> Result<Config, ConfigError> {
        let file = load_config_file()?;

        let bind = resolve(&file, "DEERBORN_BIND")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_BIND.to_string());
        let token = required(&file, "DEERBORN_TOKEN")?;
        let master_key = required(&file, "DEERBORN_MASTER_KEY")?;
        let db_path = resolve(&file, "DEERBORN_DB")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_DB_PATH.to_string());
        let clone_root = resolve(&file, "DEERBORN_CLONE_ROOT")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_CLONE_ROOT.to_string());
        let static_dir = resolve(&file, "DEERBORN_STATIC_DIR")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_STATIC_DIR.to_string());
        let stub_worker_delay_ms = resolve(&file, "DEERBORN_STUB_WORKER_DELAY_MS")
            .filter(|v| !v.is_empty())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600);

        Ok(Config {
            bind,
            token,
            master_key,
            db_path,
            clone_root,
            static_dir,
            auto_clone: true,
            stub_worker_delay_ms,
        })
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

/// Load the optional `KEY=VALUE` config file named by `DEERBORN_CONFIG`.
///
/// Returns an empty map when `DEERBORN_CONFIG` is unset. Blank lines and lines
/// starting with `#` are ignored; the value is everything after the first `=`
/// (surrounding whitespace and one layer of matching quotes are trimmed).
fn load_config_file() -> Result<HashMap<String, String>, ConfigError> {
    let path = match std::env::var("DEERBORN_CONFIG") {
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

#[cfg(test)]
impl Config {
    /// Build a config for tests without touching process-global env.
    pub fn for_test(token: &str) -> Config {
        Config {
            bind: DEFAULT_BIND.to_string(),
            token: token.to_string(),
            master_key: "test-master-key".to_string(),
            db_path: ":memory:".to_string(),
            clone_root: DEFAULT_CLONE_ROOT.to_string(),
            static_dir: DEFAULT_STATIC_DIR.to_string(),
            // Plain CRUD tests must not shell out to git; T-103 tests that
            // exercise cloning flip this on explicitly.
            auto_clone: false,
            // Tests want the stub worker to be instant (no visible delay).
            stub_worker_delay_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ignores_comments_and_blanks() {
        let map = parse_config_file("# a comment\n\nDEERBORN_TOKEN=abc\n");
        assert_eq!(map.get("DEERBORN_TOKEN"), Some(&"abc".to_string()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn parse_strips_matching_quotes_and_whitespace() {
        let map = parse_config_file("  DEERBORN_DB = \"./x.db\" \nK='v'\n");
        assert_eq!(map.get("DEERBORN_DB"), Some(&"./x.db".to_string()));
        assert_eq!(map.get("K"), Some(&"v".to_string()));
    }

    #[test]
    fn parse_keeps_equals_in_value() {
        let map = parse_config_file("DEERBORN_MASTER_KEY=aa==bb\n");
        assert_eq!(map.get("DEERBORN_MASTER_KEY"), Some(&"aa==bb".to_string()));
    }
}
