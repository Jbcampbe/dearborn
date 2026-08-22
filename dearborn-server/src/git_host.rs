//! The `GitHost` seam: push + open-PR + auth-probe, behind a trait (T-514,
//! Milestone 2 §14, D15).
//!
//! ## Why a trait at all
//!
//! Every other externally-observable side effect Dearborn performs already
//! sits behind a seam a test can fake: [`crate::planning::PlanningAgent`],
//! [`crate::breakdown::BreakdownAgent`], [`crate::task_agent::TaskAgent`].
//! Opening a pull request is the same shape of problem — it is the one step
//! in the whole executor pipeline that talks to a **third party** (GitHub's
//! API) rather than to git itself or to a locally-shelled-out agent — so it
//! gets the same treatment: [`GitHost`] is the trait, [`GithubHost`] is the
//! real implementation, and [`testing::FakeHost`] is what every test in this
//! crate (and `tests/worker_live.rs`, T-515) drives instead. Without this
//! seam, `just test` could not stay hermetic (MILESTONE_2 §10: "no network,
//! no GitHub") the moment a single test needed an epic to actually finish.
//!
//! ## Why boxed futures, not `async fn` directly on the trait
//!
//! Rust's native async-fn-in-trait (stable since 1.75) is exactly what
//! [`crate::planning::PlanningAgent`]/[`crate::task_agent::TaskAgent`] use —
//! but those traits' methods are all **synchronous** (`fn run(&self, req) ->
//! Result<(RunHandle, Receiver<RunEvent>), HarnessError>`); the actual async
//! work happens inside the harness, off the trait boundary entirely. `push`/
//! `open_pr`/`check_auth` have no such synchronous escape hatch — the whole
//! point of each is an `.await`ed network call. A trait with genuine `async
//! fn` methods is not object-safe (dyn-incompatible) without further
//! machinery, and `AppState.git_host` must be `Arc<dyn GitHost>` (mirroring
//! `Arc<dyn TaskAgent>` etc. — a single seam, injected once, shared by every
//! worker loop). The `async-trait` crate (which rewrites `async fn` methods
//! into exactly this shape via a macro) is not a dependency of this
//! workspace, and pulling it in for three methods is not worth a new crate;
//! writing the desugared form by hand — each method returns [`BoxFuture`], a
//! `Pin<Box<dyn Future<...> + Send + 'a>>` — is what the macro would have
//! generated anyway, and keeps `dyn GitHost` usable with zero extra
//! dependencies.
//!
//! ## GitHub only in v1 (D15, MILESTONE_2 §12)
//! A future Gitea host is explicitly v2 and "slots into the trait" (§14) —
//! [`GithubHost`] is the only production implementation; nothing about
//! [`GitHost`]'s shape is GitHub-specific (every method takes a plain
//! `repo_url`, not a parsed owner/repo — the GitHub-specific parsing lives
//! inside [`GithubHost`] alone, via [`parse_owner_repo`]).
//!
//! ## rustls, not native-tls (D15)
//! `reqwest`'s workspace dependency is `default-features = false` with only
//! `rustls-tls` + `json` enabled — `native-tls` would link OpenSSL, which
//! this project deliberately avoids (a `cargo tree` after this change should
//! show no `openssl*` crate anywhere in the dependency graph).
//!
//! ## Base branch: read from GitHub, never assumed (T-514 AC)
//! [`GithubHost::open_pr`] resolves the PR's base branch by calling `GET
//! /repos/{owner}/{repo}` and reading `default_branch` — **not** a hardcoded
//! `"main"` — because MILESTONE_2 §12 explicitly defers a per-project
//! `base_branch` override to v2; until that lands, the repo's own default is
//! the only correct target, and plenty of real repos still default to
//! `master` or something else entirely.
//!
//! ## Redaction discipline
//! Every error this module can produce — a `git push` failure (already
//! redacted by [`crate::git::redact`]), a non-2xx GitHub response, or a
//! `reqwest::Error` (whose `Display` can embed the request URL, though never
//! the PAT itself here since it travels only in the `Authorization` header —
//! see [`redacted_reqwest_err`]) — is passed through [`crate::git::redact`]
//! before it becomes a [`GitHostError`]. Belt-and-suspenders: nothing this
//! module returns is ever logged or stored unredacted.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use crate::git;

/// A boxed, `Send` future — the hand-written equivalent of what `#[async_trait]`
/// would generate, so [`GitHost`]'s methods can be `async` in spirit while the
/// trait itself stays object-safe (`Arc<dyn GitHost>`). See the module doc's
/// "why boxed futures" section.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A [`GitHost`] operation failure. `message` is already **redacted** of any
/// token (see the module doc's "redaction discipline" section) and is safe to
/// log or store (e.g. in an `agent_run` evidence row).
#[derive(Debug, Clone)]
pub struct GitHostError {
    pub message: String,
}

impl GitHostError {
    pub fn new(message: impl Into<String>) -> GitHostError {
        GitHostError {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for GitHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GitHostError {}

impl From<git::GitError> for GitHostError {
    fn from(err: git::GitError) -> GitHostError {
        // `GitError`'s message is already redacted (see `crate::git`'s own
        // module doc) — no PAT to strip a second time here.
        GitHostError::new(err.message)
    }
}

/// [`GitHost::push`]'s arguments.
pub struct PushRequest<'a> {
    /// The already-provisioned, already-committed-to workspace to push from.
    pub workspace_path: &'a Path,
    /// The branch to push (§2.8 naming — already checked out in
    /// `workspace_path`).
    pub branch: &'a str,
    /// The project's canonical `repo_url` (plain, no credentials embedded —
    /// the token is injected transiently, never read back from the
    /// workspace's own `origin`).
    pub repo_url: &'a str,
    pub pat: Option<&'a str>,
}

/// [`GitHost::open_pr`]'s arguments. Deliberately has no `base` field — the
/// implementation resolves the target branch itself (see the module doc's
/// "base branch" section); a caller only ever supplies what it actually
/// controls (title/body/head), never a base it would otherwise have to
/// guess.
pub struct OpenPrRequest<'a> {
    pub repo_url: &'a str,
    pub pat: Option<&'a str>,
    /// The branch to open the PR *from* (§2.8's epic branch).
    pub head: &'a str,
    pub title: &'a str,
    pub body: &'a str,
}

/// [`GitHost::check_auth`]'s arguments.
pub struct CheckAuthRequest<'a> {
    pub repo_url: &'a str,
    pub pat: Option<&'a str>,
}

/// A successfully opened PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenedPr {
    pub url: String,
    pub number: i64,
}

/// The git-hosting seam (T-514, D15). See the module doc for the full
/// rationale. Every method is network I/O (or, for [`GitHost::push`], a
/// git-over-HTTPS shell-out) — none of it belongs on the hermetic-by-default
/// path `just test` exercises, which is exactly why every production caller
/// reaches it only through `Arc<dyn GitHost>`, with [`testing::FakeHost`]
/// standing in for every test.
pub trait GitHost: Send + Sync {
    /// Push `req.branch` to `req.repo_url`'s `origin`, PAT injected
    /// transiently (never persisted). See [`crate::git::push_branch`], which
    /// every implementation of this method is expected to delegate to (or
    /// behave equivalently to, for a fake).
    fn push<'a>(&'a self, req: PushRequest<'a>) -> BoxFuture<'a, Result<(), GitHostError>>;

    /// Open a PR from `req.head` against the repo's default branch, titled
    /// `req.title` with body `req.body`. Returns the opened PR's `html_url`
    /// + `number`.
    fn open_pr<'a>(
        &'a self,
        req: OpenPrRequest<'a>,
    ) -> BoxFuture<'a, Result<OpenedPr, GitHostError>>;

    /// A cheap validity probe for a project's `repo_url`/PAT pair (not yet
    /// wired into any endpoint — a seam for later use, e.g. validating a PAT
    /// at project-create time). `Ok(())` iff the host considers the
    /// credentials valid for this repo.
    fn check_auth<'a>(
        &'a self,
        req: CheckAuthRequest<'a>,
    ) -> BoxFuture<'a, Result<(), GitHostError>>;
}

/// Parse `owner`/`repo` out of a GitHub HTTPS URL: `https://github.com/<owner>/<repo>`,
/// tolerating a trailing `.git` and/or a trailing `/`. A pure function (no
/// I/O) so it is unit-tested directly rather than only indirectly through a
/// live API call.
///
/// Rejects: a non-`https` scheme, a non-`github.com` host (Gitea etc. are
/// v2 — MILESTONE_2 §12), a missing owner or repo segment, and any URL with
/// more path segments than `owner/repo` (e.g. a sub-path, which is never a
/// valid repo URL).
pub fn parse_owner_repo(repo_url: &str) -> Result<(String, String), GitHostError> {
    let rest = repo_url
        .strip_prefix("https://")
        .ok_or_else(|| GitHostError::new("only https:// GitHub repo URLs are supported"))?;
    let rest = rest.strip_prefix("github.com/").ok_or_else(|| {
        GitHostError::new("only github.com repo URLs are supported (other hosts are v2)")
    })?;
    let rest = rest.trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);

    let mut parts = rest.splitn(2, '/');
    let owner = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| GitHostError::new(format!("malformed GitHub repo URL: {repo_url}")))?;
    let repo = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| GitHostError::new(format!("malformed GitHub repo URL: {repo_url}")))?;
    if repo.contains('/') {
        return Err(GitHostError::new(format!(
            "malformed GitHub repo URL (extra path segments): {repo_url}"
        )));
    }
    Ok((owner.to_string(), repo.to_string()))
}

const GITHUB_API_BASE: &str = "https://api.github.com";

/// `POST` target for opening a PR.
fn pulls_url(owner: &str, repo: &str) -> String {
    format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/pulls")
}

/// `GET` target for repo metadata (`default_branch`) and the `check_auth` probe.
fn repo_info_url(owner: &str, repo: &str) -> String {
    format!("{GITHUB_API_BASE}/repos/{owner}/{repo}")
}

/// The JSON body `POST .../pulls` expects. A pure function so the exact
/// shape sent to GitHub is unit-tested without a network call.
fn build_open_pr_json(title: &str, head: &str, base: &str, body: &str) -> serde_json::Value {
    serde_json::json!({ "title": title, "head": head, "base": base, "body": body })
}

/// Turn a non-2xx GitHub response into a readable, redacted [`GitHostError`].
/// GitHub's error bodies are JSON with a `message` field
/// (`{"message": "...", ...}`); fall back to the raw (trimmed) body when it
/// isn't parseable JSON, and to a bare status-code message when the body is
/// empty. Redacted defensively even though a GitHub error body should never
/// itself contain the PAT (see the module doc's "redaction discipline").
fn map_github_error(status: u16, body: &str, pat: Option<&str>) -> GitHostError {
    let reason = extract_message(body).unwrap_or_else(|| body.trim().to_string());
    let reason = if reason.is_empty() {
        format!("GitHub API returned HTTP {status}")
    } else {
        format!("GitHub API returned HTTP {status}: {reason}")
    };
    GitHostError::new(git::redact(&reason, pat))
}

/// Extract GitHub's `{"message": "..."}` field from an error body, if present
/// and parseable.
fn extract_message(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value.get("message")?.as_str().map(str::to_string)
}

/// Redact a [`reqwest::Error`] before it becomes a [`GitHostError`] — its
/// `Display` can embed the request URL (DNS failure, connect timeout, TLS
/// error), so it goes through [`crate::git::redact`] exactly like every other
/// error path in this module, even though the GitHub API URLs this module
/// builds never embed a PAT themselves (the token travels only in the
/// `Authorization` header) — defense in depth against a future caller that
/// does embed one.
fn redacted_reqwest_err(err: &reqwest::Error, pat: Option<&str>) -> GitHostError {
    GitHostError::new(git::redact(&err.to_string(), pat))
}

/// The production [`GitHost`]: real `git push` (via [`crate::git::push_branch`])
/// plus the real GitHub REST API for `open_pr`/`check_auth`.
pub struct GithubHost {
    client: reqwest::Client,
}

impl GithubHost {
    pub fn new() -> GithubHost {
        GithubHost {
            client: reqwest::Client::new(),
        }
    }

    /// Attach the standard GitHub API headers (`User-Agent` — GitHub rejects
    /// requests without one — and `Accept: application/vnd.github+json`) and
    /// the bearer token, if any.
    fn authed(
        &self,
        builder: reqwest::RequestBuilder,
        pat: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let builder = builder
            .header(reqwest::header::USER_AGENT, "dearborn")
            .header(reqwest::header::ACCEPT, "application/vnd.github+json");
        match pat {
            Some(p) if !p.is_empty() => builder.bearer_auth(p),
            _ => builder,
        }
    }

    /// `GET /repos/{owner}/{repo}` → `default_branch` (T-514 AC: never
    /// assume `main`).
    async fn fetch_default_branch(
        &self,
        owner: &str,
        repo: &str,
        pat: Option<&str>,
    ) -> Result<String, GitHostError> {
        let resp = self
            .authed(self.client.get(repo_info_url(owner, repo)), pat)
            .send()
            .await
            .map_err(|e| redacted_reqwest_err(&e, pat))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(map_github_error(status.as_u16(), &text, pat));
        }
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| redacted_reqwest_err(&e, pat))?;
        value
            .get("default_branch")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| GitHostError::new("GitHub repo response is missing default_branch"))
    }
}

impl Default for GithubHost {
    fn default() -> GithubHost {
        GithubHost::new()
    }
}

impl GitHost for GithubHost {
    fn push<'a>(&'a self, req: PushRequest<'a>) -> BoxFuture<'a, Result<(), GitHostError>> {
        Box::pin(async move {
            git::push_branch(req.workspace_path, req.branch, req.repo_url, req.pat)
                .await
                .map_err(GitHostError::from)
        })
    }

    fn open_pr<'a>(
        &'a self,
        req: OpenPrRequest<'a>,
    ) -> BoxFuture<'a, Result<OpenedPr, GitHostError>> {
        Box::pin(async move {
            let (owner, repo) = parse_owner_repo(req.repo_url)?;
            let base = self.fetch_default_branch(&owner, &repo, req.pat).await?;
            let json_body = build_open_pr_json(req.title, req.head, &base, req.body);

            let resp = self
                .authed(self.client.post(pulls_url(&owner, &repo)), req.pat)
                .json(&json_body)
                .send()
                .await
                .map_err(|e| redacted_reqwest_err(&e, req.pat))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(map_github_error(status.as_u16(), &text, req.pat));
            }
            let value: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| redacted_reqwest_err(&e, req.pat))?;
            let url = value
                .get("html_url")
                .and_then(|v| v.as_str())
                .ok_or_else(|| GitHostError::new("GitHub PR response is missing html_url"))?
                .to_string();
            let number = value
                .get("number")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| GitHostError::new("GitHub PR response is missing number"))?;
            Ok(OpenedPr { url, number })
        })
    }

    fn check_auth<'a>(
        &'a self,
        req: CheckAuthRequest<'a>,
    ) -> BoxFuture<'a, Result<(), GitHostError>> {
        Box::pin(async move {
            let (owner, repo) = parse_owner_repo(req.repo_url)?;
            let resp = self
                .authed(self.client.get(repo_info_url(&owner, &repo)), req.pat)
                .send()
                .await
                .map_err(|e| redacted_reqwest_err(&e, req.pat))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(map_github_error(status.as_u16(), &text, req.pat));
            }
            Ok(())
        })
    }
}

/// A fake [`GitHost`] reachable from every test in this crate **and** from
/// the separate `tests/worker_live.rs` integration-test crate (T-515), which
/// is exactly why this module is a plain `pub mod` rather than the more
/// usual `#[cfg(test)] pub(crate) mod testing` every other agent seam's fake
/// lives in (see e.g. `task_agent::testing`'s own doc comment on the same
/// point): an integration test compiles as its own crate and never sees
/// anything gated behind *this* crate's `#[cfg(test)]`, so `FakeHost` has to
/// be genuinely, unconditionally `pub` to be constructible there. T-515's
/// live proof needs exactly this: a real `claude` agent, a real local `git
/// push`, but a **faked** GitHub PR API call (its own AC says so) — this is
/// the type it fakes with.
pub mod testing {
    use super::*;
    use std::sync::Mutex;

    /// One recorded `open_pr` call, everything a test needs to assert the
    /// right title/head/base/body were sent.
    #[derive(Debug, Clone)]
    pub struct RecordedOpenPr {
        pub repo_url: String,
        pub head: String,
        pub base: String,
        pub title: String,
        pub body: String,
    }

    /// A fake [`GitHost`]. `push` performs a **real** local `git push` (via
    /// [`crate::git::push_branch`]) unless scripted to fail — this crate's
    /// push-path tests (including the bare-repo-origin fixture, T-514's AC)
    /// exercise real git plumbing throughout; only the GitHub HTTP calls
    /// (`open_pr`/`check_auth`) are faked, since those are the only ones that
    /// would otherwise need real network access to a real GitHub repo.
    pub struct FakeHost {
        open_pr_calls: Mutex<Vec<RecordedOpenPr>>,
        open_pr_failure: Option<String>,
        pr_url: String,
        pr_number: i64,
        /// The base branch `open_pr` reports having targeted — `FakeHost`
        /// never calls GitHub, so unlike `GithubHost` it cannot read the
        /// repo's real `default_branch`; this fixed (overridable) value
        /// stands in for it, and every call is still recorded so a test can
        /// assert the exact value used ([`FakeHost::open_pr_calls`]).
        default_branch: String,
        push_failure: Option<String>,
        /// When set, `push` returns `Ok(())` immediately without touching
        /// git at all — see [`FakeHost::stub_push_success`].
        push_stub_success: bool,
        check_auth_failure: Option<String>,
    }

    impl FakeHost {
        pub fn new() -> FakeHost {
            FakeHost {
                open_pr_calls: Mutex::new(Vec::new()),
                open_pr_failure: None,
                pr_url: "https://github.com/fake-owner/fake-repo/pull/1".to_string(),
                pr_number: 1,
                default_branch: "main".to_string(),
                push_failure: None,
                push_stub_success: false,
                check_auth_failure: None,
            }
        }

        /// Script `open_pr` to fail with `message` (verbatim — a test
        /// constructs a realistic, already-redacted-looking message when it
        /// wants to assert on the stored failure text).
        pub fn fail_open_pr(mut self, message: impl Into<String>) -> FakeHost {
            self.open_pr_failure = Some(message.into());
            self
        }

        /// Script `push` to fail with `message` **without** attempting a real
        /// git push at all (the failed-push AC needs "no commits landed",
        /// not "a push was attempted and then we pretend it failed").
        pub fn fail_push(mut self, message: impl Into<String>) -> FakeHost {
            self.push_failure = Some(message.into());
            self
        }

        /// Make `push` succeed trivially, **without** calling
        /// [`crate::git::push_branch`] at all — for a test whose subject is
        /// `open_pr`, not `push`, and whose project has a real PAT set
        /// (`git::authenticated_url` requires an `https://` `repo_url` the
        /// instant a PAT is present, which a local git-fixture `repo_url`
        /// never is; stubbing push out entirely sidesteps that entirely
        /// unrelated constraint rather than working around it).
        pub fn stub_push_success(mut self) -> FakeHost {
            self.push_stub_success = true;
            self
        }

        #[allow(dead_code)] // exercised by a future check_auth-consuming caller/test
        pub fn fail_check_auth(mut self, message: impl Into<String>) -> FakeHost {
            self.check_auth_failure = Some(message.into());
            self
        }

        /// Override the canned successful PR identity (default: a fixed
        /// fake URL + PR number `1`).
        pub fn with_pr(mut self, url: impl Into<String>, number: i64) -> FakeHost {
            self.pr_url = url.into();
            self.pr_number = number;
            self
        }

        /// Every `open_pr` call this fake has received, in order.
        pub fn open_pr_calls(&self) -> Vec<RecordedOpenPr> {
            self.open_pr_calls
                .lock()
                .expect("FakeHost mutex poisoned")
                .clone()
        }
    }

    impl Default for FakeHost {
        fn default() -> FakeHost {
            FakeHost::new()
        }
    }

    impl GitHost for FakeHost {
        fn push<'a>(&'a self, req: PushRequest<'a>) -> BoxFuture<'a, Result<(), GitHostError>> {
            Box::pin(async move {
                if let Some(message) = &self.push_failure {
                    return Err(GitHostError::new(message.clone()));
                }
                if self.push_stub_success {
                    return Ok(());
                }
                git::push_branch(req.workspace_path, req.branch, req.repo_url, req.pat)
                    .await
                    .map_err(GitHostError::from)
            })
        }

        fn open_pr<'a>(
            &'a self,
            req: OpenPrRequest<'a>,
        ) -> BoxFuture<'a, Result<OpenedPr, GitHostError>> {
            Box::pin(async move {
                self.open_pr_calls
                    .lock()
                    .expect("FakeHost mutex poisoned")
                    .push(RecordedOpenPr {
                        repo_url: req.repo_url.to_string(),
                        head: req.head.to_string(),
                        base: self.default_branch.clone(),
                        title: req.title.to_string(),
                        body: req.body.to_string(),
                    });
                if let Some(message) = &self.open_pr_failure {
                    return Err(GitHostError::new(message.clone()));
                }
                Ok(OpenedPr {
                    url: self.pr_url.clone(),
                    number: self.pr_number,
                })
            })
        }

        fn check_auth<'a>(
            &'a self,
            req: CheckAuthRequest<'a>,
        ) -> BoxFuture<'a, Result<(), GitHostError>> {
            Box::pin(async move {
                let _ = req;
                if let Some(message) = &self.check_auth_failure {
                    return Err(GitHostError::new(message.clone()));
                }
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_owner_repo --------------------------------------------------

    #[test]
    fn parses_owner_repo_from_a_plain_url() {
        let (owner, repo) = parse_owner_repo("https://github.com/octocat/Hello-World").unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "Hello-World");
    }

    #[test]
    fn parses_owner_repo_tolerating_dot_git_suffix() {
        let (owner, repo) = parse_owner_repo("https://github.com/octocat/Hello-World.git").unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "Hello-World");
    }

    #[test]
    fn parses_owner_repo_tolerating_trailing_slash() {
        let (owner, repo) = parse_owner_repo("https://github.com/octocat/Hello-World/").unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "Hello-World");
    }

    #[test]
    fn parses_owner_repo_tolerating_trailing_slash_and_dot_git() {
        let (owner, repo) =
            parse_owner_repo("https://github.com/octocat/Hello-World.git/").unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "Hello-World");
    }

    #[test]
    fn rejects_a_non_github_host() {
        let err = parse_owner_repo("https://gitea.example.com/o/r").unwrap_err();
        assert!(!err.message.is_empty());
    }

    #[test]
    fn rejects_a_non_https_scheme() {
        assert!(parse_owner_repo("git@github.com:o/r.git").is_err());
        assert!(parse_owner_repo("http://github.com/o/r").is_err());
    }

    #[test]
    fn rejects_a_url_missing_the_repo_segment() {
        assert!(parse_owner_repo("https://github.com/onlyowner").is_err());
        assert!(parse_owner_repo("https://github.com/").is_err());
        assert!(parse_owner_repo("https://github.com").is_err());
    }

    #[test]
    fn rejects_a_url_with_extra_path_segments() {
        assert!(parse_owner_repo("https://github.com/o/r/extra").is_err());
    }

    // ---- request-building (pure, no network) -------------------------------

    #[test]
    fn pulls_url_targets_the_right_repo() {
        assert_eq!(
            pulls_url("octocat", "Hello-World"),
            "https://api.github.com/repos/octocat/Hello-World/pulls"
        );
    }

    #[test]
    fn repo_info_url_targets_the_right_repo() {
        assert_eq!(
            repo_info_url("octocat", "Hello-World"),
            "https://api.github.com/repos/octocat/Hello-World"
        );
    }

    #[test]
    fn open_pr_json_carries_title_head_base_body() {
        let body = build_open_pr_json("Ship it", "dearborn/ship-it-abc123", "main", "the body");
        assert_eq!(body["title"], "Ship it");
        assert_eq!(body["head"], "dearborn/ship-it-abc123");
        assert_eq!(body["base"], "main");
        assert_eq!(body["body"], "the body");
    }

    // ---- error mapping + redaction ------------------------------------------

    #[test]
    fn maps_a_github_json_error_body_to_a_readable_message() {
        let err = map_github_error(422, r#"{"message": "Validation Failed"}"#, None);
        assert!(err.message.contains("422"));
        assert!(err.message.contains("Validation Failed"));
    }

    #[test]
    fn maps_a_non_json_error_body_to_a_readable_message() {
        let err = map_github_error(503, "Service Unavailable", None);
        assert!(err.message.contains("503"));
        assert!(err.message.contains("Service Unavailable"));
    }

    #[test]
    fn maps_an_empty_error_body_to_a_status_only_message() {
        let err = map_github_error(500, "", None);
        assert!(err.message.contains("500"));
    }

    #[test]
    fn github_error_mapping_redacts_the_pat_if_present_in_the_body() {
        // Defensive: even if a token somehow ended up echoed in a response
        // body, it must not survive into the stored error.
        let pat = "ghp_leakedToken123";
        let body = format!(r#"{{"message": "bad token {pat}"}}"#);
        let err = map_github_error(401, &body, Some(pat));
        assert!(
            !err.message.contains(pat),
            "token must not survive: {}",
            err.message
        );
    }

    #[test]
    fn git_error_conversion_preserves_the_already_redacted_message() {
        let git_err = git::GitError {
            message: "fatal: boom".to_string(),
        };
        let host_err: GitHostError = git_err.into();
        assert_eq!(host_err.message, "fatal: boom");
    }
}
