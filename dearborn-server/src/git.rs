//! Git-over-HTTPS clone/fetch by shelling out to the `git` CLI (T-103).
//!
//! Per §1/§14 of the architecture we shell out to `git` rather than link a git
//! library. This module builds the authenticated HTTPS URL, runs `git`, and
//! surfaces failures as [`GitError`] whose message is **redacted of any token**
//! so it is safe to store in `project.clone_error` and to log.
//!
//! ## Token injection & redaction
//!
//! A PAT is injected into the HTTPS URL as userinfo —
//! `https://x-access-token:<pat>@github.com/owner/repo.git` — which GitHub
//! accepts for git-over-HTTPS. This token-bearing URL is passed to `git` as a
//! process argument and is **never logged**; every log line / stored error goes
//! through [`redact`], which strips the token string and any URL userinfo.
//!
//! The token is never persisted to disk: after a successful clone the remote is
//! reset to the clean (token-free) URL, and `git fetch` re-injects credentials
//! transiently via `-c remote.origin.url=<auth>` (process-scoped, not written to
//! `.git/config`).
//!
//! ## Epic-workspace helpers (T-511)
//!
//! [`clone_local`], [`set_remote_url`], [`checkout_new_branch`],
//! [`current_branch`], and [`reset_hard_and_clean`] back
//! [`crate::workspace`]'s clone-off-canonical provisioning (D3): a `git clone`
//! of a local path needs no PAT at all (it's disk-to-disk), so these never
//! take one — the token only ever re-enters the picture at [`refresh_repo`]
//! (the canonical checkout's fetch) and, later, at push time (T-514).
//!
//! ## The implement-walk's commit helpers (T-513)
//!
//! [`add_all`], [`status_porcelain`], [`current_commit`], and [`commit_all`]
//! back the T-513 DAG walk's per-task commit step. `status_porcelain` (rather
//! than e.g. `git diff --cached --quiet`) is deliberate: git's `--quiet`
//! diff variants signal "there is a diff" via a **non-zero exit code**, which
//! [`run_git`]/[`run_git_capture`] treat uniformly as failure — reusing them
//! for a check that is allowed to come back either way would mean special-
//! casing exit code 1 as "not an error" right there, muddying the "non-zero
//! means [`GitError`]" contract every other helper in this module relies on.
//! `git status --porcelain` always exits `0`, clean or not, so it composes
//! with the rest of this module unchanged: an empty (trimmed) result means
//! nothing to commit.

use std::fmt;
use std::path::Path;

use tokio::process::Command;

/// A git operation failure. `message` is already **redacted** of any token and
/// is safe to log or store in `clone_error`.
#[derive(Debug, Clone)]
pub struct GitError {
    pub message: String,
}

impl GitError {
    fn new(message: impl Into<String>) -> GitError {
        GitError {
            message: message.into(),
        }
    }
}

impl fmt::Display for GitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GitError {}

/// Build the HTTPS URL `git` is invoked with, injecting the PAT as userinfo.
///
/// * `pat == None` → the URL is returned unchanged (public repo).
/// * Only `https://` URLs are supported (GitHub-only, git-over-HTTPS in v1).
/// * Any userinfo already present in `repo_url` is dropped and replaced.
pub fn authenticated_url(repo_url: &str, pat: Option<&str>) -> Result<String, GitError> {
    let pat = match pat {
        Some(p) if !p.is_empty() => p,
        _ => return Ok(repo_url.to_string()),
    };

    let rest = repo_url.strip_prefix("https://").ok_or_else(|| {
        GitError::new("only https:// repository URLs are supported (git-over-HTTPS)")
    })?;

    // Split authority from path, drop any existing userinfo on the authority.
    let host_path = match rest.split_once('/') {
        Some((authority, path)) => {
            let host = strip_userinfo(authority);
            format!("{host}/{path}")
        }
        None => strip_userinfo(rest).to_string(),
    };

    Ok(format!("https://x-access-token:{pat}@{host_path}"))
}

/// Drop `user[:pass]@` from an authority, keeping just `host[:port]`.
fn strip_userinfo(authority: &str) -> &str {
    match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    }
}

/// Redact any secret from text destined for a log or `clone_error`.
///
/// Removes the exact `pat` string (if any) and, defensively, replaces the
/// userinfo of every `scheme://user@host` URL with `***`, so a token can never
/// survive into stored/logged output even if it appears in an unexpected form.
pub fn redact(text: &str, pat: Option<&str>) -> String {
    let mut out = text.to_string();
    if let Some(p) = pat {
        if !p.is_empty() {
            out = out.replace(p, "***");
        }
    }
    redact_url_userinfo(&out)
}

/// Replace `scheme://userinfo@host` with `scheme://***@host` for every URL.
fn redact_url_userinfo(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(idx) = rest.find("://") {
        let split = idx + 3; // keep "scheme://"
        result.push_str(&rest[..split]);
        let after = &rest[split..];
        // Authority ends at the first path/query/fragment/whitespace char.
        let auth_end = after
            .find(|c: char| c == '/' || c == '?' || c == '#' || c.is_whitespace())
            .unwrap_or(after.len());
        let authority = &after[..auth_end];
        match authority.rsplit_once('@') {
            Some((_, host)) => {
                result.push_str("***@");
                result.push_str(host);
            }
            None => result.push_str(authority),
        }
        rest = &after[auth_end..];
    }
    result.push_str(rest);
    result
}

/// Clone `repo_url` (default branch, full checkout) into `dest`.
///
/// This is the **canonical read-only** checkout: no epic branch, just the
/// default branch. Any stale `dest` is removed first for a clean clone. On
/// success with a PAT, the remote URL is reset to the token-free form so the
/// token is never persisted in `.git/config`.
pub async fn clone_repo(repo_url: &str, pat: Option<&str>, dest: &Path) -> Result<(), GitError> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .map_err(|e| GitError::new(format!("failed to clear clone directory: {e}")))?;
    }
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| GitError::new(format!("failed to create clone root: {e}")))?;
    }

    let auth_url = authenticated_url(repo_url, pat)?;
    let dest_str = dest.to_string_lossy();
    run_git(&["clone", &auth_url, &dest_str], None, pat).await?;

    // Never leave the token on disk: point the persisted remote at the clean URL.
    if pat.is_some() {
        run_git(&["remote", "set-url", "origin", repo_url], Some(dest), pat).await?;
    }
    Ok(())
}

/// Refresh the canonical checkout at `dest`: `git fetch` then hard-reset to the
/// origin default branch (a read-only mirror always matches origin). If `dest`
/// is not yet a repository, this performs an initial [`clone_repo`] instead.
pub async fn refresh_repo(repo_url: &str, pat: Option<&str>, dest: &Path) -> Result<(), GitError> {
    if !dest.join(".git").exists() {
        return clone_repo(repo_url, pat, dest).await;
    }

    let auth_url = authenticated_url(repo_url, pat)?;
    // Inject credentials transiently via -c (process-scoped; not written to disk).
    let url_override = format!("remote.origin.url={auth_url}");
    run_git(
        &["-c", &url_override, "fetch", "--prune", "origin"],
        Some(dest),
        pat,
    )
    .await?;
    run_git(&["reset", "--hard", "origin/HEAD"], Some(dest), pat).await?;
    Ok(())
}

/// Clone `src` (a local filesystem path, not a network URL) into `dest` — the
/// T-511 epic-workspace clone (`git clone <canonical> <workspace>`, D3). No
/// PAT is ever involved: cloning off a path on the same disk needs no
/// authentication regardless of what the *canonical* checkout's own remote
/// requires. Any stale `dest` is cleared first, mirroring [`clone_repo`].
pub async fn clone_local(src: &Path, dest: &Path) -> Result<(), GitError> {
    if dest.exists() {
        tokio::fs::remove_dir_all(dest)
            .await
            .map_err(|e| GitError::new(format!("failed to clear workspace directory: {e}")))?;
    }
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| GitError::new(format!("failed to create workspace root: {e}")))?;
    }
    let src_str = src.to_string_lossy();
    let dest_str = dest.to_string_lossy();
    run_git(&["clone", src_str.as_ref(), dest_str.as_ref()], None, None).await
}

/// Repoint `repo_dir`'s `origin` remote at `url` (plain, no credentials
/// embedded). Used after [`clone_local`] to swap the workspace's origin from
/// the canonical checkout's local path to the real (token-free) remote URL,
/// so a later push (T-514) has somewhere real to push to — the PAT itself is
/// injected transiently at push time, exactly like [`refresh_repo`]'s fetch.
pub async fn set_remote_url(repo_dir: &Path, url: &str) -> Result<(), GitError> {
    run_git(&["remote", "set-url", "origin", url], Some(repo_dir), None).await
}

/// Create and switch to a new local branch `branch` in `repo_dir` (the epic
/// workspace's branch, §2.8). Fails if the branch already exists — callers
/// only take this path on a fresh clone, never on re-attach.
pub async fn checkout_new_branch(repo_dir: &Path, branch: &str) -> Result<(), GitError> {
    run_git(&["checkout", "-b", branch], Some(repo_dir), None).await
}

/// The current branch name (`git rev-parse --abbrev-ref HEAD`), trimmed.
/// Used by the T-511 re-attach check: a workspace is only re-attached (rather
/// than re-cloned) when it is already checked out on the expected branch.
pub async fn current_branch(repo_dir: &Path) -> Result<String, GitError> {
    run_git_capture(&["rev-parse", "--abbrev-ref", "HEAD"], Some(repo_dir), None).await
}

/// Discard any working-tree changes and untracked files in `repo_dir`: `git
/// reset --hard HEAD` then `git clean -fd`. This is the T-511 re-attach path
/// — cheaper than a fresh clone and exactly what's needed to drop a previous
/// (failed or interrupted) attempt's dirty tree before resuming work on the
/// same branch.
pub async fn reset_hard_and_clean(repo_dir: &Path) -> Result<(), GitError> {
    run_git(&["reset", "--hard", "HEAD"], Some(repo_dir), None).await?;
    run_git(&["clean", "-fd"], Some(repo_dir), None).await
}

/// Stage every change in `repo_dir` (`git add -A`) — T-513's first commit-step
/// action, run unconditionally before checking whether there is anything to
/// commit ([`status_porcelain`]), so a brand-new untracked file the implement
/// stage created is staged too, not just edits to files already tracked.
pub async fn add_all(repo_dir: &Path) -> Result<(), GitError> {
    run_git(&["add", "-A"], Some(repo_dir), None).await
}

/// `git status --porcelain` in `repo_dir`, trimmed. See the module doc's
/// "implement-walk's commit helpers" section for why this (rather than a
/// `--quiet` diff variant) is the right primitive here: it always exits `0`,
/// so an empty result unambiguously means "nothing to commit" without
/// needing to special-case a nonzero-but-not-an-error exit code. Callers
/// call this **after** [`add_all`], so a nonempty result here always means
/// something is staged.
pub async fn status_porcelain(repo_dir: &Path) -> Result<String, GitError> {
    run_git_capture(&["status", "--porcelain"], Some(repo_dir), None).await
}

/// The current `HEAD` commit SHA (`git rev-parse HEAD`) in `repo_dir` — T-513
/// records this as a task's `base_sha` the moment the task starts (before the
/// implement stage runs), so a later review (T-530) can diff the cumulative
/// change against exactly the tree the task began from, no matter how many
/// commits land on top of it in the meantime.
pub async fn current_commit(repo_dir: &Path) -> Result<String, GitError> {
    run_git_capture(&["rev-parse", "HEAD"], Some(repo_dir), None).await
}

/// Commit everything currently staged in `repo_dir` with `subject`, using an
/// explicit, deterministic committer identity (`-c user.name=<committer_name>
/// -c user.email=<committer_email>`) passed on the commit invocation itself —
/// never written to `repo_dir`'s `.git/config` (mirrors how a PAT is injected
/// transiently elsewhere in this module rather than persisted). This is
/// deliberate, not incidental: the workspace is a fresh local clone (D3) on a
/// server host that may have **no** global `user.name`/`user.email`
/// configured at all, and git refuses to commit without one; overriding it
/// per-invocation means every Dearborn-authored commit succeeds and is
/// attributed to Dearborn itself, regardless of the host's own git config.
/// Returns the resulting commit's SHA ([`current_commit`] read back
/// immediately after the commit succeeds).
pub async fn commit_all(
    repo_dir: &Path,
    subject: &str,
    committer_name: &str,
    committer_email: &str,
) -> Result<String, GitError> {
    let name_arg = format!("user.name={committer_name}");
    let email_arg = format!("user.email={committer_email}");
    run_git(
        &["-c", &name_arg, "-c", &email_arg, "commit", "-m", subject],
        Some(repo_dir),
        None,
    )
    .await?;
    current_commit(repo_dir).await
}

/// Run `git` with `args`, optionally in `cwd`, discarding stdout. On
/// non-zero exit the (redacted) stderr becomes the [`GitError`] message.
/// `GIT_TERMINAL_PROMPT=0` guarantees git never blocks on an interactive
/// credential prompt. Thin wrapper over [`run_git_capture`] for the (common)
/// callers that don't need stdout.
async fn run_git(args: &[&str], cwd: Option<&Path>, pat: Option<&str>) -> Result<(), GitError> {
    run_git_capture(args, cwd, pat).await.map(|_| ())
}

/// Run `git` with `args`, optionally in `cwd`, returning trimmed stdout on
/// success. On non-zero exit the (redacted) stderr becomes the [`GitError`]
/// message. `GIT_TERMINAL_PROMPT=0` guarantees git never blocks on an
/// interactive credential prompt.
async fn run_git_capture(
    args: &[&str],
    cwd: Option<&Path>,
    pat: Option<&str>,
) -> Result<String, GitError> {
    let mut cmd = Command::new("git");
    cmd.args(args).env("GIT_TERMINAL_PROMPT", "0");
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| GitError::new(format!("failed to run git: {e}")))?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let redacted = redact(stderr.trim(), pat);
    let message = if redacted.is_empty() {
        format!("git exited with a non-zero status ({})", output.status)
    } else {
        redacted
    };
    Err(GitError::new(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAT: &str = "ghp_superSecretToken123";

    #[test]
    fn injects_token_as_userinfo() {
        let url = authenticated_url("https://github.com/octocat/Hello-World.git", Some(PAT)).unwrap();
        assert_eq!(
            url,
            "https://x-access-token:ghp_superSecretToken123@github.com/octocat/Hello-World.git"
        );
    }

    #[test]
    fn no_pat_leaves_url_unchanged() {
        let clean = "https://github.com/octocat/Hello-World.git";
        assert_eq!(authenticated_url(clean, None).unwrap(), clean);
        assert_eq!(authenticated_url(clean, Some("")).unwrap(), clean);
    }

    #[test]
    fn replaces_any_existing_userinfo() {
        let url = authenticated_url("https://olduser:oldpw@github.com/o/r.git", Some(PAT)).unwrap();
        assert_eq!(
            url,
            "https://x-access-token:ghp_superSecretToken123@github.com/o/r.git"
        );
        assert!(!url.contains("olduser"));
    }

    #[test]
    fn non_https_url_is_rejected() {
        assert!(authenticated_url("git@github.com:o/r.git", Some(PAT)).is_err());
        assert!(authenticated_url("http://github.com/o/r.git", Some(PAT)).is_err());
    }

    #[test]
    fn redact_removes_the_exact_token() {
        let auth = authenticated_url("https://github.com/o/r.git", Some(PAT)).unwrap();
        let msg = format!("fatal: could not read from '{auth}'");
        let red = redact(&msg, Some(PAT));
        assert!(!red.contains(PAT), "token must not survive redaction: {red}");
        assert!(!red.contains("ghp_"));
        assert!(red.contains("***@github.com/o/r.git"));
    }

    #[test]
    fn redact_strips_url_userinfo_even_without_known_pat() {
        // Defense in depth: even if we do not pass the pat, URL userinfo is gone.
        let msg = "cloning https://x-access-token:leaked@github.com/o/r.git failed";
        let red = redact(msg, None);
        assert!(!red.contains("leaked"));
        assert!(red.contains("https://***@github.com/o/r.git"));
    }

    #[test]
    fn redact_leaves_ordinary_text_untouched() {
        let msg = "fatal: repository 'https://github.com/o/r.git' not found";
        assert_eq!(redact(msg, Some(PAT)), msg);
    }

    #[tokio::test]
    async fn clone_of_bad_url_errors_with_redacted_reason() {
        // A syntactically valid but non-resolvable https URL -> git fails fast
        // (GIT_TERMINAL_PROMPT=0 prevents any auth prompt hang).
        let dir = std::env::temp_dir().join(format!(
            "dearborn-git-badurl-{}-{}",
            std::process::id(),
            now_nanos()
        ));
        let dest = dir.join("repo");
        let bad = "https://dearborn.invalid/nope/nope.git";
        let err = clone_repo(bad, Some(PAT), &dest)
            .await
            .expect_err("clone of a bad URL must fail");
        assert!(!err.message.is_empty(), "error reason must be readable");
        assert!(!err.message.contains(PAT), "no token in error: {}", err.message);
        assert!(!err.message.contains("ghp_"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn now_nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }

    // ---- T-513: add_all / status_porcelain / current_commit / commit_all ----

    /// A local `git init`'d repo with one commit, entirely offline — the same
    /// shape `worker.rs`'s/`workspace.rs`'s own fixtures use, kept local here
    /// so this module's tests don't depend on either.
    async fn init_repo(dir: &Path) {
        tokio::fs::create_dir_all(dir).await.unwrap();
        run_git_ok(dir, &["init", "-b", "main"]).await;
        run_git_ok(dir, &["config", "user.email", "fixture@example.com"]).await;
        run_git_ok(dir, &["config", "user.name", "Fixture"]).await;
        std::fs::write(dir.join("README.md"), "hello\n").unwrap();
        run_git_ok(dir, &["add", "."]).await;
        run_git_ok(dir, &["commit", "-m", "init"]).await;
    }

    async fn run_git_ok(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {dir:?}");
    }

    fn temp_repo_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("dearborn-git-commit-{name}-{}-{}", std::process::id(), now_nanos()))
    }

    #[tokio::test]
    async fn status_porcelain_is_empty_on_a_clean_tree() {
        let dir = temp_repo_dir("clean");
        init_repo(&dir).await;
        let status = status_porcelain(&dir).await.unwrap();
        assert!(status.trim().is_empty(), "a freshly committed tree must be clean: {status:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn add_all_stages_new_and_modified_files_for_status_porcelain_to_see() {
        let dir = temp_repo_dir("dirty");
        init_repo(&dir).await;
        std::fs::write(dir.join("README.md"), "changed\n").unwrap();
        std::fs::write(dir.join("new.txt"), "brand new\n").unwrap();

        add_all(&dir).await.unwrap();
        let status = status_porcelain(&dir).await.unwrap();
        assert!(!status.trim().is_empty(), "staged changes must show up: {status:?}");
        assert!(status.contains("README.md"));
        assert!(status.contains("new.txt"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn current_commit_returns_the_head_sha() {
        let dir = temp_repo_dir("head-sha");
        init_repo(&dir).await;
        let sha = current_commit(&dir).await.unwrap();
        assert_eq!(sha.len(), 40, "a full SHA-1 hex string: {sha:?}");
        assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_all_commits_staged_changes_with_the_given_subject_and_identity() {
        let dir = temp_repo_dir("commit-all");
        init_repo(&dir).await;
        let base_sha = current_commit(&dir).await.unwrap();

        std::fs::write(dir.join("new.txt"), "brand new\n").unwrap();
        add_all(&dir).await.unwrap();

        let sha = commit_all(&dir, "impl(abc123): Do the thing", "Dearborn", "dearborn@noreply.localhost")
            .await
            .unwrap();
        assert_ne!(sha, base_sha, "a new commit must have landed");
        assert_eq!(current_commit(&dir).await.unwrap(), sha, "commit_all must return the new HEAD");

        // Subject + identity landed on the commit itself, not the workspace's
        // persistent git config.
        let subject = run_git_capture(&["log", "-1", "--format=%s"], Some(&dir), None)
            .await
            .unwrap();
        assert_eq!(subject, "impl(abc123): Do the thing");
        let author = run_git_capture(&["log", "-1", "--format=%an <%ae>"], Some(&dir), None)
            .await
            .unwrap();
        assert_eq!(author, "Dearborn <dearborn@noreply.localhost>");

        let config = std::fs::read_to_string(dir.join(".git/config")).unwrap();
        assert!(
            !config.contains("Dearborn") && !config.contains("dearborn@noreply.localhost"),
            "the -c identity must never be persisted to .git/config: {config}"
        );

        // Clean again after the commit.
        let status = status_porcelain(&dir).await.unwrap();
        assert!(status.trim().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn status_porcelain_after_add_all_with_no_changes_stays_empty() {
        let dir = temp_repo_dir("noop-add");
        init_repo(&dir).await;
        add_all(&dir).await.unwrap();
        let status = status_porcelain(&dir).await.unwrap();
        assert!(status.trim().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
