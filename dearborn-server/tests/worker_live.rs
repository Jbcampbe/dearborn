//! LIVE end-to-end proof for T-515 — Milestone 2's Phase 1 **spike**.
//!
//! MILESTONE_2 §11 risk 1 names the biggest unknown left after Phase 1:
//! "Headless write-mode behavior is unproven — M1 only ever ran agents
//! read-only. `RunMode::Edit` + `--permission-mode` + tool flags need
//! empirical settling." A test that only compiles does not retire that risk
//! — this one has to actually be *run* against the real `claude` CLI, which
//! is exactly why it exists this early (Phase 1, not Phase 6) rather than
//! after the whole pipeline is built out.
//!
//! This drives the REAL [`dearborn_server::task_agent::ClaudeTaskAgent`] (a
//! genuine `claude -p --permission-mode acceptEdits ...` subprocess, per
//! `agent-harness`'s Claude adapter — see `task_agent.rs`'s module doc and
//! `build_claude_args` in the `agent-harness` crate) through the REAL worker
//! pipeline ([`dearborn_server::worker::run_epic_pipeline`]): workspace
//! provisioning, the implement stage, `git add -A` + commit, and a real `git
//! push` to a local bare-repo origin fixture. Only the GitHub PR call is
//! faked ([`dearborn_server::git_host::testing::FakeHost`]) — MILESTONE_2
//! T-514's own AC is explicit that the live proof stops short of a real
//! GitHub API call.
//!
//! ## How to run
//!
//! ```sh
//! # from the repo root; `claude` must be on PATH and logged in (or
//! # ANTHROPIC_API_KEY set in the environment) — this spends real tokens.
//! cargo test -p dearborn-server --test worker_live -- --ignored --nocapture
//! ```
//!
//! Takes roughly 20s-2min for one real `claude` turn against a trivial
//! one-file task (cold CLI start dominates); bounded by [`LIVE_RUN_TIMEOUT`]
//! below so a hung agent fails the test rather than hanging a terminal or CI
//! forever — T-543's per-stage timeout isn't built yet (Phase 4), so this
//! bound lives in the test itself, not in the pipeline.
//!
//! ## Why `#[ignore]` (mandatory, MILESTONE_2 §10)
//!
//! `just test` must stay hermetic: no network, no `claude`, no GitHub. This
//! is the one test in the suite that violates all three on purpose (network:
//! yes, to Anthropic's API only; `claude`: yes; GitHub: no, `FakeHost` stands
//! in). `#[ignore]` is what keeps it out of the default `cargo test`/`just
//! test` run — see the bottom of this file for the "excluded from the gate"
//! proof, and `tests/mcp_live.rs` (T-203) for the identical convention this
//! follows.
//!
//! ## The fixture: hermetic except for the one `claude` API call
//!
//! - `fixture_dir`: `git init`, a local `user.name`/`user.email`, one commit.
//!   A bare repo can't be committed into directly, so this ordinary working
//!   repo exists only to *seed* the bare origin below via a local push.
//! - `bare_dir`: `git init --bare`, seeded with `fixture_dir`'s one commit.
//!   This becomes `project.repo_url` — the **same** field
//!   [`dearborn_server::workspace::provision_epic_workspace`] clones the
//!   canonical checkout from *and* the field
//!   [`dearborn_server::worker`]'s finalize step pushes the epic branch back
//!   to, exactly like a real GitHub remote plays both roles. No PAT is ever
//!   configured (`project.pat_encrypted` stays `NULL`), and no network beyond
//!   local disk touches git at any point.
//!
//! ## What "real" vs. "faked" means here
//!
//! Real: the `claude` subprocess, the git clone/add/commit/push machinery,
//! the DAG walk (claim-free direct call via
//! [`dearborn_server::worker::run_epic_pipeline`], the same seam
//! `worker.rs`'s own hermetic tests use to drive a walk without the
//! claim/heartbeat pool around it). Faked: only
//! [`dearborn_server::git_host::testing::FakeHost::open_pr`].
//!
//! ## Why the assertions read the *bare origin*, not the (by-then-deleted) workspace
//!
//! A successful finalize deletes the epic workspace once the PR opens
//! (T-511/T-514 — see `worker::finalize_epic`'s doc). By the time this test's
//! call to `run_epic_pipeline` returns, the workspace directory the agent
//! actually wrote into is already gone — that's real, intended production
//! behavior, not a test artifact to work around. The strongest surviving
//! evidence that the real agent modified the *working tree*, that Dearborn
//! *committed* it, and that it was *pushed*, is the git history of the bare
//! origin: `git show <branch>:HELLO.md` can only return the requested
//! content if a real Write actually landed on disk in the workspace, was
//! staged, committed, and the resulting object made it across the (real,
//! local) push. This is not a weaker check than "look inside the workspace
//! directory" — it is arguably stronger, since it proves the bytes actually
//! transferred to the remote rather than merely that a local directory once
//! contained them.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dearborn_server::breakdown::ClaudeBreakdownAgent;
use dearborn_server::git_host::testing::FakeHost;
use dearborn_server::planning::ClaudePlanningAgent;
use dearborn_server::task_agent::ClaudeTaskAgent;
use dearborn_server::workspace;
use dearborn_server::{worker, AppState, Config, Db, ExecutorConfig};

const TOKEN: &str = "s3cret-token";

/// Wall-clock ceiling for the whole pipeline call. Generous for a real
/// `claude` cold start plus one trivial file-write turn, but bounded — see
/// the module doc's "how to run" section for why this lives in the test
/// rather than in production config (`DEARBORN_AGENT_STAGE_TIMEOUT_SECS`
/// exists as a config knob since T-501 but nothing enforces it against a
/// running stage until T-543).
const LIVE_RUN_TIMEOUT: Duration = Duration::from_secs(300);

const EPIC_TITLE: &str = "Live executor proof";
const TASK_TITLE: &str = "Create HELLO.md";
const TASK_DESCRIPTION: &str = "Add a small marker file at the repository root.";
/// Deliberately small, unambiguous, and mechanically verifiable — MILESTONE_2's
/// T-515 AC asks for exactly this shape of task so a pass/fail reading of the
/// live run is never in doubt.
const TASK_ACCEPTANCE: &str = "A file named `HELLO.md` exists at the repository root and \
     contains exactly one line of text: `hello from dearborn` (a trailing newline is fine).";
const EXPECTED_FILE_CONTENT: &str = "hello from dearborn";

#[tokio::test]
#[ignore = "drives the live `claude` CLI and a real git push; run with --ignored"]
async fn live_implement_writes_commits_pushes_to_bare_origin_and_opens_a_fake_pr() {
    // ---- fixture: a source repo (identity + one commit) seeding a bare origin ----
    let root = std::env::temp_dir().join(format!("dearborn-t515-live-{}", ulid::Ulid::new()));
    let fixture_dir = root.join("fixture");
    let bare_dir = root.join("bare-origin.git");
    let clone_root = root.join("clones");
    std::fs::create_dir_all(&fixture_dir).unwrap();
    std::fs::create_dir_all(&bare_dir).unwrap();
    std::fs::create_dir_all(&clone_root).unwrap();

    git_ok(&fixture_dir, &["init", "-b", "main"]).await;
    git_ok(&fixture_dir, &["config", "user.email", "test@example.com"]).await;
    git_ok(&fixture_dir, &["config", "user.name", "Test"]).await;
    std::fs::write(fixture_dir.join("README.md"), "hello\n").unwrap();
    git_ok(&fixture_dir, &["add", "."]).await;
    git_ok(&fixture_dir, &["commit", "-m", "init"]).await;

    git_ok(&bare_dir, &["init", "--bare", "-b", "main"]).await;
    // Seed the bare origin with the fixture's one commit — a bare repo has
    // no working tree to commit into directly, only refs to push into.
    let bare_url = bare_dir.to_string_lossy().to_string();
    git_ok(&fixture_dir, &["push", &bare_url, "main"]).await;

    // ---- real server state: real ClaudeTaskAgent + FakeHost (T-514's seam) ----
    let db = Db::connect(":memory:").await.unwrap();
    db.run_migrations().await.unwrap();
    let config = Config {
        bind: "127.0.0.1:0".to_string(),
        token: TOKEN.to_string(),
        master_key: "test-master-key".to_string(),
        db_path: ":memory:".to_string(),
        clone_root: clone_root.to_string_lossy().to_string(),
        static_dir: "./client/dist".to_string(),
        auto_clone: false,
        executor: ExecutorConfig {
            worker_concurrency: 1,
            lease_ttl_secs: 300,
            heartbeat_secs: 30,
            agent_stage_timeout_secs: 1800,
            cmd_timeout_secs: 900,
            max_test_fix_attempts: 3,
            max_fix_rounds: 3,
            verdict_retries: 1,
            poll_interval_ms: 50,
        },
    };
    let fake_host = Arc::new(FakeHost::new());
    let state = AppState::with_all_agents_and_host(
        config,
        db,
        Arc::new(ClaudePlanningAgent::new()),
        Arc::new(ClaudeBreakdownAgent::new()),
        Arc::new(ClaudeTaskAgent::new()), // <-- the real, live agent (T-512/T-515)
        fake_host.clone(),
    );

    // ---- seed one project / one epic / one task ----
    let now = 1_700_000_000_000i64;
    let project_id = ulid::Ulid::new().to_string();
    let epic_id = ulid::Ulid::new().to_string();
    let task_id = ulid::Ulid::new().to_string();
    let conn = state.db.conn();
    conn.execute(
        "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
         VALUES (?1, 'Live', ?2, ?3, 'ready', ?4, ?4)",
        libsql::params![
            project_id.clone(),
            bare_url.clone(),
            clone_root.join(&project_id).to_string_lossy().to_string(),
            now
        ],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
         VALUES (?1, ?2, ?3, 'InProgress', ?4, ?4)",
        libsql::params![epic_id.clone(), project_id.clone(), EPIC_TITLE, now],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO task \
         (id, epic_id, project_id, title, description, acceptance, status, position, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Todo', 1, ?7, ?7)",
        libsql::params![
            task_id.clone(),
            epic_id.clone(),
            project_id.clone(),
            TASK_TITLE,
            TASK_DESCRIPTION,
            TASK_ACCEPTANCE,
            now
        ],
    )
    .await
    .unwrap();

    // ---- drive the REAL pipeline end to end ----
    //
    // `run_epic_pipeline` is the exact lease-unaware direct-call seam
    // `worker.rs`'s own hermetic tests use to drive a walk without the
    // claim/heartbeat pool machinery around it (see `worker::run_epic_pipeline`'s
    // doc) — the only thing different about this run is that `state.task_agent`
    // is the real `ClaudeTaskAgent` instead of a scripted fake.
    let run_result = tokio::time::timeout(
        LIVE_RUN_TIMEOUT,
        worker::run_epic_pipeline(state.clone(), epic_id.clone()),
    )
    .await;
    assert!(
        run_result.is_ok(),
        "the pipeline did not finish within {LIVE_RUN_TIMEOUT:?} — the live agent likely hung \
         or is stuck on an approval prompt. This is exactly the failure mode T-543's per-stage \
         timeout will bound in production; today the test itself is the only backstop."
    );

    // ---- assertions ----

    // 1. The epic reached Completed — `finalize_epic` only sets this after a
    //    real push *and* a successful `open_pr` (T-514: never on a bare-DAG
    //    completion alone). Anything else (still `InProgress`, or `Blocked`
    //    with a `blocked_reason`) means either the live agent misbehaved or
    //    Dearborn's plumbing around it did — surface the reason plainly
    //    rather than a bare `assert_eq` failure.
    let mut rows = conn
        .query(
            "SELECT status, blocked_reason FROM epic WHERE id = ?1",
            libsql::params![epic_id.clone()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("epic row must exist");
    let epic_status: String = row.get(0).unwrap();
    let blocked_reason: Option<String> = row.get(1).unwrap();
    assert_eq!(
        epic_status, "Completed",
        "epic must reach Completed via a real push + (faked) PR; got status={epic_status} \
         blocked_reason={blocked_reason:?}"
    );

    // 2. The task itself closed Done.
    let mut rows = conn
        .query(
            "SELECT status FROM task WHERE id = ?1",
            libsql::params![task_id.clone()],
        )
        .await
        .unwrap();
    let task_status: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(task_status, "Done");

    // 3. The real agent modified the working tree, Dearborn committed it with
    //    the §2.8 subject, and it was pushed to the bare origin — read back
    //    from the bare origin itself (see the module doc's "why the bare
    //    origin, not the workspace" section for why this is the right vantage
    //    point given the workspace is deleted by the time we get here).
    let branch = workspace::epic_branch_name(EPIC_TITLE, &epic_id);
    let subjects = git_capture(&bare_dir, &["log", "--reverse", "--format=%s", &branch]).await;
    let subjects: Vec<&str> = subjects.lines().collect();
    assert!(
        subjects.contains(&"init"),
        "the bare origin's log for {branch} must still contain the seeded init commit: {subjects:?}"
    );
    let expected_short_id = &task_id[task_id.len().saturating_sub(6)..];
    let expected_subject = format!("impl({expected_short_id}): {TASK_TITLE}");
    assert!(
        subjects.iter().any(|s| *s == expected_subject),
        "expected a commit subtitled `{expected_subject}` (MILESTONE_2 §2.8) in the pushed \
         branch's history; got {subjects:?} — if the agent produced no diff, T-513's \
         no-diff-is-fine-for-now behavior means this commit never happens at all, which is \
         itself the headless-write-mode risk this test exists to catch"
    );

    let content = git_capture(&bare_dir, &["show", &format!("{branch}:HELLO.md")]).await;
    assert_eq!(
        content, EXPECTED_FILE_CONTENT,
        "HELLO.md must contain exactly the requested content once read back from the pushed \
         branch in the bare origin"
    );

    // 4. FakeHost recorded exactly one `open_pr` call, from the right branch,
    //    against the right (fake) repo — proving the real push handed off to
    //    the (faked) PR step rather than the walk stopping short.
    let calls = fake_host.open_pr_calls();
    assert_eq!(calls.len(), 1, "exactly one open_pr call for one finalized epic");
    assert_eq!(calls[0].head, branch);
    assert_eq!(calls[0].repo_url, bare_url);

    let _ = std::fs::remove_dir_all(&root);
}

/// `git <args>` in `dir`, asserting success — the fixture-setup half of the
/// pattern `dearborn-server/src/git.rs` and `worker.rs`'s own tests already
/// use, duplicated here (rather than reused) because integration tests are a
/// separate crate and cannot see those modules' `#[cfg(test)]`-private helpers.
async fn git_ok(dir: &Path, args: &[&str]) {
    let status = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .await
        .expect("git must be on PATH");
    assert!(status.success(), "git {args:?} failed in {dir:?}");
}

/// `git <args>` in `dir`, asserting success and returning trimmed stdout —
/// used here to read the bare origin's own log/blob content back, which is
/// the strongest available proof that the real agent's edit, Dearborn's
/// commit, and the real push all actually happened (see the module doc).
async fn git_capture(dir: &Path, args: &[&str]) -> String {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .await
        .expect("git must be on PATH");
    assert!(
        output.status.success(),
        "git {args:?} failed in {dir:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}
