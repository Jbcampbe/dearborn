//! The per-stage `agent_run` evidence table: open/flush/close for every
//! stage (agent and non-agent alike), the D13 log cap, and the two read
//! endpoints (§2.5) a human uses to inspect a task's pipeline history
//! (T-512, generalizing T-511's original single `setup`-stage write).
//!
//! ## The lifecycle every stage follows
//!
//! 1. [`open_stage`] inserts a `running` row the instant a stage starts —
//!    visible to `GET /tasks/{id}/runs` immediately, so a client watching a
//!    task sees a stage begin before it has produced any output.
//! 2. A streaming (agent) stage calls [`flush_stage_log`] periodically (D14,
//!    driven by [`crate::task_agent::run_agent_stage`]) so a mid-run joiner
//!    can hydrate the transcript-so-far over REST without waiting for the
//!    stage to finish.
//! 3. [`close_stage`] writes the terminal `status`/`session_id`/`verdict`/
//!    `exit_code`/(capped) `log` — **exactly once**, on every exit path.
//!    [`guard_stage_close`] is the structural guarantee for that "every exit
//!    path" claim: it closes the row whether the stage body succeeds,
//!    returns an application error, or panics, so no stage author can leave
//!    a row stuck `running` by forgetting a branch.
//!
//! ## Why capping lives here (D13)
//!
//! [`cap_log`] is the one place a transcript is ever truncated, so every
//! caller (agent stages via [`close_stage`]/[`flush_stage_log`], a
//! shell-command stage's terminal write) gets the same head+tail-with-
//! elision-marker shape for free instead of reimplementing (and potentially
//! getting UTF-8 boundary safety wrong) at each call site.
//!
//! ## `setup`/`preflight`/`test_gate`, folded into the same lifecycle
//!
//! T-511 originally gave the `setup` stage its own after-the-fact write
//! (`record_setup_run`: the command had already finished by the time it was
//! called, so there was no `running` row to open first). T-520 replaced that
//! with [`crate::cmd::run_stage_command`], which drives the exact
//! [`open_stage`]/[`guard_stage_close`] lifecycle above for every non-agent,
//! shell-backed stage (`setup`, `preflight`, `test_gate`) — one code path
//! writes all three instead of each stage growing its own bespoke insert.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use axum::extract::{Path, State};
use axum::Json;
use futures_util::FutureExt;
use libsql::{params, Connection, Row};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{AppError, AppResult, AppState};

// ---- D13: log capping ------------------------------------------------------

/// Cap on a stored `agent_run.log`: 256 KB. Chosen so a busy epic's
/// tens-of-MB of transcripts (§11 risk 4) stay bounded per row while still
/// keeping enough of a real Claude Code run's output to be useful evidence.
pub const LOG_CAP_BYTES: usize = 256 * 1024;

/// Marks where a capped log's middle was cut. Kept short relative to
/// [`LOG_CAP_BYTES`] so it doesn't meaningfully eat into the head/tail budget.
const ELISION_MARKER: &str =
    "\n\n... [dearborn: log elided — exceeded 256 KB; showing head + tail] ...\n\n";

/// Cap `log` at [`LOG_CAP_BYTES`], keeping the **head and the tail** with
/// [`ELISION_MARKER`] in between when it doesn't fit whole. The beginning of
/// a transcript (what the agent was asked, its first moves) and its end (the
/// final answer, or the error that ended it) are the informative parts; a
/// long, repetitive tool-call loop is the likeliest thing to live in a
/// truncated middle. **UTF-8 safe**: the head/tail cut points are walked back
/// (head) / forward (tail) to the nearest `char` boundary, so a multi-byte
/// character straddling a cut is never split into invalid bytes — `log` is
/// already a `&str`, and every slice this function takes is on a valid
/// boundary, so the result is always a well-formed `String`.
///
/// A `log` already at or under the cap is returned unchanged (cheap common
/// case — most stages never come close to 256 KB).
pub fn cap_log(log: &str) -> String {
    if log.len() <= LOG_CAP_BYTES {
        return log.to_string();
    }

    let budget = LOG_CAP_BYTES.saturating_sub(ELISION_MARKER.len());
    let head_budget = budget / 2;
    let tail_budget = budget - head_budget;

    let head_end = floor_char_boundary(log, head_budget);
    // `tail_start` is clamped to be no earlier than `head_end` so a `log`
    // only slightly over the cap can never have its head and tail overlap
    // (defensive; in practice `log.len() > LOG_CAP_BYTES >> tail_budget`
    // makes this a no-op).
    let tail_start = ceil_char_boundary(log, log.len().saturating_sub(tail_budget)).max(head_end);

    let mut out = String::with_capacity(LOG_CAP_BYTES);
    out.push_str(&log[..head_end]);
    out.push_str(ELISION_MARKER);
    out.push_str(&log[tail_start..]);
    out
}

/// The largest byte index `<= index` that lands on a `char` boundary of `s`.
/// Hand-rolled because `str::floor_char_boundary` is nightly-only.
fn floor_char_boundary(s: &str, index: usize) -> usize {
    let mut i = index.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// The smallest byte index `>= index` that lands on a `char` boundary of `s`.
/// Hand-rolled because `str::ceil_char_boundary` is nightly-only.
fn ceil_char_boundary(s: &str, index: usize) -> usize {
    let mut i = index.min(s.len());
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

// ---- the per-stage row lifecycle ------------------------------------------

/// What [`open_stage`] needs to insert a stage's `running` row.
pub struct OpenStage<'a> {
    pub task_id: Option<&'a str>,
    pub epic_id: Option<&'a str>,
    /// `agent_run.stage` — the §2.2 vocabulary string (see
    /// [`crate::task_agent::Stage::as_str`]). Taken as a plain string, not
    /// the `Stage` enum, so this module stays decoupled from the task-agent
    /// seam: `agent_run` also carries stages this module knows nothing about
    /// (`planning`, `breakdown`, `setup`), and any future stage only needs a
    /// string here, never a new enum variant wired through this module.
    pub stage: &'a str,
    pub attempt: i64,
    /// Which harness key this run was resolved to (T8 evidence). `None` for
    /// rows predating the column (no backfill) and for non-agent stages.
    pub harness: Option<&'a str>,
    /// The resolved model passed to the CLI, if any (T8). `None` = CLI default.
    pub model: Option<&'a str>,
    /// SHA-256 hex of the resolved instruction prompt (T8) — see
    /// [`crate::agent_settings::prompt_hash`]. Hash, not text: prompts are
    /// user-authored and can be large; the hash correlates a run with the
    /// override that produced it without duplicating that text per row.
    pub prompt_hash: Option<&'a str>,
}

/// A stage's open row: just enough to flush/close it later without a
/// round-trip read. Hand it to exactly one flush/close sequence — cloning it
/// and closing twice would double-write (harmlessly, since `close_stage` is
/// an `UPDATE` by id, but it's not a pattern to encourage).
#[derive(Debug, Clone)]
pub struct StageHandle {
    pub id: String,
    pub started_at: i64,
}

/// Open a `running` row for a stage starting now.
pub async fn open_stage(
    conn: &Connection,
    open: OpenStage<'_>,
) -> Result<StageHandle, libsql::Error> {
    let id = ulid::Ulid::new().to_string();
    let started_at = now_ms();
    conn.execute(
        "INSERT INTO agent_run \
         (id, task_id, epic_id, stage, session_id, log, created_at, \
          attempt, status, verdict, started_at, ended_at, exit_code, \
          harness, model, prompt_hash) \
         VALUES (?1, ?2, ?3, ?4, NULL, '', ?5, ?6, 'running', NULL, ?5, NULL, NULL, \
          ?7, ?8, ?9)",
        params![
            id.clone(),
            open.task_id,
            open.epic_id,
            open.stage,
            started_at,
            open.attempt,
            open.harness,
            open.model,
            open.prompt_hash
        ],
    )
    .await?;
    Ok(StageHandle { id, started_at })
}

/// Overwrite the row's `log` without touching status/timestamps — the D14
/// ~2s partial flush a streaming agent stage calls while it runs (see
/// [`crate::task_agent::run_agent_stage`]).
pub async fn flush_stage_log(
    conn: &Connection,
    handle: &StageHandle,
    log: &str,
) -> Result<(), libsql::Error> {
    conn.execute(
        "UPDATE agent_run SET log = ?1 WHERE id = ?2",
        params![cap_log(log), handle.id.clone()],
    )
    .await?;
    Ok(())
}

/// Terminal fields a stage closes its row with.
pub struct CloseStage {
    /// `ok|error|timeout|cancelled` (§2.1's vocabulary; `running` is never
    /// written here — [`open_stage`] already wrote it).
    pub status: &'static str,
    pub session_id: Option<String>,
    /// Only ever set by a review/verify-complete stage (T-530's job);
    /// every other stage passes `None`. The column exists and this struct
    /// accepts a value for it starting now, so T-530 needs no schema or
    /// evidence-module change to populate it.
    pub verdict: Option<String>,
    pub exit_code: Option<i32>,
    /// The raw (uncapped) log; [`close_stage`] applies [`cap_log`] itself so
    /// no caller has to remember to.
    pub log: String,
}

/// Close a stage's row: cap the final log (D13), stamp `ended_at`, and write
/// the terminal status/verdict/exit_code/session_id. Called exactly once per
/// row — see [`guard_stage_close`] for the helper that makes "exactly once,
/// even on panic" structural rather than a discipline every call site has to
/// remember.
pub async fn close_stage(
    conn: &Connection,
    handle: &StageHandle,
    close: CloseStage,
) -> Result<(), libsql::Error> {
    conn.execute(
        "UPDATE agent_run SET status = ?1, session_id = ?2, verdict = ?3, \
         exit_code = ?4, log = ?5, ended_at = ?6 WHERE id = ?7",
        params![
            close.status,
            close.session_id,
            close.verdict,
            close.exit_code.map(|c| c as i64),
            cap_log(&close.log),
            now_ms(),
            handle.id.clone(),
        ],
    )
    .await?;
    Ok(())
}

// ---- boot-time orphan reconciliation --------------------------------------

/// The log note stamped into every row [`cancel_orphaned_running`] closes.
/// Written into the evidence trail (not just the status column) so a human
/// reading the run later sees *why* a stage that streamed real work ends in
/// `cancelled` with no matching user action — the exact confusion behind
/// "the UI showed two implementation agents": a server restart mid-stage
/// left the old row `running` forever next to the new owner's fresh one.
pub const ORPHANED_RUNNING_NOTE: &str =
    "[dearborn: this stage was still marked running when the server booted — \
     the process that owned it went away mid-run; closed as cancelled]";

/// Close every `agent_run` row still `status='running'` as `cancelled`,
/// appending [`ORPHANED_RUNNING_NOTE`] to each closed row's log. Called once
/// at boot, right after [`crate::worker::clear_all_leases`]: under Dearborn's
/// single-server assumption nothing can legitimately hold an open stage
/// across a restart, so any `running` row at boot is by definition an
/// orphan of a crashed/killed previous process — the agent it belonged to is
/// gone and nothing else would ever write its terminal fields. Without this,
/// the stale row sits next to the new owner's fresh attempt forever, and a
/// task detail view presents both as live agents.
///
/// Returns the number of rows reconciled (0 on the common clean-restart
/// path). Best-effort at the call site: a failure here logs but must not
/// block boot — the lease machinery already guarantees correctness; this is
/// purely evidence hygiene.
pub async fn cancel_orphaned_running(conn: &Connection) -> Result<u64, libsql::Error> {
    let now = now_ms();
    conn.execute(
        "UPDATE agent_run SET status = 'cancelled', ended_at = ?1, \
         log = CASE WHEN log = '' THEN ?2 ELSE log || char(10) || ?2 END \
         WHERE status = 'running'",
        params![now, ORPHANED_RUNNING_NOTE],
    )
    .await
}

// ---- attempt numbering -----------------------------------------------------

/// Next `attempt` value for (`task_id`, `stage`): one past the highest
/// attempt already recorded for that pair, or 1 for a first-ever run.
///
/// Stages whose caller drives its own counter (review rounds, fix rounds,
/// test-gate attempts) don't use this — but the initial implement stage used
/// to hardcode `attempt = 1`, which made every re-run of a previously
/// attempted task (a failed stage reset to Todo, or an orphaned InProgress
/// task reset by a new owner after a crash) read as another indistinguishable
/// "Attempt 1" in the timeline. Computing the number from the table instead
/// makes a re-run honestly read "Attempt 2" without any pipeline state.
pub async fn next_attempt(
    conn: &Connection,
    task_id: &str,
    stage: &str,
) -> Result<i64, libsql::Error> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(attempt), 0) + 1 FROM agent_run \
             WHERE task_id = ?1 AND stage = ?2",
            params![task_id, stage],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .expect("a MAX aggregate always returns exactly one row");
    row.get::<i64>(0)
}

/// Set `agent_run.verdict` on an **already-closed** row (T-530). A review (or
/// future verify-complete) stage's caller only learns the D9 verdict by
/// parsing [`crate::task_agent::AgentStageOutcome::text`] *after*
/// [`crate::task_agent::run_agent_stage`] has already called [`close_stage`]
/// with `verdict: None` (T-512 closes every row before its caller has had a
/// chance to look at the text) — by the time the verdict is known, the
/// `StageHandle` that `close_stage` needs is gone, so this is a plain,
/// independent `UPDATE` by row id rather than a second field on [`CloseStage`].
/// `verdict` is the exact [`crate::spec::Verdict::as_str`] token (`"PASS"` |
/// `"NEEDS_CHANGES"` | `"BLOCKED"`), matching what `GET /tasks/{id}/runs` and
/// the `stage_changed` WS frame both surface.
pub async fn set_verdict(
    conn: &Connection,
    run_id: &str,
    verdict: &str,
) -> Result<(), libsql::Error> {
    conn.execute(
        "UPDATE agent_run SET verdict = ?1 WHERE id = ?2",
        params![verdict, run_id],
    )
    .await?;
    Ok(())
}

/// Run `body` against an already-[`open_stage`]d stage, guaranteeing
/// [`close_stage`] is called **exactly once** no matter how `body` exits: it
/// completes, it returns `Err`, or it panics. This is the AC's "a stage that
/// panics or errors still closes its row" made structural rather than a
/// discipline every stage author has to remember on every branch — a
/// non-agent stage (a future shell-command runner, T-520) is the intended
/// caller; [`crate::task_agent::run_agent_stage`] achieves the same
/// guarantee its own way (via `tokio::task::JoinHandle`'s built-in panic
/// isolation across its `spawn_blocking` drain) because its "body" is a
/// spawned task rather than a single in-place `Future`, so it does not
/// route through this helper — see its own doc comment.
///
/// `body` returns `Result<(T, CloseStage), E>` — the value to hand back to
/// the caller plus the close payload to use, on success. On `Err(e)`, the row
/// closes `status = "error"` with `e`'s `Display` as the log, and `e` is
/// returned to the caller. On a panic, the row closes the same way (log = a
/// best-effort stringified panic payload) and the panic **resumes
/// unwinding** — the caller sees the identical panic it would have without
/// this wrapper; only the evidence row's fate changes. `E: Display` so even
/// a bare error type still produces a readable log.
pub async fn guard_stage_close<F, Fut, T, E>(
    conn: &Connection,
    handle: &StageHandle,
    body: F,
) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(T, CloseStage), E>>,
    E: std::fmt::Display,
{
    match AssertUnwindSafe(body()).catch_unwind().await {
        Ok(Ok((value, close))) => {
            let _ = close_stage(conn, handle, close).await;
            Ok(value)
        }
        Ok(Err(err)) => {
            let _ = close_stage(
                conn,
                handle,
                CloseStage {
                    status: "error",
                    session_id: None,
                    verdict: None,
                    exit_code: None,
                    log: err.to_string(),
                },
            )
            .await;
            Err(err)
        }
        Err(panic_payload) => {
            let message = panic_message(&panic_payload);
            let _ = close_stage(
                conn,
                handle,
                CloseStage {
                    status: "error",
                    session_id: None,
                    verdict: None,
                    exit_code: None,
                    log: format!("stage panicked: {message}"),
                },
            )
            .await;
            std::panic::resume_unwind(panic_payload);
        }
    }
}

/// Best-effort stringification of a `catch_unwind` payload — covers the two
/// overwhelmingly common panic payload shapes (`&str` from `panic!("lit")`,
/// `String` from `panic!("{}", ...)`); anything else (a custom payload type)
/// falls back to a generic message rather than failing to close the row.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

// ---- REST: task stage history + one stage's log (§2.5) ---------------------

/// One `agent_run` row as returned by `GET /tasks/{id}/runs` — **without**
/// `log`. A busy task's stages can each carry up to [`LOG_CAP_BYTES`] (256
/// KB) of transcript; a timeline view only needs to know what happened
/// (stage/attempt/status/verdict/timing), not download every stage's full
/// text just to render a list. `GET /runs/{id}` fetches one stage's full log
/// on demand — see [`AgentRunDetail`]. Documented in `CONVENTIONS.md`.
#[derive(Debug, Clone, Serialize)]
pub struct AgentRunSummary {
    pub id: String,
    pub task_id: Option<String>,
    pub epic_id: Option<String>,
    pub stage: String,
    pub attempt: i64,
    pub status: String,
    pub verdict: Option<String>,
    pub session_id: Option<String>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub exit_code: Option<i64>,
    pub created_at: i64,
}

/// One `agent_run` row **with** its full (capped) log — `GET /runs/{id}`.
#[derive(Debug, Clone, Serialize)]
pub struct AgentRunDetail {
    #[serde(flatten)]
    pub summary: AgentRunSummary,
    pub log: String,
}

/// Columns [`row_to_summary`] expects, in order. `GET /runs/{id}` appends
/// `log` after these.
const RUN_SUMMARY_COLUMNS: &str = "id, task_id, epic_id, stage, attempt, status, verdict, \
     session_id, started_at, ended_at, exit_code, created_at";

fn row_to_summary(row: &Row) -> Result<AgentRunSummary, libsql::Error> {
    Ok(AgentRunSummary {
        id: row.get(0)?,
        task_id: row.get(1)?,
        epic_id: row.get(2)?,
        stage: row.get(3)?,
        attempt: row.get(4)?,
        status: row.get(5)?,
        verdict: row.get(6)?,
        session_id: row.get(7)?,
        started_at: row.get(8)?,
        ended_at: row.get(9)?,
        exit_code: row.get(10)?,
        created_at: row.get(11)?,
    })
}

/// All `agent_run` rows for `task_id`, **oldest first** (`created_at` then
/// `rowid` for stable ordering among rows sharing a millisecond timestamp).
/// `rowid`, not `id`, is the tiebreak: `id` is a ULID, and two ULIDs minted
/// within the same millisecond differ only in their random tail — nothing
/// about that tail is guaranteed to sort in generation order, so `id ASC`
/// can (and, observed as a real flake, does) reorder same-millisecond rows.
/// SQLite's implicit `rowid` (this table has no `WITHOUT ROWID` clause and no
/// `INTEGER PRIMARY KEY` alias for it) always increases with insertion order,
/// which is exactly the tiebreak this function's contract promises.
pub async fn list_runs_for_task(
    conn: &Connection,
    task_id: &str,
) -> AppResult<Vec<AgentRunSummary>> {
    let sql = format!(
        "SELECT {RUN_SUMMARY_COLUMNS} FROM agent_run WHERE task_id = ?1 \
         ORDER BY created_at ASC, rowid ASC"
    );
    let mut rows = conn.query(&sql, params![task_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_summary(&row)?);
    }
    Ok(items)
}

/// One `agent_run` row with its full log, or `None` if `id` is unknown.
pub async fn fetch_run_detail(conn: &Connection, id: &str) -> AppResult<Option<AgentRunDetail>> {
    let sql = format!("SELECT {RUN_SUMMARY_COLUMNS}, log FROM agent_run WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => {
            let summary = row_to_summary(&row)?;
            let log: String = row.get(12)?;
            Ok(Some(AgentRunDetail { summary, log }))
        }
        None => Ok(None),
    }
}

/// `GET /tasks/{id}/runs` — a task's stage history, oldest first (§2.5).
/// `404` if the task does not exist.
pub async fn list_task_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    let conn = state.db.conn();
    if crate::tasks::fetch_task(conn, &id).await?.is_none() {
        return Err(AppError::NotFound(format!("task {id} not found")));
    }
    let items = list_runs_for_task(conn, &id).await?;
    Ok(Json(json!({ "items": items })))
}

/// `GET /runs/{id}` — one stage's full (capped) log (§2.5). `404` if `id` is
/// unknown.
pub async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<AgentRunDetail>> {
    let run = fetch_run_detail(state.db.conn(), &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("run {id} not found")))?;
    Ok(Json(run))
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;

    // The `setup` stage's row shape (task_id NULL, session_id NULL, attempt
    // 1, redacted+capped log) is now exercised where it's written —
    // `crate::cmd`'s `run_stage_command` tests, plus `crate::workspace`'s
    // `setup_cmd`-through-provisioning tests — rather than here against a
    // hand-called `record_setup_run`, which T-520 removed (see the module
    // doc's "folded into the same lifecycle" section).

    // ---- D13: log capping ------------------------------------------------

    #[test]
    fn short_log_is_returned_unchanged() {
        let log = "a short transcript, well under the cap";
        assert_eq!(cap_log(log), log);
    }

    #[test]
    fn a_300kb_log_caps_to_at_most_256kb_with_head_tail_and_marker() {
        // Each of head/tail is deliberately bigger than half the cap (~128 KB)
        // so the head/tail budget lands entirely *within* the marker text,
        // never spilling into the elided middle — otherwise this test would
        // be asserting the wrong thing about where the cut falls.
        let head = "HEAD-MARKER-".repeat(15_000); // 180,000 bytes
        let middle = "x".repeat(300_000);
        let tail = "TAIL-MARKER-".repeat(15_000); // 180,000 bytes
        let log = format!("{head}{middle}{tail}");
        assert!(log.len() > LOG_CAP_BYTES, "fixture must exceed the cap");

        let capped = cap_log(&log);
        assert!(
            capped.len() <= LOG_CAP_BYTES,
            "capped log must be <= {LOG_CAP_BYTES} bytes, got {}",
            capped.len()
        );
        assert!(capped.starts_with("HEAD-MARKER-"), "must keep the head");
        assert!(capped.ends_with("TAIL-MARKER-"), "must keep the tail");
        assert!(capped.contains("elided"), "must contain the elision marker");
        // The huge, uninformative middle is gone.
        assert!(
            !capped.contains(&"x".repeat(1000)),
            "the elided middle must not survive"
        );
    }

    #[test]
    fn cap_log_is_utf8_safe_across_multibyte_boundaries() {
        // Build a log whose head/tail cut points would land mid-character if
        // sliced naively: multi-byte emoji/CJK characters positioned right
        // around where the head/tail budgets fall.
        let budget = (LOG_CAP_BYTES
            - "\n\n... [dearborn: log elided — exceeded 256 KB; showing head + tail] ...\n\n"
                .len())
            / 2;
        let mut head = "a".repeat(budget - 2);
        head.push_str("💥💥💥💥💥"); // 4-byte chars straddling the head cut
        let middle = "z".repeat(300_000);
        let mut tail = "望".repeat(5); // 3-byte CJK chars straddling the tail cut
        tail.push_str(&"b".repeat(budget - 2));
        let log = format!("{head}{middle}{tail}");
        assert!(log.len() > LOG_CAP_BYTES);

        // Must not panic (would, on a mid-character slice) and must produce
        // valid UTF-8 (guaranteed by `String`'s invariant — the assertion
        // here is really just "this returns at all").
        let capped = cap_log(&log);
        assert!(capped.len() <= LOG_CAP_BYTES);
        assert!(capped.contains("elided"));
    }

    // ---- boot-time orphan reconciliation -----------------------------------

    #[tokio::test]
    async fn cancel_orphaned_running_closes_only_running_rows() {
        let db = seeded_db().await;
        let conn = db.conn();

        let open = |stage: &'static str, attempt: i64| {
            let conn = conn.clone();
            async move {
                open_stage(
                    &conn,
                    OpenStage {
                        task_id: Some("task-1"),
                        epic_id: Some("epic-1"),
                        stage,
                        attempt,
                        harness: None,
                        model: None,
                        prompt_hash: None,
                    },
                )
                .await
                .unwrap()
            }
        };

        // The orphan the reconciliation exists for: mid-flight, log already
        // partially flushed.
        let orphan_with_log = open("implement", 1).await;
        flush_stage_log(conn, &orphan_with_log, "half a transcript")
            .await
            .unwrap();
        // Second orphan whose log was never flushed (empty-log branch).
        let orphan_empty_log = open("review", 0).await;
        // A row that already reached a terminal state must be untouched.
        let already_closed = open("fix", 1).await;
        close_stage(
            conn,
            &already_closed,
            CloseStage {
                status: "ok",
                session_id: Some("sess-7".to_string()),
                verdict: None,
                exit_code: Some(0),
                log: "done normally".to_string(),
            },
        )
        .await
        .unwrap();

        let closed = cancel_orphaned_running(conn).await.unwrap();
        assert_eq!(closed, 2, "exactly the two `running` rows close");

        let row = fetch_run_detail(conn, &orphan_with_log.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.summary.status, "cancelled");
        assert!(row.summary.ended_at.is_some());
        assert_eq!(
            row.log,
            format!("half a transcript\n{ORPHANED_RUNNING_NOTE}"),
            "the note is appended to a flushed log"
        );

        let empty = fetch_run_detail(conn, &orphan_empty_log.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(empty.summary.status, "cancelled");
        assert_eq!(empty.log, ORPHANED_RUNNING_NOTE);

        let untouched = fetch_run_detail(conn, &already_closed.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(untouched.summary.status, "ok");
        assert_eq!(untouched.log, "done normally");
    }

    #[tokio::test]
    async fn cancel_orphaned_running_is_a_noop_when_nothing_runs() {
        let db = seeded_db().await;
        let n = cancel_orphaned_running(db.conn()).await.unwrap();
        assert_eq!(n, 0);
    }

    // ---- attempt numbering -------------------------------------------------

    #[tokio::test]
    async fn next_attempt_is_one_past_the_highest_recorded_attempt() {
        let db = seeded_db().await;
        let conn = db.conn();
        let open = |stage: &'static str, attempt: i64| {
            let conn = conn.clone();
            async move {
                open_stage(
                    &conn,
                    OpenStage {
                        task_id: Some("task-1"),
                        epic_id: Some("epic-1"),
                        stage,
                        attempt,
                        harness: None,
                        model: None,
                        prompt_hash: None,
                    },
                )
                .await
                .unwrap()
            }
        };

        // A first-ever implement run is attempt 1...
        assert_eq!(next_attempt(conn, "task-1", "implement").await.unwrap(), 1);
        open("implement", 1).await;
        // ...and after prior attempts exist (a re-run of a reset task), one
        // past the highest — not another indistinguishable 1.
        open("implement", 2).await;
        assert_eq!(next_attempt(conn, "task-1", "implement").await.unwrap(), 3);

        // Attempt counters are per (task, stage): review's rows don't leak
        // into implement's numbering.
        open("review", 0).await;
        open("review", 1).await;
        assert_eq!(next_attempt(conn, "task-1", "review").await.unwrap(), 2);
        // A different task counts from scratch even at the same stage.
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES ('task-2', 'epic-1', 'proj-1', 'T2', 'Todo', 2, 0, 0)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(next_attempt(conn, "task-2", "implement").await.unwrap(), 1);
    }

    // ---- open_stage / flush_stage_log / close_stage -----------------------

    async fn seeded_db() -> Db {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let conn = db.conn();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES ('proj-1', 'P', 'https://example.com/p.git', 'ready', 0, 0)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES ('epic-1', 'proj-1', 'E', 'InProgress', 0, 0)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES ('task-1', 'epic-1', 'proj-1', 'T', 'InProgress', 1, 0, 0)",
            (),
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn open_flush_close_round_trip() {
        let db = seeded_db().await;
        let conn = db.conn();

        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "implement",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();

        // Immediately visible as `running`, empty log.
        let row = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(row.summary.status, "running");
        assert_eq!(row.log, "");
        assert_eq!(row.summary.stage, "implement");
        assert_eq!(row.summary.attempt, 1);

        // A partial flush updates the log but not the status.
        flush_stage_log(conn, &handle, "partial output so far")
            .await
            .unwrap();
        let row = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(
            row.summary.status, "running",
            "flush must not change status"
        );
        assert_eq!(row.log, "partial output so far");

        // Closing writes the terminal fields.
        close_stage(
            conn,
            &handle,
            CloseStage {
                status: "ok",
                session_id: Some("sess-9".to_string()),
                verdict: Some("PASS".to_string()),
                exit_code: Some(0),
                log: "final output".to_string(),
            },
        )
        .await
        .unwrap();
        let row = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(row.summary.status, "ok");
        assert_eq!(row.summary.session_id.as_deref(), Some("sess-9"));
        assert_eq!(row.summary.verdict.as_deref(), Some("PASS"));
        assert_eq!(row.summary.exit_code, Some(0));
        assert_eq!(row.log, "final output");
        assert!(row.summary.ended_at.is_some());
    }

    // ---- set_verdict (T-530) ----------------------------------------------

    #[tokio::test]
    async fn set_verdict_updates_an_already_closed_row() {
        let db = seeded_db().await;
        let conn = db.conn();
        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "review",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();
        close_stage(
            conn,
            &handle,
            CloseStage {
                status: "ok",
                session_id: None,
                verdict: None,
                exit_code: Some(0),
                log: "findings...\nVERDICT: PASS".to_string(),
            },
        )
        .await
        .unwrap();

        let before = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(before.summary.verdict, None);

        set_verdict(conn, &handle.id, "PASS").await.unwrap();

        let after = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(after.summary.verdict.as_deref(), Some("PASS"));
        // Nothing else about the already-closed row changed.
        assert_eq!(after.summary.status, "ok");
        assert_eq!(after.log, "findings...\nVERDICT: PASS");
    }

    // ---- guard_stage_close: the finally guarantee -------------------------

    #[tokio::test]
    async fn guard_stage_close_closes_ok_on_success() {
        let db = seeded_db().await;
        let conn = db.conn();
        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "commit",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();

        let result: Result<&str, String> = guard_stage_close(conn, &handle, || async {
            Ok::<_, String>((
                "value",
                CloseStage {
                    status: "ok",
                    session_id: None,
                    verdict: None,
                    exit_code: Some(0),
                    log: "all good".to_string(),
                },
            ))
        })
        .await;

        assert_eq!(result.unwrap(), "value");
        let row = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(row.summary.status, "ok");
        assert_eq!(row.log, "all good");
    }

    #[tokio::test]
    async fn guard_stage_close_closes_error_when_body_returns_err() {
        let db = seeded_db().await;
        let conn = db.conn();
        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "test_gate",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();

        let result: Result<(), String> = guard_stage_close(conn, &handle, || async {
            Err::<((), CloseStage), String>("boom: tests failed".to_string())
        })
        .await;

        assert_eq!(result.unwrap_err(), "boom: tests failed");
        let row = fetch_run_detail(conn, &handle.id).await.unwrap().unwrap();
        assert_eq!(
            row.summary.status, "error",
            "an Err body must still close the row"
        );
        assert!(row.log.contains("boom: tests failed"));
        assert!(
            row.summary.ended_at.is_some(),
            "ended_at must be stamped even on error"
        );
    }

    #[tokio::test]
    async fn guard_stage_close_closes_error_when_body_panics_and_resumes_the_panic() {
        let db = seeded_db().await;
        let conn = db.conn().clone();
        let handle = open_stage(
            &conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "implement",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();

        // Drive the panicking guard on its own task so the panic doesn't tear
        // down the test's own task; a panicking `spawn` surfaces as
        // `JoinHandle::is_panic()` on await, which is exactly what "the panic
        // resumes unwinding" should look like from the outside.
        let handle_id = handle.id.clone();
        let join = tokio::spawn(async move {
            guard_stage_close(&conn, &handle, || async {
                if true {
                    panic!("stage body exploded");
                }
                #[allow(unreachable_code)]
                Ok::<((), CloseStage), String>((
                    (),
                    CloseStage {
                        status: "ok",
                        session_id: None,
                        verdict: None,
                        exit_code: None,
                        log: String::new(),
                    },
                ))
            })
            .await
        })
        .await;
        assert!(
            join.is_err(),
            "the panic must propagate out of guard_stage_close"
        );
        assert!(join.unwrap_err().is_panic());

        // The row still closed instead of sticking `running`.
        let row = fetch_run_detail(db.conn(), &handle_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.summary.status, "error",
            "a panicking body must still close the row"
        );
        assert!(row.log.contains("stage panicked"));
        assert!(row.summary.ended_at.is_some());
    }

    // ---- REST: GET /tasks/{id}/runs, GET /runs/{id} ------------------------

    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, Config};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// The bearer credential HTTP tests present, minted **once per process**
    /// from a seeded active admin (`crate::users::testing::seed_user` +
    /// `crate::sessions::testing::login_as`) — the replacement for the deleted
    /// static `TOKEN` constant. Access-token verification is stateless (one
    /// HMAC check against the fixed test master key, no database read), so a
    /// token minted here authenticates against every in-memory instance these
    /// tests boot.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
            // Seeding and login are async store calls, and `req` below is
            // synchronous. Mint on a dedicated OS thread: `Runtime::block_on`
            // panics if called from inside a test's own async context, but a
            // plain thread has none, so a throwaway current-thread runtime is
            // legal there.
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                let token = runtime.block_on(async {
                    let db = crate::Db::connect(":memory:").await.unwrap();
                    db.run_migrations().await.unwrap();
                    let state = crate::AppState::new(crate::Config::for_test(), db);
                    let user = crate::users::testing::seed_user(
                        &state,
                        "tester",
                        crate::users::Role::Admin,
                        true,
                    )
                    .await;
                    crate::sessions::testing::login_as(&state, &user).await
                });
                tx.send(token).expect("bearer receiver dropped");
            });
            rx.recv().expect("bearer minter panicked")
        })
    }

    fn req(method: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", auth_bearer()))
            .body(Body::empty())
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn app_over(db: Db) -> axum::Router {
        let state = AppState::with_agents(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
        );
        app(state)
    }

    #[tokio::test]
    async fn list_task_runs_orders_oldest_first_and_404s_unknown_task() {
        let db = seeded_db().await;
        let conn = db.conn();

        // Two rows for task-1, created in order: implement (attempt 1), then
        // fix (attempt 1). created_at ties broken by insertion order (id).
        let a = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "implement",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();
        close_stage(
            conn,
            &a,
            CloseStage {
                status: "ok",
                session_id: None,
                verdict: None,
                exit_code: Some(0),
                log: "impl done".to_string(),
            },
        )
        .await
        .unwrap();
        let b = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "review",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();
        close_stage(
            conn,
            &b,
            CloseStage {
                status: "ok",
                session_id: None,
                verdict: Some("PASS".to_string()),
                exit_code: Some(0),
                log: "review done".to_string(),
            },
        )
        .await
        .unwrap();

        let app = app_over(db).await;
        let response = app
            .clone()
            .oneshot(req("GET", "/tasks/task-1/runs"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["stage"], "implement");
        assert_eq!(items[0]["attempt"], 1);
        assert_eq!(items[1]["stage"], "review");
        assert_eq!(items[1]["verdict"], "PASS");
        // The list endpoint omits the full log field.
        assert!(items[0].get("log").is_none());

        let missing = app
            .oneshot(req("GET", "/tasks/does-not-exist/runs"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_run_returns_the_full_log_and_404s_unknown_id() {
        let db = seeded_db().await;
        let conn = db.conn();
        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "implement",
                attempt: 1,
                harness: None,
                model: None,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();
        close_stage(
            conn,
            &handle,
            CloseStage {
                status: "ok",
                session_id: Some("sess-1".to_string()),
                verdict: None,
                exit_code: Some(0),
                log: "the full transcript".to_string(),
            },
        )
        .await
        .unwrap();

        let run_id = handle.id.clone();
        let app = app_over(db).await;
        let response = app
            .clone()
            .oneshot(req("GET", &format!("/runs/{run_id}")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["log"], "the full transcript");
        assert_eq!(body["session_id"], "sess-1");

        let missing = app
            .oneshot(req("GET", "/runs/does-not-exist"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}
