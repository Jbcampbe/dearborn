//! The review-poller subsystem (epic plan §5, §7): a **single sequential,
//! long-lived** task that periodically scans items sitting in `InReview` with
//! an open PR and acts on their PR's lifecycle state.
//!
//! ## Why a separate poller, not a worker-pool loop
//!
//! [`crate::worker`]'s pool is lease-based and claims only `InProgress`
//! items; an `InReview` item is factory-`done` waiting on the human and must
//! never be re-claimed by the code-writing pipeline (the epic owner of the
//! post-PR-review loop — see `worker`'s module doc). So the poller is a
//! distinct, single-sequential task (concurrency 1, **no lease** — it relies
//! on the documented single-server assumption). When a later task's feedback
//! flow needs to hand work back to the leased pool it will flip the item to
//! `InProgress` and notify [`crate::worker`]; this task's step never does.
//!
//! ## This task's scope: merge/close detection + feedback fetch & filter (§5–§7)
//!
//! Each per-PR pass runs two steps:
//!
//! 1. **Merge / close detection** (§7, AC #7/#8): `get_pull` and react to
//!    lifecycle state.
//!    - `merged` → the epic moves to `Completed` (a standalone task to `Done`),
//!      the board is republished, and the workspace is **deleted now** (the
//!      exit from `InReview` — §7 makes merge/close the only place a retained
//!      workspace is torn down). No further work.
//!    - `state == "closed" && !merged` → the item moves to `Cancelled` and the
//!      workspace is deleted.
//!    - `state == "open"` → proceed to step 2.
//!    - **Never merged**: no code path in this module (or anywhere in the
//!      crate) calls a merge API. [`crate::git_host::GitHost`] has no merge
//!      method — exactly what AC #8 asserts.
//! 2. **Feedback fetch → actionable filter** (§6.2): on an open PR, fetch the
//!    reviews / review-comments / issue-comments / review-threads via the
//!    [`crate::git_host::GitHost`] seam, load what this PR already has recorded
//!    in `pr_feedback`, and compute the **deduped actionable set** — see
//!    [`compute_actionable`]. The poller only *computes the list* here; it does
//!    **not** act on it (triage + reply + spawn-work belong to later tasks), so
//!    an open PR still stays `InReview` and its workspace stays retained.
//!
//! ## Per-item error boundary
//!
//! Each candidate is processed in its own error-isolating scope: a failure
//! to load a project, decrypt a PAT, or query GitHub for one PR is logged
//! and skipped, letting the remaining candidates proceed. One failing PR can
//! never stall the rest of the poll.
//!
//! ## Wiring
//!
//! [`spawn_review_poller`] is called from `crate::main` immediately after
//! [`crate::worker::spawn_pool`], with the poll interval read from
//! `crate::config::ExecutorConfig::review_poll_interval_secs`.

use std::collections::HashSet;
use std::time::Duration;

use libsql::params;

use crate::epics::fetch_epic;
use crate::git_host::{
    GetPullRequest, GitHostError, InlineComment, IssueComment, ListIssueCommentsRequest,
    ListReviewCommentsRequest, ListReviewThreadsRequest, ListReviewsRequest, PullState, Review,
    Thread,
};
use crate::projects::load_decrypted_pat;
use crate::tasks::fetch_task;
use crate::workspace::{epic_workspace_path, task_workspace_path};
use crate::AppState;

/// Start the single review-poller task. Concurrency 1 by construction: this
/// function spawns exactly one [`review_loop`], which performs one tick's
/// work at a time — there is never more than one candidate being processed at
/// once. Returns the `JoinHandle` so the caller can hold it (production drops
/// it — the poller runs for the life of the process, exactly like the worker
/// pool; tests hold it so the runtime keeps ticking).
pub fn spawn_review_poller(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(review_loop(state))
}

/// One long-lived loop. Idles on `sleep`, then runs one [`review_tick`] and
/// repeats. Never returns — the `JoinHandle` only resolves when the process /
/// its runtime is torn down.
async fn review_loop(state: AppState) {
    let interval_secs = state.config.executor.review_poll_interval_secs.max(1);
    let interval = Duration::from_secs(interval_secs);
    loop {
        tokio::time::sleep(interval).await;
        review_tick(&state).await;
    }
}

/// Query candidates and process every one. Sequential: candidates (epics
/// first, then standalone tasks) are handled one at a time in this call, and
/// no two `review_tick` calls ever overlap (the single `review_loop`
/// serializes them).
pub async fn review_tick(state: &AppState) {
    let epic_candidates = load_epic_candidates(state).await;
    for candidate in epic_candidates {
        // Per-item error boundary: process_*_candidate swallows its own
        // failures, so one bad PR can never stall the rest.
        process_epic_candidate(state, &candidate).await;
    }

    let task_candidates = load_task_candidates(state).await;
    for candidate in task_candidates {
        process_task_candidate(state, &candidate).await;
    }
}

/// A candidate row's identity — everything the poller needs to reach its PR
/// lifecycle state and act on it, read in a single query. Kept deliberately
/// lean: the poller re-fetches the full row (`fetch_epic`/`fetch_task`) only
/// **after** a state transition lands, purely to build a publish payload.
struct Candidate {
    id: String,
    project_id: String,
    pr_number: i64,
}

/// The candidate rows: `epic WHERE status='InReview' AND pr_number IS NOT
/// NULL` (plan §5). An `InReview` epic has a recorded PR by construction
/// (finalize sets `pr_number` when it lands there), but the `IS NOT NULL`
/// guard makes the query self-documenting and robust against any hand-rolled
/// `InReview` row.
async fn load_epic_candidates(state: &AppState) -> Vec<Candidate> {
    let mut out = Vec::new();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT id, project_id, pr_number FROM epic \
             WHERE status = 'InReview' AND pr_number IS NOT NULL",
            (),
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "review poll: failed to query epic candidates");
            return out;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(error = %err, "review poll: failed to read an epic candidate row");
                continue;
            }
        };
        let id: String = match row.get(0) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let project_id: String = match row.get(1) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let pr_number: i64 = match row.get(2) {
            Ok(n) => n,
            Err(_) => continue,
        };
        out.push(Candidate {
            id,
            project_id,
            pr_number,
        });
    }
    out
}

/// Standalone-task candidate rows (`task WHERE status='InReview' AND epic_id
/// IS NULL AND pr_number IS NOT NULL`) — the task-table mirror of
/// [`load_epic_candidates`]. Epic-owned tasks never carry the terminal
/// `InReview` (the epic row does); only standalone tasks, whose PR lifecycle
/// lives on the task row itself, are polled here.
async fn load_task_candidates(state: &AppState) -> Vec<Candidate> {
    let mut out = Vec::new();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT id, project_id, pr_number FROM task \
             WHERE status = 'InReview' AND epic_id IS NULL AND pr_number IS NOT NULL",
            (),
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "review poll: failed to query task candidates");
            return out;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(error = %err, "review poll: failed to read a task candidate row");
                continue;
            }
        };
        let id: String = match row.get(0) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let project_id: String = match row.get(1) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let pr_number: i64 = match row.get(2) {
            Ok(n) => n,
            Err(_) => continue,
        };
        out.push(Candidate {
            id,
            project_id,
            pr_number,
        });
    }
    out
}

/// Process one epic [`Candidate`]'s merge/close state: `get_pull` the PR and
/// act on [`PullState`] (§7). Fully error-isolated — an outward failure is
/// logged and swallowed so the poll's remaining candidates still run. Never
/// calls a merge API.
async fn process_epic_candidate(state: &AppState, candidate: &Candidate) {
    let pull = match get_pull_state(state, candidate).await {
        Ok(pull) => pull,
        Err(err) => {
            tracing::warn!(
                epic = %candidate.id,
                error = %err,
                "review poll: could not read PR state; leaving epic untouched"
            );
            return;
        }
    };
    apply_epic_pull_state(state, candidate, &pull).await;
}

/// The standalone-task mirror of [`process_epic_candidate`].
async fn process_task_candidate(state: &AppState, candidate: &Candidate) {
    let pull = match get_pull_state(state, candidate).await {
        Ok(pull) => pull,
        Err(err) => {
            tracing::warn!(
                task = %candidate.id,
                error = %err,
                "review poll: could not read PR state; leaving task untouched"
            );
            return;
        }
    };
    apply_task_pull_state(state, candidate, &pull).await;
}

/// The shared `get_pull` read: load the project's `repo_url` + PAT and query
/// the hosted PR's lifecycle state. A failure to load project/PAT or to reach
/// GitHub is returned as-is (the caller isolates it). No merge path exists.
async fn get_pull_state(
    state: &AppState,
    candidate: &Candidate,
) -> Result<PullState, GitHostError> {
    let repo_url = load_repo_url(state, &candidate.project_id).await?;
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))?;
    state
        .git_host
        .get_pull(GetPullRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await
}

/// The GitHub entity a piece of feedback is backed as. Maps 1:1 to
/// `pr_feedback.source_kind` (`'review'` / `'review_comment'` /
/// `'issue_comment'`). Identity is DB-tracked (Decision 1) — the kind plus
/// the GitHub id together identify a feedback item in the dedup skip-sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedbackKind {
    /// A formal review (body = the review summary). In scope regardless of
    /// state; its inline comments are in scope too (emitted as
    /// `review_comment` items tagged with `review_id`).
    Review,
    /// A diff (review) comment — inline, possibly under a formal review.
    ReviewComment,
    /// A top-level PR (issue) comment.
    IssueComment,
}

impl FeedbackKind {
    /// The `pr_feedback.source_kind` this kind records as.
    pub fn as_source_kind(&self) -> &'static str {
        match self {
            FeedbackKind::Review => "review",
            FeedbackKind::ReviewComment => "review_comment",
            FeedbackKind::IssueComment => "issue_comment",
        }
    }

    /// Inverse of [`FeedbackKind::as_source_kind`]; `None` for unknown strings.
    fn from_source_kind(s: &str) -> Option<FeedbackKind> {
        match s {
            "review" => Some(FeedbackKind::Review),
            "review_comment" => Some(FeedbackKind::ReviewComment),
            "issue_comment" => Some(FeedbackKind::IssueComment),
            _ => None,
        }
    }
}

/// One piece of newly-discovered, **actionable** human feedback on a PR. The
/// [`compute_actionable`] filter guarantees:
/// - a formal review is always reported regardless of its state (its body,
///   plus its inline comments as separate `review_comment` items tagged with
///   `review_id`);
/// - a standalone (non-review) review comment is reported only when its body
///   starts with `dearborn:`;
/// - a top-level issue comment is reported only when its body starts with
///   `dearborn:`;
/// - anything that is our own tracked post, already handled, or whose inline
///   thread `is_resolved` is omitted.
///
/// It carries the raw identity plus the thread links the (later) triage/action
/// pipeline needs to reply and resolve. Nothing is acted on yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionableItem {
    /// Which feedback kind owns this item (`pr_feedback.source_kind`).
    pub kind: FeedbackKind,
    /// The GitHub id of the review / comment.
    pub github_id: i64,
    /// The text to triage (review body or comment body).
    pub body: String,
    /// The review this item belongs to, when it is an inline comment under a
    /// formal review (`pull_request_review_id`); `None` otherwise.
    pub review_id: Option<i64>,
    /// The GraphQL thread id for an inline (review-comment) item; `None` for
    /// reviews and top-level issue comments (no resolvable thread).
    pub thread_id: Option<String>,
}

/// Everything a PR already has on record in `pr_feedback` so the loop can
/// skip items that must not be reprocessed (identity is DB-tracked, §6.3).
#[derive(Debug, Default)]
pub struct KnownFeedback {
    /// GitHub ids of comments/reviews Dearborn itself posted (`source_kind =
    /// 'our_post'`). A fetched item with one of these ids is our own reply /
    /// post, never feedback to act on.
    pub our_comment_ids: HashSet<i64>,
    /// `(kind, github_id)` identities already recorded as handled (`state` =
    /// `'handled_reply'` / `'addressed'`).
    pub handled: HashSet<(FeedbackKind, i64)>,
}

/// True iff `body` starts with the `dearborn:` activation prefix (leading
/// whitespace tolerated). The convention that turns a standalone comment into
/// actionable feedback (Decision 3, §6.2).
fn starts_with_dearborn(body: &str) -> bool {
    body.trim_start().starts_with("dearborn:")
}

/// The pure actionable-item filter (epic plan §6.2): given everything the
/// GitHost fetched for one PR plus what the PR already has on record, return
/// the deduped list of feedback the factory should consider acting on. No I/O
/// — fully unit-testable.
///
/// Inclusion rules:
/// - every formal review, regardless of state — body + inline comments;
/// - `dearborn:`-prefixed standalone review comments and issue comments.
/// Exclusion rules (in order):
/// - our own tracked post (id in `known.our_comment_ids`);
/// - already handled (`(kind, id)` in `known.handled`);
/// - any inline item whose thread `is_resolved`.
pub fn compute_actionable(
    reviews: &[Review],
    review_comments: &[InlineComment],
    issue_comments: &[IssueComment],
    threads: &[Thread],
    known: &KnownFeedback,
) -> Vec<ActionableItem> {
    let mut out = Vec::new();

    let review_ids: HashSet<i64> = reviews.iter().map(|r| r.id).collect();

    // 1. Every formal review, any state — its body, in scope regardless of its
    //    own content. Its inline comments are emitted below as review_comment
    //    items tagged with their review_id.
    for review in reviews {
        if known.our_comment_ids.contains(&review.id)
            || known.handled.contains(&(FeedbackKind::Review, review.id))
        {
            continue;
        }
        out.push(ActionableItem {
            kind: FeedbackKind::Review,
            github_id: review.id,
            body: review.body.clone(),
            review_id: None,
            thread_id: None,
        });
    }

    // Inline threads: map each thread's root comment id -> thread (so a
    // review comment can be correlated to its thread + `is_resolved`). GitHub
    // REST comment ids and GraphQL root comment ids are the same integer.
    let thread_by_root: std::collections::HashMap<i64, &Thread> = threads
        .iter()
        .filter_map(|t| {
            t.root_comment_id
                .as_ref()
                .and_then(|r| r.parse::<i64>().ok())
                .map(|n| (n, t))
        })
        .collect();

    // 2. Diff (review) comments — inline feedback.
    for comment in review_comments {
        let thread = thread_by_root.get(&comment.id).copied();
        let resolved = thread.map(|t| t.is_resolved).unwrap_or(false);
        // In scope when it belongs to a formal review (review in scope), else
        // only when `dearborn:`-prefixed.
        let within_review = comment
            .pull_request_review_id
            .map_or(false, |rid| review_ids.contains(&rid));
        let in_scope = within_review || starts_with_dearborn(&comment.body);
        if !in_scope || resolved {
            continue;
        }
        if known.our_comment_ids.contains(&comment.id)
            || known
                .handled
                .contains(&(FeedbackKind::ReviewComment, comment.id))
        {
            continue;
        }
        out.push(ActionableItem {
            kind: FeedbackKind::ReviewComment,
            github_id: comment.id,
            body: comment.body.clone(),
            review_id: comment.pull_request_review_id,
            thread_id: thread.map(|t| t.id.clone()),
        });
    }

    // 3. Top-level issue comments — only the `dearborn:`-prefixed ones.
    for comment in issue_comments {
        if !starts_with_dearborn(&comment.body) {
            continue;
        }
        if known.our_comment_ids.contains(&comment.id)
            || known
                .handled
                .contains(&(FeedbackKind::IssueComment, comment.id))
        {
            continue;
        }
        out.push(ActionableItem {
            kind: FeedbackKind::IssueComment,
            github_id: comment.id,
            body: comment.body.clone(),
            review_id: None,
            thread_id: None,
        });
    }

    out
}

/// Load the dedup skip-sets for one PR from `pr_feedback`: which ids we posted
/// ourselves (`our_post`) and which `(kind, id)` identities are already
/// handled. A read failure is logged and yields an empty [`KnownFeedback`] so
/// a single bad candidate can't stall the poll's other candidates.
async fn load_known_feedback(state: &AppState, pr_number: i64) -> KnownFeedback {
    let mut known = KnownFeedback::default();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT source_kind, github_id, state FROM pr_feedback WHERE pr_number = ?1",
            params![pr_number],
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, pr = pr_number, "review poll: failed to read pr_feedback");
            return known;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let source_kind: String = match row.get(0) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let github_id: i64 = match row.get(1) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let state: String = match row.get(2) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if source_kind == "our_post" {
            known.our_comment_ids.insert(github_id);
        } else if state == "handled_reply" || state == "addressed" {
            if let Some(kind) = FeedbackKind::from_source_kind(&source_kind) {
                known.handled.insert((kind, github_id));
            }
        }
    }
    known
}

/// Fetch a candidate's open-PR feedback via the [`crate::git_host::GitHost`]
/// seam and compute the deduped actionable list (§6.2). This is the read+filter
/// step only — it never writes anything (no replies, no merge, no workspace
/// change); triage and action belong to later tasks. A host failure returns
/// `Err` (caller isolates it); a `pr_feedback` read error degrades to an empty
/// [`KnownFeedback`] harmlessly.
async fn fetch_actionable(
    state: &AppState,
    candidate: &Candidate,
) -> Result<Vec<ActionableItem>, GitHostError> {
    let repo_url = load_repo_url(state, &candidate.project_id).await?;
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))?
        .as_deref()
        .map(str::to_owned);

    let reviews = state
        .git_host
        .list_reviews(ListReviewsRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await?;
    let review_comments = state
        .git_host
        .list_review_comments(ListReviewCommentsRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await?;
    let issue_comments = state
        .git_host
        .list_issue_comments(ListIssueCommentsRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await?;
    let threads = state
        .git_host
        .list_review_threads(ListReviewThreadsRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await?;

    let known = load_known_feedback(state, candidate.pr_number).await;
    Ok(compute_actionable(
        &reviews,
        &review_comments,
        &issue_comments,
        &threads,
        &known,
    ))
}

/// The open-PR step-2 action (called from the `open` arm of the epic and task
/// handlers): compute the actionable list and log it. Nothing is written — the
/// item stays `InReview` — and failures are isolated so one bad PR can't stall
/// the rest of the poll.
async fn handle_open_feedback(state: &AppState, candidate: &Candidate) {
    match fetch_actionable(state, candidate).await {
        Ok(items) => {
            if items.is_empty() {
                tracing::debug!(
                    id = %candidate.id,
                    pr = %candidate.pr_number,
                    "review poll: no actionable feedback on open PR"
                );
            } else {
                for item in &items {
                    tracing::info!(
                        id = %candidate.id,
                        pr = %candidate.pr_number,
                        kind = ?item.kind,
                        github_id = item.github_id,
                        thread_id = item.thread_id.as_deref().unwrap_or(""),
                        "review poll: actionable feedback"
                    );
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                error = %err,
                "review poll: could not fetch actionable feedback for open PR"
            );
        }
    }
}

/// Apply an epic's [`PullState`] (§7):
/// - `merged` → `Completed` + board publish + delete workspace;
/// - `state == "closed" && !merged` → `Cancelled` + delete workspace;
/// - `state == "open"` → fetch+log actionable feedback (this task's step).
async fn apply_epic_pull_state(state: &AppState, candidate: &Candidate, pull: &PullState) {
    let status = match (pull.merged, pull.state.as_str()) {
        (true, _) => "Completed",
        (false, "closed") => "Cancelled",
        (false, "open") => {
            tracing::debug!(
                epic = %candidate.id,
                "review poll: PR still open; fetching actionable feedback"
            );
            handle_open_feedback(state, candidate).await;
            return;
        }
        (false, other) => {
            tracing::warn!(
                epic = %candidate.id,
                pr_state = other,
                "review poll: unrecognized PR state; leaving epic untouched"
            );
            return;
        }
    };

    let conn = state.db.conn();
    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE epic SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = 'InReview'",
            params![status, now, candidate.id.clone()],
        )
        .await;

    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                epic = %candidate.id,
                pr = %candidate.pr_number,
                to = status,
                "review poll: epic moved out of InReview"
            );
            publish_updated_epic(state, &candidate.id).await;
            let workspace_path = epic_workspace_path(&state.config.clone_root, &candidate.id);
            if let Err(err) = crate::workspace::delete_workspace(&workspace_path).await {
                tracing::warn!(
                    epic = %candidate.id,
                    error = %err,
                    "review poll: failed to delete epic workspace"
                );
            }
        }
        Ok(_) => {
            tracing::debug!(
                epic = %candidate.id,
                "review poll: epic no longer InReview (something else moved it); leaving as-is"
            );
        }
        Err(err) => {
            tracing::error!(
                epic = %candidate.id,
                error = %err,
                "review poll: failed to update epic status"
            );
        }
    }
}

/// The standalone-task mirror of [`apply_epic_pull_state`]. `merged` →
/// `Done`; `closed && !merged` → `Cancelled`; `open` → fetch+log feedback.
async fn apply_task_pull_state(state: &AppState, candidate: &Candidate, pull: &PullState) {
    let status = match (pull.merged, pull.state.as_str()) {
        (true, _) => "Done",
        (false, "closed") => "Cancelled",
        (false, "open") => {
            tracing::debug!(
                task = %candidate.id,
                "review poll: PR still open; fetching actionable feedback"
            );
            handle_open_feedback(state, candidate).await;
            return;
        }
        (false, other) => {
            tracing::warn!(
                task = %candidate.id,
                pr_state = other,
                "review poll: unrecognized PR state; leaving task untouched"
            );
            return;
        }
    };

    let conn = state.db.conn();
    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE task SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = 'InReview'",
            params![status, now, candidate.id.clone()],
        )
        .await;

    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                task = %candidate.id,
                pr = %candidate.pr_number,
                to = status,
                "review poll: standalone task moved out of InReview"
            );
            publish_updated_task(state, &candidate.id).await;
            let workspace_path = task_workspace_path(&state.config.clone_root, &candidate.id);
            if let Err(err) = crate::workspace::delete_workspace(&workspace_path).await {
                tracing::warn!(
                    task = %candidate.id,
                    error = %err,
                    "review poll: failed to delete task workspace"
                );
            }
        }
        Ok(_) => {
            tracing::debug!(
                task = %candidate.id,
                "review poll: task no longer InReview (something else moved it); leaving as-is"
            );
        }
        Err(err) => {
            tracing::error!(
                task = %candidate.id,
                error = %err,
                "review poll: failed to update task status"
            );
        }
    }
}

/// Best-effort re-fetch + publish of an epic after a merge/close transition,
/// mirroring `worker::finalize_epic`'s publish shape (`epic_updated` on
/// `epic:<id>` + the board on `project:<id>`).
async fn publish_updated_epic(state: &AppState, epic_id: &str) {
    let Ok(Some(updated)) = fetch_epic(state.db.conn(), epic_id).await else {
        return;
    };
    let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
    state
        .hub
        .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
    crate::board::publish_board(state, &updated.project_id).await;
}

/// Best-effort publish of a standalone task after a merge/close transition,
/// mirroring `worker::finalize_task`'s publish shape (a board publish on
/// `project:<id>`).
async fn publish_updated_task(state: &AppState, task_id: &str) {
    let Ok(Some(updated)) = fetch_task(state.db.conn(), task_id).await else {
        return;
    };
    crate::board::publish_board(state, &updated.project_id).await;
}

/// Load a candidate project's `repo_url` for the `get_pull` read.
async fn load_repo_url(state: &AppState, project_id: &str) -> Result<String, GitHostError> {
    let mut rows = state
        .db
        .conn()
        .query(
            "SELECT repo_url FROM project WHERE id = ?1",
            params![project_id],
        )
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project {project_id}: {err}")))?;
    match rows.next().await {
        Ok(Some(row)) => row
            .get(0)
            .map_err(|err| GitHostError::new(format!("failed to read project repo_url: {err}"))),
        Ok(None) => Err(GitHostError::new(format!("project {project_id} not found"))),
        Err(err) => Err(GitHostError::new(format!(
            "failed to read project repo_url: {err}"
        ))),
    }
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dir| dir.as_millis() as i64)
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::config::Config;
    use crate::git_host::testing::FakeHost;
    use crate::git_host::{self, GitHost};
    use crate::planning::testing::SilentPlanningAgent;
    use crate::task_agent::testing::ScriptedTaskAgent;
    use crate::Db;

    struct TestEnv {
        state: AppState,
        fake: Arc<FakeHost>,
        clone_root: PathBuf,
    }

    /// Build an [`AppState`] with an injected [`GitHost`] and a temp
    /// `clone_root` (so a test can seed fake workspace directories the poller
    /// deletes on merge/close).
    async fn make_env_with_host(host: Arc<dyn GitHost>, clone_root: PathBuf) -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        std::fs::create_dir_all(&clone_root).unwrap();

        let mut config = Config::for_test();
        config.clone_root = clone_root.to_string_lossy().into_owned();

        AppState::with_all_agents_and_host(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            Arc::new(ScriptedTaskAgent::new()),
            host,
        )
    }

    async fn make_env_with(fake: FakeHost) -> TestEnv {
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-{}", ulid::Ulid::new()));
        let fake = Arc::new(fake);
        let state = make_env_with_host(fake.clone(), clone_root.clone()).await;
        TestEnv {
            state,
            fake,
            clone_root,
        }
    }

    async fn seed_project(state: &AppState, repo_url: &str) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                 VALUES (?1, 'P', ?2, 'ready', ?3, ?3)",
                params![id.clone(), repo_url, now],
            )
            .await
            .unwrap();
        id
    }

    async fn seed_in_review_epic(
        state: &AppState,
        project_id: &str,
        pr_number: i64,
        title: &str,
    ) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, pr_number, pr_url, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'InReview', ?4, ?5, ?6, ?6)",
                params![
                    id.clone(),
                    project_id,
                    title,
                    pr_number,
                    format!("https://github.com/o/r/pull/{pr_number}"),
                    now
                ],
            )
            .await
            .unwrap();
        id
    }

    async fn seed_in_review_task(
        state: &AppState,
        project_id: &str,
        pr_number: i64,
        title: &str,
    ) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, pr_number, pr_url, created_at, updated_at) \
                 VALUES (?1, NULL, ?2, ?3, 'InReview', ?4, ?5, ?6, ?6)",
                params![
                    id.clone(),
                    project_id,
                    title,
                    pr_number,
                    format!("https://github.com/o/r/pull/{pr_number}"),
                    now
                ],
            )
            .await
            .unwrap();
        id
    }

    async fn epic_status(state: &AppState, epic_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query("SELECT status FROM epic WHERE id = ?1", params![epic_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    async fn task_status(state: &AppState, task_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query("SELECT status FROM task WHERE id = ?1", params![task_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// Seed a fake retained workspace at `<clone_root>/<kind>/<id>`.
    fn seed_workspace(root: &Path, kind: &str, id: &str) -> PathBuf {
        let p = root.join(kind).join(id);
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("retained.txt"), "x").unwrap();
        p
    }

    fn merged() -> git_host::PullState {
        PullState {
            merged: true,
            state: "closed".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    fn closed_unmerged() -> git_host::PullState {
        PullState {
            merged: false,
            state: "closed".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    fn open() -> git_host::PullState {
        PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    // ---- merged → Completed, publishes, deletes workspace; never merges ----

    #[tokio::test]
    async fn merged_epic_becomes_completed_publishes_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(merged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 7, "I merged PR").await;
        let ws = seed_workspace(&env.clone_root, "epics", &epic);

        // Subscribe before the tick so we can assert an `epic_updated` arrives.
        let mut rx = env.state.hub.subscribe(&format!("epic:{epic}"));

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "Completed");
        assert!(!ws.exists(), "a merged PR deletes the epic's workspace");
        assert_eq!(env.fake.get_pull_calls(), vec![7]);

        // The poller only ever *reads* the PR (`get_pull`); it never asks Git
        // host to mutate anything — and never to merge (AC #8).
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());

        let envelope: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(envelope["type"], "epic_updated");
        assert_eq!(envelope["payload"]["status"], "Completed");
    }

    #[tokio::test]
    async fn merged_standalone_task_becomes_done_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(merged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&env.state, &project, 9, "task title").await;
        let ws = seed_workspace(&env.clone_root, "tasks", &task);

        let mut rx = env.state.hub.subscribe(&format!("project:{project}"));

        review_tick(&env.state).await;

        assert_eq!(task_status(&env.state, &task).await, "Done");
        assert!(!ws.exists(), "a merged task's workspace is deleted");
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());

        let envelope: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(envelope["type"], "board_updated");
        let tasks = envelope["payload"]["tasks"].as_array().unwrap();
        assert_eq!(tasks[0]["status"], "Done");
    }

    // ---- closed-unmerged → Cancelled + workspace deleted -------------------

    #[tokio::test]
    async fn closed_unmerged_epic_becomes_cancelled_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(closed_unmerged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 11, "closed unmerged").await;
        let ws = seed_workspace(&env.clone_root, "epics", &epic);

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "Cancelled");
        assert!(
            !ws.exists(),
            "a closed-unmerged epic's workspace is deleted"
        );
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());
    }

    #[tokio::test]
    async fn closed_unmerged_task_becomes_cancelled_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(closed_unmerged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&env.state, &project, 13, "closed task").await;
        let ws = seed_workspace(&env.clone_root, "tasks", &task);

        review_tick(&env.state).await;

        assert_eq!(task_status(&env.state, &task).await, "Cancelled");
        assert!(
            !ws.exists(),
            "a closed-unmerged task's workspace is deleted"
        );
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
    }

    // ---- open PR → left untouched by this step ----------------------------

    #[tokio::test]
    async fn open_pr_epic_and_task_are_left_untouched() {
        let env = make_env_with(FakeHost::new().with_pull_state(open())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 15, "open epic").await;
        let epic_ws = seed_workspace(&env.clone_root, "epics", &epic);
        let task = seed_in_review_task(&env.state, &project, 16, "open task").await;
        let task_ws = seed_workspace(&env.clone_root, "tasks", &task);

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "InReview");
        assert_eq!(task_status(&env.state, &task).await, "InReview");
        assert!(epic_ws.exists(), "open PR → workspace retained");
        assert!(task_ws.exists(), "open PR → workspace retained");
        // No mutations at all — feedback/fetch is a later task.
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());
    }

    // ---- per-item error isolation ------------------------------------------

    /// A [`GitHost`] decorator over a [`FakeHost`] that makes `get_pull`
    /// fail for exactly one PR number, delegating everything else (including
    /// every other `get_pull`) to the inner fake — the minimal surface needed
    /// to prove one failing PR doesn't stall its neighbours.
    struct FailOnPull {
        inner: FakeHost,
        fail_pr: i64,
    }

    impl GitHost for FailOnPull {
        fn get_pull<'a>(
            &'a self,
            req: git_host::GetPullRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<PullState, git_host::GitHostError>> {
            Box::pin(async move {
                if req.number == self.fail_pr {
                    return Err(git_host::GitHostError::new("simulated get_pull failure"));
                }
                self.inner.get_pull(req).await
            })
        }

        fn push<'a>(
            &'a self,
            req: git_host::PushRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.push(req)
        }

        fn open_pr<'a>(
            &'a self,
            req: git_host::OpenPrRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<git_host::OpenedPr, git_host::GitHostError>> {
            self.inner.open_pr(req)
        }

        fn check_auth<'a>(
            &'a self,
            req: git_host::CheckAuthRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.check_auth(req)
        }

        fn list_reviews<'a>(
            &'a self,
            req: git_host::ListReviewsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::Review>, git_host::GitHostError>>
        {
            self.inner.list_reviews(req)
        }

        fn list_review_comments<'a>(
            &'a self,
            req: git_host::ListReviewCommentsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::InlineComment>, git_host::GitHostError>>
        {
            self.inner.list_review_comments(req)
        }

        fn list_issue_comments<'a>(
            &'a self,
            req: git_host::ListIssueCommentsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::IssueComment>, git_host::GitHostError>>
        {
            self.inner.list_issue_comments(req)
        }

        fn post_issue_comment<'a>(
            &'a self,
            req: git_host::PostIssueCommentRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<i64, git_host::GitHostError>> {
            self.inner.post_issue_comment(req)
        }

        fn reply_review_comment<'a>(
            &'a self,
            req: git_host::ReplyReviewCommentRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<i64, git_host::GitHostError>> {
            self.inner.reply_review_comment(req)
        }

        fn list_review_threads<'a>(
            &'a self,
            req: git_host::ListReviewThreadsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::Thread>, git_host::GitHostError>>
        {
            self.inner.list_review_threads(req)
        }

        fn resolve_thread<'a>(
            &'a self,
            req: git_host::ResolveThreadRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.resolve_thread(req)
        }
    }

    #[tokio::test]
    async fn a_failing_pr_does_not_stall_the_rest() {
        // PR #7's `get_pull` fails; #8's succeeds and merges. The failing item
        // must be left untouched while the healthy neighbour is still processed.
        let host = Arc::new(FailOnPull {
            inner: FakeHost::new().with_pull_state(merged()),
            fail_pr: 7,
        });
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-iso-{}", ulid::Ulid::new()));
        let state = make_env_with_host(host, clone_root.clone()).await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic_failing = seed_in_review_epic(&state, &project, 7, "failing epic").await;
        let epic_ok = seed_in_review_epic(&state, &project, 8, "ok epic").await;

        review_tick(&state).await;

        assert_eq!(
            epic_status(&state, &epic_failing).await,
            "InReview",
            "the failing PR's item must be left untouched"
        );
        assert_eq!(
            epic_status(&state, &epic_ok).await,
            "Completed",
            "the other PR must still be processed despite the failing neighbour"
        );
    }

    // ---- the actionable-item filter (pure unit tests, §6.2) --------------

    /// Build an empty [`KnownFeedback`] (fresh PR, nothing on record).
    fn no_known() -> KnownFeedback {
        KnownFeedback::default()
    }

    /// A review with a body + state (identity is DB-tracked, so only id/body
    /// matter for the filter).
    fn review(id: i64, body: &str) -> Review {
        Review {
            id,
            state: "COMMENTED".into(),
            body: body.into(),
            submitted_at: None,
        }
    }

    fn inline(id: i64, body: &str, review_id: Option<i64>) -> InlineComment {
        InlineComment {
            id,
            body: body.into(),
            in_reply_to: None,
            pull_request_review_id: review_id,
            path: None,
            line: None,
        }
    }

    /// A thread whose root is `root` (a review-comment id), with the given
    /// resolution state.
    fn thread(id: &str, root: i64, resolved: bool) -> Thread {
        Thread {
            id: id.into(),
            is_resolved: resolved,
            root_comment_id: Some(root.to_string()),
        }
    }

    #[test]
    fn formal_review_is_always_in_scope_regardless_of_state() {
        // An Approve* and a CHANGES_REQUESTED* review with arbitrary bodies —
        // both must be actionable; review *state* is irrelevant to scope (it
        // only ever governs the never-performed merge).
        let reviews = vec![
            review(100, "looks good"),
            review(101, "CHANGES_REQUESTED: please fix"),
            review(102, ""), // empty body is still a formal review
        ];
        let items = compute_actionable(&reviews, &[], &[], &[], &no_known());
        let kinds: Vec<(i64, String)> = items
            .iter()
            .map(|i| (i.github_id, i.body.clone()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (100, "looks good".to_string()),
                (101, "CHANGES_REQUESTED: please fix".to_string()),
                (102, "".to_string()),
            ]
        );
    }

    #[test]
    fn dearborn_prefixed_review_and_issue_comments_are_included() {
        let review_comments = vec![inline(200, "dearborn: how does this work?", None)];
        let issue_comments = vec![IssueComment {
            id: 300,
            body: "dearborn: fix the docs".into(),
        }];
        let items = compute_actionable(&[], &review_comments, &issue_comments, &[], &no_known());
        let ids: Vec<(FeedbackKind, i64)> = items.iter().map(|i| (i.kind, i.github_id)).collect();
        assert_eq!(
            ids,
            vec![
                (FeedbackKind::ReviewComment, 200),
                (FeedbackKind::IssueComment, 300),
            ]
        );
    }

    #[test]
    fn non_prefixed_standalone_comments_are_excluded() {
        // A standalone review comment and a top-level issue comment without the
        // `dearborn:` convention are ignored.
        let review_comments = vec![inline(210, "plain inline note", None)];
        let issue_comments = vec![IssueComment {
            id: 310,
            body: "no prefix here".into(),
        }];
        let items = compute_actionable(&[], &review_comments, &issue_comments, &[], &no_known());
        assert!(items.is_empty());
    }

    #[test]
    fn inline_comment_under_a_formal_review_is_in_scope_without_prefix() {
        // An inline comment attached to a review is part of that review's
        // scope — no `dearborn:` needed.
        let reviews = vec![review(100, "")];
        let review_comments = vec![inline(200, "change the timeout", Some(100))];
        let items = compute_actionable(&reviews, &review_comments, &[], &[], &no_known());
        let inline_item = items
            .iter()
            .find(|i| i.kind == FeedbackKind::ReviewComment)
            .expect("an inline comment under a review is actionable even without a prefix");
        assert_eq!(inline_item.review_id, Some(100));
    }

    #[test]
    fn our_own_tracked_post_is_skipped() {
        // A comment/review Dearborn itself posted (id on record as `our_post`)
        // is never reprocessed.
        let mut known = KnownFeedback::default();
        known.our_comment_ids.insert(200);
        known.our_comment_ids.insert(100);

        let reviews = vec![review(100, "dearborn: our own review")];
        let review_comments = vec![inline(200, "dearborn: our own comment", None)];
        let items = compute_actionable(&reviews, &review_comments, &[], &[], &known);
        assert!(items.is_empty());
    }

    #[test]
    fn already_handled_item_is_skipped() {
        let mut known = KnownFeedback::default();
        known.handled.insert((FeedbackKind::ReviewComment, 200));
        known.handled.insert((FeedbackKind::IssueComment, 300));

        let review_comments = vec![inline(200, "dearborn: already handled", None)];
        let issue_comments = vec![IssueComment {
            id: 300,
            body: "dearborn: handled".into(),
        }];
        let items = compute_actionable(&[], &review_comments, &issue_comments, &[], &known);
        assert!(items.is_empty());
    }

    #[test]
    fn resolved_inline_thread_is_skipped() {
        // A `dearborn:`-prefixed inline comment whose thread is_resolved is
        // skipped; the unresolved one is still actionable.
        let review_comments = vec![
            inline(201, "dearborn: unresolved", None),
            inline(202, "dearborn: resolved", None),
        ];
        let threads = vec![thread("thr-2", 202, true)];
        let items = compute_actionable(&[], &review_comments, &[], &threads, &no_known());
        let ids: Vec<i64> = items.iter().map(|i| i.github_id).collect();
        assert_eq!(ids, vec![201]);
    }

    // ---- integration slice over FakeHost (scripted PR) -------------------

    #[tokio::test]
    async fn fetch_actionable_returns_expected_list_for_a_scripted_pr() {
        // A realistic mixed PR: two formal reviews, a review-comment under one
        // of them, a standalone `dearbon:` review-comment, a resolved-thread
        // standalone, plus two top-level issue comments (one prefixed, one
        // plain). The filter must yield exactly the dedupe actionable set.
        let fake = FakeHost::new()
            .with_pull_state(open())
            .with_reviews(vec![
                git_host::Review {
                    id: 100,
                    state: "APPROVED".into(),
                    body: "looks good".into(),
                    submitted_at: None,
                },
                git_host::Review {
                    id: 101,
                    state: "CHANGES_REQUESTED".into(),
                    body: "please address this".into(),
                    submitted_at: None,
                },
            ])
            .with_review_comments(vec![
                git_host::InlineComment {
                    id: 201,
                    body: "inline under review".into(),
                    in_reply_to: None,
                    pull_request_review_id: Some(100),
                    path: None,
                    line: None,
                },
                git_host::InlineComment {
                    id: 202,
                    body: "dearborn: standalone question".into(),
                    in_reply_to: None,
                    pull_request_review_id: None,
                    path: None,
                    line: None,
                },
                git_host::InlineComment {
                    id: 203,
                    body: "dearborn: resolved one".into(),
                    in_reply_to: None,
                    pull_request_review_id: None,
                    path: None,
                    line: None,
                },
            ])
            .with_issue_comments(vec![
                git_host::IssueComment {
                    id: 300,
                    body: "dearborn: top-level question".into(),
                },
                git_host::IssueComment {
                    id: 301,
                    body: "just a plain comment".into(),
                },
            ])
            .with_threads(vec![
                git_host::Thread {
                    id: "thr-1".into(),
                    is_resolved: false,
                    root_comment_id: Some("201".into()),
                },
                git_host::Thread {
                    id: "thr-3".into(),
                    is_resolved: true,
                    root_comment_id: Some("203".into()),
                },
            ]);

        let env = make_env_with(fake).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 7, "scripted feedback").await;

        let items = fetch_actionable(
            &env.state,
            &Candidate {
                id: epic.clone(),
                project_id: project.clone(),
                pr_number: 7,
            },
        )
        .await
        .unwrap();

        let got: Vec<(FeedbackKind, i64, Option<i64>, Option<String>)> = items
            .iter()
            .map(|i| (i.kind, i.github_id, i.review_id, i.thread_id.clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                (FeedbackKind::Review, 100, None, None),
                (FeedbackKind::Review, 101, None, None),
                (
                    FeedbackKind::ReviewComment,
                    201,
                    Some(100),
                    Some("thr-1".to_string())
                ),
                (FeedbackKind::ReviewComment, 202, None, None),
                (FeedbackKind::IssueComment, 300, None, None),
            ]
        );

        // Same pair: every GitHost read fan was hit exactly once for this PR,
        // and — this filter builds the dedup list only — no write calls.
        assert_eq!(env.fake.list_reviews_calls(), vec![7]);
        assert_eq!(env.fake.list_review_comments_calls(), vec![7]);
        assert_eq!(env.fake.list_issue_comments_calls(), vec![7]);
        assert_eq!(env.fake.list_review_threads_calls(), vec![7]);
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());
    }

    #[tokio::test]
    async fn compute_actionable_skips_our_own_and_already_handled() {
        // Pre-record handled / our-own rows for PR 7 in pr_feedback; the
        // filter must drop those items on the next fetch.
        let mut known = KnownFeedback::default();
        known.handled.insert((FeedbackKind::Review, 100));
        known.handled.insert((FeedbackKind::IssueComment, 300));
        known.our_comment_ids.insert(300);

        let reviews = vec![review(100, "handled review")];
        let review_comments = vec![inline(200, "dearborn: q", None)];
        let issue_comments = vec![IssueComment {
            id: 300,
            body: "dearborn: ours".into(),
        }];
        let items = compute_actionable(&reviews, &review_comments, &issue_comments, &[], &known);
        let ids: Vec<(FeedbackKind, i64)> = items.iter().map(|i| (i.kind, i.github_id)).collect();
        assert_eq!(ids, vec![(FeedbackKind::ReviewComment, 200)]);
    }
}
