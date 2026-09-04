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
//! 2. **Feedback fetch → actionable filter → triage + question-reply**
//!    (§6.2/§6.4): on an open PR, fetch the reviews / review-comments /
//!    issue-comments / review-threads via the [`crate::git_host::GitHost`]
//!    seam, load what this PR already has recorded in `pr_feedback`, compute
//!    the **deduped actionable set** (see [`compute_actionable`]), then run the
//!    `Triage` agent stage over each item. A `QUESTION` gets a posted reply
//!    (issue comment for a review body / top-level comment; in-thread reply +
//!    thread resolution for an inline comment) and is marked handled in
//!    `pr_feedback` (`handled_reply` + `our_post`), so the next poll skips it.
//!    A `CHANGE` (AC #4) is handed back to the existing pipeline: an epic
//!    change creates one linked `Todo` task per triaged spec, records their ids
//!    + `base_sha` in `pr_feedback` (`in_progress`), flips the epic to
//!    `InProgress` + `notify_waiters()`; a standalone change amends the task's
//!    own spec, records `base_sha` + `in_progress`, and flips the task to
//!    `InProgress`. Both post the interim "Picked up — implementing." reply
//!    without resolving. The worker pool then runs the work and finalize pushes
//!    the same branch back to `InReview`.
//! 3. **Close the loop after work lands** (AC #5): once the item is back in
//!    `InReview`, any `pr_feedback` row in `state='in_progress'` whose spawned
//!    work has completed and whose branch `head_sha` (via [`crate::git_host::
//!    GitHost::get_pull`]) has advanced past the row's `base_sha` is answered
//!    with the closing "Addressed in `<commit>`" reply (an in-thread reply +
//!    `resolve_thread` for an inline row; an issue comment for a formal-review
//!    body / top-level comment), the row moves to `state='addressed'`, and the
//!    reply is recorded as an `our_post`. Already-addressed items are skipped on
//!    subsequent polls; rows whose branch hasn't advanced stay `in_progress`.
//!
//!    An open PR stays `InReview` until the feedback spawns work or settles,
//!    and its workspace stays retained.
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
    ListReviewCommentsRequest, ListReviewThreadsRequest, ListReviewsRequest,
    PostIssueCommentRequest, PullState, ReplyReviewCommentRequest, ResolveThreadRequest, Review,
    Thread,
};
use crate::projects::load_decrypted_pat;
use crate::task_agent::{AgentStageOutcome, AgentStageParams, TaskRunRequest};
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
/// It carries the raw identity plus the thread links the triage/action step
/// ([`handle_open_feedback`]) needs to reply and resolve. The filter itself
/// never acts on an item — this is the passive, pure dedup result.
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
/// change); the caller triages and acts on the result. A host failure returns
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

/// Which `InReview` row a [`Candidate`] was queried from — the post-PR
/// feedback loop targets epics and standalone tasks alike, but the triage
/// context, the `pr_feedback.epic_id`/`task_id` provenance, and the workspace
/// the triage agent runs in all differ by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkKind {
    Epic,
    Task,
}

/// The `(epic_id, task_id)` provenance columns a feedback state row records:
/// exactly one is set, matching the candidate's kind (the `pr_feedback`
/// invariant — see the migration's doc).
fn provenance(kind: WorkKind, candidate: &Candidate) -> (Option<String>, Option<String>) {
    match kind {
        WorkKind::Epic => (Some(candidate.id.clone()), None),
        WorkKind::Task => (None, Some(candidate.id.clone())),
    }
}

/// The open-PR step-2 action (called from the `open` arm of the epic and task
/// handlers): first [`close_the_loop`] answers any `in_progress` change whose
/// spawned work has landed (AC #5), then for each deduped actionable item run
/// the triage agent and act on the classification — a `QUESTION` posts a reply
/// and records handled state; a `CHANGE` (AC #4) hands work back to the
/// existing pipeline via [`handle_change`]. An item's triage/reply is isolated
/// so one failure can't stall the other items or the rest of the poll. `head_sha`
/// is the current PR-branch HEAD from [`crate::git_host::GitHost::get_pull`];
/// it is the post-work address that proves an `in_progress` row's branch
/// advanced past its `base_sha`.
async fn handle_open_feedback(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    head_sha: &str,
) {
    // Close the loop first: address any landed change requests before fetching
    // so their rows move to `addressed` and drop out of the actionable set via
    // `known.handled` on the same poll.
    close_the_loop(state, candidate, head_sha).await;

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
                    act_on_item(state, candidate, kind, item).await;
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

/// One `pr_feedback` row in `state='in_progress'` — a change request whose
/// spawned work may have landed and now needs the closing "Addressed in
/// `<commit>`" reply (AC #5). Carries everything the closing pass needs to
/// prove completion and post/resolve.
#[derive(Debug, Clone)]
struct InProgressRow {
    id: String,
    epic_id: Option<String>,
    task_id: Option<String>,
    source_kind: String,
    github_id: i64,
    thread_id: Option<String>,
    spawned_task_ids: Option<String>,
    base_sha: Option<String>,
}

/// Load every `state='in_progress'` row for one PR — the change requests whose
/// spawned work may have landed and needs the closing reply. A read failure is
/// logged and yields an empty vec so one bad candidate can't stall the rest of
/// the poll.
async fn load_in_progress_rows(state: &AppState, pr_number: i64) -> Vec<InProgressRow> {
    let mut out = Vec::new();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT id, epic_id, task_id, source_kind, github_id, thread_id, \
             spawned_task_ids, base_sha FROM pr_feedback \
             WHERE pr_number = ?1 AND state = 'in_progress'",
            params![pr_number],
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(
                error = %err,
                pr = pr_number,
                "review poll: failed to read in_progress pr_feedback rows"
            );
            return out;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(_) => continue,
        };
        let id: String = match row.get(0) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let epic_id: Option<String> = row.get(1).unwrap_or(None);
        let task_id: Option<String> = row.get(2).unwrap_or(None);
        let source_kind: String = match row.get(3) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let github_id: i64 = match row.get(4) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let thread_id: Option<String> = row.get(5).unwrap_or(None);
        let spawned_task_ids: Option<String> = row.get(6).unwrap_or(None);
        let base_sha: Option<String> = row.get(7).unwrap_or(None);
        out.push(InProgressRow {
            id,
            epic_id,
            task_id,
            source_kind,
            github_id,
            thread_id,
            spawned_task_ids,
            base_sha,
        });
    }
    out
}

/// The db-backed close-the-loop pass (AC #5): for each `in_progress` row whose
/// spawned work has completed (`spawned_work_complete`) and whose branch HEAD
/// has advanced past the stored `base_sha` (`get_pull`'s `head_sha`), post the
/// closing "Addressed in `<commit>`" reply, resolve the inline row's thread,
/// move the source row to `state='addressed'`, and record the reply id as an
/// `our_post`. Rows whose head hasn't advanced (or whose work isn't complete)
/// are left `in_progress`; an already-addressed row is never reprocessed. Each
/// row is isolated — one row's failure can't stall the others or the poll.
async fn close_the_loop(state: &AppState, candidate: &Candidate, head_sha: &str) {
    for row in load_in_progress_rows(state, candidate.pr_number).await {
        if !spawned_work_complete(state, &row).await {
            continue;
        }
        let Some(base_sha) = row.base_sha.as_deref() else {
            // No base to prove advancement against; leave it in_progress.
            continue;
        };
        if head_sha == base_sha {
            // The branch hasn't advanced past the point work was picked up.
            tracing::debug!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                row = %row.id,
                "review poll: in_progress row's branch has not advanced; leaving in_progress"
            );
            continue;
        }

        // `<commit>` is the post-work branch HEAD (`get_pull`'s head_sha).
        let commit = head_sha;
        let reply_body = format!("Addressed in {commit}");
        let Some(reply_id) = post_closing_reply(state, candidate, &row, &reply_body).await else {
            continue;
        };

        mark_addressed(state, candidate, &row).await;
        record_our_post(state, candidate, candidate_kind_for(&row), reply_id).await;
        tracing::info!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            row = %row.id,
            kind = %row.source_kind,
            github_id = row.github_id,
            commit = %commit,
            "review poll: change work landed; posted \"Addressed in <commit>\" and marked addressed"
        );
    }
}

/// Map a `pr_feedback` change row's provenance to the kind it was recorded
/// under, so its `our_post` row keeps the same `epic_id`/`task_id` shape.
fn candidate_kind_for(row: &InProgressRow) -> WorkKind {
    if row.task_id.is_some() && row.epic_id.is_none() {
        WorkKind::Task
    } else {
        WorkKind::Epic
    }
}

/// Whether an `in_progress` row's spawned work has completed: an epic change's
/// `spawned_task_ids` must all be `Done` (the DAG walk drives them there before
/// the epic can come back to `InReview`); a standalone change spawned no tasks,
/// and its own task returning to `InReview` is the completion signal. A row we
/// can't verify as complete is treated as not-yet (stays `in_progress`).
async fn spawned_work_complete(state: &AppState, row: &InProgressRow) -> bool {
    // A standalone (option-C) change spawned no tasks — completion is its own
    // task coming back to `InReview`.
    if row.spawned_task_ids.is_none() {
        return task_status_is(state, row.task_id.as_deref(), "InReview").await;
    }

    let ids: Vec<String> = row
        .spawned_task_ids
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    if ids.is_empty() {
        // A change row with no spawned task ids is not verifiable; stay
        // in_progress until the record is consistent.
        return false;
    }
    for id in &ids {
        if !task_status_is(state, Some(id.as_str()), "Done").await {
            return false;
        }
    }
    true
}

/// True iff a task row exists and its status equals `wanted`; false on any
/// missing row / read failure (so callers conservatively stay `in_progress`).
async fn task_status_is(state: &AppState, task_id: Option<&str>, wanted: &str) -> bool {
    let Some(task_id) = task_id else {
        return false;
    };
    let Ok(mut rows) = state
        .db
        .conn()
        .query("SELECT status FROM task WHERE id = ?1", params![task_id])
        .await
    else {
        return false;
    };
    match rows.next().await {
        Ok(Some(row)) => row.get::<String>(0).ok().as_deref() == Some(wanted),
        _ => false,
    }
}

/// Post the closing "Addressed in `<commit>`" reply for one row (AC #5),
/// routed exactly like a question's reply: an issue comment for a formal-review
/// body / top-level comment, an in-thread reply + `resolve_thread` for an
/// inline comment. Returns the created reply's GitHub id on success, `None` on
/// any failure (logged — the row is left `in_progress` to be retried next
/// poll).
async fn post_closing_reply(
    state: &AppState,
    candidate: &Candidate,
    row: &InProgressRow,
    body: &str,
) -> Option<i64> {
    let repo_url = load_repo_url(state, &candidate.project_id).await.ok()?;
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))
        .ok()
        .flatten();
    let pat = pat.as_deref();

    let reply_id = match FeedbackKind::from_source_kind(&row.source_kind) {
        Some(FeedbackKind::Review) | Some(FeedbackKind::IssueComment) => {
            match state
                .git_host
                .post_issue_comment(PostIssueCommentRequest {
                    repo_url: &repo_url,
                    pat,
                    number: candidate.pr_number,
                    body,
                })
                .await
            {
                Ok(id) => id,
                Err(err) => {
                    tracing::warn!(
                        id = %candidate.id,
                        pr = %candidate.pr_number,
                        error = %err,
                        "review poll: failed to post closing top-level reply"
                    );
                    return None;
                }
            }
        }
        Some(FeedbackKind::ReviewComment) => match state
            .git_host
            .reply_review_comment(ReplyReviewCommentRequest {
                repo_url: &repo_url,
                pat,
                number: candidate.pr_number,
                in_reply_to_id: row.github_id,
                body,
            })
            .await
        {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!(
                    id = %candidate.id,
                    pr = %candidate.pr_number,
                    github_id = row.github_id,
                    error = %err,
                    "review poll: failed to post closing inline reply"
                );
                return None;
            }
        },
        None => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                source_kind = %row.source_kind,
                "review poll: skipping closing reply for unrecognized source_kind"
            );
            return None;
        }
    };

    // Resolve the inline row's thread so the resolved-thread guard also skips
    // it next poll. Best-effort: the DB addressed state is authoritative.
    if let Some(thread_id) = row.thread_id.as_deref() {
        if let Err(err) = state
            .git_host
            .resolve_thread(ResolveThreadRequest {
                repo_url: &repo_url,
                pat,
                thread_id,
            })
            .await
        {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                thread_id = thread_id,
                error = %err,
                "review poll: failed to resolve inline thread after addressing"
            );
        }
    }

    Some(reply_id)
}

/// Move an `in_progress` row to `state='addressed'` after its closing reply
/// landed. Fenced on `state='in_progress'` so a race/retry is a no-op rather
/// than re-answering.
async fn mark_addressed(state: &AppState, candidate: &Candidate, row: &InProgressRow) {
    let now = now_ms();
    let res = state.db.conn().execute(
        "UPDATE pr_feedback SET state = 'addressed', updated_at = ?1 \
         WHERE id = ?2 AND state = 'in_progress'",
        params![now, row.id.clone()],
    );
    if let Err(err) = res.await {
        tracing::error!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            row = %row.id,
            error = %err,
            "review poll: failed to mark change row addressed"
        );
    }
}

/// Triage one actionable item and act on the outcome: run the `Triage` agent
/// stage (in the candidate's retained workspace, with the item's text passed
/// as the feedback and the item's epic/task context as background), parse its
/// classification, and handle it. A `QUESTION` posts a reply and records
/// handled state; a `CHANGE` (AC #4) hands work back to the existing pipeline
/// ([`handle_change`]). An unparseable/failed run (retriable next poll) is
/// logged and left unhandled — no state is written, so nothing is skipped.
async fn act_on_item(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
) {
    let outcome = match run_triage(state, candidate, kind, item).await {
        Some(outcome) => outcome,
        None => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                kind = ?item.kind,
                github_id = item.github_id,
                "review poll: triage run failed for item; leaving it unhandled"
            );
            return;
        }
    };
    let text = outcome.text;
    match crate::spec::parse_triage(&text) {
        Some(crate::spec::Triage::Question { reply }) => {
            tracing::info!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                kind = ?item.kind,
                github_id = item.github_id,
                "review poll: triaged as question; posting reply"
            );
            handle_question(state, candidate, kind, item, &reply).await;
        }
        Some(crate::spec::Triage::Change { tasks }) => {
            tracing::info!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                kind = ?item.kind,
                github_id = item.github_id,
                "review poll: triaged as change request; handing work back to the pipeline"
            );
            handle_change(state, candidate, kind, item, &tasks).await;
        }
        None => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                kind = ?item.kind,
                github_id = item.github_id,
                "review poll: triage output failed to parse as a classification"
            );
        }
    }
}

/// Run the `Triage` agent stage once for one actionable item, returning its
/// outcome on the happy path. Resolves the triage slot's live spawn config
/// (T6/T7), assembles the item's prompt (feedback + [`crate::spec::TaskContext`]
/// background), and runs it in the candidate's retained workspace `cwd`. `None`
/// on any non-happy path (config resolution failure, missing epic/task row, a
/// stage run that wasn't cleanly `ok`) — the caller logs and leaves unhandled.
async fn run_triage(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
) -> Option<AgentStageOutcome> {
    let project_id = &candidate.project_id;
    let default = crate::spec::prompt_for(crate::task_agent::Stage::Triage)
        .expect("Stage::Triage always has a prompt");
    let slot = crate::agent_slot::AgentSlot::from_stage(crate::task_agent::Stage::Triage)
        .expect("Stage::Triage maps to an agent slot");
    let cfg = match crate::agent_settings::spawn_config(&state.db, project_id, slot, default).await
    {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!(
                id = %candidate.id,
                error = %err,
                "review poll: could not resolve triage agent settings"
            );
            return None;
        }
    };

    let conn = state.db.conn();
    let (
        title,
        description,
        acceptance,
        epic_id,
        task_id,
        workspace_path,
    ) = match kind {
        WorkKind::Epic => {
            let epic = match fetch_epic(conn, &candidate.id).await {
                Ok(Some(epic)) => epic,
                _ => {
                    tracing::warn!(id = %candidate.id, "review poll: epic row missing for triage");
                    return None;
                }
            };
            (
                epic.title.clone(),
                epic.description.clone(),
                None,
                Some(candidate.id.clone()),
                None,
                epic_workspace_path(&state.config.clone_root, &candidate.id),
            )
        }
        WorkKind::Task => {
            let task = match fetch_task(conn, &candidate.id).await {
                Ok(Some(task)) => task,
                _ => {
                    tracing::warn!(id = %candidate.id, "review poll: task row missing for triage");
                    return None;
                }
            };
            (
                task.title.clone(),
                task.description.clone(),
                task.acceptance.clone(),
                None,
                Some(candidate.id.clone()),
                task_workspace_path(&state.config.clone_root, &candidate.id),
            )
        }
    };

    let _ = std::fs::create_dir_all(&workspace_path);
    let acceptance = acceptance.as_deref();
    let context = crate::spec::TaskContext {
        spec: crate::spec::SpecFields {
            title: &title,
            description: description.as_deref(),
            acceptance,
        },
        epic: match kind {
            WorkKind::Epic => Some(crate::spec::EpicContext {
                title: &title,
                description: description.as_deref(),
            }),
            WorkKind::Task => None,
        },
        siblings: &[],
        base_sha: None,
    };
    let prompt = crate::task_agent::assemble_triage_prompt_text(&cfg.prompt, &item.body, &context);

    let run_id = ulid::Ulid::new().to_string();
    let outcome = crate::task_agent::run_agent_stage(
        state,
        &*state.task_agent,
        AgentStageParams {
            task_id: task_id.as_deref(),
            epic_id: epic_id.as_deref(),
            attempt: 1,
        },
        TaskRunRequest {
            run_id,
            stage: crate::task_agent::Stage::Triage,
            prompt,
            cwd: workspace_path,
            harness: cfg.harness,
            model: cfg.model,
            prompt_hash: cfg.prompt_hash,
        },
    )
    .await;

    match outcome {
        Ok(outcome) if outcome.is_ok() => Some(outcome),
        _ => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                kind = ?item.kind,
                github_id = item.github_id,
                "review poll: triage agent run did not report ok"
            );
            None
        }
    }
}

/// Post the triaged reply for a `QUESTION` item and record the handled state
/// (`handled_reply` for the source item + an `our_post` row for the created
/// reply id), so the next poll skips it. Reply routing: a review's summary
/// body and a top-level issue comment get a top-level issue comment; an inline
/// review comment gets an in-thread reply and its (resolvable) thread resolved.
/// A post or resolve failure is logged and leaves the item unhandled — the
/// next poll will re-triage and retry (and the idempotent state writes are
/// `INSERT OR IGNORE`, so a crash between post and record can't error out).
async fn handle_question(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
    reply: &str,
) {
    let repo_url = match load_repo_url(state, &candidate.project_id).await {
        Ok(url) => url,
        Err(err) => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                error = %err,
                "review poll: could not load repo url to post reply"
            );
            return;
        }
    };
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))
        .ok()
        .flatten();
    let pat = pat.as_deref();

    let reply_id = match item.kind {
        FeedbackKind::Review | FeedbackKind::IssueComment => {
            match state
                .git_host
                .post_issue_comment(PostIssueCommentRequest {
                    repo_url: &repo_url,
                    pat,
                    number: candidate.pr_number,
                    body: reply,
                })
                .await
            {
                Ok(id) => id,
                Err(err) => {
                    tracing::warn!(
                        id = %candidate.id,
                        pr = %candidate.pr_number,
                        error = %err,
                        "review poll: failed to post top-level reply"
                    );
                    return;
                }
            }
        }
        FeedbackKind::ReviewComment => {
            match state
                .git_host
                .reply_review_comment(ReplyReviewCommentRequest {
                    repo_url: &repo_url,
                    pat,
                    number: candidate.pr_number,
                    in_reply_to_id: item.github_id,
                    body: reply,
                })
                .await
            {
                Ok(id) => id,
                Err(err) => {
                    tracing::warn!(
                        id = %candidate.id,
                        pr = %candidate.pr_number,
                        error = %err,
                        "review poll: failed to post inline reply"
                    );
                    return;
                }
            }
        }
    };

    // Resolve the inline thread (when the item has one) so the resolved-thread
    // guard also skips it on the next poll. Best-effort: the DB `handled_reply`
    // row below is the authoritative skip; a resolve failure is logged, not fatal.
    if let Some(thread_id) = item.thread_id.as_deref() {
        if let Err(err) = state
            .git_host
            .resolve_thread(ResolveThreadRequest {
                repo_url: &repo_url,
                pat,
                thread_id,
            })
            .await
        {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                thread_id = thread_id,
                error = %err,
                "review poll: failed to resolve inline thread after replying"
            );
        }
    }

    record_handled_reply(state, candidate, kind, item).await;
    record_our_post(state, candidate, kind, reply_id).await;
}

/// Record the `pr_feedback` `handled_reply` row for a triaged-and-replied
/// source item: identity is DB-tracked (Decision 1), so once this lands the
/// next poll's [`load_known_feedback`] puts `(kind, id)` in `known.handled` and
/// [`compute_actionable`] skips it. `INSERT OR IGNORE` keeps the record
/// idempotent across a crash/retry (the unique index is on
/// `(pr_number, source_kind, github_id)`).
async fn record_handled_reply(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
) {
    let (epic_id, task_id) = provenance(kind, candidate);
    let now = now_ms();
    let id = ulid::Ulid::new().to_string();
    let res = state
        .db
        .conn()
        .execute(
            "INSERT OR IGNORE INTO pr_feedback \
             (id, project_id, epic_id, task_id, pr_number, source_kind, github_id, thread_id, \
              classification, state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'question', 'handled_reply', ?9, ?9)",
            params![
                id,
                candidate.project_id.clone(),
                epic_id,
                task_id,
                candidate.pr_number,
                item.kind.as_source_kind(),
                item.github_id,
                item.thread_id.as_deref(),
                now
            ],
        )
        .await;
    if let Err(err) = res {
        tracing::error!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            error = %err,
            "review poll: failed to record handled_reply"
        );
    }
}

/// Record the `our_post` row for the reply the factory just created (its
/// source_kind `our_post` id is the GitHub id handed back by the posting
/// call), so [`load_known_feedback`] adds it to `our_comment_ids` and the next
/// poll never treats Dearborn's own reply as feedback. Idempotent `INSERT OR
/// IGNORE`; the `epic_id`/`task_id` provenance matches the item's kind.
async fn record_our_post(state: &AppState, candidate: &Candidate, kind: WorkKind, reply_id: i64) {
    let (epic_id, task_id) = provenance(kind, candidate);
    let now = now_ms();
    let id = ulid::Ulid::new().to_string();
    let res = state
        .db
        .conn()
        .execute(
            "INSERT OR IGNORE INTO pr_feedback \
             (id, project_id, epic_id, task_id, pr_number, source_kind, github_id, thread_id, \
              classification, state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'our_post', ?6, NULL, NULL, 'handled_reply', ?7, ?7)",
            params![
                id,
                candidate.project_id.clone(),
                epic_id,
                task_id,
                candidate.pr_number,
                reply_id,
                now
            ],
        )
        .await;
    if let Err(err) = res {
        tracing::error!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            error = %err,
            "review poll: failed to record our_post"
        );
    }
}

/// Handle a triaged `CHANGE` (epic plan §6.5, AC #4): hand the work back to
/// the existing pipeline, routing by the candidate's kind.
///
/// In both cases an interim "Picked up — implementing." reply is posted first
/// (routed exactly like [`handle_question`]'s reply: an in-thread reply for an
/// inline comment, a top-level issue comment for a review body / issue
/// comment), but the thread is **not** resolved — resolution belongs to the
/// closing-the-loop half of the loop (a later task). The reply id is recorded
/// as an `our_post` row so it is never reprocessed as feedback.
///
/// - **Epic**: capture the branch HEAD (`base_sha`), create one linked `Todo`
///   task per triaged spec, record their ids + `base_sha` in `pr_feedback`
///   (`state='in_progress'`), set the epic `InProgress`, publish the DAG +
///   board, and `notify_waiters()` — the worker's DAG walk runs the new tasks
///   and finalize pushes to the same branch/PR, returning the item to
///   `InReview`.
/// - **Standalone** (option C): amend the task's own spec (append the feedback
///   to description/acceptance), record `base_sha` + `state='in_progress'`,
///   set the task `InProgress` + notify (mirrors standalone retry-to-InProgress
///   — the worker pool's `claim_task` picks `InProgress AND epic_id IS NULL`
///   back up). No new tasks; a standalone never becomes an epic.
async fn handle_change(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
    tasks: &[crate::spec::TriageTaskSpec],
) {
    // Post the interim reply before handing work back (best-effort: a post
    // failure is logged and the work still proceeds — the reply is not a
    // precondition for the pipeline, but the AC's "Picked up" trail is missed
    // and logged if it fails).
    let reply_id = post_interim_reply(state, candidate, item).await;
    let base_sha = capture_base_sha(state, candidate, kind).await;

    match kind {
        WorkKind::Epic => {
            spawn_epic_change_tasks(state, candidate, item, tasks, base_sha.as_deref()).await;
        }
        WorkKind::Task => {
            amend_standalone_task(state, candidate, item, base_sha.as_deref()).await;
        }
    }

    if let Some(reply_id) = reply_id {
        record_our_post(state, candidate, kind, reply_id).await;
    }
}

/// Post the interim "Picked up — implementing." reply for a triaged change
/// item (AC #4): an in-thread reply for an inline comment, a top-level issue
/// comment for a review body / top-level comment — the same routing
/// [`handle_question`] uses, but with **no** thread resolution (the thread is
/// resolved only when the addressing reply lands later). Returns the created
/// reply id on success, `None` on any failure (logged — the caller still
/// spawns the work).
async fn post_interim_reply(
    state: &AppState,
    candidate: &Candidate,
    item: &ActionableItem,
) -> Option<i64> {
    let repo_url = match load_repo_url(state, &candidate.project_id).await {
        Ok(url) => url,
        Err(err) => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                error = %err,
                "review poll: could not load repo url to post interim reply"
            );
            return None;
        }
    };
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))
        .ok()
        .flatten();
    let pat = pat.as_deref();
    let reply = "Picked up — implementing.";

    match item.kind {
        FeedbackKind::Review | FeedbackKind::IssueComment => {
            match state
                .git_host
                .post_issue_comment(PostIssueCommentRequest {
                    repo_url: &repo_url,
                    pat,
                    number: candidate.pr_number,
                    body: reply,
                })
                .await
            {
                Ok(id) => Some(id),
                Err(err) => {
                    tracing::warn!(
                        id = %candidate.id,
                        pr = %candidate.pr_number,
                        error = %err,
                        "review poll: failed to post interim top-level reply"
                    );
                    None
                }
            }
        }
        FeedbackKind::ReviewComment => match state
            .git_host
            .reply_review_comment(ReplyReviewCommentRequest {
                repo_url: &repo_url,
                pat,
                number: candidate.pr_number,
                in_reply_to_id: item.github_id,
                body: reply,
            })
            .await
        {
            Ok(id) => Some(id),
            Err(err) => {
                tracing::warn!(
                    id = %candidate.id,
                    pr = %candidate.pr_number,
                    error = %err,
                    "review poll: failed to post interim inline reply"
                );
                None
            }
        },
    }
}

/// Capture the branch `HEAD` SHA the moment change-request work is picked up
/// (`base_sha`, §6.5) — mirroring [`crate::worker`]'s own base-sha capture via
/// `git::current_commit` against the item's retained workspace. `None` when the
/// workspace has no readable HEAD (the `pr_feedback.base_sha` column is
/// nullable; the closing-the-loop half of the loop uses it only to prove the
/// branch advanced past this point, so a missing HEAD degrades to an empty
/// base rather than failing the spawn).
async fn capture_base_sha(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
) -> Option<String> {
    let workspace = match kind {
        WorkKind::Epic => epic_workspace_path(&state.config.clone_root, &candidate.id),
        WorkKind::Task => task_workspace_path(&state.config.clone_root, &candidate.id),
    };
    match crate::git::current_commit(&workspace).await {
        Ok(sha) => Some(sha),
        Err(err) => {
            tracing::warn!(
                id = %candidate.id,
                pr = %candidate.pr_number,
                error = %err,
                "review poll: could not read workspace HEAD (base_sha stays null)"
            );
            None
        }
    }
}

/// The epic arm of [`handle_change`]: create one linked `Todo` task per
/// triaged change spec, record the source item's `in_progress` row with the
/// spawned task ids + `base_sha`, flip the epic to `InProgress`, publish the
/// DAG + board, and `notify_waiters()` so the worker pool's DAG walk picks the
/// new tasks up immediately.
async fn spawn_epic_change_tasks(
    state: &AppState,
    candidate: &Candidate,
    item: &ActionableItem,
    tasks: &[crate::spec::TriageTaskSpec],
    base_sha: Option<&str>,
) {
    if tasks.is_empty() {
        return;
    }
    let conn = state.db.conn();
    let mut spawned: Vec<String> = Vec::new();
    for spec in tasks {
        match crate::tasks::create_task(
            conn,
            &candidate.id,
            &candidate.project_id,
            &spec.title,
            Some(&spec.spec),
            None,
        )
        .await
        {
            Ok(task) => {
                tracing::info!(
                    task = %task.id,
                    epic = %candidate.id,
                    "review poll: created change task"
                );
                spawned.push(task.id);
            }
            Err(err) => {
                tracing::error!(
                    id = %candidate.id,
                    pr = %candidate.pr_number,
                    error = %err,
                    "review poll: failed to create change task"
                );
            }
        }
    }

    if spawned.is_empty() {
        tracing::warn!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            "review poll: no change tasks created; leaving epic InReview"
        );
        return;
    }

    record_change_in_progress(
        state,
        candidate,
        WorkKind::Epic,
        item,
        base_sha,
        Some(&spawned),
    )
    .await;

    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE epic SET status = 'InProgress', updated_at = ?1 \
             WHERE id = ?2 AND status = 'InReview'",
            params![now, candidate.id.clone()],
        )
        .await;
    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                epic = %candidate.id,
                spawned = %spawned.len(),
                "review poll: epic moved to InProgress for change work"
            );
            crate::capability::publish_dag(state, &candidate.id).await;
            crate::board::publish_board(state, &candidate.project_id).await;
            state.notify.notify_waiters();
        }
        Ok(_) => {
            tracing::debug!(
                epic = %candidate.id,
                "review poll: epic no longer InReview (something else moved it); spawned tasks stay Todo"
            );
        }
        Err(err) => {
            tracing::error!(
                epic = %candidate.id,
                error = %err,
                "review poll: failed to move epic to InProgress"
            );
        }
    }
}

/// The standalone (option C) arm of [`handle_change`]: amend the task's own
/// spec by appending the feedback to its description and acceptance, record
/// `base_sha` + `state='in_progress'`, flip the task to `InProgress`, publish
/// the board, and `notify_waiters()` (mirrors standalone retry-to-InProgress;
/// no new tasks are created).
async fn amend_standalone_task(
    state: &AppState,
    candidate: &Candidate,
    item: &ActionableItem,
    base_sha: Option<&str>,
) {
    let conn = state.db.conn();
    let task = match fetch_task(conn, &candidate.id).await {
        Ok(Some(task)) => task,
        _ => {
            tracing::warn!(
                id = %candidate.id,
                "review poll: standalone task row missing for change"
            );
            return;
        }
    };

    // Append the raw feedback to the task's own spec so the re-run sees it.
    let feedback_block = format!("\n\n## Review feedback\n{}", item.body);
    let new_description = task
        .description
        .as_deref()
        .map(|d| format!("{d}{feedback_block}"))
        .unwrap_or_else(|| item.body.clone());
    let new_acceptance = task
        .acceptance
        .as_deref()
        .map(|a| format!("{a}{feedback_block}"))
        .unwrap_or_else(|| item.body.clone());

    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE task SET status = 'InProgress', description = ?1, acceptance = ?2, updated_at = ?3 \
             WHERE id = ?4 AND epic_id IS NULL AND status = 'InReview'",
            params![new_description, new_acceptance, now, candidate.id.clone()],
        )
        .await;
    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                task = %candidate.id,
                "review poll: standalone task moved to InProgress for change work"
            );
            record_change_in_progress(state, candidate, WorkKind::Task, item, base_sha, None).await;
            crate::board::publish_board(state, &candidate.project_id).await;
            state.notify.notify_waiters();
        }
        Ok(_) => {
            tracing::debug!(
                task = %candidate.id,
                "review poll: standalone task no longer InReview (something else moved it); leaving as-is"
            );
        }
        Err(err) => {
            tracing::error!(
                task = %candidate.id,
                error = %err,
                "review poll: failed to move standalone task to InProgress"
            );
        }
    }
}

/// Record the `pr_feedback` `in_progress` row for a triaged change item: the
/// source item's identity (`source_kind`/`github_id`/`thread_id`), its
/// `classification='change_request'`, the spawned task-id JSON array (an epic
/// change; `None` for a standalone), and the `base_sha` picked up. `INSERT OR
/// IGNORE` keeps the record idempotent across a crash/retry (the unique index
/// is on `(pr_number, source_kind, github_id)`), and once the item leaves
/// `InReview` it is no longer a poll candidate until finalize returns it — the
/// closing-the-loop task reads this row's `base_sha`/`spawned_task_ids` to post
/// the "Addressed in <commit>" reply and flip it to `addressed`.
async fn record_change_in_progress(
    state: &AppState,
    candidate: &Candidate,
    kind: WorkKind,
    item: &ActionableItem,
    base_sha: Option<&str>,
    spawned: Option<&[String]>,
) {
    let (epic_id, task_id) = provenance(kind, candidate);
    let now = now_ms();
    let id = ulid::Ulid::new().to_string();
    let spawned_json = spawned
        .map(serde_json::to_string)
        .transpose()
        .ok()
        .flatten();
    let (kind_str, github_id, thread_id) = (
        item.kind.as_source_kind().to_string(),
        item.github_id,
        item.thread_id.clone(),
    );
    let res = state
        .db
        .conn()
        .execute(
            "INSERT OR IGNORE INTO pr_feedback \
             (id, project_id, epic_id, task_id, pr_number, source_kind, github_id, thread_id, \
              classification, state, spawned_task_ids, base_sha, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'change_request', 'in_progress', ?9, ?10, ?11, ?11)",
            params![
                id,
                candidate.project_id.clone(),
                epic_id,
                task_id,
                candidate.pr_number,
                kind_str,
                github_id,
                thread_id,
                spawned_json,
                base_sha,
                now
            ],
        )
        .await;
    if let Err(err) = res {
        tracing::error!(
            id = %candidate.id,
            pr = %candidate.pr_number,
            error = %err,
            "review poll: failed to record change in_progress"
        );
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
            handle_open_feedback(state, candidate, WorkKind::Epic, &pull.head_sha).await;
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
            handle_open_feedback(state, candidate, WorkKind::Task, &pull.head_sha).await;
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
    use crate::task_agent::testing::{ScriptedRun, ScriptedTaskAgent};
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

    /// Like [`make_env_with_host`] but with an explicitly scripted task agent
    /// (the triage tests need to pin the triage agent's reply).
    async fn make_env_with_host_and_agent(
        host: Arc<dyn GitHost>,
        clone_root: PathBuf,
        agent: Arc<ScriptedTaskAgent>,
    ) -> AppState {
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
            agent,
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

    // ---- triage + question reply (this task: act on actionable items) ----

    /// The (source_kind, github_id, state) triples recorded for one PR in
    /// `pr_feedback`.
    async fn feedback_rows(state: &AppState, pr_number: i64) -> Vec<(String, i64, String)> {
        let mut out = Vec::new();
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT source_kind, github_id, state FROM pr_feedback WHERE pr_number = ?1",
                params![pr_number],
            )
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            ));
        }
        out
    }

    /// A triage agent scripted to answer a question.
    fn triage_question(reply: &str) -> ScriptedRun {
        ScriptedRun {
            text: vec![format!("TRIAGE: QUESTION\n{reply}")],
            ..ScriptedRun::default()
        }
    }

    /// A triage agent scripted to request a change (one or more `## Task:`
    /// specs following the `TRIAGE: CHANGE` line).
    fn triage_change(tasks: &str) -> ScriptedRun {
        ScriptedRun {
            text: vec![format!("TRIAGE: CHANGE\n{tasks}")],
            ..ScriptedRun::default()
        }
    }

    #[tokio::test]
    async fn triaged_top_level_question_posts_reply_and_marks_handled() {
        // AC: a `dearborn:` top-level issue comment classified as a question
        // gets a posted top-level reply, a handled_reply row, an our_post row,
        // and is not reprocessed on the next poll.
        let fake = Arc::new(
            FakeHost::new()
                .with_pull_state(open())
                .with_issue_comments(vec![git_host::IssueComment {
                    id: 300,
                    body: "dearborn: what does this function do?".into(),
                }]),
        );
        let agent = ScriptedTaskAgent::new().script(
            crate::task_agent::Stage::Triage,
            triage_question("Great question — this covers the empty case too."),
        );
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-q-{}", ulid::Ulid::new()));
        let state = make_env_with_host_and_agent(
            Arc::clone(&fake) as Arc<dyn GitHost>,
            clone_root,
            Arc::new(agent),
        )
        .await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&state, &project, 7, "question loop").await;

        review_tick(&state).await;

        // A top-level question item gets a top-level issue-comment reply.
        let posts = fake.post_issue_comment_calls();
        assert_eq!(posts.len(), 1);
        assert_eq!(
            posts[0].body,
            "Great question — this covers the empty case too."
        );
        assert!(fake.reply_review_comment_calls().is_empty());

        // handled_reply for the source item + our_post for the created reply id.
        let rows = feedback_rows(&state, 7).await;
        assert!(rows.contains(&(
            "issue_comment".to_string(),
            300,
            "handled_reply".to_string()
        )));
        assert!(rows.contains(&("our_post".to_string(), 10_001, "handled_reply".to_string())));

        // The second poll must not reprocess the handled item (DB skip).
        review_tick(&state).await;
        assert_eq!(
            fake.post_issue_comment_calls().len(),
            1,
            "a handled question must not be replied to twice"
        );
        let _ = epic;
    }

    #[tokio::test]
    async fn triaged_inline_question_replies_in_thread_resolves_and_marks_handled() {
        // AC (inline routing): a `dearborn:` inline review comment classified
        // as a question gets an in-thread reply, its thread resolved, a
        // handled_reply row, an our_post row, and is not reprocessed on the
        // next poll (DB + resolved-thread guards both skip it).
        let fake = Arc::new(
            FakeHost::new()
                .with_pull_state(open())
                .with_review_comments(vec![git_host::InlineComment {
                    id: 200,
                    body: "dearborn: how is this populated?".into(),
                    in_reply_to: None,
                    pull_request_review_id: None,
                    path: None,
                    line: None,
                }])
                .with_threads(vec![git_host::Thread {
                    id: "thr-1".into(),
                    is_resolved: false,
                    root_comment_id: Some("200".into()),
                }]),
        );
        let agent = ScriptedTaskAgent::new().script(
            crate::task_agent::Stage::Triage,
            triage_question("It's populated lazily on first access."),
        );
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-inline-{}", ulid::Ulid::new()));
        let state = make_env_with_host_and_agent(
            Arc::clone(&fake) as Arc<dyn GitHost>,
            clone_root,
            Arc::new(agent),
        )
        .await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&state, &project, 7, "inline q").await;

        review_tick(&state).await;

        let replies = fake.reply_review_comment_calls();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].in_reply_to_id, 200);
        assert_eq!(replies[0].body, "It's populated lazily on first access.");
        // Inline items reply in-thread and resolve the thread.
        assert_eq!(fake.resolve_thread_calls(), vec!["thr-1"]);

        let rows = feedback_rows(&state, 7).await;
        assert!(rows
            .iter()
            .any(|(k, id, s)| k == "review_comment" && *id == 200 && s == "handled_reply"));
        assert!(rows
            .iter()
            .any(|(k, id, s)| k == "our_post" && *id == 10_001 && s == "handled_reply"));

        // Second poll: handled in the DB and the thread resolved on the host —
        // no duplicate reply.
        review_tick(&state).await;
        assert_eq!(
            fake.reply_review_comment_calls().len(),
            1,
            "a handled inline question must not be replied to twice"
        );
        let _ = epic;
    }

    // ---- triaged change requests: spawn work, return to InProgress ----

    /// The task rows created under `epic_id` (`title`, `status`).
    async fn epic_tasks(state: &AppState, epic_id: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT title, status FROM task WHERE epic_id = ?1 ORDER BY position",
                params![epic_id],
            )
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((row.get(0).unwrap(), row.get(1).unwrap()));
        }
        out
    }

    /// The full `in_progress` feedback row for a given source github id on a
    /// PR: (`source_kind`, `github_id`, `classification`, `state`,
    /// `spawned_task_ids`, `base_sha`).
    async fn in_progress_row(
        state: &AppState,
        pr_number: i64,
        github_id: i64,
    ) -> (
        String,
        i64,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
    ) {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT source_kind, github_id, classification, state, spawned_task_ids, base_sha \
                 FROM pr_feedback WHERE pr_number = ?1 AND github_id = ?2 AND state = 'in_progress'",
                params![pr_number, github_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (
            row.get(0).unwrap(),
            row.get(1).unwrap(),
            row.get(2).unwrap(),
            row.get(3).unwrap(),
            row.get(4).unwrap(),
            row.get(5).unwrap(),
        )
    }

    #[tokio::test]
    async fn epic_change_request_spawns_tasks_in_progress_and_picks_up() {
        // AC #4 (epic): a `dearborn:` top-level comment triaged as a change
        // request creates the linked Todo task(s), records base_sha + spawned
        // ids + in_progress, flips the epic to InProgress, and notifies the
        // pool — plus a non-resolving "Picked up" reply.
        let fake = Arc::new(
            FakeHost::new()
                .with_pull_state(open())
                .with_issue_comments(vec![git_host::IssueComment {
                    id: 300,
                    body: "dearborn: please handle the empty branch".into(),
                }]),
        );
        let agent = ScriptedTaskAgent::new().script(
            crate::task_agent::Stage::Triage,
            triage_change("## Task: Handle the empty branch\nGuard against an empty input early."),
        );
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-change-{}", ulid::Ulid::new()));
        let state = make_env_with_host_and_agent(
            Arc::clone(&fake) as Arc<dyn GitHost>,
            clone_root,
            Arc::new(agent),
        )
        .await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&state, &project, 7, "change loop").await;

        review_tick(&state).await;

        // The epic is flipped to InProgress (the worker pool owns it now).
        assert_eq!(epic_status(&state, &epic).await, "InProgress");

        // The triaged spec produced one linked Todo task under the epic.
        let tasks = epic_tasks(&state, &epic).await;
        assert_eq!(
            tasks,
            vec![("Handle the empty branch".to_string(), "Todo".to_string())]
        );

        // An interim "Picked up" reply is posted; the thread is not resolved.
        let posts = fake.post_issue_comment_calls();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].body, "Picked up — implementing.");
        assert!(fake.resolve_thread_calls().is_empty());

        // The in_progress feedback row records base_sha + spawned ids, and the
        // posted reply is recorded as an our_post.
        let (kind, gid, class, state_, spawned, base) = in_progress_row(&state, 7, 300).await;
        assert_eq!(
            (kind.as_str(), gid, class.as_deref(), state_.as_str()),
            ("issue_comment", 300, Some("change_request"), "in_progress")
        );
        let spawned: Vec<String> =
            serde_json::from_str(&spawned.expect("epic change records spawned ids")).unwrap();
        assert_eq!(spawned.len(), 1);
        assert_eq!(tasks[0].0, "Handle the empty branch");
        // base_sha is null here because the test seeds no real git workspace.
        assert!(base.is_none());
        let rows = feedback_rows(&state, 7).await;
        assert!(rows.contains(&("our_post".to_string(), 10_001, "handled_reply".to_string())));
    }

    #[tokio::test]
    async fn epic_inline_change_replies_in_thread_without_resolving() {
        // AC #4 (inline routing): an inline change-request comment gets an
        // in-thread "Picked up" reply but the thread is left unresolved, and
        // work is spawned / the epic flips to InProgress regardless.
        let fake = Arc::new(
            FakeHost::new()
                .with_pull_state(open())
                .with_review_comments(vec![git_host::InlineComment {
                    id: 200,
                    body: "dearborn: refactor this helper".into(),
                    in_reply_to: None,
                    pull_request_review_id: None,
                    path: None,
                    line: None,
                }])
                .with_threads(vec![git_host::Thread {
                    id: "thr-9".into(),
                    is_resolved: false,
                    root_comment_id: Some("200".into()),
                }]),
        );
        let agent = ScriptedTaskAgent::new().script(
            crate::task_agent::Stage::Triage,
            triage_change("## Task: Refactor helper\nExtract the duplicated logic."),
        );
        let clone_root = std::env::temp_dir().join(format!(
            "dearborn-review-inline-change-{}",
            ulid::Ulid::new()
        ));
        let state = make_env_with_host_and_agent(
            Arc::clone(&fake) as Arc<dyn GitHost>,
            clone_root,
            Arc::new(agent),
        )
        .await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&state, &project, 8, "inline change").await;

        review_tick(&state).await;

        // In-thread "Picked up" reply, no resolution.
        let replies = fake.reply_review_comment_calls();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].in_reply_to_id, 200);
        assert_eq!(replies[0].body, "Picked up — implementing.");
        assert!(
            fake.resolve_thread_calls().is_empty(),
            "thread must stay unresolved"
        );

        assert_eq!(epic_status(&state, &epic).await, "InProgress");
        assert_eq!(epic_tasks(&state, &epic).await.len(), 1);
    }

    #[tokio::test]
    async fn standalone_change_request_amends_spec_in_progress_no_new_tasks() {
        // AC #4 (standalone, option C): a change request amends the task's own
        // spec (appends feedback), flips the task to InProgress, notifies, and
        // creates no new tasks — with a non-resolving "Picked up" reply.
        let fake = Arc::new(
            FakeHost::new()
                .with_pull_state(open())
                .with_issue_comments(vec![git_host::IssueComment {
                    id: 300,
                    body: "dearborn: add input validation".into(),
                }]),
        );
        let agent = ScriptedTaskAgent::new().script(
            crate::task_agent::Stage::Triage,
            triage_change("## Task: Add validation\nReject invalid inputs before processing."),
        );
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-task-change-{}", ulid::Ulid::new()));
        let state = make_env_with_host_and_agent(
            Arc::clone(&fake) as Arc<dyn GitHost>,
            clone_root,
            Arc::new(agent),
        )
        .await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&state, &project, 9, "parse").await;
        // Seed a description so the amendment appends to it.
        state
            .db
            .conn()
            .execute(
                "UPDATE task SET description = 'Parse the payload.', acceptance = 'Parses ok.' WHERE id = ?1",
                params![task.clone()],
            )
            .await
            .unwrap();

        review_tick(&state).await;

        // The standalone task flips to InProgress (worker pool claims it next).
        assert_eq!(task_status(&state, &task).await, "InProgress");

        // Its spec was amended with the feedback.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT description, acceptance FROM task WHERE id = ?1",
                params![task.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let desc: String = row.get(0).unwrap();
        let acc: String = row.get(1).unwrap();
        assert!(desc.contains("Parse the payload."));
        assert!(desc.contains("Review feedback"));
        assert!(desc.contains("dearborn: add input validation"));
        assert!(acc.contains("Review feedback"));

        // No new task was created (an Option-C standalone never becomes an epic).
        let mut rows = state
            .db
            .conn()
            .query("SELECT COUNT(*) FROM task WHERE epic_id IS NOT NULL", ())
            .await
            .unwrap();
        let epic_owned: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(epic_owned, 0);

        // Interim reply + in_progress row (no spawned ids) + our_post.
        let posts = fake.post_issue_comment_calls();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].body, "Picked up — implementing.");
        let (kind, gid, class, state_, spawned, _base) = in_progress_row(&state, 9, 300).await;
        assert_eq!(
            (kind.as_str(), gid, class.as_deref(), state_.as_str()),
            ("issue_comment", 300, Some("change_request"), "in_progress")
        );
        assert!(spawned.is_none(), "a standalone change spawns no tasks");
    }

    // ---- close the loop: "Addressed in <commit>" after work lands (AC #5) --

    /// Seed an epic-owned task already in `Done` (the completed change work
    /// whose `pr_feedback` row we then close the loop on). Returns its id.
    async fn seed_done_task(state: &AppState, project_id: &str, epic_id: &str) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'change task', 'Done', ?4, ?4)",
                params![id.clone(), epic_id, project_id, now],
            )
            .await
            .unwrap();
        id
    }

    /// Seed a `state='in_progress'` change-request row against an already-open
    /// PR, as `handle_change`'s `record_change_in_progress` would have left it.
    #[allow(clippy::too_many_arguments)]
    async fn seed_in_progress_row(
        state: &AppState,
        pr_number: i64,
        project_id: &str,
        epic_id: Option<&str>,
        task_id: Option<&str>,
        source_kind: &str,
        github_id: i64,
        thread_id: Option<&str>,
        spawned: Option<&str>,
        base_sha: Option<&str>,
    ) {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO pr_feedback \
                 (id, project_id, epic_id, task_id, pr_number, source_kind, github_id, thread_id, \
                  classification, state, spawned_task_ids, base_sha, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'change_request', 'in_progress', ?9, ?10, ?11, ?11)",
                params![
                    id,
                    project_id,
                    epic_id,
                    task_id,
                    pr_number,
                    source_kind,
                    github_id,
                    thread_id,
                    spawned,
                    base_sha,
                    now
                ],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn addressed_epic_change_posts_issue_reply_and_marks_addressed() {
        // AC #5 (top-level): after the spawned change task lands, a `dearborn:`
        // issue-comment change is answered with an "Addressed in <commit>"
        // issue-comment reply, its row moves to `addressed`, and an `our_post`
        // is recorded — and it is not reprocessed on the next poll.
        let env = make_env_with(FakeHost::new().with_pull_state(PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "new-head-abc".to_string(),
        }))
        .await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 7, "addressed loop").await;
        let task_id = seed_done_task(&env.state, &project, &epic).await;
        seed_in_progress_row(
            &env.state,
            7,
            &project,
            Some(&epic),
            None,
            "issue_comment",
            300,
            None,
            Some(&format!("[\"{task_id}\"]")),
            Some("old-base"),
        )
        .await;

        review_tick(&env.state).await;

        // The addressed reply is a top-level issue comment carrying the post-work
        // branch HEAD as <commit>, and the change row moves to `addressed`.
        let posts = env.fake.post_issue_comment_calls();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].body, "Addressed in new-head-abc");
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());

        let rows = feedback_rows(&env.state, 7).await;
        assert!(rows.contains(&("issue_comment".to_string(), 300, "addressed".to_string())));
        // The created reply id is recorded as our_post.
        assert!(rows.contains(&("our_post".to_string(), 10_001, "handled_reply".to_string())));

        // The addressed row's spawned task stays Done; the epic stays InReview.
        assert_eq!(epic_status(&env.state, &epic).await, "InReview");

        // Second poll: the addressed item is skipped (DB handled set) — no
        // duplicate "Addressed in" reply.
        review_tick(&env.state).await;
        assert_eq!(
            env.fake.post_issue_comment_calls().len(),
            1,
            "an addressed change must not be re-answered"
        );
    }

    #[tokio::test]
    async fn addressed_inline_change_replies_in_thread_and_resolves() {
        // AC #5 (inline): an inline change whose work landed gets an in-thread
        // "Addressed in <commit>" reply, its thread resolved, its row moved to
        // `addressed`, and an our_post recorded.
        let env = make_env_with(FakeHost::new().with_pull_state(PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "new-head-inline".to_string(),
        }))
        .await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 8, "inline addressed").await;
        let task_id = seed_done_task(&env.state, &project, &epic).await;
        seed_in_progress_row(
            &env.state,
            8,
            &project,
            Some(&epic),
            None,
            "review_comment",
            200,
            Some("thr-1"),
            Some(&format!("[\"{task_id}\"]")),
            Some("old-base"),
        )
        .await;

        review_tick(&env.state).await;

        let replies = env.fake.reply_review_comment_calls();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].in_reply_to_id, 200);
        assert_eq!(replies[0].body, "Addressed in new-head-inline");
        assert_eq!(env.fake.resolve_thread_calls(), vec!["thr-1"]);
        assert!(env.fake.post_issue_comment_calls().is_empty());

        let rows = feedback_rows(&env.state, 8).await;
        assert!(rows
            .iter()
            .any(|(k, id, s)| k == "review_comment" && *id == 200 && s == "addressed"));
        assert!(rows
            .iter()
            .any(|(k, id, s)| k == "our_post" && *id == 10_001 && s == "handled_reply"));
    }

    #[tokio::test]
    async fn addressed_standalone_change_posts_reply_and_marks_addressed() {
        // AC #5 (standalone, option C): a standalone change spawned no tasks;
        // completion is its own task coming back to `InReview`. Once its branch
        // advances, the closing reply is posted and the row moves to `addressed`.
        let env = make_env_with(FakeHost::new().with_pull_state(PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "new-head-task".to_string(),
        }))
        .await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&env.state, &project, 9, "standalone addressed").await;
        seed_in_progress_row(
            &env.state,
            9,
            &project,
            None,
            Some(&task),
            "issue_comment",
            300,
            None,
            None,
            Some("old-base"),
        )
        .await;

        review_tick(&env.state).await;

        let posts = env.fake.post_issue_comment_calls();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].body, "Addressed in new-head-task");
        let rows = feedback_rows(&env.state, 9).await;
        assert!(rows.contains(&("issue_comment".to_string(), 300, "addressed".to_string())));
    }

    #[tokio::test]
    async fn not_advanced_row_stays_in_progress() {
        // AC: rows whose branch has NOT advanced past base_sha are left
        // in_progress — no closing reply is posted.
        let env = make_env_with(FakeHost::new().with_pull_state(PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "same-head".to_string(),
        }))
        .await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 10, "not advanced").await;
        let task_id = seed_done_task(&env.state, &project, &epic).await;
        seed_in_progress_row(
            &env.state,
            10,
            &project,
            Some(&epic),
            None,
            "issue_comment",
            300,
            None,
            Some(&format!("[\"{task_id}\"]")),
            Some("same-head"),
        )
        .await;

        review_tick(&env.state).await;

        assert!(env.fake.post_issue_comment_calls().is_empty());
        let rows = feedback_rows(&env.state, 10).await;
        assert!(rows.contains(&("issue_comment".to_string(), 300, "in_progress".to_string())));
    }

    #[tokio::test]
    async fn incomplete_spawned_work_stays_in_progress() {
        // AC: rows whose spawned work is NOT complete (a spawned task still
        // active) are left in_progress even if the head advanced.
        let env = make_env_with(FakeHost::new().with_pull_state(PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "new-head".to_string(),
        }))
        .await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 11, "incomplete work").await;
        // Seed the spawned task still in-progress (not Done) so completion fails.
        let task_id = ulid::Ulid::new().to_string();
        let now = now_ms();
        env.state
            .db
            .conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'active task', 'InProgress', ?4, ?4)",
                params![task_id.clone(), epic.clone(), project.clone(), now],
            )
            .await
            .unwrap();
        seed_in_progress_row(
            &env.state,
            11,
            &project,
            Some(&epic),
            None,
            "issue_comment",
            300,
            None,
            Some(&format!("[\"{task_id}\"]")),
            Some("old-base"),
        )
        .await;

        review_tick(&env.state).await;

        assert!(env.fake.post_issue_comment_calls().is_empty());
        let rows = feedback_rows(&env.state, 11).await;
        assert!(rows.contains(&("issue_comment".to_string(), 300, "in_progress".to_string())));
    }
}
