//! Epic workspace provisioning & re-attach (T-511, Milestone 2 §4, D3, §2.8).
//!
//! An **epic workspace** is where the (still-stub, T-513+ real) task DAG walk
//! actually does its work: a checked-out git branch on disk that the pipeline
//! commits to and, eventually (T-514), pushes and opens a PR from.
//!
//! ## Why a local clone off the canonical checkout, not a git worktree (D3)
//!
//! Every project already has a canonical read-only checkout at
//! `<clone_root>/<project id>` (T-103), kept in sync with origin. A `git
//! worktree` sharing that checkout's object store would be the more
//! "obvious" way to give an epic its own working directory cheaply — but
//! worktrees share the parent repo's `.git` and its refs/lock files, so two
//! epics' worktrees (or a worktree and the canonical checkout's own refresh)
//! can collide on git's internal locks under concurrent access, exactly the
//! kind of interaction the per-project refresh lock below exists to avoid in
//! the first place. A `git clone` of the canonical checkout (still local,
//! still fast — no network — since both source and destination are on the
//! same disk) gives the epic workspace its **own** `.git`: no shared locks,
//! no risk of one epic's branch work corrupting another's or the canonical
//! mirror's. The cost is a one-time local copy of the object store per epic,
//! which v1 accepts (§12 explicitly defers true worktree parallelism).
//!
//! ## Why the token is never persisted
//!
//! The workspace's `origin` is repointed at the *real* remote (`git remote
//! set-url origin <repo_url>`, plain, no credentials) immediately after the
//! local clone, so T-514 has somewhere to push. No PAT is embedded in that
//! URL and none is ever written to the workspace's `.git/config` — exactly
//! the discipline [`crate::git`] already uses for the canonical checkout
//! (see its module doc): the token is injected transiently at the moment a
//! network operation needs it (`-c remote.origin.url=<auth>`, process-scoped)
//! and never touches disk.
//!
//! ## Why re-attach, not re-clone, on a re-claim
//!
//! A workspace **persists across re-claims** (a worker restart, a lease
//! theft-then-reclaim, or T-541's retry) — deleting and re-cloning it would
//! throw away any partial progress the *previous* claim already committed to
//! the branch. So [`provision_epic_workspace`] checks first: if the
//! workspace directory already exists, has its own `.git`, and is checked out
//! on the branch this epic already committed to (`epic.branch_name`, read
//! back rather than recomputed — see below), it is **re-attached**: `git
//! reset --hard HEAD` + `git clean -fd` drop only the uncommitted mess a
//! previous attempt left behind (a half-written file, a stray build
//! artifact), while every real commit on the branch survives. Re-cloning
//! every time would also multiply the (already nontrivial) cost of copying a
//! full object store per claim for no benefit.
//!
//! `epic.branch_name` is read back (not recomputed from the current title)
//! specifically so a mid-epic title edit (`PATCH /epics/:id`) can never
//! desync the workspace's actual branch from what a re-claim expects to find
//! — the branch name is decided once, at first provision, and is thereafter
//! just data.
//!
//! ## `setup_cmd` re-runs on re-attach
//!
//! Deliberately **yes**: `setup_cmd` is documented (MILESTONE_1 §5) as
//! idempotent by contract (installing dependencies, priming a cache — safe to
//! run twice), and re-running it is cheap next to the alternative of somehow
//! detecting "has setup already run in this workspace" durably across a
//! restart. Skipping it on re-attach would also leave a re-claimed workspace
//! unprotected against drift (a dependency lockfile changed since the first
//! provision, say) that a fresh `setup_cmd` run would catch.
//!
//! ## `test_cmd` is carried through, not run here
//!
//! [`ProvisionedWorkspace::test_cmd`] is populated from the same project row
//! this module already loads to find `setup_cmd` — but this module never
//! *runs* it. T-521's preflight gate (`worker.rs`, immediately after
//! provisioning returns) is what actually invokes `test_cmd` against the
//! now-`setup_cmd`'d, untouched tree; carrying the value through here just
//! saves that caller a second project query for a field this module already
//! has in hand.
//!
//! ## The per-project refresh lock (§11 risk 3)
//!
//! Every epic provision refreshes the *same* project's canonical checkout
//! (`git::refresh_repo`, a `fetch` + `reset --hard origin/HEAD`). Two workers
//! provisioning epics in the same project concurrently could otherwise
//! interleave one's `reset --hard` with the other's in-flight `fetch`,
//! corrupting the shared mirror both epics clone from. [`AppState::refresh_locks`]
//! hands out one `tokio::sync::Mutex` per project id, held only across the
//! `refresh_repo` call — the epic-specific clone/checkout/setup steps that
//! follow touch only that epic's own workspace directory and never need to
//! serialize with anything. Provisions in *different* projects get different
//! locks and never block each other.
//!
//! ## Standalone-task workspaces (T-551, D17)
//!
//! Everything above this note was written when an "epic workspace" was the
//! only kind of workspace there could ever be. D17's standalone tasks need
//! the identical sequence — refresh canonical, clone-or-reattach, run
//! `setup_cmd`, persist a branch name once — just against
//! `<clone_root>/tasks/<task id>` and `task.branch_name` instead of the epic
//! table (§2.8's `dearborn/task-<slug>-<id>` branch shape). Rather than a
//! second, hand-copied `provision_task_workspace` function, [`WorkspaceContainer`]
//! (`Epic` | `Task`) names the one axis the two cases differ on — which table
//! a handful of small reads/writes hit — and [`provision_workspace`] is the
//! single body both [`provision_epic_workspace`] and [`provision_task_workspace`]
//! call into with their own container, path, and branch-name-format function.
//! `provision_epic_workspace` itself keeps its exact pre-T-551 signature and
//! behavior, so every T-511+ test calling it directly still compiles and
//! passes unchanged — the refactor moved what's *inside* it, not its shape.

use std::path::{Path, PathBuf};
use std::time::Duration;

use libsql::{params, Connection};

use crate::cmd::{self, StageCommand, StageOutcome};
use crate::git;
use crate::projects::load_decrypted_pat;
use crate::AppState;

/// Design-doc §5's base-branch resolution chain, minus its terminal: an
/// explicit **epic**-level branch wins (set at creation, immutable), else the
/// **project** default, else `None` — which every caller renders as "the
/// remote's own HEAD" (`origin/HEAD` for resets, [`git::origin_default_branch`]
/// for PR targets). Pure so the chain is unit-tested directly rather than only
/// through full provisioning runs.
fn resolve_base_branch<'a>(
    epic_base: Option<&'a str>,
    project_base: Option<&'a str>,
) -> Option<&'a str> {
    epic_base.or(project_base)
}

/// §2.8: the epic workspace path — `<clone_root>/epics/<epic id>`. The
/// canonical checkout stays `<clone_root>/<project id>` (T-103, unchanged).
pub fn epic_workspace_path(clone_root: &str, epic_id: &str) -> PathBuf {
    Path::new(clone_root).join("epics").join(epic_id)
}

/// §2.8: the standalone-task workspace path — `<clone_root>/tasks/<task
/// id>` (T-551) — the task-table mirror of [`epic_workspace_path`].
pub fn task_workspace_path(clone_root: &str, task_id: &str) -> PathBuf {
    Path::new(clone_root).join("tasks").join(task_id)
}

/// Cap on a slugged title's length (§2.8), applied *after* trimming stray
/// leading/trailing hyphens so the cap never itself produces a trailing one.
/// 48 keeps `dearborn/<slug>-<6 chars>` comfortably under git's practical ref
/// length limits even for a maximally long title, while still leaving a
/// title recognizable in a branch name.
const SLUG_MAX_LEN: usize = 48;

/// §2.8's `slug(...)`: lowercase ASCII alphanumerics kept as-is; every other
/// character (punctuation, whitespace, non-ASCII letters/digits) collapses a
/// run into a single `-`; leading/trailing `-` trimmed; capped at
/// [`SLUG_MAX_LEN`]. An empty or all-punctuation title (or one that slugs to
/// nothing, e.g. all emoji) falls back to `"epic"` — a branch name needs some
/// non-empty component before the id suffix, and a bare `-<id>` or empty
/// segment reads as broken rather than intentional.
///
/// Non-ASCII alphanumerics (e.g. `é`, `日`) are dropped rather than
/// transliterated or collapsed to a hyphen: a git ref name is safest kept
/// pure ASCII, and dropping keeps `"café"` reading as `"caf"` instead of the
/// noisier `"caf-"`.
pub fn slug(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut last_was_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if ch.is_alphanumeric() {
            // Non-ASCII letter/digit: drop silently (see doc comment above).
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    let capped = if trimmed.len() > SLUG_MAX_LEN {
        // Safe to byte-slice: everything surviving to `trimmed` is ASCII
        // (non-ASCII chars were dropped above), so every byte index is a
        // char boundary.
        trimmed[..SLUG_MAX_LEN].trim_end_matches('-')
    } else {
        trimmed
    };
    if capped.is_empty() {
        "epic".to_string()
    } else {
        capped.to_string()
    }
}

/// §2.8's epic branch name: `dearborn/<slug(epic.title)>-<last 6 of epic id,
/// lowercased>`. Computed once, at first provision, then persisted
/// (`epic.branch_name`) and read back thereafter — see the module doc's
/// "why re-attach" section for why callers must not recompute this from a
/// possibly-since-edited title.
pub fn epic_branch_name(title: &str, epic_id: &str) -> String {
    format!("dearborn/{}-{}", slug(title), last_n_lower(epic_id, 6))
}

/// §2.8's standalone-task branch name: `dearborn/task-<slug(task.title)>-<last
/// 6 of task id>` (T-551) — the task-table mirror of [`epic_branch_name`].
/// The extra `task-` infix is the one naming difference §2.8 specifies
/// between the two branch shapes; kept as a distinct format string rather
/// than parameterizing `epic_branch_name` over an optional infix, since a
/// third shape is never coming (D17: exactly epic and standalone) and a
/// boolean/enum parameter for "insert this literal or don't" would read as
/// more general than the problem actually is.
pub fn task_branch_name(title: &str, task_id: &str) -> String {
    format!("dearborn/task-{}-{}", slug(title), last_n_lower(task_id, 6))
}

/// The last (up to) `n` characters of `s`, lowercased.
fn last_n_lower(s: &str, n: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    let start = chars.len().saturating_sub(n);
    chars[start..].iter().collect::<String>().to_lowercase()
}

/// A successfully provisioned (or re-attached) epic workspace.
#[derive(Debug, Clone)]
pub struct ProvisionedWorkspace {
    pub workspace_path: PathBuf,
    pub branch_name: String,
    /// The project's `test_cmd`, carried through from the same project row
    /// this function already loaded to run `setup_cmd` — so T-521's
    /// preflight gate (the caller, in `worker.rs`, immediately after
    /// provisioning) doesn't need a second project query just to find out
    /// whether a gate command is even configured. Untrimmed, possibly
    /// blank/`None` — the same "skip means no row" filtering
    /// [`cmd::run_stage_command`] already applies is left to the caller,
    /// exactly as `setup_cmd` is filtered via [`non_empty`] rather than here.
    pub test_cmd: Option<String>,
}

/// Why [`provision_epic_workspace`] failed — the two §2.3 reasons this
/// module owns. `message` is human-readable and already redacted of any PAT
/// (git errors flow through [`crate::git::redact`] already; `setup_cmd`
/// output is explicitly redacted before it reaches this variant — see
/// [`run_setup`]) so it is safe to log.
#[derive(Debug, Clone)]
pub enum ProvisionFailure {
    /// Any git/filesystem failure, or a project whose `clone_path` is
    /// missing or not `ready`. Maps to `epic.blocked_reason = 'workspace_error'`.
    Workspace(String),
    /// `setup_cmd` exited non-zero (or failed to even spawn). Maps to
    /// `epic.blocked_reason = 'setup_failed'`. The caller does **not** need
    /// to separately persist evidence — [`run_setup`] has already written
    /// the `agent_run` row by the time this variant is returned.
    Setup {
        message: String,
        exit_code: Option<i32>,
    },
}

/// Which row backs a provisioned workspace (T-551) — the *only* axis
/// [`provision_epic_workspace`]/[`provision_task_workspace`] differ on: where
/// `branch_name` is read/persisted, and which evidence identity `setup_cmd`'s
/// `agent_run` row carries. Mirrors `worker::LeaseTable`'s "enum plus one
/// shared core" shape (T-550) rather than a generic/trait parameter — D17
/// means there are exactly two containers to ever support, so a third
/// implementor is never coming. Kept private: nothing outside this module
/// needs to name a container — every caller already knows which kind of
/// workspace it wants via which public function it calls.
enum WorkspaceContainer<'a> {
    Epic(&'a str),
    Task(&'a str),
}

impl WorkspaceContainer<'_> {
    fn id(&self) -> &str {
        match self {
            WorkspaceContainer::Epic(id) | WorkspaceContainer::Task(id) => id,
        }
    }

    /// The `(epic_id, task_id)` pair `setup_cmd`'s evidence row carries —
    /// exactly the shape `worker::AgentStageParams`/`FailureContext` already
    /// use at this same epic/standalone boundary.
    fn evidence_ids(&self) -> (Option<&str>, Option<&str>) {
        match self {
            WorkspaceContainer::Epic(id) => (Some(*id), None),
            WorkspaceContainer::Task(id) => (None, Some(*id)),
        }
    }
}

/// Provision (or re-attach) `epic_id`'s workspace in `project_id`. See the
/// module doc for the full sequence and its rationale. On success, the
/// workspace is checked out on `branch_name` with `setup_cmd` (if any) having
/// run cleanly; on a first provision, `branch_name` has also been persisted
/// to the epic row.
pub async fn provision_epic_workspace(
    state: &AppState,
    epic_id: &str,
    project_id: &str,
) -> Result<ProvisionedWorkspace, ProvisionFailure> {
    let workspace_path = epic_workspace_path(&state.config.clone_root, epic_id);
    provision_workspace(
        state,
        WorkspaceContainer::Epic(epic_id),
        project_id,
        workspace_path,
        epic_branch_name,
    )
    .await
}

/// The standalone-task mirror of [`provision_epic_workspace`] (T-551, §2.8):
/// identical sequence — refresh canonical → clone/re-attach → `setup_cmd` →
/// persist `branch_name` on first provision — against `<clone_root>/tasks/<task
/// id>` and `task.branch_name` instead of the epic table. See
/// [`provision_workspace`] for the shared body; this function supplies only
/// the two things that differ (which path, which branch-name format).
pub async fn provision_task_workspace(
    state: &AppState,
    task_id: &str,
    project_id: &str,
) -> Result<ProvisionedWorkspace, ProvisionFailure> {
    let workspace_path = task_workspace_path(&state.config.clone_root, task_id);
    provision_workspace(
        state,
        WorkspaceContainer::Task(task_id),
        project_id,
        workspace_path,
        task_branch_name,
    )
    .await
}

/// The shared guts of [`provision_epic_workspace`]/[`provision_task_workspace`]
/// (T-551): every step of the module doc's sequence, parameterized on which
/// [`WorkspaceContainer`] this claim is for, its already-computed
/// `workspace_path` (the one thing that genuinely differs in *shape*, not
/// just in which table a query hits — an epic's and a standalone task's
/// workspace paths live under different subtrees of `clone_root`), and
/// `branch_name_fn` (`epic_branch_name`/`task_branch_name`, §2.8's two branch
/// formats). Everything else — the canonical refresh under the per-project
/// lock, the clone-vs-reattach decision, `setup_cmd`, persisting
/// `branch_name` once on first provision — is exactly the same code path for
/// both containers, run here once.
async fn provision_workspace(
    state: &AppState,
    container: WorkspaceContainer<'_>,
    project_id: &str,
    workspace_path: PathBuf,
    branch_name_fn: impl Fn(&str, &str) -> String,
) -> Result<ProvisionedWorkspace, ProvisionFailure> {
    let conn = state.db.conn();
    let container_id = container.id();

    let project = load_project(conn, project_id)
        .await
        .map_err(|e| ProvisionFailure::Workspace(format!("failed to load project: {e}")))?
        .ok_or_else(|| ProvisionFailure::Workspace(format!("project {project_id} not found")))?;

    let canonical_path = match (&project.clone_path, project.clone_status.as_str()) {
        (Some(p), "ready") => PathBuf::from(p),
        (Some(_), status) => {
            return Err(ProvisionFailure::Workspace(format!(
                "project {project_id} clone is not ready (clone_status = {status})"
            )))
        }
        (None, _) => {
            return Err(ProvisionFailure::Workspace(format!(
                "project {project_id} has no clone_path"
            )))
        }
    };

    let pat = load_decrypted_pat(state, project_id)
        .await
        .map_err(|e| ProvisionFailure::Workspace(format!("failed to load project PAT: {e}")))?;

    // 1. Refresh the canonical checkout, serialized per-project (§11 risk 3
    //    — see module doc). Held only across this call. The reset target is
    //    the §5-resolved explicit base branch when one is recorded, so the
    //    clone below branches off exactly that commit graph.
    let (title, existing_branch_name, existing_base_branch) =
        load_container_for_provision(conn, &container)
            .await
            .map_err(|e| ProvisionFailure::Workspace(format!("failed to load container: {e}")))?
            .ok_or_else(|| ProvisionFailure::Workspace(format!("{container_id} not found")))?;
    let resolved_base = resolve_base_branch(
        existing_base_branch.as_deref(),
        project.base_branch.as_deref(),
    );
    {
        let lock = state.project_refresh_lock(project_id);
        let _guard = lock.lock().await;
        git::refresh_repo(
            &project.repo_url,
            pat.as_deref(),
            &canonical_path,
            resolved_base,
        )
        .await
        .map_err(|e| ProvisionFailure::Workspace(e.message))?;
    }

    // Read back a previously persisted branch name rather than recomputing —
    // see the module doc's "why re-attach" section.
    let (branch_name, is_first_provision) = match existing_branch_name {
        Some(existing) => (existing, false),
        None => (branch_name_fn(&title, container_id), true),
    };

    // 2. Clone (first provision / a workspace that vanished or drifted since
    //    one) or re-attach (an existing, on-branch workspace).
    if workspace_reattachable(&workspace_path, &branch_name).await {
        git::reset_hard_and_clean(&workspace_path)
            .await
            .map_err(|e| ProvisionFailure::Workspace(e.message))?;
    } else {
        git::clone_local(&canonical_path, &workspace_path)
            .await
            .map_err(|e| ProvisionFailure::Workspace(e.message))?;
        git::set_remote_url(&workspace_path, &project.repo_url)
            .await
            .map_err(|e| ProvisionFailure::Workspace(e.message))?;
        git::checkout_new_branch(&workspace_path, &branch_name)
            .await
            .map_err(|e| ProvisionFailure::Workspace(e.message))?;
    }

    // 3. `setup_cmd`, if any — re-runs on re-attach too (see module doc).
    if let Some(setup_cmd) = non_empty(project.setup_cmd.as_deref()) {
        run_setup(
            state,
            container.evidence_ids(),
            &workspace_path,
            setup_cmd,
            pat.as_deref(),
        )
        .await?;
    }

    // 4. Persist the branch name — only on first provision; thereafter it is
    //    read back, never rewritten (see module doc). On an epic's first
    //    provision this same write snapshots the §5-resolved base branch onto
    //    `epic.base_branch` when one is recorded (design doc §5: the snapshot
    //    happens when the branch is cut, so a later project-default edit can
    //    never retarget an already-provisioned epic's PR). An epic whose chain
    //    resolved to "repo default" stays NULL deliberately — that *is* its
    //    recorded base.
    if is_first_provision {
        // What to write to `epic.base_branch`: an epic whose base was set at
        // creation keeps that exact value (the write below must never clobber
        // it with NULL); one without gets the §5-resolved branch snapshotted
        // now (`None` = repo default — which *is* its recorded state). Tasks
        // have no per-item column, so they always write NULL there (ignored
        // by the task arm of the SQL).
        let base_snapshot = match container {
            WorkspaceContainer::Epic(_) => match existing_base_branch.as_deref() {
                Some(existing) => Some(existing),
                None => resolved_base,
            },
            WorkspaceContainer::Task(_) => None,
        };
        persist_container_branch_name(conn, &container, &branch_name, base_snapshot)
            .await
            .map_err(|e| {
                ProvisionFailure::Workspace(format!("failed to persist branch_name: {e}"))
            })?;
    }

    Ok(ProvisionedWorkspace {
        workspace_path,
        branch_name,
        test_cmd: project.test_cmd,
    })
}

/// Whether `workspace_path` is a workspace that can be re-attached rather
/// than re-cloned: it exists, has its own `.git`, and is currently checked
/// out on `expected_branch`.
async fn workspace_reattachable(workspace_path: &Path, expected_branch: &str) -> bool {
    if !workspace_path.join(".git").exists() {
        return false;
    }
    matches!(
        git::current_branch(workspace_path).await,
        Ok(branch) if branch == expected_branch
    )
}

/// Run the project's `setup_cmd` in the workspace via [`cmd::run_stage_command`]
/// (T-520) — one `agent_run` row per call, `stage = "setup"`, `attempt = 1`
/// (`setup_cmd` never retries) — and turn a non-`ok` outcome into
/// [`ProvisionFailure::Setup`]. `setup_cmd` is only ever called with a
/// non-empty command (the caller already filtered via [`non_empty`]), so the
/// [`StageOutcome::Skipped`] arm below is unreachable in practice — it is
/// still matched explicitly (rather than assumed away) so a future caller
/// that stops pre-filtering gets a loud panic instead of a silently wrong
/// "setup succeeded".
///
/// Redaction happens in the `sanitize` hook handed to `run_stage_command`:
/// `setup_cmd` output should never contain the project's PAT, but a command
/// that echoes its own environment (or a misconfigured build script) could
/// otherwise leak it into stored evidence, so it is redacted ([`git::redact`])
/// before either the returned message or the persisted `agent_run.log` sees
/// it — the same defensive redaction T-511 established, now happening for
/// free wherever [`cmd::run_stage_command`] is used with this hook, instead
/// of only at this one call site.
///
/// `evidence_ids` (T-551) is the `(epic_id, task_id)` pair
/// [`WorkspaceContainer::evidence_ids`] hands `setup_cmd`'s own `agent_run`
/// row — `(Some(epic_id), None)` for an epic, `(None, Some(task_id))` for a
/// standalone task, mirroring `worker::AgentStageParams`'s identical
/// epic/standalone shape.
async fn run_setup(
    state: &AppState,
    evidence_ids: (Option<&str>, Option<&str>),
    workspace_path: &Path,
    setup_cmd: &str,
    pat: Option<&str>,
) -> Result<(), ProvisionFailure> {
    let (epic_id, task_id) = evidence_ids;
    let outcome = cmd::run_stage_command(
        state.db.conn(),
        StageCommand {
            task_id,
            epic_id,
            stage: "setup",
            attempt: 1,
            cwd: workspace_path,
            timeout: Duration::from_secs(state.config.executor.cmd_timeout_secs),
        },
        Some(setup_cmd),
        |raw: &str| git::redact(raw, pat),
    )
    .await
    .map_err(|e| {
        ProvisionFailure::Workspace(format!("failed to record setup_cmd evidence: {e}"))
    })?;

    match outcome {
        StageOutcome::Skipped => unreachable!(
            "run_setup is only called with a non-empty setup_cmd (see non_empty's caller)"
        ),
        StageOutcome::Ran(ran) if ran.status == "ok" => Ok(()),
        StageOutcome::Ran(ran) => Err(ProvisionFailure::Setup {
            message: ran.output,
            exit_code: ran.exit_code,
        }),
    }
}

/// Delete an epic workspace. **Not called from anywhere yet** — T-514 calls
/// this after a PR successfully opens; retention on `Blocked`/`Cancelled` is
/// the default (simply never calling it). Best-effort: a directory that is
/// already gone is not an error.
pub async fn delete_workspace(workspace_path: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_dir_all(workspace_path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

// ---- small private loaders (kept local rather than growing projects.rs /
// epics.rs's public surface for a handful of fields the executor alone needs)

struct ProjectForProvision {
    repo_url: String,
    setup_cmd: Option<String>,
    /// Carried through to [`ProvisionedWorkspace::test_cmd`] — see that
    /// field's doc for why the preflight gate (T-521) reuses this load
    /// instead of querying the project a second time.
    test_cmd: Option<String>,
    clone_path: Option<String>,
    clone_status: String,
    /// §5: the project default base branch (`None` = repo default), fed into
    /// [`resolve_base_branch`] together with the container's own override.
    base_branch: Option<String>,
}

async fn load_project(
    conn: &Connection,
    project_id: &str,
) -> Result<Option<ProjectForProvision>, libsql::Error> {
    let mut rows = conn
        .query(
            "SELECT repo_url, setup_cmd, test_cmd, clone_path, clone_status, base_branch \
             FROM project WHERE id = ?1",
            params![project_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(ProjectForProvision {
            repo_url: row.get(0)?,
            setup_cmd: row.get(1)?,
            test_cmd: row.get(2)?,
            clone_path: row.get(3)?,
            clone_status: row.get(4)?,
            base_branch: row.get(5)?,
        })),
        None => Ok(None),
    }
}

/// Just enough of a container row (epic or task) for [`provision_workspace`]:
/// its title (to compute a branch name on first provision) and its
/// already-persisted `branch_name` (`None` on first provision, `Some` on a
/// re-claim — see the module doc's "why re-attach" section).
/// Just enough of a container row (epic or task) for [`provision_workspace`]:
/// its title (to compute a branch name on first provision), its
/// already-persisted `branch_name` (`None` on first provision, `Some` on a
/// re-claim — see the module doc's "why re-attach" section), and its
/// already-recorded `base_branch` (§5; always `None` for a standalone task —
/// tasks have no per-item base override by design).
async fn load_container_for_provision(
    conn: &Connection,
    container: &WorkspaceContainer<'_>,
) -> Result<Option<(String, Option<String>, Option<String>)>, libsql::Error> {
    match container {
        WorkspaceContainer::Epic(epic_id) => {
            let mut rows = conn
                .query(
                    "SELECT title, branch_name, base_branch FROM epic WHERE id = ?1",
                    params![*epic_id],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?))),
                None => Ok(None),
            }
        }
        WorkspaceContainer::Task(task_id) => {
            let mut rows = conn
                .query(
                    "SELECT title, branch_name FROM task WHERE id = ?1",
                    params![*task_id],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.get(0)?, row.get(1)?, None))),
                None => Ok(None),
            }
        }
    }
}

/// Persist `branch_name` on the container's own row (T-551) — the
/// epic/task-table mirror pair `provision_workspace`'s first-provision step
/// calls into. See the module doc's "why re-attach" section for why this
/// only ever runs once per container, never on a re-attach.
/// Persist `branch_name` on the container's own row (T-551) — the
/// epic/task-table mirror pair `provision_workspace`'s first-provision step
/// calls into. See the module doc's "why re-attach" section for why this
/// only ever runs once per container, never on a re-attach. For an epic,
/// `base_snapshot` (§5) rides along in the same write: the resolved explicit
/// base branch, snapshotted exactly when the branch is cut (`None` keeps the
/// column NULL — "repo default" — which is itself the recorded state). The
/// task arm simply ignores it (no per-task base override exists).
async fn persist_container_branch_name(
    conn: &Connection,
    container: &WorkspaceContainer<'_>,
    branch_name: &str,
    base_snapshot: Option<&str>,
) -> Result<(), libsql::Error> {
    let now = now_ms();
    // Each arm names its own columns explicitly: the `task` table has no
    // `base_branch` column (§5 is epic/project-level only), so the shared
    // "branch_name + updated_at" shape cannot be one interpolated statement.
    // `table` is one of two compile-time string literals chosen just below by
    // `WorkspaceContainer`'s own match, never caller-supplied data — the same
    // pattern `worker::claim_row`'s doc already establishes for interpolating
    // a table name safely.
    let (table, id) = match container {
        WorkspaceContainer::Epic(epic_id) => ("epic", *epic_id),
        WorkspaceContainer::Task(task_id) => ("task", *task_id),
    };
    let sql = match container {
        WorkspaceContainer::Epic(_) => {
            format!(
                "UPDATE {table} SET branch_name = ?1, base_branch = ?2, updated_at = ?3 WHERE id = ?4"
            )
        }
        WorkspaceContainer::Task(_) => {
            format!("UPDATE {table} SET branch_name = ?1, updated_at = ?3 WHERE id = ?4")
        }
    };
    conn.execute(&sql, params![branch_name, base_snapshot, now, id])
        .await?;
    Ok(())
}

/// Trim and reject an empty/whitespace-only optional command, mirroring how
/// `test_cmd`/`setup_cmd` absence is treated everywhere else (§5: `NULL` or
/// blank means "skip").
fn non_empty(cmd: Option<&str>) -> Option<&str> {
    cmd.map(str::trim).filter(|s| !s.is_empty())
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
    use crate::{Config, Db};
    use std::sync::Arc;
    use std::time::Duration;

    // ---- §5 base-branch resolution chain -----------------------------------

    #[test]
    fn resolve_base_branch_epic_wins_over_project() {
        assert_eq!(
            resolve_base_branch(Some("epic-branch"), Some("project-main")),
            Some("epic-branch")
        );
    }

    #[test]
    fn resolve_base_branch_falls_back_to_project_default() {
        assert_eq!(resolve_base_branch(None, Some("release")), Some("release"));
    }

    #[test]
    fn resolve_base_branch_none_means_repo_default() {
        // The terminal of the chain: `None` renders as origin/HEAD everywhere
        // it is consumed — never as a guessed literal branch name.
        assert_eq!(resolve_base_branch(None, None), None);
    }

    // ---- slug() ----------------------------------------------------------

    #[test]
    fn slug_lowercases_and_collapses_punctuation() {
        assert_eq!(slug("Ship The Thing!!"), "ship-the-thing");
        assert_eq!(slug("Fix   multiple   spaces"), "fix-multiple-spaces");
        assert_eq!(slug("snake_case_title"), "snake-case-title");
    }

    #[test]
    fn slug_trims_leading_and_trailing_junk() {
        assert_eq!(slug("  --Hello World--  "), "hello-world");
        assert_eq!(slug("***Loud Title***"), "loud-title");
    }

    #[test]
    fn slug_drops_non_ascii_rather_than_hyphenating() {
        // 'é' is dropped (not turned into a hyphen): "café" -> "caf",
        // "résumé" -> "rsum" (the interior drops don't themselves introduce
        // hyphens — only the space between the two words does).
        assert_eq!(slug("café résumé"), "caf-rsum");
        assert_eq!(slug("日本語 title"), "title");
    }

    #[test]
    fn slug_of_empty_or_all_punctuation_falls_back() {
        assert_eq!(slug(""), "epic");
        assert_eq!(slug("   "), "epic");
        assert_eq!(slug("!!!---***"), "epic");
        assert_eq!(slug("🎉🎉🎉"), "epic");
    }

    #[test]
    fn slug_caps_length_without_trailing_hyphen() {
        let long = "a very ".repeat(20); // way over SLUG_MAX_LEN once slugged
        let slugged = slug(&long);
        assert!(slugged.len() <= SLUG_MAX_LEN);
        assert!(!slugged.ends_with('-'));
    }

    #[test]
    fn epic_branch_name_matches_the_section_2_8_format() {
        let name = epic_branch_name("Ship The Thing!!", "01HXYZDEADBEEF6789AB");
        // dearborn/<slug>-<last 6 of id, lowercased>
        assert_eq!(name, "dearborn/ship-the-thing-6789ab");
    }

    // ---- workspace path ----------------------------------------------------

    #[test]
    fn epic_workspace_path_matches_section_2_8() {
        let p = epic_workspace_path("/clones", "epic-123");
        assert_eq!(p, std::path::PathBuf::from("/clones/epics/epic-123"));
    }

    // ---- test scaffolding --------------------------------------------------

    /// A local git fixture: `git init`s a source repo with one commit in a
    /// fresh temp dir, entirely offline. Used as a project's `repo_url` so
    /// canonical-refresh + workspace-clone have something real to clone from
    /// without any network access. Cleans itself up on drop.
    struct GitFixture {
        dir: PathBuf,
    }

    impl GitFixture {
        async fn new() -> GitFixture {
            let dir = std::env::temp_dir().join(format!(
                "dearborn-ws-fixture-{}-{}",
                std::process::id(),
                ulid::Ulid::new()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            run_git_ok(&dir, &["init", "-b", "main"]).await;
            run_git_ok(&dir, &["config", "user.email", "test@example.com"]).await;
            run_git_ok(&dir, &["config", "user.name", "Test"]).await;
            std::fs::write(dir.join("README.md"), "hello\n").unwrap();
            run_git_ok(&dir, &["add", "."]).await;
            run_git_ok(&dir, &["commit", "-m", "init"]).await;
            GitFixture { dir }
        }

        fn path(&self) -> &Path {
            &self.dir
        }
    }

    impl Drop for GitFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn run_git_ok(dir: &Path, args: &[&str]) {
        let status = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {dir:?}");
    }

    /// A private temp `clone_root` for one test (so tests never share/collide
    /// on real paths). Cleaned up on drop.
    struct TempCloneRoot {
        dir: PathBuf,
    }

    impl TempCloneRoot {
        fn new(name: &str) -> TempCloneRoot {
            let dir = std::env::temp_dir().join(format!(
                "dearborn-ws-root-{name}-{}-{}",
                std::process::id(),
                ulid::Ulid::new()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            TempCloneRoot { dir }
        }

        fn path_str(&self) -> String {
            self.dir.to_string_lossy().to_string()
        }
    }

    impl Drop for TempCloneRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn test_state(clone_root: &str) -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test();
        config.clone_root = clone_root.to_string();
        AppState::new(config, db)
    }

    async fn seed_project(
        state: &AppState,
        repo_url: &str,
        clone_status: &str,
        with_clone_path: bool,
        setup_cmd: Option<&str>,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = if with_clone_path {
            Some(
                Path::new(&state.config.clone_root)
                    .join(&id)
                    .to_string_lossy()
                    .to_string(),
            )
        } else {
            None
        };
        conn.execute(
            "INSERT INTO project (id, name, repo_url, setup_cmd, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, ?4, ?5, ?6, ?6)",
            params![id.clone(), repo_url, setup_cmd, clone_path, clone_status, now],
        )
        .await
        .unwrap();
        id
    }

    async fn seed_project_with_pat(state: &AppState, repo_url: &str, pat: &str) -> String {
        let id = seed_project(state, repo_url, "ready", true, None).await;
        let blob = state.crypto.encrypt_pat(pat).unwrap();
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET pat_encrypted = ?1 WHERE id = ?2",
                params![blob, id.clone()],
            )
            .await
            .unwrap();
        id
    }

    async fn seed_epic(state: &AppState, project_id: &str, title: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, ?3, 'InProgress', ?4, ?4)",
            params![id.clone(), project_id, title, now],
        )
        .await
        .unwrap();
        id
    }

    async fn epic_branch_name_column(state: &AppState, epic_id: &str) -> Option<String> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT branch_name FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// The epic row's §5 `base_branch` snapshot (`None` = repo default).
    async fn epic_base_branch_column(state: &AppState, epic_id: &str) -> Option<String> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT base_branch FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    // ---- per-project refresh lock serialization ---------------------------

    /// The load-bearing primitive itself (§11 risk 3): two holders of the
    /// *same* project's lock never run their critical section concurrently.
    /// Instrumented with an active-count that would exceed 1 if the lock
    /// failed to serialize. No git/provisioning involved — this isolates the
    /// lock mechanism from everything else that could flake.
    #[tokio::test]
    async fn same_project_refresh_lock_serializes_critical_sections() {
        let state = test_state("./unused-clones-lock-test").await;
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let lock = state.project_refresh_lock("project-a");
            let active = active.clone();
            let max_active = max_active.clone();
            handles.push(tokio::spawn(async move {
                let _guard = lock.lock().await;
                let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_active.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            max_active.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "same-project critical sections must never overlap"
        );
    }

    /// Different projects get different locks and run concurrently — proven
    /// by having each hold its lock until a shared gate releases both, which
    /// would deadlock (timeout) if they secretly shared one lock.
    #[tokio::test]
    async fn different_project_refresh_locks_do_not_serialize() {
        let state = test_state("./unused-clones-lock-test-2").await;
        let gate = Arc::new(tokio::sync::Barrier::new(2));

        let lock_a = state.project_refresh_lock("project-a");
        let lock_b = state.project_refresh_lock("project-b");
        let gate_a = gate.clone();
        let gate_b = gate.clone();

        let task_a = tokio::spawn(async move {
            let _guard = lock_a.lock().await;
            gate_a.wait().await;
        });
        let task_b = tokio::spawn(async move {
            let _guard = lock_b.lock().await;
            gate_b.wait().await;
        });

        // Both must reach the barrier — if the locks secretly shared a
        // project id, the second would never acquire its lock and this
        // would hang past the timeout.
        tokio::time::timeout(Duration::from_secs(5), async {
            task_a.await.unwrap();
            task_b.await.unwrap();
        })
        .await
        .expect("different-project locks must not serialize");
    }

    /// The same project id always hands back the *same* lock instance
    /// (identity, via `Arc::ptr_eq`) — otherwise two callers "locking the
    /// same project" would each get their own independent mutex and never
    /// actually exclude each other.
    #[test]
    fn project_refresh_lock_is_stable_per_project_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let state = test_state("./unused-clones-lock-test-3").await;
            let a1 = state.project_refresh_lock("proj");
            let a2 = state.project_refresh_lock("proj");
            let b = state.project_refresh_lock("other");
            assert!(Arc::ptr_eq(&a1, &a2));
            assert!(!Arc::ptr_eq(&a1, &b));
        });
    }

    // ---- full provisioning: first call, idempotent re-attach ---------------

    /// §5 end-to-end: with a project default base branch set, provisioning
    /// resets the canonical checkout to `origin/<base>`, the epic workspace
    /// branches off that commit graph (the release-only file exists), and the
    /// resolved branch is snapshotted onto `epic.base_branch` at first
    /// provision.
    #[tokio::test]
    async fn provision_branches_off_the_project_base_and_snapshots_it_onto_the_epic() {
        let root = TempCloneRoot::new("base-branch");
        let fixture = GitFixture::new().await;
        // A `release` branch carrying a file `main` never gets.
        run_git_ok(fixture.path(), &["checkout", "-b", "release"]).await;
        std::fs::write(fixture.path().join("release-only.txt"), "from release\n").unwrap();
        run_git_ok(fixture.path(), &["add", "."]).await;
        run_git_ok(fixture.path(), &["commit", "-m", "release"]).await;
        run_git_ok(fixture.path(), &["checkout", "main"]).await;

        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            None,
        )
        .await;
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET base_branch = 'release' WHERE id = ?1",
                params![project_id.clone()],
            )
            .await
            .unwrap();
        let epic_id = seed_epic(&state, &project_id, "Based On Release").await;

        let outcome = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning must succeed against the local fixture");

        // The workspace actually branched off `release`'s commit graph.
        assert!(outcome.workspace_path.join("release-only.txt").exists());
        let current = git::current_branch(&outcome.workspace_path).await.unwrap();
        assert_eq!(current, outcome.branch_name);

        // The snapshot landed on the epic row in the same write as branch_name.
        assert_eq!(
            epic_base_branch_column(&state, &epic_id).await,
            Some("release".to_string())
        );
    }

    /// §5 chain permutation (epic set, project set) at the I/O level: the
    /// epic's own recorded base wins over the project default — provisioning
    /// branches off `origin/<epic base>` (the release-only file exists, the
    /// develop-only file does not) and snapshots *that* onto `epic.base_branch`.
    /// Complements the pure [`resolve_base_branch`] unit tests by proving the
    /// winning value actually drives git and the snapshot write.
    #[tokio::test]
    async fn provision_prefers_the_epics_base_over_the_project_default() {
        let root = TempCloneRoot::new("base-branch-epic-wins");
        let fixture = GitFixture::new().await;
        // Two non-default branches, each carrying a file the others lack.
        run_git_ok(fixture.path(), &["checkout", "-b", "release"]).await;
        std::fs::write(fixture.path().join("release-only.txt"), "from release\n").unwrap();
        run_git_ok(fixture.path(), &["add", ".", "release-only.txt"]).await;
        run_git_ok(fixture.path(), &["commit", "-m", "release"]).await;
        run_git_ok(fixture.path(), &["checkout", "-b", "develop", "main"]).await;
        std::fs::write(fixture.path().join("develop-only.txt"), "from develop\n").unwrap();
        run_git_ok(fixture.path(), &["add", ".", "develop-only.txt"]).await;
        run_git_ok(fixture.path(), &["commit", "-m", "develop"]).await;
        run_git_ok(fixture.path(), &["checkout", "main"]).await;

        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            None,
        )
        .await;
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET base_branch = 'develop' WHERE id = ?1",
                params![project_id.clone()],
            )
            .await
            .unwrap();
        let epic_id = seed_epic(&state, &project_id, "Stacked On Release").await;
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET base_branch = 'release' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        let outcome = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning must succeed against the local fixture");

        // Epic's base won: branched off `release`'s graph, not `develop`'s.
        assert!(outcome.workspace_path.join("release-only.txt").exists());
        assert!(
            !outcome.workspace_path.join("develop-only.txt").exists(),
            "the project default must lose to the epic's recorded base"
        );
        assert_eq!(
            epic_base_branch_column(&state, &epic_id).await,
            Some("release".to_string()),
            "the snapshot must record the epic's own base"
        );
    }

    /// §5 chain permutation (epic set, project NULL) at the I/O level: with no
    /// project default, the epic's explicit base is still used and snapshotted —
    /// an epic-level override never silently falls back to the repo default.
    #[tokio::test]
    async fn provision_uses_the_epics_base_when_the_project_has_none() {
        let root = TempCloneRoot::new("base-branch-epic-only");
        let fixture = GitFixture::new().await;
        run_git_ok(fixture.path(), &["checkout", "-b", "hotfix"]).await;
        std::fs::write(fixture.path().join("hotfix-only.txt"), "from hotfix\n").unwrap();
        run_git_ok(fixture.path(), &["add", "."]).await;
        run_git_ok(fixture.path(), &["commit", "-m", "hotfix"]).await;
        run_git_ok(fixture.path(), &["checkout", "main"]).await;

        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Based On Hotfix").await;
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET base_branch = 'hotfix' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        let outcome = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning must succeed against the local fixture");

        // Branched off `hotfix`'s graph (its own commit is present).
        assert!(outcome.workspace_path.join("hotfix-only.txt").exists());
        // And the snapshot recorded it.
        assert_eq!(
            epic_base_branch_column(&state, &epic_id).await,
            Some("hotfix".to_string())
        );
    }

    #[tokio::test]
    async fn first_provision_clones_checks_out_branch_and_persists_it() {
        let root = TempCloneRoot::new("first");
        let fixture = GitFixture::new().await;
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Ship The Thing!!").await;

        let outcome = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning must succeed against a local fixture");

        assert!(outcome.workspace_path.join(".git").exists());
        let expected_branch = format!(
            "dearborn/ship-the-thing-{}",
            &epic_id[epic_id.len() - 6..].to_lowercase()
        );
        assert_eq!(outcome.branch_name, expected_branch);

        // branch_name persisted on the epic row.
        assert_eq!(
            epic_branch_name_column(&state, &epic_id).await,
            Some(expected_branch.clone())
        );

        // Actually checked out on that branch.
        let current = git::current_branch(&outcome.workspace_path).await.unwrap();
        assert_eq!(current, expected_branch);
    }

    #[tokio::test]
    async fn reprovisioning_reattaches_instead_of_recloning() {
        let root = TempCloneRoot::new("reattach");
        let fixture = GitFixture::new().await;
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Reattach Me").await;

        let first = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("first provision must succeed");

        // A marker inside .git/ proves the .git directory itself survives
        // (i.e. was not deleted and recreated by a second clone).
        let git_marker = first.workspace_path.join(".git").join("dearborn-marker");
        std::fs::write(&git_marker, "still here").unwrap();

        // Dirty the tracked file and drop an untracked sentinel.
        std::fs::write(first.workspace_path.join("README.md"), "DIRTY EDIT").unwrap();
        let sentinel = first.workspace_path.join("untracked-sentinel.txt");
        std::fs::write(&sentinel, "should be cleaned").unwrap();

        let second = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("re-provision must succeed");

        assert_eq!(second.workspace_path, first.workspace_path);
        assert_eq!(second.branch_name, first.branch_name);

        // .git survived (re-attach, not re-clone).
        assert!(
            git_marker.exists(),
            ".git directory must not have been recreated"
        );

        // Dirty tracked-file edit reverted by `reset --hard HEAD`.
        let readme = std::fs::read_to_string(first.workspace_path.join("README.md")).unwrap();
        assert_eq!(
            readme, "hello\n",
            "tracked-file edit must be reverted on re-attach"
        );

        // Untracked sentinel removed by `clean -fd`.
        assert!(
            !sentinel.exists(),
            "untracked file must be removed on re-attach"
        );
    }

    #[tokio::test]
    async fn missing_clone_path_is_a_workspace_error() {
        let root = TempCloneRoot::new("no-clone-path");
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            "https://example.invalid/x.git",
            "ready",
            false,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "No Clone Path").await;

        let err = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect_err("a project with no clone_path must fail provisioning");
        match err {
            ProvisionFailure::Workspace(msg) => assert!(msg.contains("clone_path")),
            other => panic!("expected Workspace failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pending_clone_status_is_a_workspace_error() {
        let root = TempCloneRoot::new("pending-clone");
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            "https://example.invalid/x.git",
            "pending",
            true,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Pending Clone").await;

        let err = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect_err("a not-yet-ready clone must fail provisioning");
        assert!(matches!(err, ProvisionFailure::Workspace(_)));
    }

    // ---- token redaction ---------------------------------------------------

    /// A forced git failure (unreachable https host, mirroring `git.rs`'s own
    /// bad-url test) with a PAT-bearing project: the error text returned to
    /// the caller must never contain the token.
    #[tokio::test]
    async fn forced_git_failure_never_leaks_the_pat() {
        let root = TempCloneRoot::new("bad-url");
        let state = test_state(&root.path_str()).await;
        let pat = "ghp_superSecretToken123";
        let project_id =
            seed_project_with_pat(&state, "https://dearborn.invalid/nope/nope.git", pat).await;
        let epic_id = seed_epic(&state, &project_id, "Bad Url").await;

        let err = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect_err("an unreachable host must fail provisioning");
        let message = match err {
            ProvisionFailure::Workspace(msg) => msg,
            other => panic!("expected Workspace failure, got {other:?}"),
        };
        assert!(
            !message.contains(pat),
            "token must not leak into the error: {message}"
        );
        assert!(!message.contains("ghp_"));
    }

    /// After a successful (local-fixture) provisioning, neither the
    /// canonical checkout's nor the workspace's `.git/config` contains any
    /// credential, and the workspace's `origin` is the plain `repo_url` with
    /// no userinfo embedded — the "token-free real remote" contract.
    #[tokio::test]
    async fn git_config_never_contains_credentials_after_provisioning() {
        let root = TempCloneRoot::new("config-check");
        let fixture = GitFixture::new().await;
        let repo_url = fixture.path().to_string_lossy().to_string();
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(&state, &repo_url, "ready", true, None).await;
        let epic_id = seed_epic(&state, &project_id, "Config Check").await;

        let outcome = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .unwrap();

        let ws_config =
            std::fs::read_to_string(outcome.workspace_path.join(".git/config")).unwrap();
        assert!(
            ws_config.contains(&repo_url),
            "origin must point at the real remote"
        );
        assert!(
            !ws_config.contains('@'),
            "no userinfo/credentials belong in .git/config: {ws_config}"
        );

        let canonical_path = Path::new(&state.config.clone_root).join(&project_id);
        let canonical_config = std::fs::read_to_string(canonical_path.join(".git/config")).unwrap();
        assert!(
            !canonical_config.contains('@'),
            "canonical .git/config must be credential-free too"
        );
    }

    // ---- setup_cmd -----------------------------------------------------------

    #[tokio::test]
    async fn setup_cmd_failure_blocks_with_captured_evidence() {
        let root = TempCloneRoot::new("setup-fail");
        let fixture = GitFixture::new().await;
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            Some("echo setting-up && exit 3"),
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Setup Fails").await;

        let err = provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect_err("a failing setup_cmd must fail provisioning");
        match err {
            ProvisionFailure::Setup { exit_code, message } => {
                assert_eq!(exit_code, Some(3));
                assert!(message.contains("setting-up"));
            }
            other => panic!("expected Setup failure, got {other:?}"),
        }

        // The workspace is retained (never deleted) on a setup failure.
        let workspace_path = epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "workspace must be retained on setup failure"
        );

        // Evidence landed in agent_run.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT stage, status, exit_code, log FROM agent_run WHERE epic_id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("an agent_run row for setup");
        assert_eq!(row.get::<String>(0).unwrap(), "setup");
        assert_eq!(row.get::<String>(1).unwrap(), "error");
        assert_eq!(row.get::<Option<i64>>(2).unwrap(), Some(3));
        let log: String = row.get(3).unwrap();
        assert!(log.contains("setting-up"));
    }

    #[tokio::test]
    async fn setup_cmd_success_records_ok_evidence_and_leaves_workspace_ready() {
        let root = TempCloneRoot::new("setup-ok");
        let fixture = GitFixture::new().await;
        let state = test_state(&root.path_str()).await;
        let project_id = seed_project(
            &state,
            &fixture.path().to_string_lossy(),
            "ready",
            true,
            Some("echo ok"),
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Setup Ok").await;

        provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("a passing setup_cmd must not fail provisioning");

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, exit_code FROM agent_run WHERE epic_id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("an agent_run row for setup");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
        assert_eq!(row.get::<Option<i64>>(1).unwrap(), Some(0));
    }

    /// `setup_cmd` output is redacted of the project's PAT even though this
    /// path never touches real git network calls — isolates the redaction
    /// behavior of [`run_setup`] from provisioning's other git steps (which,
    /// per the module's other tests, can't combine a local-path `repo_url`
    /// with a PAT — `authenticated_url` rejects non-`https` URLs when a PAT
    /// is given).
    #[tokio::test]
    async fn setup_cmd_output_is_redacted_of_the_pat() {
        let root = TempCloneRoot::new("setup-redact");
        let state = test_state(&root.path_str()).await;
        // `agent_run.epic_id` is a foreign key — seed a real project + epic.
        let project_id = seed_project(
            &state,
            "https://example.invalid/x.git",
            "ready",
            false,
            None,
        )
        .await;
        let epic_id = seed_epic(&state, &project_id, "Redact Me").await;
        let workspace = root.dir.join("standalone-setup-dir");
        std::fs::create_dir_all(&workspace).unwrap();

        let pat = "s3cr3t-pat-value";
        let setup_cmd = format!("echo {pat} && exit 1");

        let err = run_setup(
            &state,
            (Some(&epic_id), None),
            &workspace,
            &setup_cmd,
            Some(pat),
        )
        .await
        .expect_err("exit 1 must surface as a Setup failure");
        let (message, exit_code) = match err {
            ProvisionFailure::Setup { message, exit_code } => (message, exit_code),
            other => panic!("expected Setup failure, got {other:?}"),
        };
        assert_eq!(exit_code, Some(1));
        assert!(
            !message.contains(pat),
            "PAT must not appear in the returned message: {message}"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT log FROM agent_run WHERE epic_id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let log: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert!(
            !log.contains(pat),
            "PAT must not appear in the stored evidence log: {log}"
        );
        assert!(
            log.contains("***"),
            "redaction marker expected in place of the token: {log}"
        );
    }
}
