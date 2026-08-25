//! Pure spec rendering, D8 prompt-context assembly, and D9 verdict parsing
//! (T-502).
//!
//! Everything in this module is a **pure function**: no I/O, no DB, no
//! filesystem, no async. That is the point — it is the seam between "what a
//! task-stage agent is told" (a pile of strings) and "how that pile gets
//! assembled from the database" (the worker, landing in T-510+). Keeping the
//! two separate means the rendering/parsing logic here is exercised by plain
//! unit tests with no `AppState`, no `Db`, and no harness at all.
//!
//! ## The three responsibilities
//!
//! 1. [`render_spec`] — MILESTONE_1 §2.1's rendered-spec format, matched
//!    byte-for-byte (it's frozen: both halves of the milestone consume it).
//! 2. [`build_context`] — decision D8: the rendered spec plus epic background
//!    plus a sibling manifest split into "already built" (done) and "owned by
//!    later tasks" (not done). The not-yet-done framing is load-bearing: it's
//!    what stops an implement/fix agent from building the whole epic in one
//!    task, since D7 gives it no other way to discover epic scope.
//! 3. [`parse_verdict`] — decision D9: the last `VERDICT:` line wins (ralph's
//!    bash reads the *first* line; Dearborn's contract is intentionally the
//!    opposite so a reviewer can write its findings before the verdict).
//!
//! ## Prompts
//!
//! The five agent-stage prompts (§2.2: `implement`, `fix`, `review`,
//! `verify_complete`, `summarize`) live as markdown files under
//! `dearborn-server/prompts/`, `include_str!`-compiled into the binary and
//! exposed by [`prompt_for`]. They are adapted from
//! `references/prompts/*-v2.md` with the slash-command frontmatter and
//! `$1`/`$2` argument conventions stripped — Dearborn pre-bakes all context
//! (spec, epic context, sibling manifest, base SHA, test output, review
//! findings) into the prompt per D8, rather than pointing the agent at files
//! to read.
//!
//! ## `Stage` lives in `task_agent`, not here
//!
//! `prompt_for` takes [`crate::task_agent::Stage`] — the real, full §2.2
//! vocabulary (T-512), which superseded this module's original placeholder
//! `PromptStage` (five variants, one job: pick a prompt). `Stage` lives next
//! to the `TaskAgent` trait it drives instead of here, because this module's
//! whole reason to exist is staying a **pure, dependency-light leaf**: no
//! I/O, no DB, no async, no `AppState`. `Stage` itself is just data (and one
//! reference to `harness::RunMode`, itself a plain enum with no I/O
//! semantics), so depending on its *type* costs this module nothing — but
//! defining the stage → `RunMode` → tool-flags policy here would have meant
//! importing the `harness` crate's run machinery into what is otherwise a
//! string-formatting module, which is exactly the kind of scope creep this
//! module's doc has always warned against. See `task_agent`'s module doc for
//! the full rationale.

/// Rendered-spec fields for [`render_spec`] — the only task fields an
/// implement/review agent ever sees (MILESTONE_1 §2.1): `title`,
/// `description`, `acceptance`. Deliberately its own borrowed struct rather
/// than `&Task`, so tests (and any future caller) can render a spec without
/// touching `crate::tasks` at all; [`render_task_spec`] is the convenience
/// wrapper for callers that already hold a `Task` row.
#[derive(Clone, Copy)]
pub struct SpecFields<'a> {
    /// The task's title, rendered as the `# <title>` heading.
    pub title: &'a str,
    /// Free-text description of the end-to-end behavior. Absent or
    /// whitespace-only renders as `(none provided)`.
    pub description: Option<&'a str>,
    /// Free-text acceptance criteria. Absent or whitespace-only renders as
    /// `(none provided)`.
    pub acceptance: Option<&'a str>,
}

/// Fallback text for an absent or whitespace-only description/acceptance
/// field, frozen by MILESTONE_1 §2.1.
const NONE_PROVIDED: &str = "(none provided)";

/// Render a task to exactly the markdown spec format frozen in MILESTONE_1
/// §2.1 (mirroring `render_spec` in `references/ralph-v2.sh`):
///
/// ```text
/// # <title>
///
/// ## Description
/// <description | "(none provided)">
///
/// ## Acceptance Criteria
/// <acceptance | "(none provided)">
/// ```
///
/// A `None` or whitespace-only `description`/`acceptance` is treated as
/// absent (falls back to `(none provided)`); a non-empty value is rendered
/// verbatim, untrimmed. There is **no trailing newline** — the string ends
/// immediately after the acceptance-criteria content, matching the reference
/// `jq` template's concatenation (asserted in this module's tests).
pub fn render_spec(fields: &SpecFields) -> String {
    format!(
        "# {title}\n\n## Description\n{description}\n\n## Acceptance Criteria\n{acceptance}",
        title = fields.title,
        description = non_empty(fields.description),
        acceptance = non_empty(fields.acceptance),
    )
}

/// Convenience wrapper for callers already holding a [`crate::tasks::Task`]
/// row. `Task` is a plain serializable struct (not a DB handle), so borrowing
/// its fields here doesn't reintroduce I/O — [`render_spec`] itself never
/// needs to know `Task` exists.
pub fn render_task_spec(task: &crate::tasks::Task) -> String {
    render_spec(&SpecFields {
        title: &task.title,
        description: task.description.as_deref(),
        acceptance: task.acceptance.as_deref(),
    })
}

/// `value` if it's `Some` and non-whitespace, else the frozen fallback.
/// Preserves the original (untrimmed) content when present — only the
/// presence check is whitespace-insensitive.
fn non_empty(value: Option<&str>) -> &str {
    match value {
        Some(s) if !s.trim().is_empty() => s,
        _ => NONE_PROVIDED,
    }
}

/// Like [`non_empty`] but returning `Option` — used by [`build_context`] to
/// decide whether to emit a section at all (vs. render a fallback in place).
fn non_empty_opt(value: Option<&str>) -> Option<&str> {
    match value {
        Some(s) if !s.trim().is_empty() => Some(s),
        _ => None,
    }
}

// ---- D8: prompt-context assembly -----------------------------------------

/// One sibling task in the same epic, as [`build_context`] needs it to build
/// the manifest: a title, an id (shortened for display per §2.8's "last 6"
/// convention), and whether it's `Done`. The worker (T-513+) builds a slice
/// of these from the epic's other tasks; this module never queries anything.
pub struct SiblingTask<'a> {
    /// The sibling task's id (full length; shortened for display).
    pub id: &'a str,
    /// The sibling task's title.
    pub title: &'a str,
    /// Whether the sibling is `Done`. `false` covers every other status
    /// (`Todo`, `InProgress`, `Failed`, `Cancelled`) — all of them are "not
    /// safe to assume exists yet" from the current task's point of view.
    pub done: bool,
}

/// The parent epic's background, when the task being rendered belongs to
/// one. `None` for a standalone task (D17) — [`build_context`] then omits the
/// epic-context section entirely rather than rendering an empty one.
#[derive(Clone, Copy)]
pub struct EpicContext<'a> {
    /// The epic's title.
    pub title: &'a str,
    /// The epic's free-text description, if recorded.
    pub description: Option<&'a str>,
    /// Product-planning context maintained live during planning, if any.
    pub product_context: Option<&'a str>,
    /// Technical-planning context maintained live during planning, if any.
    pub technical_context: Option<&'a str>,
}

/// Everything [`build_context`] needs to render the D8 prompt-context block:
/// this task's own spec fields, its epic's background (`None` for a
/// standalone task), the sibling manifest (empty for a standalone task, or
/// for an epic with no other tasks yet), and — T-530 — the task's `base_sha`,
/// when the caller wants the context to tell the agent where to diff from.
/// Plain borrowed data assembled by the caller — the worker builds this from
/// the DB; this struct and [`build_context`] do no I/O of their own.
///
/// Every field here is `Copy` (borrows and `Option`s of borrows only), so
/// `TaskContext` itself derives `Copy`: a caller that already built one for
/// `Stage::Implement` can cheaply produce a second, `base_sha`-bearing copy
/// for `Stage::Review` via struct-update syntax (`TaskContext { base_sha:
/// Some(sha), ..implement_ctx }`) instead of re-borrowing every field by hand.
#[derive(Clone, Copy)]
pub struct TaskContext<'a> {
    /// This task's own rendered-spec fields.
    pub spec: SpecFields<'a>,
    /// The parent epic's background, or `None` for a standalone task.
    pub epic: Option<EpicContext<'a>>,
    /// Every other task in the same epic (empty for a standalone task).
    pub siblings: &'a [SiblingTask<'a>],
    /// The task's `base_sha` — the commit this task branched from — when the
    /// caller wants [`build_context`] to tell the agent where to diff from
    /// (T-530's review stage: "run `git diff <base_sha>..HEAD` yourself to
    /// see the cumulative diff"). `None` renders **exactly** as this module
    /// did before this field existed (MILESTONE_2 T-530's AC): every other
    /// stage (`Implement`, `Fix`'s prompt doesn't even use `TaskContext`) has
    /// no cumulative-diff concept and simply omits this field's section.
    pub base_sha: Option<&'a str>,
}

/// The literal "don't touch this" framing for sibling tasks a later task
/// owns. Kept as a named constant (rather than inlined) so a test can assert
/// on the exact phrase — this sentence is the entire mechanism that stops an
/// autonomous implement/fix agent from building the rest of the epic in one
/// task, since D7 gives it no other way to learn the epic's scope.
const DO_NOT_IMPLEMENT_NOTICE: &str = "Do NOT implement, modify, or complete the tasks listed \
above under \"Owned by later tasks\" — each belongs to a separate pipeline run, and touching \
its territory here will cause conflicts and duplicate work. Implement only what this task's \
own spec requires.";

/// Build the D8 prompt-context block a task-stage agent sees: the rendered
/// spec, then (if present, T-530) the task's base-commit note, then (if
/// present) the epic's background, then (if any siblings exist) the sibling
/// manifest partitioned into "Already built" (done) and "Owned by later
/// tasks" (not done, with the explicit do-not-implement framing).
///
/// A standalone task with no epic, no siblings, and no `base_sha` renders to
/// just the rendered spec — no dangling empty headings. An epic with no
/// recorded product/technical context still gets an "Epic Context" section
/// (title + description if any) but skips the context sub-headings that
/// would otherwise be empty. An epic whose siblings are *all* done still
/// emits the "Owned by later tasks" heading with an explicit "(none — ...)"
/// rather than silently dropping it, so the section's absence is never
/// ambiguous with "no siblings at all". `base_sha`'s section is the newest
/// addition (T-530, closing the gap `prompts/review.md` already assumed was
/// closed): `ctx.base_sha` is `None` for every stage but `Review` today, and
/// `None` renders nothing at all — the byte-for-byte output for the implement
/// path (§2.1's frozen renderer output) is unchanged from before this field
/// existed.
pub fn build_context(ctx: &TaskContext) -> String {
    let mut out = render_spec(&ctx.spec);

    if let Some(sha) = ctx.base_sha {
        out.push_str("\n\n---\n\n## Base Commit\n\n");
        out.push_str(&format!(
            "This task branched from commit `{sha}`. Run `git diff {sha}..HEAD` yourself to \
             see the cumulative diff for this task — it may span several commits across review \
             rounds, so review the whole diff, not just the latest commit.\n"
        ));
    }

    if let Some(epic) = &ctx.epic {
        out.push_str("\n\n---\n\n## Epic Context\n\n");
        out.push_str("This task is part of a larger epic; the following is background, not this task's own spec.\n\n");
        out.push_str(&format!("Epic: \"{}\"\n", epic.title));
        if let Some(description) = non_empty_opt(epic.description) {
            out.push('\n');
            out.push_str(description);
            out.push('\n');
        }
        if let Some(product) = non_empty_opt(epic.product_context) {
            out.push_str("\n### Product Context\n");
            out.push_str(product);
            out.push('\n');
        }
        if let Some(technical) = non_empty_opt(epic.technical_context) {
            out.push_str("\n### Technical Context\n");
            out.push_str(technical);
            out.push('\n');
        }
    }

    if !ctx.siblings.is_empty() {
        let (done, pending): (Vec<&SiblingTask>, Vec<&SiblingTask>) =
            ctx.siblings.iter().partition(|s| s.done);

        out.push_str("\n\n---\n\n## Sibling Tasks\n\n");

        out.push_str("### Already built\n\n");
        out.push_str("These tasks in the same epic are already `Done` — treat them as ground truth about what already exists in the codebase.\n\n");
        if done.is_empty() {
            out.push_str("(none yet)\n");
        } else {
            for s in &done {
                out.push_str(&format!("- {} (id: {})\n", s.title, short_id(s.id)));
            }
        }

        out.push_str("\n### Owned by later tasks\n\n");
        if pending.is_empty() {
            out.push_str("(none — every other task in this epic is already done)\n");
        } else {
            for s in &pending {
                out.push_str(&format!("- {} (id: {})\n", s.title, short_id(s.id)));
            }
            out.push('\n');
            out.push_str(DO_NOT_IMPLEMENT_NOTICE);
            out.push('\n');
        }
    }

    out
}

/// Shorten an id for display, per §2.8's "last 6 of id" naming convention.
/// Returns the whole id unchanged if it's 6 characters or shorter.
///
/// `pub(crate)` rather than private: the T-513 DAG walk (`worker.rs`) reuses
/// this exact convention to build the `impl(<short task id>): <title>` commit
/// subject (§2.8) — the naming convention is decided in exactly one place,
/// never re-derived at the call site.
pub(crate) fn short_id(id: &str) -> &str {
    let len = id.len();
    if len <= 6 {
        id
    } else {
        &id[len - 6..]
    }
}

// ---- D9: verdict parsing --------------------------------------------------

/// The three verdicts an `Ask`-mode review/verify-complete stage can emit
/// (§2.2, D9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The task's acceptance criteria are met; close it.
    Pass,
    /// A fixable, in-scope defect remains; route to the fix stage.
    NeedsChanges,
    /// The task cannot proceed via a code fix; a human must intervene.
    Blocked,
}

impl Verdict {
    /// The exact `VERDICT:` token (and the exact string stored in
    /// `agent_run.verdict` / published in a `stage_changed` frame) for this
    /// verdict — the inverse of [`parse_verdict`]'s token matching, kept next
    /// to it so the two can never drift apart.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Pass => "PASS",
            Verdict::NeedsChanges => "NEEDS_CHANGES",
            Verdict::Blocked => "BLOCKED",
        }
    }
}

/// Parse an agent's raw output for the D9 verdict line: the **last** line
/// matching `^VERDICT:\s*(PASS|NEEDS_CHANGES|BLOCKED)\s*$`.
///
/// Notably **not** ralph's semantics (`references/ralph-v2.sh` parses the
/// first line) — Dearborn's `review` prompt asks for findings first and the
/// verdict last, so the last matching line is the one that counts; an earlier
/// mention (in the findings prose, or a contract reminder on re-run) is
/// deliberately overridable by a later, real verdict line.
///
/// The match is anchored to the start of the line (no leading whitespace
/// tolerated — a `VERDICT:` mentioned mid-sentence, or indented, does not
/// count as "its own line"); trailing whitespace after the token is
/// tolerated. The verdict token is matched case-sensitively — `PASS`, not
/// `pass` — so a lowercase verdict is rejected (treated as no match on that
/// line). Returns `None` if no line matches at all.
pub fn parse_verdict(output: &str) -> Option<Verdict> {
    let mut last = None;
    for raw_line in output.lines() {
        // Tolerate a stray CR from CRLF line endings without treating it as
        // part of the trailing whitespace the caller might be probing for.
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        let Some(rest) = line.strip_prefix("VERDICT:") else {
            continue;
        };
        let verdict = match rest.trim() {
            "PASS" => Verdict::Pass,
            "NEEDS_CHANGES" => Verdict::NeedsChanges,
            "BLOCKED" => Verdict::Blocked,
            _ => continue,
        };
        last = Some(verdict);
    }
    last
}

// ---- prompts (D6/D7/D9 content, `include_str!`-compiled) ------------------

use crate::task_agent::Stage;

const IMPLEMENT_PROMPT: &str = include_str!("../prompts/implement.md");
const FIX_PROMPT: &str = include_str!("../prompts/fix.md");
const REVIEW_PROMPT: &str = include_str!("../prompts/review.md");
const VERIFY_COMPLETE_PROMPT: &str = include_str!("../prompts/verify_complete.md");
const SUMMARIZE_PROMPT: &str = include_str!("../prompts/summarize.md");

/// The static prompt text for an agent stage — `include_str!`-compiled into
/// the binary at build time (D6), so fetching it is pure (no filesystem read
/// at call time). `None` for a non-agent stage (`Setup`/`Preflight`/
/// `TestGate`/`Commit`/`Push`), which has no prompt at all. The caller
/// ([`crate::task_agent::assemble_prompt`]) appends the D8 context block
/// ([`build_context`]) after this text before handing the whole thing to the
/// harness.
pub fn prompt_for(stage: Stage) -> Option<&'static str> {
    match stage {
        Stage::Implement => Some(IMPLEMENT_PROMPT),
        Stage::Fix => Some(FIX_PROMPT),
        Stage::Review => Some(REVIEW_PROMPT),
        Stage::VerifyComplete => Some(VERIFY_COMPLETE_PROMPT),
        Stage::Summarize => Some(SUMMARIZE_PROMPT),
        Stage::Setup | Stage::Preflight | Stage::TestGate | Stage::Commit | Stage::Push => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- render_spec --------------------------------------------------

    #[test]
    fn renders_full_spec_byte_for_byte() {
        let fields = SpecFields {
            title: "Add login form",
            description: Some("Users can sign in with email + password."),
            acceptance: Some("- Form renders\n- Invalid creds show an error"),
        };
        let expected = "# Add login form\n\n\
             ## Description\n\
             Users can sign in with email + password.\n\n\
             ## Acceptance Criteria\n\
             - Form renders\n- Invalid creds show an error";
        assert_eq!(render_spec(&fields), expected);
        // No trailing newline: the string ends immediately after the last
        // acceptance-criteria content.
        assert!(!render_spec(&fields).ends_with('\n'));
    }

    #[test]
    fn renders_none_provided_fallback_for_absent_fields() {
        let fields = SpecFields {
            title: "Bare task",
            description: None,
            acceptance: None,
        };
        let expected = "# Bare task\n\n\
             ## Description\n\
             (none provided)\n\n\
             ## Acceptance Criteria\n\
             (none provided)";
        assert_eq!(render_spec(&fields), expected);
    }

    #[test]
    fn whitespace_only_fields_are_treated_as_absent() {
        let fields = SpecFields {
            title: "Whitespace task",
            description: Some("   \n\t  "),
            acceptance: Some(""),
        };
        let expected = "# Whitespace task\n\n\
             ## Description\n\
             (none provided)\n\n\
             ## Acceptance Criteria\n\
             (none provided)";
        assert_eq!(render_spec(&fields), expected);
    }

    #[test]
    fn render_task_spec_matches_render_spec() {
        let task = crate::tasks::Task {
            id: "t1".to_string(),
            epic_id: None,
            project_id: "p1".to_string(),
            title: "From a Task row".to_string(),
            description: Some("desc".to_string()),
            acceptance: None,
            status: "Todo".to_string(),
            failure_reason: None,
            failure_detail: None,
            agent_session_id: None,
            position: None,
            branch_name: None,
            pr_url: None,
            pr_number: None,
            created_at: 0,
            updated_at: 0,
        };
        let expected = render_spec(&SpecFields {
            title: "From a Task row",
            description: Some("desc"),
            acceptance: None,
        });
        assert_eq!(render_task_spec(&task), expected);
    }

    // ---- build_context -------------------------------------------------

    fn spec(title: &str) -> SpecFields<'_> {
        SpecFields {
            title,
            description: Some("do the thing"),
            acceptance: Some("thing is done"),
        }
    }

    #[test]
    fn standalone_task_with_no_epic_and_no_siblings_is_just_the_spec() {
        let ctx = TaskContext {
            spec: spec("Standalone task"),
            epic: None,
            siblings: &[],
            base_sha: None,
        };
        let rendered = build_context(&ctx);
        assert_eq!(rendered, render_spec(&ctx.spec));
        assert!(!rendered.contains("Epic Context"));
        assert!(!rendered.contains("Sibling Tasks"));
        assert!(!rendered.contains("Base Commit"));
    }

    #[test]
    fn emits_epic_context_with_product_and_technical_context() {
        let ctx = TaskContext {
            spec: spec("Task in an epic"),
            epic: Some(EpicContext {
                title: "Checkout flow",
                description: Some("Let users pay."),
                product_context: Some("Users abandon carts at the payment step."),
                technical_context: Some("Stripe, webhook-driven confirmation."),
            }),
            siblings: &[],
            base_sha: None,
        };
        let rendered = build_context(&ctx);
        assert!(rendered.contains("## Epic Context"));
        assert!(rendered.contains("Checkout flow"));
        assert!(rendered.contains("Let users pay."));
        assert!(rendered.contains("### Product Context"));
        assert!(rendered.contains("Users abandon carts at the payment step."));
        assert!(rendered.contains("### Technical Context"));
        assert!(rendered.contains("Stripe, webhook-driven confirmation."));
    }

    #[test]
    fn epic_with_no_recorded_context_skips_empty_subsections() {
        let ctx = TaskContext {
            spec: spec("Task in a thin epic"),
            epic: Some(EpicContext {
                title: "Bare epic",
                description: None,
                product_context: None,
                technical_context: None,
            }),
            siblings: &[],
            base_sha: None,
        };
        let rendered = build_context(&ctx);
        assert!(rendered.contains("## Epic Context"));
        assert!(rendered.contains("Bare epic"));
        assert!(!rendered.contains("### Product Context"));
        assert!(!rendered.contains("### Technical Context"));
    }

    #[test]
    fn partitions_siblings_done_vs_not_yet_with_do_not_implement_framing() {
        let siblings = [
            SiblingTask {
                id: "task-aaaaa1",
                title: "Schema migration",
                done: true,
            },
            SiblingTask {
                id: "task-bbbbb2",
                title: "Client polish",
                done: false,
            },
        ];
        let ctx = TaskContext {
            spec: spec("Middle task"),
            epic: None,
            siblings: &siblings,
            base_sha: None,
        };
        let rendered = build_context(&ctx);

        assert!(rendered.contains("### Already built"));
        assert!(rendered.contains("Schema migration"));
        assert!(rendered.contains("### Owned by later tasks"));
        assert!(rendered.contains("Client polish"));

        // The load-bearing instruction: assert the exact phrase, so the
        // framing that stops an agent from building the whole epic can never
        // silently drift.
        assert!(rendered.contains(DO_NOT_IMPLEMENT_NOTICE));

        // Done sibling appears before the "owned by later tasks" list.
        let built_at = rendered.find("### Already built").unwrap();
        let owned_at = rendered.find("### Owned by later tasks").unwrap();
        let schema_at = rendered.find("Schema migration").unwrap();
        let polish_at = rendered.find("Client polish").unwrap();
        assert!(built_at < schema_at);
        assert!(owned_at < polish_at);
        assert!(schema_at < owned_at);
    }

    #[test]
    fn all_siblings_done_still_labels_the_owned_by_later_section_explicitly() {
        let siblings = [SiblingTask {
            id: "task-ccccc3",
            title: "Only sibling",
            done: true,
        }];
        let ctx = TaskContext {
            spec: spec("Last task"),
            epic: None,
            siblings: &siblings,
            base_sha: None,
        };
        let rendered = build_context(&ctx);
        assert!(rendered.contains("### Owned by later tasks"));
        assert!(rendered.contains("none"));
        // Nothing to warn about implementing when there's nothing pending.
        assert!(!rendered.contains(DO_NOT_IMPLEMENT_NOTICE));
    }

    #[test]
    fn short_id_takes_the_last_six_characters() {
        assert_eq!(short_id("01HXYZ9ABCDE"), "9ABCDE");
        assert_eq!(short_id("abc"), "abc");
    }

    // ---- base_sha (T-530) ------------------------------------------------

    #[test]
    fn present_base_sha_renders_a_diff_instruction() {
        let ctx = TaskContext {
            spec: spec("Reviewed task"),
            epic: None,
            siblings: &[],
            base_sha: Some("deadbeef1234"),
        };
        let rendered = build_context(&ctx);
        assert!(rendered.contains("## Base Commit"));
        assert!(rendered.contains("deadbeef1234"));
        assert!(rendered.contains("git diff deadbeef1234..HEAD"));
    }

    #[test]
    fn absent_base_sha_renders_identically_to_before_the_field_existed() {
        // T-530's AC: an absent base SHA must render exactly as it did before
        // this field existed — no "Base Commit" section, no byte-for-byte
        // change to the implement-path output.
        let with_none = TaskContext {
            spec: spec("Implement task"),
            epic: None,
            siblings: &[],
            base_sha: None,
        };
        let rendered = build_context(&with_none);
        assert!(!rendered.contains("Base Commit"));
        assert_eq!(rendered, render_spec(&with_none.spec));
    }

    // ---- parse_verdict ---------------------------------------------------

    #[test]
    fn parses_pass_after_preamble() {
        let output = "Reviewed the diff, looks solid.\nNo blocking issues.\nVERDICT: PASS";
        assert_eq!(parse_verdict(output), Some(Verdict::Pass));
    }

    #[test]
    fn multiple_verdict_mentions_the_last_one_wins() {
        let output = "\
First pass I thought VERDICT: BLOCKED but let me re-check.
Findings: [NIT] minor naming.
VERDICT: NEEDS_CHANGES
Actually on reflection everything required is met.
VERDICT: PASS";
        assert_eq!(parse_verdict(output), Some(Verdict::Pass));
    }

    #[test]
    fn tolerates_trailing_whitespace_after_the_token() {
        let output = "Findings here.\nVERDICT: NEEDS_CHANGES   \n";
        assert_eq!(parse_verdict(output), Some(Verdict::NeedsChanges));
    }

    #[test]
    fn tolerates_extra_whitespace_between_colon_and_token() {
        let output = "VERDICT:    BLOCKED";
        assert_eq!(parse_verdict(output), Some(Verdict::Blocked));
    }

    #[test]
    fn lowercase_verdict_token_is_rejected() {
        let output = "Findings.\nverdict: pass";
        assert_eq!(parse_verdict(output), None);
    }

    #[test]
    fn verdict_mentioned_mid_sentence_does_not_match() {
        let output = "The agent's VERDICT: PASS claim appeared mid-sentence, not on its own line.";
        assert_eq!(parse_verdict(output), None);
    }

    #[test]
    fn absent_verdict_returns_none() {
        let output = "Just some findings, no verdict line at all.";
        assert_eq!(parse_verdict(output), None);
    }

    #[test]
    fn garbage_after_the_token_does_not_match() {
        let output = "VERDICT: PASS because reasons";
        assert_eq!(parse_verdict(output), None);
    }

    #[test]
    fn realistic_preamble_laden_output_parses_all_three_verdicts() {
        // T-530's AC: all three verdicts parse from realistic, preamble-laden
        // reviewer output — prose findings with severity tags, a fenced code
        // block that itself mentions "VERDICT:", then the real trailing
        // verdict line.
        let pass = "\
I reviewed the cumulative diff against the task's acceptance criteria.\n\n\
[NIT] `foo.rs:12` — consider a shorter variable name, purely stylistic.\n\n\
Here's the relevant snippet for reference:\n\
```\n\
// VERDICT: NEEDS_CHANGES  <- this is just example text in a code fence\n\
fn foo() {}\n\
```\n\n\
Everything the acceptance criteria require is met; no in-scope defects.\n\n\
VERDICT: PASS";
        assert_eq!(parse_verdict(pass), Some(Verdict::Pass));

        let needs_changes = "\
Findings:\n\n\
[BLOCKING] `worker.rs:42` — the fenced UPDATE never checks the affected-row \
count, so a lost lease silently proceeds. Add an `if affected == 0 { return }` \
guard before the next write.\n\n\
[IMPORTANT] `worker.rs:80` — missing a doc comment explaining why this branch \
exists.\n\n\
VERDICT: NEEDS_CHANGES";
        assert_eq!(parse_verdict(needs_changes), Some(Verdict::NeedsChanges));

        let blocked = "\
[SPEC-CONFLICT] The acceptance criteria ask for a column this migration never \
adds — `blocked_reason` on `task`, not `epic`. This needs a human to resolve \
the spec before any code change can proceed.\n\n\
VERDICT: BLOCKED";
        assert_eq!(parse_verdict(blocked), Some(Verdict::Blocked));
    }

    // ---- Verdict::as_str ---------------------------------------------------

    #[test]
    fn verdict_as_str_round_trips_through_parse_verdict() {
        for verdict in [Verdict::Pass, Verdict::NeedsChanges, Verdict::Blocked] {
            let line = format!("VERDICT: {}", verdict.as_str());
            assert_eq!(parse_verdict(&line), Some(verdict));
        }
    }

    // ---- prompts -----------------------------------------------------

    /// Every agent stage, for tests that iterate over all five.
    const AGENT_STAGES: [Stage; 5] = [
        Stage::Implement,
        Stage::Fix,
        Stage::Review,
        Stage::VerifyComplete,
        Stage::Summarize,
    ];

    #[test]
    fn every_agent_stage_prompt_is_non_empty() {
        for stage in AGENT_STAGES {
            assert!(!prompt_for(stage).unwrap().trim().is_empty());
        }
    }

    #[test]
    fn non_agent_stages_have_no_prompt() {
        for stage in [
            Stage::Setup,
            Stage::Preflight,
            Stage::TestGate,
            Stage::Commit,
            Stage::Push,
        ] {
            assert_eq!(prompt_for(stage), None, "{stage:?} must have no prompt");
        }
    }

    #[test]
    fn review_and_verify_complete_prompts_state_the_verdict_contract() {
        assert!(prompt_for(Stage::Review).unwrap().contains("VERDICT:"));
        assert!(prompt_for(Stage::VerifyComplete)
            .unwrap()
            .contains("VERDICT:"));
    }

    #[test]
    fn implement_fix_and_summarize_prompts_do_not_ask_for_a_verdict() {
        // These stages never emit VERDICT lines (only review/verify_complete
        // do, per D9/T-532) — a cheap guard against a copy-paste mistake.
        assert!(!prompt_for(Stage::Implement).unwrap().contains("VERDICT:"));
        assert!(!prompt_for(Stage::Fix).unwrap().contains("VERDICT:"));
        assert!(!prompt_for(Stage::Summarize).unwrap().contains("VERDICT:"));
    }

    #[test]
    fn every_stage_prompt_mentions_add_comment_as_the_autonomous_tool_surface() {
        // D7: the only MCP tool an autonomous task agent gets is `add_comment`.
        for stage in AGENT_STAGES {
            assert!(prompt_for(stage).unwrap().contains("add_comment"));
        }
    }
}
