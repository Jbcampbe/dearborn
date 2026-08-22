//! PR title/body construction (T-514, D16).
//!
//! Pure, dependency-light, no I/O — the same "pure leaf module" discipline
//! [`crate::spec`] holds itself to, so the template is unit-testable without
//! a database, a git checkout, or a network call. [`crate::worker`]'s
//! finalize step is the only caller: it gathers the epic + its tasks' commit
//! SHAs (and, T-560, review-round counts and verify-complete reasoning) from
//! the DB and hands them to [`build_pr_body`], and separately runs a
//! `Stage::Summarize` agent turn (worker-side I/O this module deliberately
//! knows nothing about) whose text — or `None`, on any failure — is threaded
//! straight through as this function's own `summary` parameter.
//!
//! ## D16: template + agent summary, hard fallback to template-only
//!
//! D16 calls for "deterministic template + an agent-written summary section,
//! with hard fallback to template-only." [`build_pr_body`]'s `summary`
//! parameter is that fallback made structural rather than a discipline the
//! caller has to remember: it is a plain `Option<&str>`, not a `Result` or
//! anything the caller could propagate an error through, so there is no path
//! from "the summarize stage failed/timed out/produced nothing" to "the PR
//! doesn't open" that runs through this module at all — `worker::finalize_epic`/
//! `finalize_task` (T-560) simply pass `None` whenever the agent run didn't
//! produce something worth showing, and this function renders the template
//! exactly as it would with zero agent involvement. [`SUMMARY_MARKER`] is
//! still emitted unconditionally, right where the summary section belongs —
//! kept even now that this function fills the section in, both so a `None`
//! run still leaves a stable, greppable anchor in the body and so the two
//! literal tests (`body.contains(SUMMARY_MARKER)`) that predate this text
//! still hold unchanged.

/// One task's line in the PR body's checklist, plus (T-560) the two other
/// per-task scaffold elements §9 asks for: how many `Stage::Review` rounds it
/// went through, and — for a task closed with zero commits — the T-532
/// verify-complete reasoning that justified doing so. Both ride on this
/// struct rather than arriving as separate parallel slices/maps, because
/// both are facts *about a specific task*, exactly like `commit_sha` already
/// is; `crate::worker`'s DB-side gathering (`build_task_checklist`/
/// `build_standalone_checklist`) populates them from the same `agent_run`
/// evidence table it already reads for the commit SHA.
#[derive(Debug, Clone)]
pub struct TaskChecklistItem {
    pub title: String,
    /// The task's [`crate::spec::short_id`], included so a reader can
    /// correlate a checklist line with an `impl(<short id>): ...` commit
    /// subject without opening the task detail view.
    pub short_id: String,
    /// The commit SHA that landed for this task, if any. `None` for a task
    /// whose implement stage produced no diff (nothing to commit — the
    /// tracer-bullet AC in MILESTONE_2 §4; the real "verify this really means
    /// done" handling is T-532's job).
    pub commit_sha: Option<String>,
    /// How many completed `Stage::Review` rounds (T-530/T-531) this task went
    /// through — one `agent_run` row (`status = 'ok'`) per round, so this is
    /// a plain count, not the stage's own 0-based `attempt` value. `0` for a
    /// task that never reached review at all (T-532's `PASS`-on-first-look
    /// path skips review entirely — see `verified_complete_reasoning` below).
    pub review_rounds: u32,
    /// The `Stage::VerifyComplete` reasoning that closed this task with zero
    /// commits (T-532), when that's how it closed — `None` for a task that
    /// produced a diff and went through the ordinary implement/review path.
    /// This is the *log text* of the `PASS`ing verify-complete run, not just
    /// a boolean, because MILESTONE_2 §9 asks for "verified-already-complete
    /// **slices**" — the evidence itself, not merely a flag — mirroring
    /// T-532's own AC that this reasoning be visible to a human, just
    /// surfaced in the PR body instead of only the task's run history.
    pub verified_complete_reasoning: Option<String>,
}

/// The PR title: the epic's title, verbatim. Deliberately undecorated — the
/// branch name (`dearborn/<slug>-<id>`, §2.8) already carries the
/// Dearborn/epic-id identity, so the title stays exactly what a human named
/// the epic, matching ordinary PR-authoring convention (a short,
/// human-readable summary of the change, not a machine id).
pub fn epic_pr_title(epic_title: &str) -> String {
    epic_title.to_string()
}

/// Kept as a named constant (rather than an inline literal duplicated between
/// this module and the tests that assert on it) so there is one source of
/// truth for the marker text. Emitted unconditionally by [`build_pr_body`],
/// whether or not `summary` is present (see the module doc's D16 section) —
/// a stable, greppable anchor for the "Summary of changes" section whether
/// it's filled in or not.
pub const SUMMARY_MARKER: &str = "<!-- dearborn:summary -->";

/// The largest number of characters of T-532 verify-complete reasoning shown
/// per task in the PR body's "Verified already complete" section. §9 asks
/// for "slices," not the whole transcript — [`crate::evidence::cap_log`]
/// solves the analogous problem for the *stored* log (256 KB, head+tail);
/// this is the much smaller "one bullet in a PR description" version, keeping
/// only the head (verify-complete reasoning reads front-to-back, unlike a
/// back-and-forth agent transcript, so there's no tail worth preserving the
/// way a capped transcript's is).
const MAX_SLICE_CHARS: usize = 400;

/// Truncate `text` to at most [`MAX_SLICE_CHARS`] characters (not bytes —
/// this walks `char`s, so a multi-byte character is never split), appending
/// an ellipsis when cut. `text` shorter than the cap is returned unchanged
/// (trimmed).
fn truncate_for_pr_body(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= MAX_SLICE_CHARS {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(MAX_SLICE_CHARS).collect();
    format!("{head}…")
}

/// Render the deterministic PR body: the epic's description, a task
/// checklist (title + short id + commit SHA, or "no changes needed" for a
/// no-diff task), a "Review rounds" section (T-560, §9), a "Verified already
/// complete" section (T-560, §9), the [`SUMMARY_MARKER`] plus — when
/// `summary` is `Some` and non-blank — a "Summary of changes" section built
/// from it (T-560, D16), and a footer naming Dearborn as the author.
///
/// The two new §9 sections are each omitted entirely when nothing qualifies
/// (no task went through more than zero review rounds; no task closed via
/// verify-complete) rather than rendered with a "none" placeholder the way
/// `## Tasks` does for an empty list — an epic where every task took exactly
/// the ordinary implement → review → done path is the common case, and a
/// template that always prints two empty sections for it would be more noise
/// than signal. `## Tasks` keeps its placeholder because an *empty task
/// list* is itself a meaningful, surprising fact about the PR; "no task
/// needed more than one review round" is not.
///
/// `summary`, like `epic_description`, is trimmed and treated as absent if
/// blank — the same "hard fallback to template-only" contract applies to
/// this parameter as to every other optional text this function renders (see
/// the module doc's D16 section for why blank/`None` are deliberately
/// indistinguishable here).
pub fn build_pr_body(
    epic_description: Option<&str>,
    items: &[TaskChecklistItem],
    summary: Option<&str>,
) -> String {
    let mut out = String::new();

    out.push_str("## Description\n\n");
    match epic_description.map(str::trim).filter(|s| !s.is_empty()) {
        Some(desc) => {
            out.push_str(desc);
            out.push('\n');
        }
        None => out.push_str("(no description provided)\n"),
    }

    out.push_str("\n## Tasks\n\n");
    if items.is_empty() {
        out.push_str("_(no tasks)_\n");
    } else {
        for item in items {
            let detail = match &item.commit_sha {
                Some(sha) => format!("`{}`", short_sha(sha)),
                None => "no changes needed".to_string(),
            };
            out.push_str(&format!(
                "- [x] {} (`{}`, {})\n",
                item.title, item.short_id, detail
            ));
        }
    }

    let reviewed: Vec<&TaskChecklistItem> = items.iter().filter(|i| i.review_rounds > 0).collect();
    if !reviewed.is_empty() {
        out.push_str("\n## Review rounds\n\n");
        for item in reviewed {
            let noun = if item.review_rounds == 1 {
                "round"
            } else {
                "rounds"
            };
            out.push_str(&format!(
                "- {} (`{}`): {} review {}\n",
                item.title, item.short_id, item.review_rounds, noun
            ));
        }
    }

    let verified: Vec<&TaskChecklistItem> = items
        .iter()
        .filter(|i| i.verified_complete_reasoning.is_some())
        .collect();
    if !verified.is_empty() {
        out.push_str("\n## Verified already complete\n\n");
        for item in verified {
            let reasoning = item
                .verified_complete_reasoning
                .as_deref()
                .unwrap_or_default();
            out.push_str(&format!(
                "- **{}** (`{}`): {}\n",
                item.title,
                item.short_id,
                truncate_for_pr_body(reasoning)
            ));
        }
    }

    out.push('\n');
    out.push_str(SUMMARY_MARKER);
    out.push('\n');
    if let Some(summary) = summary.map(str::trim).filter(|s| !s.is_empty()) {
        out.push_str("\n## Summary of changes\n\n");
        out.push_str(summary);
        out.push('\n');
    }
    out.push_str("\n---\n_Opened automatically by Dearborn._\n");
    out
}

/// The first 7 hex characters of a full SHA (git's conventional "short SHA"
/// length), or the whole string if it is already shorter.
fn short_sha(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// Parse the commit SHA out of a `Stage::Commit` evidence row's `log`
/// (`"commit {sha}: {subject}"` — see `worker.rs`'s per-task commit step,
/// which writes exactly that format). A pure string parse, kept separate
/// from the DB query that fetches the row so the parsing logic is
/// unit-tested without any database at all.
pub fn parse_commit_sha_from_commit_log(log: &str) -> Option<&str> {
    let rest = log.strip_prefix("commit ")?;
    let (sha, _) = rest.split_once(':')?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn title_is_the_epic_title_verbatim() {
        assert_eq!(epic_pr_title("Ship the thing"), "Ship the thing");
    }

    /// A bare, "nothing extra going on" checklist item — the T-560 fields
    /// zeroed out — so tests about unrelated sections don't have to spell
    /// out `review_rounds`/`verified_complete_reasoning` themselves.
    fn plain_item(title: &str, short_id: &str, commit_sha: Option<&str>) -> TaskChecklistItem {
        TaskChecklistItem {
            title: title.to_string(),
            short_id: short_id.to_string(),
            commit_sha: commit_sha.map(str::to_string),
            review_rounds: 0,
            verified_complete_reasoning: None,
        }
    }

    #[test]
    fn body_renders_description_and_checklist_with_shas() {
        let items = vec![
            plain_item(
                "Add the form",
                "abc123",
                Some("deadbeefcafefeed0000000000000000000000"),
            ),
            plain_item("Already satisfied", "def456", None),
        ];
        let body = build_pr_body(Some("Let users manage their profile."), &items, None);

        assert!(body.contains("## Description"));
        assert!(body.contains("Let users manage their profile."));
        assert!(body.contains("## Tasks"));
        assert!(body.contains("- [x] Add the form (`abc123`, `deadbee`)"));
        assert!(body.contains("- [x] Already satisfied (`def456`, no changes needed)"));
        assert!(body.contains(SUMMARY_MARKER));
        assert!(body.contains("Opened automatically by Dearborn."));
        // Neither T-560 section applies to either item (no review rounds, no
        // verify-complete reasoning) — both are omitted entirely.
        assert!(!body.contains("## Review rounds"));
        assert!(!body.contains("## Verified already complete"));
        // No summary was supplied — no "Summary of changes" heading either.
        assert!(!body.contains("## Summary of changes"));
    }

    #[test]
    fn missing_description_falls_back_to_a_placeholder() {
        let body = build_pr_body(None, &[], None);
        assert!(body.contains("(no description provided)"));
    }

    #[test]
    fn blank_description_falls_back_to_a_placeholder() {
        let body = build_pr_body(Some("   "), &[], None);
        assert!(body.contains("(no description provided)"));
    }

    #[test]
    fn empty_task_list_renders_a_placeholder_line() {
        let body = build_pr_body(Some("desc"), &[], None);
        assert!(body.contains("_(no tasks)_"));
    }

    // ---- T-560: review rounds ----------------------------------------------

    #[test]
    fn review_rounds_section_lists_only_tasks_that_went_through_review() {
        let items = vec![
            TaskChecklistItem {
                review_rounds: 2,
                ..plain_item("Add the form", "abc123", Some("deadbeef"))
            },
            TaskChecklistItem {
                review_rounds: 1,
                ..plain_item("Tweak styling", "aaa111", Some("cafefeed"))
            },
            plain_item("Never reviewed", "bbb222", None),
        ];
        let body = build_pr_body(Some("desc"), &items, None);

        assert!(body.contains("## Review rounds"));
        assert!(body.contains("- Add the form (`abc123`): 2 review rounds"));
        // Singular "round" for exactly one.
        assert!(body.contains("- Tweak styling (`aaa111`): 1 review round"));
        // "Never reviewed" legitimately appears in `## Tasks`' own checklist;
        // it must not *also* show up inside the Review rounds section itself.
        let section_start = body.find("## Review rounds").unwrap();
        let section = &body[section_start..];
        let section_end = section[1..]
            .find("\n## ")
            .map(|i| i + 1)
            .unwrap_or(section.len());
        assert!(!section[..section_end].contains("Never reviewed"));
    }

    #[test]
    fn review_rounds_section_is_omitted_when_no_task_has_any() {
        let items = vec![plain_item("Add the form", "abc123", Some("deadbeef"))];
        let body = build_pr_body(Some("desc"), &items, None);
        assert!(!body.contains("## Review rounds"));
    }

    // ---- T-560: verified already complete ----------------------------------

    #[test]
    fn verified_complete_section_shows_the_reasoning_for_a_no_diff_task() {
        let items = vec![
            TaskChecklistItem {
                verified_complete_reasoning: Some(
                    "Already implemented in api.py; nothing further needed.".to_string(),
                ),
                ..plain_item("Add the endpoint", "ccc333", None)
            },
            plain_item("Add the form", "abc123", Some("deadbeef")),
        ];
        let body = build_pr_body(Some("desc"), &items, None);

        assert!(body.contains("## Verified already complete"));
        assert!(body.contains(
            "- **Add the endpoint** (`ccc333`): Already implemented in api.py; nothing further needed."
        ));
        // The section lists exactly the one qualifying task; the committed
        // one must not spuriously match here too.
        let section = &body[body.find("## Verified already complete").unwrap()..];
        let next_heading = section[1..]
            .find("\n## ")
            .map(|i| i + 1)
            .unwrap_or(section.len());
        assert!(!section[..next_heading].contains("Add the form"));
    }

    #[test]
    fn verified_complete_reasoning_is_truncated_to_a_slice() {
        let long_reasoning = "x".repeat(1_000);
        let items = vec![TaskChecklistItem {
            verified_complete_reasoning: Some(long_reasoning),
            ..plain_item("Add the endpoint", "ccc333", None)
        }];
        let body = build_pr_body(Some("desc"), &items, None);
        assert!(
            body.contains('…'),
            "an over-long reasoning slice must be truncated with an ellipsis"
        );
        // 400-char cap plus the ellipsis character; well under the full 1000.
        assert!(body.len() < 1_000);
    }

    #[test]
    fn verified_complete_section_is_omitted_when_no_task_qualifies() {
        let items = vec![plain_item("Add the form", "abc123", Some("deadbeef"))];
        let body = build_pr_body(Some("desc"), &items, None);
        assert!(!body.contains("## Verified already complete"));
    }

    // ---- T-560: agent summary -----------------------------------------------

    #[test]
    fn summary_renders_under_its_own_heading_after_the_marker() {
        let body = build_pr_body(Some("desc"), &[], Some("This epic adds profile editing."));
        assert!(body.contains(SUMMARY_MARKER));
        assert!(body.contains("## Summary of changes"));
        assert!(body.contains("This epic adds profile editing."));
        // The heading/content must follow the marker, not precede it — T-560
        // is meant to slot in *around* the marker, not relocate it.
        let marker_pos = body.find(SUMMARY_MARKER).unwrap();
        let heading_pos = body.find("## Summary of changes").unwrap();
        assert!(heading_pos > marker_pos);
    }

    #[test]
    fn absent_summary_leaves_only_the_bare_marker() {
        let body = build_pr_body(Some("desc"), &[], None);
        assert!(body.contains(SUMMARY_MARKER));
        assert!(!body.contains("## Summary of changes"));
    }

    #[test]
    fn blank_summary_is_treated_identically_to_absent() {
        let body = build_pr_body(Some("desc"), &[], Some("   \n  "));
        assert!(!body.contains("## Summary of changes"));
    }

    #[test]
    fn parses_sha_out_of_a_well_formed_commit_log_line() {
        let log = "commit deadbeefcafefeed0000000000000000000000: impl(abc123): Do the thing";
        assert_eq!(
            parse_commit_sha_from_commit_log(log),
            Some("deadbeefcafefeed0000000000000000000000")
        );
    }

    #[test]
    fn parse_sha_returns_none_for_an_unrecognized_format() {
        assert_eq!(
            parse_commit_sha_from_commit_log("not a commit log line"),
            None
        );
        assert_eq!(parse_commit_sha_from_commit_log(""), None);
        assert_eq!(parse_commit_sha_from_commit_log("commit : subject"), None);
    }
}
