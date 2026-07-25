//! PR title/body construction (T-514, D16).
//!
//! Pure, dependency-light, no I/O — the same "pure leaf module" discipline
//! [`crate::spec`] holds itself to, so the template is unit-testable without
//! a database, a git checkout, or a network call. [`crate::worker`]'s
//! finalize step is the only caller: it gathers the epic + its tasks' commit
//! SHAs from the DB and hands them to [`build_pr_body`].
//!
//! ## D16: template now, agent summary later
//!
//! D16 calls for "deterministic template + an agent-written summary section,
//! with hard fallback to template-only." This module builds **only** the
//! template half — T-560 is the one that adds a `Stage::Summarize` agent run
//! and slots its output in. [`build_pr_body`] leaves a fixed marker,
//! [`SUMMARY_MARKER`], at the exact point that section belongs, specifically
//! so T-560 can find-and-replace (or insert around) that one line rather
//! than restructuring this function — the two tasks then never touch the
//! same lines of this file.

/// One task's line in the PR body's checklist.
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
}

/// The PR title: the epic's title, verbatim. Deliberately undecorated — the
/// branch name (`dearborn/<slug>-<id>`, §2.8) already carries the
/// Dearborn/epic-id identity, so the title stays exactly what a human named
/// the epic, matching ordinary PR-authoring convention (a short,
/// human-readable summary of the change, not a machine id).
pub fn epic_pr_title(epic_title: &str) -> String {
    epic_title.to_string()
}

/// The exact line [`build_pr_body`] emits for T-560 to find. Kept as a named
/// constant (rather than an inline literal duplicated between this module and
/// a future T-560 test) so the two tasks share one source of truth for the
/// marker text.
pub const SUMMARY_MARKER: &str = "<!-- dearborn:summary -->";

/// Render the deterministic PR body: the epic's description, a task
/// checklist (title + short id + commit SHA, or "no changes needed" for a
/// no-diff task), the [`SUMMARY_MARKER`] T-560 will replace, and a footer
/// naming Dearborn as the author.
pub fn build_pr_body(epic_description: Option<&str>, items: &[TaskChecklistItem]) -> String {
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

    out.push('\n');
    out.push_str(SUMMARY_MARKER);
    out.push('\n');
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

    #[test]
    fn body_renders_description_and_checklist_with_shas() {
        let items = vec![
            TaskChecklistItem {
                title: "Add the form".to_string(),
                short_id: "abc123".to_string(),
                commit_sha: Some("deadbeefcafefeed0000000000000000000000".to_string()),
            },
            TaskChecklistItem {
                title: "Already satisfied".to_string(),
                short_id: "def456".to_string(),
                commit_sha: None,
            },
        ];
        let body = build_pr_body(Some("Let users manage their profile."), &items);

        assert!(body.contains("## Description"));
        assert!(body.contains("Let users manage their profile."));
        assert!(body.contains("## Tasks"));
        assert!(body.contains("- [x] Add the form (`abc123`, `deadbee`)"));
        assert!(body.contains("- [x] Already satisfied (`def456`, no changes needed)"));
        assert!(body.contains(SUMMARY_MARKER));
        assert!(body.contains("Opened automatically by Dearborn."));
    }

    #[test]
    fn missing_description_falls_back_to_a_placeholder() {
        let body = build_pr_body(None, &[]);
        assert!(body.contains("(no description provided)"));
    }

    #[test]
    fn blank_description_falls_back_to_a_placeholder() {
        let body = build_pr_body(Some("   "), &[]);
        assert!(body.contains("(no description provided)"));
    }

    #[test]
    fn empty_task_list_renders_a_placeholder_line() {
        let body = build_pr_body(Some("desc"), &[]);
        assert!(body.contains("_(no tasks)_"));
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
        assert_eq!(parse_commit_sha_from_commit_log("not a commit log line"), None);
        assert_eq!(parse_commit_sha_from_commit_log(""), None);
        assert_eq!(parse_commit_sha_from_commit_log("commit : subject"), None);
    }
}
