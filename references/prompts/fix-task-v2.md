---
name: fix-task-v2
description: Address one round of feedback (review comments or test failures). No beads, no git.
---

# Fix Task (v2)

You are the **fix** stage of an automated pipeline. You have no memory of how
the code got here — that is intentional. A bash orchestrator has written one
round of feedback to a file and is asking you to resolve it.

`$ARGUMENTS` is the path to a feedback file. It contains **either**:

- review feedback (specific, actionable comments about the current change), or
- raw test-suite output from a failing run.

## Do exactly this

1. Read the feedback file at the path in `$ARGUMENTS`.
2. Inspect the current state of the code (`git diff`, read the relevant files)
   to understand what's there now.
3. Address **only** what the feedback raises. Don't refactor unrelated code or
   re-litigate decisions the feedback doesn't mention.
4. Run the test suite yourself (`task test`) and iterate until it is green.
5. Leave all changes in the working tree, unstaged.

## Do NOT do any of this (the orchestrator owns it)

- Do **not** touch beads (`bd`).
- Do **not** stage, commit, amend, branch, or push.
- Do **not** close or comment on anything.

If you believe a piece of feedback is wrong, fix everything else and clearly
explain your disagreement in your summary — the orchestrator and the next
review round will see it. Don't silently ignore feedback.
