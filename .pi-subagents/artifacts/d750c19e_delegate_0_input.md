# Task for delegate

You are a read-only code reviewer. Review the **T-303 — Ready-lane DAG editor UI** change for the Deerborn project (uncommitted in the worktree at /Users/josiahcampbell/projects/personal/deerborn). This is MILESTONE_1.md §6 Phase 3, task T-303.

Read MILESTONE_1.md §6 (T-303 AC), deerborn-server/CONVENTIONS.md (the task/DAG REST routes and the `dag_updated`/`epic_updated` WS frames), and the existing client patterns (client/src/components/PlanningView.vue, client/src/planning/stream.ts, client/src/planning/useEpicStream.ts, client/src/api/epics.ts) to judge consistency.

Read these NEW/changed files in full:
- client/src/api/tasks.ts — the task DAG REST client.
- client/src/dag/stream.ts — the pure WS-event reducer.
- client/src/dag/useDagStream.ts — the WS composable.
- client/src/components/DagEditorView.vue — the editor component.
- client/test/dag.test.ts — the reducer unit tests.
- client/src/router/index.ts — the new `/epic/:id/tasks` route.
- client/src/components/PlanningView.vue — the added breakdown trigger + button.
- client/vite.config.ts — the `/tasks` dev proxy.

Check specifically:
1. AC compliance: a user can edit tasks and dependencies of a `Ready` epic and the persisted DAG reflects it; invalid edits (cycles, orphaned deps, missing title) are blocked/surfaced. Does the UI call the right endpoints and surface server errors (409 cycle, 400 cross-epic/self-edge)?
2. Live updates: does the editor subscribe to `epic:<id>` and fold `dag_updated`/`epic_updated` frames so two browsers see the same board update in real time? Does it match the planning stream's reconnect/backoff pattern?
3. Wire-shape consistency: do the TS DTOs match the server's Rust DTOs in deerborn-server/src/tasks.rs (DagNode flattened fields: ready, blocked_by; Dependency blocker_id/blocked_id)? Does patchTask correctly use the double-option (null = clear, string = set, absent = untouched)?
4. Reducer correctness: does applyDagFrame replace nodes+edges atomically on dag_updated and ignore malformed/unrelated frames? Are the dag.test.ts cases sufficient?
5. Style/discipline: does it mirror the existing code style (module comments, ApiError.isAuth bounce, onBeforeUnmount cleanup, scoped CSS)?
6. Any bugs, type holes, or missing edge cases (e.g. editing a task whose epic_id is null; the props.id template reference in Vue; race between REST hydrate and WS frames; the patchTask sending null vs empty string).

Do NOT modify any files — read-only review. Return a concise findings report: a verdict (ship / fix-first), a bulleted list of any issues by severity (blocker / minor / nit), and any AC gaps. Be specific with file:line where possible. Use `read` and `bash` (grep/rg) only.

---
**Output:**
Write your findings to exactly this path: /Users/josiahcampbell/projects/personal/deerborn/.pi-subagents/artifacts/outputs/d750c19e/file-only:/Users/josiahcampbell/projects/personal/deerborn/.pi-subagents/t303-review.md
This path is authoritative for this run.
Ignore any other output filename or output path mentioned elsewhere, including output destinations in the base agent prompt, system prompt, or task instructions.

## Acceptance Contract
Acceptance level: attested
Completion is not accepted from prose alone. End with a structured acceptance report.

Criteria:
- criterion-1: Return concrete findings with file paths and severity when applicable

Required evidence: review-findings, residual-risks

Finish with a fenced JSON block tagged `acceptance-report` in this shape:
Use empty arrays when no items apply; array fields contain strings unless object entries are shown.
```acceptance-report
{
  "criteriaSatisfied": [
    {
      "id": "criterion-1",
      "status": "satisfied",
      "evidence": "specific proof"
    }
  ],
  "changedFiles": [
    "src/file.ts"
  ],
  "testsAddedOrUpdated": [
    "test/file.test.ts"
  ],
  "commandsRun": [
    {
      "command": "command",
      "result": "passed",
      "summary": "short result"
    }
  ],
  "validationOutput": [
    "validation output or concise summary"
  ],
  "residualRisks": [
    "none"
  ],
  "noStagedFiles": true,
  "diffSummary": "short description of the diff",
  "reviewFindings": [
    "blocker: file.ts:12 - issue found, or no blockers"
  ],
  "manualNotes": "anything else the parent should know"
}
```