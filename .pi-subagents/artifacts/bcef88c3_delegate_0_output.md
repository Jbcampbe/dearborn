# T-303 — Ready-lane DAG editor UI — Review

**Verdict: FIX-FIRST.** One blocker (a WS-payload/type mismatch that crashes the
task list on every live update and breaks the core live-editing AC), plus a few
minor issues. The REST client, reducer, composable, routing, and styling are
otherwise clean and closely mirror the established planning patterns.

---

## Blocker

### B1 — `dag_updated` WS frames carry plain `Task`s, not `DagNode`s → runtime crash + broken readiness after any live update
The client treats the `dag_updated` payload's `nodes` as `DagNode[]`
(`ready` + `blocked_by`), but the **server publishes plain `Task`s** on that frame:

- `deerborn-server/src/mcp.rs:572-586` `publish_dag` builds the payload from
  `list_tasks_for_epic` (returns `Vec<Task>` — no `ready`, no `blocked_by`):
  `let payload = json!({ "nodes": nodes, "edges": edges });`
- `CONVENTIONS.md:194` / `:216` confirm the asymmetry explicitly:
  `dag_updated` payload = `{ nodes: [Task], … }`, whereas
  `GET /epics/{id}/dag` (`:91`) returns `{ nodes: [DagNode] … }` (with readiness,
  via `compute_dag`, `tasks.rs:366`).

The editor hydrates from `GET /dag` (DagNodes, correct), then `applyDagFrame`
replaces `state.nodes` wholesale with the WS frame's plain `Task`s:

- `client/src/dag/stream.ts:65-70` — passes the `Array.isArray(dag.nodes)` guard
  (it *is* an array, just of `Task`) and assigns `state.nodes = dag.nodes`.

After the first `dag_updated` frame every node loses `ready`/`blocked_by`, so in
`client/src/components/DagEditorView.vue`:

- template `v-if="n.blocked_by.length"` (the "Blocked by" line) throws
  `TypeError: Cannot read properties of undefined (reading 'length')` → Vue
  render error, task list breaks.
- `readinessLabel(n)` (`n.ready ? …`) and `:data-ready="n.ready"` degrade — every
  Todo node renders as "blocked", ready borders/badges are lost.

This fires on the user's **own** edits (add/edit/delete/link all rely solely on
the `dag_updated` frame to refresh — there is no optimistic local update), so it
is not an exotic two-browser edge case; it breaks the primary AC ("the persisted
DAG reflects it" live) on the very first mutation.

**Fix (server, cleanest):** make `mcp::publish_dag` use `tasks::compute_dag`
(which already exists, T-302) so WS nodes carry readiness like the REST DAG, and
update `CONVENTIONS.md` to `nodes: [DagNode]`. **Or (client):** re-fetch
`getDag` on each frame, or defensively default `blocked_by`/`ready` in the
reducer — but the server fix keeps live readiness correct, which the AC wants.

---

## Minor

- **M1 — `dag.test.ts` masks the blocker.** `client/test/dag.test.ts` builds its
  `dag_updated` payloads with the local `node()` helper, which *includes*
  `ready`/`blocked_by` (`test/dag.test.ts:40-60`, `:83-92`). Real WS frames don't.
  No test folds a plain-`Task` `dag_updated` frame, so the suite gives false
  confidence and misses B1. Add a case that applies a frame whose nodes lack
  `ready`/`blocked_by` and asserts the view model stays renderable.

- **M2 — PATCH clears to `""`, never `null`, from the edit UI.** The double-option
  is wired correctly in `api/tasks.ts` and `saveEdit` always sends
  `description`/`acceptance`. But the textareas bind `v-model` to `editDescription`
  (`DagEditorView.vue:47-48`); emptying a textarea yields `""`, not `null`. So the
  "clear to NULL" path is never exercised — a cleared field is persisted as empty
  string. The in-code comments at `startEdit`/`saveEdit` ("saving sends `null`
  only when the field is emptied") are therefore inaccurate. Functionally benign
  (empty string vs NULL) but inconsistent with the server's documented semantics.

- **M3 — "read-mostly until Ready" hint is not enforced.** `DagEditorView.vue`
  shows a hint when `!isReady` but leaves every create/edit/delete/link control
  enabled; the server endpoints also don't gate on `status='Ready'`. The AC scopes
  editing to a `Ready` epic; the hint implies a restriction that doesn't exist.
  Either enforce (disable the forms when `!isReady`) or reword the hint.

- **M4 — reconnect does not re-hydrate.** `useDagStream` mirrors the planning
  backoff/reconnect exactly (good), but after a drop+reconnect it only resubscribes
  — any `dag_updated`/`epic_updated` missed while disconnected is lost, leaving a
  stale DAG until a manual reload. Same known limitation as the planning stream;
  worth a note since the DAG (unlike a chat) has no other refresh path.

## Nits

- **N1 — edge `:key="i"`** (`DagEditorView.vue`, edge list) uses the array index;
  `` `${e.blocker_id}-${e.blocked_id}` `` would be a stable key.
- **N2 — `props.id` in template.** `:to="{ …, params: { id: props.id } }"` works
  (top-level `const props` is exposed to the template in `<script setup>`), but a
  computed/`toRef` would be more idiomatic. Not a bug.
- **N3 — patch of a null-`epic_id` task publishes no frame** (`tasks.rs:520`
  guards on `epic_id`). Not reachable from this editor (all nodes are epic-scoped),
  so informational only.

---

## AC assessment

- "User edits tasks + dependencies of a Ready epic; persisted DAG reflects it" —
  REST calls and endpoints are correct and wired to the right routes; **but B1
  breaks the live reflection** (crash on first `dag_updated`). Fails until B1 is
  fixed.
- "Invalid edits (cycles, orphaned deps, missing title) blocked/surfaced" —
  Correct: cycle → 409, self/cross-epic → 400, missing title → 400 are all
  surfaced through `ApiError.message` via `error.value`; client also pre-checks
  self-edge and empty title. Good.
- Live cross-browser updates + reconnect/backoff parity with the planning stream —
  Subscription to `epic:<id>` and the reducer folding are structurally correct and
  match `useEpicStream`; blocked by B1 for the readiness fields and by M4 for
  missed-frame recovery.
- Wire-shape consistency with `tasks.rs` — REST DTOs match (`DagNode` flatten,
  `Dependency` fields, double-option PATCH); the **one** inconsistency is the
  `dag_updated` frame (B1).
- Style/discipline — module header comments, `ApiError.isAuth` bounce,
  `onBeforeUnmount` cleanup, scoped CSS, router singular-path rationale, and the
  `/tasks` dev proxy all mirror the existing conventions. Good.

## Residual risks
- If B1 is fixed client-side (defensive defaults) rather than server-side, live
  readiness will silently be wrong after edits even though the crash is gone —
  prefer the `compute_dag` fix in `publish_dag`.
- M4 means a flaky WS connection can leave the editor showing a stale DAG with no
  in-app recovery.