# Design — Agent & Project Settings

Derived from a design interview (this document is the resolved record). Covers:
global agent settings, per-project per-agent-slot overrides, editable system
prompts, and the epic base branch. Follows the ARCHITECTURE.md conventions;
references to module files are relative to `dearborn-server/src/`.

---

## Scope in one paragraph

Make Dearborn's agent configuration runtime-editable instead of compile-time:
a global settings page chooses which coding-agent harnesses are enabled and the
default harness/model; each project can override harness, model, and system
prompt **per agent slot**; and each epic can be based on a chosen base branch
(defaulting to a project-level default) so epics can stack on unmerged work.
Everything else — pipeline step configuration, credential management, a second
live harness, WS broadcast of setting changes — stays deferred.

---

## 1. Agent slots: closed enum, fine-grained

- The unit of configuration is the **agent slot**: a closed Rust enum, mirroring
  the existing stage vocabulary. Exactly eight slots:

  | Key | Role today |
  |---|---|
  | `planning_product` | Interactive epic planning (product context) |
  | `planning_technical` | Interactive epic planning (technical plan) |
  | `breakdown` | One-shot epic → task DAG breakdown |
  | `implement` | Per-task implementation |
  | `fix` | Test-gate / review fix loop |
  | `review` | Review + VERDICT |
  | `verify_complete` | Completion check |
  | `summarize` | Task summary |

- Deliberately fine-grained (one config per slot, not grouped roles): most
  users leave everything on defaults, but full control is available. The UI
  shows all eight plainly.
- Closed enum, not an open registry: the pipeline in `worker.rs` is fixed in
  code, and a compile-time enum guarantees UI and worker can never disagree
  about what exists. A new slot arrives with a code change anyway.
- Slot keys serialize as stable snake_case strings on the wire (same pattern as
  the existing `Stage` vocabulary).

## 2. Harnesses

- Harness identity is a **string key** (`"claude"`, `"codex"`, …) everywhere —
  schema, API, settings rows — never a Rust-only enum crossing the wire.
- **v1 exposes Claude only** (`ClaudeTaskAgent`, `ClaudePlanningAgent`,
  `ClaudeBreakdownAgent` stay). The schema and picker are harness-ready;
  enabling Codex later is adapter wiring + tests + a picker row, not a
  migration. (`agent-harness` already compiles Claude/Codex/Bob adapters via
  default features.)
- **No credential management.** Hosts are assumed pre-authed (same stance as
  toolchains for `setup_cmd`). Deferred along with multi-harness support.
- Disabling a harness that any project slot still references is refused by the
  API (**409** with the referencing slots) — explicit cleanup, no silent
  fallback to another CLI mid-pipeline.

## 3. Resolution model

Effective config per slot resolves through three layers:

```
Layer 1 (global):   default harness + default-model-per-harness map
Layer 2 (project):  per-slot {harness?, model?, system_prompt?}, all nullable
Layer 3 (CLI):      model = NULL → the CLI's own configured default
```

- Global holds a **model map keyed by harness** (`{"claude": "…"}`), not a
  single model string, because model ids are harness-specific. With one
  enabled harness this looks like a single field but doesn't need revisiting.
- Inheritance is **harness-scoped**: effective model = project slot's model,
  else the global map entry for the *effective* harness, else NULL. Overriding
  a slot's harness without naming a model therefore drops to that CLI's own
  default rather than passing a foreign model string.
- The server computes and returns the **resolved effective value** for every
  slot (`GET /projects/{id}/agent-settings` returns raw overrides *and*
  effective `{harness, model, prompt_source: "override"|"default"}`). The
  layered scheme must be debuggable at a glance.

## 4. Editable prompts

- Editable text = the **instruction portion only**, exactly what
  `spec::prompt_for` returns today (including `PRODUCT_PLANNING_PROMPT` /
  `TECHNICAL_PLANNING_PROMPT` / the breakdown prompt for planning slots).
- Dearborn's machine-owned blocks are **never editable or reorderable**:
  `build_context` output (rendered spec, epic context, sibling manifest,
  base-SHA note) and fix-loop feedback are appended after the instruction text,
  unconditionally, exactly as `assemble_prompt` does now. Planning slots have
  no appended block (pure system prompt).
- Rationale: this bounds the blast radius. Worst case a user writes bad
  instructions; they cannot amputate the sibling manifest or spec. Template
  placeholders were rejected because a deleted placeholder silently drops the
  rendered spec — incompatible with the D8/D19 determinism discipline.
- **Reset = clear the override, don't copy.** Resetting deletes/NULLs the
  stored prompt; the effective value re-resolves live to the compiled
  `include_str!` default. Built-in prompt improvements from Dearborn updates
  then reach every non-overridden slot automatically.
- Guardrail for `review`: its default text carries the `VERDICT:` contract
  parsed by `spec.rs`. No server-side enforcement — instead the editor shows a
  warning ("this prompt must instruct the agent to emit a `VERDICT:` first
  line").
- Prompt editor is a plain textarea in v1 (no markdown preview — prompts are
  literal bytes, and rendering misleads about what the agent receives).

## 5. Base branch (home branch)

Reshaped during design: the project-level setting is a **default base branch**;
the **epic** is the point of commitment.

- Schema: `project.base_branch TEXT NULL` (NULL → repo default branch),
  `epic.base_branch TEXT NULL`.
- Epic creation (`POST /projects/{id}/epics`) accepts an optional validated
  `base_branch`; omitted → resolved from the project default at provision.
- **Set once at creation, immutable afterward** (Option B — keep it simple).
  Validation = `git ls-remote --heads origin <branch>` with the project PAT at
  creation time when explicitly provided; unknown branch → 400. Editing after
  creation is out of scope (rebasing an in-flight epic is future "iteration"
  work, not configuration).
- Three touch points, one concept:
  1. workspace provisioning branches off `origin/<epic.base_branch>`
     (replacing the unconditional `origin/HEAD` reset),
  2. epic branch creation inherits it,
  3. PR base (`open_pr`) targets `epic.base_branch` instead of the fetched
     repo default (this was the seam `git_host.rs` already named for v2).
- Resolution chain at provision: `epic.base_branch ?? project.base_branch ??
  repo default`; the snapshot is written onto the epic row when the branch is
  cut. Pre-existing epics (`base_branch NULL`) behave exactly as before.
- Accepted sharp edge: if an epic bases on another epic's `dearborn/…` branch
  and that base dies (cancelled, or squash-merged away), the dependent epic
  fails at finalize with the ordinary `pr_failed`/`workspace_error` routing.
  Nothing automatic; re-point by hand (cancel + recreate).

## 6. Storage

All state lives in libSQL — consistent with D12 (server DB is the source of
truth); env/config-file and JSON-file options rejected as second sources of
truth. Migration `0005_agent_settings.sql`:

- `global_settings`: single row, typed columns —
  `default_harness TEXT NOT NULL`, `default_models TEXT NOT NULL` (JSON map),
  `enabled_harnesses TEXT NOT NULL` (JSON array). Seeded with
  `enabled=["claude"]`, `default_harness="claude"`, `default_models={}` —
  i.e., byte-for-byte today's behavior out of the box.
- `agent_setting (project_id, slot)` PK, columns
  `harness TEXT NULL, model TEXT NULL, system_prompt TEXT NULL`. Absent row =
  inherit global everywhere.
- `project.base_branch`, `epic.base_branch` as above. No data migration.

Reset semantics throughout: reset = write NULL / delete row; nothing ever
copies defaults into rows.

## 7. API

| Endpoint | Purpose |
|---|---|
| `GET /settings` / `PUT /settings` | Global agent settings. PUT validates: default must be enabled; disabling a referenced harness → 409 |
| `GET /projects/{id}/agent-settings` | All eight slots; raw overrides + server-resolved effective values |
| `PUT /projects/{id}/agent-settings/{slot}` | Partial update; `null` fields clear that override (= reset) |
| `POST /projects/{id}/epics` | Gains optional validated `base_branch` |

No dedicated reset endpoints anywhere. Model strings are free text passed
verbatim to `RunTuning.model` (trimmed, non-empty check only); a bogus id
surfaces as an ordinary `agent_error` with the CLI's message in the stage log —
no model catalog, they churn too fast.

## 8. Client (Vue)

- **Global:** new top-level Settings view (alongside Projects in `AppShell`):
  harness toggles, default-harness selection, model-per-harness inputs.
- **Project:** Settings tab in `ProjectDetailView`: project default base branch
  at top, then eight slot cards — harness select, model input, "Edit prompt"
  modal (textarea + **Reset to default**), and an always-visible effective line
  ("runs on claude · sonnet-4-5 · custom prompt" / "· default prompt").
- **Epic creation:** optional Base branch field in `CreateEpicModal`.

## 9. Runtime semantics & evidence

- **Live-read:** every stage run reads effective config at spawn; the running
  stage finishes on what it started with, the next stage picks up edits.
  Matches how specs/retries already behave and needs no invalidation machinery.
- **Evidence:** `agent_run` gains `harness`, `model`, and a **hash** of the
  resolved instruction prompt at spawn (text itself optional via the log blob
  mechanism). Without this, live-read makes historical runs unauditable —
  preserves ARCHITECTURE §7's evidence principle under configurable agents.
- Changing `project.base_branch` affects new epics only (epics snapshot their
  base; §5).
- **No WS broadcast of setting changes** (deferred): rare, self-inflicted
  changes; refetch-on-navigation suffices in v1.

## 10. Explicitly deferred

Second-harness enablement (Codex/Bob) · credential/API-key management ·
pipeline-step configuration (skipping stages, loop counts) · WS broadcast of
settings · rebasing in-flight epics onto a new base · markdown/rich prompt
editor · automatic handling of dead stacked bases.

---

## Task breakdown

Ordered by dependency; each task is one reviewable unit. `[B]` = backend,
`[C]` = client, `[T]` = tests ride along unless called out separately.

### Phase 1 — Schema & core resolution (backend foundation)

- [x] **T-1. Migration `0005_agent_settings.sql`** — `global_settings` singleton
      table (seeded: `enabled_harnesses=["claude"]`, `default_harness="claude"`,
      `default_models={}`), `agent_setting` table (`(project_id, slot)` PK;
      nullable `harness`/`model`/`system_prompt`), `project.base_branch TEXT NULL`,
      `epic.base_branch TEXT NULL`. Register in `db.rs`. No data migration.
- [x] **T-2. Agent-slot enum** — closed Rust enum of the eight slots with stable
      snake_case wire keys (`serde`), plus validation of arbitrary strings into
      slots (for path params / rows). New module (e.g. `agent_slot.rs`).
- [x] **T-3. Global-settings store** — typed read/write of the singleton row
      (`default_harness`, `default_models` JSON map, `enabled_harnesses` JSON
      array); seed-if-absent guard at boot for pre-migration DBs is unnecessary
      (seed lives in SQL) but the accessor must tolerate an empty table.
- [x] **T-4. Effective-config resolver** — pure, unit-tested function:
      `(global, Option<agent_setting>) → {harness, model, system_prompt,
      prompt_source}` implementing §3's harness-scoped inheritance (model map
      keyed by *effective* harness). This is the heart of the feature — test
      every null-combination exhaustively.
- [x] **T-5. Agent-setting store** — CRUD for `agent_setting` rows keyed by
      `(project_id, slot)`; "reset" semantics = delete/NULL, never copy defaults.

### Phase 2 — Prompts & evidence wiring (backend)

- [x] **T-6. Prompt override plumbing** — `spec::prompt_for` (task stages) and
      the planning/breakdown system-prompt sites accept an optional override
      string; when absent, compiled defaults serve exactly as today. Compute a
      content hash of the resolved instruction text alongside it.
      `assemble_prompt` composition order unchanged (instruction → context →
      feedback).
- [x] **T-7. Harness/model at spawn** — thread effective `{harness, model}` into
      all three spawn sites (`ClaudeTaskAgent`, `ClaudePlanningAgent`,
      `ClaudeBreakdownAgent`) so `RunTuning.model` carries the resolved model.
      v1: any non-`"claude"` key resolves to an error at spawn-validation time
      (unreachable via API until a second harness ships, but fail loudly).
- [x] **T-8. Evidence columns** — add `harness`, `model`, `prompt_hash` to
      `agent_run`; write them at stage spawn (all agent stages incl. planning
      runs). Backfill not needed (NULL = predates feature).
- [x] **T-9. Live-read verification** — test that a mid-epic settings change is
      picked up by the *next* stage run and that the running stage is unaffected
      (spawn-time read, no caching).

### Phase 3 — Settings API (backend)

- [x] **T-10. `GET /settings` / `PUT /settings`** — full replace or partial
      update of globals; validate default ∈ enabled set; refuse disabling a
      harness referenced by any `agent_setting` row (409 + referencing slots);
      model values trimmed/non-empty.
- [x] **T-11. `GET /projects/{id}/agent-settings`** — all eight slots in one
      response: raw overrides **plus** server-resolved effective values
      (`prompt_source: "override"|"default"`).
- [x] **T-12. `PUT /projects/{id}/agent-settings/{slot}`** — partial update;
      `null` clears that field (= reset); unknown slot → 404; harness must be
      enabled globally.

### Phase 4 — Base branch (backend)

- [ ] **T-13. Epic-create `base_branch`** — `POST /projects/{id}/epics` accepts
      optional `base_branch`; validated via `git ls-remote --heads origin`
      with the project PAT (400 on miss); stored on the epic row. No PATCH
      surface (immutable after creation).
- [ ] **T-14. Provision-time resolution & snapshot** — chain
      `epic.base_branch ?? project.base_branch ?? repo default`; provisioning
      branches off `origin/<resolved>` instead of unconditional `origin/HEAD`
      (`git.rs::refresh_repo` gains a branch parameter); persist the resolved
      value onto `epic.base_branch` when the branch is cut.
- [ ] **T-15. PR base** — `open_pr` targets the epic's resolved base branch
      (from the epic row / finalize-time resolution) instead of the fetched
      repo default; remove/retire the `fetch_default_branch` call on that path.

### Phase 5 — Client

- [ ] **T-16. API layer** — `client/src/api/settings.ts` (+ project
      agent-settings calls): types mirroring the wire format (slot keys as
      string literals union), fetchers for all four endpoints.
- [ ] **T-17. Global Settings view** — new route + nav entry in `AppShell`:
      harness toggles, default-harness selection, model-per-harness inputs;
      save via `PUT /settings`; show 409 reference errors inline.
- [ ] **T-18. Project Settings tab** — tab in `ProjectDetailView`: default base
      branch field; eight slot cards (harness select limited to enabled
      harnesses, model input, effective-values line); Edit-prompt modal with
      textarea, VERDICT warning on the `review` slot, and Reset-to-default
      (writes `null`).
- [ ] **T-19. CreateEpicModal base branch** — optional field, surfaced only as
      advanced/optional input; passes through to epic-create; server-side
      validation error shown inline.

### Phase 6 — Hardening & docs

- [ ] **T-20. API/integration tests** — global PUT validation matrix (disable-
      referenced 409, default-not-enabled); slot PUT reset semantics; epic
      create with bad `base_branch` (400); provision + PR-base against a fake
      remote exercising the resolution chain (epic NULL → project NULL →
      repo default).
- [ ] **T-21. Docs** — ARCHITECTURE.md pointer to this design doc (§14 git-host
      note about the retired live-default-branch lookup; §10/§11 agent runtime
      notes); SCRATCHPAD: strike the Global/Project settings bullets this
      implements, keep the deferred ones.
