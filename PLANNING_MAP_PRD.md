# PRD — Planning Map & Living Document (wayfinder-style epic planning)

---

## Summary

Replaces the current linear epic-planning workflow (product planning →
technical planning → one-shot breakdown) with a **map of decision nodes** worked
one at a time, each resolution **evolving a living HTML Document** that the
existing breakdown step then reads to emit the executor task DAG. Inspired by the
`wayfinder` skill (`matt-pocock-skills/wayfinder/SKILL.md`).


---

## Mockup

https://claude.ai/code/artifact/58108b40-0c10-4a75-9d41-359b5a9bd904

---

## 1. Motivation & framing

Today an epic is planned by two long, linear agent chats (`PlanningProduct`,
`PlanningTechnical`) whose transcripts feed a one-shot `breakdown` agent that
writes the task DAG (`dearborn-server/src/planning.rs`,
`dearborn-server/src/breakdown.rs`). A big, foggy idea doesn't fit that shape:
the *decisions* aren't visible up front, and a single session can't hold them.

Wayfinder reframes planning as **charting a route through fog**: a shared **map**
of *decision tickets* (questions whose resolution is a decision, not a slice of
build), resolved one per session until the way to the **destination** is clear.
We adopt that model natively in Dearborn — Dearborn itself is the tracker — and
add a **living Document** as the settled-decisions deliverable.

**The loop:** type a destination → a seed grilling node begins mapping → work the
frontier one node at a time → each resolution edits the Document and graduates
the next layer of nodes → when nothing is left to decide, a human confirms and
**breakdown reads the Document** → executor builds. The executor half
(Milestone 2, the task DAG) is **unchanged**.

---

## 2. Goals / non-goals

**Goals (v1)**
- Replace product/technical planning + breakdown-from-transcripts with the
  map + living Document workflow.
- All four wayfinder node types: **grilling, research, prototype, task**.
- Multi-user, flat-permission participation with full attribution.
- Harness-agnostic agent tooling (Claude **and** `pi`) via a `dearborn` CLI.
- A fresh graph-based Map UI, a Document view, and a shared comment panel.

**Non-goals (v1 → deferred to v2)**
- Sanitization of agent-authored Document HTML (§12).
- Multi-*human* org features: per-epic membership, functional roles, a "driver",
  presence avatars, animations.
- Dearborn-internal git versioning of the Document (DB blob + version table
  suffices).
- Refactoring the executor **task DAG** to share the new Map look/feel (intended
  follow-up, §11).

---

## 3. Core concepts

| Concept | What it is |
|---|---|
| **Destination** | Human-typed statement of what the finished plan looks like. Fixes scope. `epic.destination`. |
| **Map** | The graph of decision **nodes** + dependency edges for one epic. |
| **Node** | One decision/investigation, sized to a single agent session. Kinds: grilling / research / prototype / task. |
| **Fog** (`not_yet_specified`) | Prose describing in-scope decisions not yet sharp enough to be nodes. **Never nodes.** `epic.not_yet_specified`. |
| **Out of scope** | Work ruled beyond the destination. A terminal node state **and** a prose line. `epic.out_of_scope`. |
| **Document** | The living, sectioned **HTML** spec the map produces. Source of truth for its prose. Read by breakdown. |
| **Frontier** | Open, dependency-unblocked nodes — **computed**, not stored. |
| **Decisions so far** | Derived view: the one-line gists of resolved nodes. |

The wayfinder "map body" dissolves into epic fields: `destination`, `notes`,
`not_yet_specified`, `out_of_scope` (all short prose, edited via CLI). These
**replace** the vestigial `product_context` / `technical_context` columns.

---

## 4. Data model

New tables mirror the *shape* of `task`/`task_dependency` but are semantically
distinct — **planning nodes and implementation tasks evolve separately** and must
not share a table (the executor's `idx_task_claim`, readiness computation, and
lease/claim stay untouched).

### 4.1 `map_node`
```
id            TEXT PRIMARY KEY
epic_id       TEXT NOT NULL REFERENCES epic(id)
kind          TEXT NOT NULL   -- grilling | research | prototype | task
task_mode     TEXT            -- for kind=task only: afk | hitl  (fixed at creation)
state         TEXT NOT NULL   -- open | in_progress | resolved | out_of_scope
title         TEXT NOT NULL
question      TEXT            -- the decision/investigation this node resolves
gist          TEXT            -- one-line resolution answer (set on resolve)
out_of_scope_reason TEXT
created_by    TEXT REFERENCES user(id)
resolved_by   TEXT REFERENCES user(id)
position_x    REAL            -- graph layout (nullable; auto-layout may own this)
position_y    REAL
created_at    INTEGER NOT NULL
updated_at    INTEGER NOT NULL
```
- **Readiness is computed, not stored:** `frontier = open ∧ all deps resolved`;
  `blocked = open ∧ some dep unresolved`. Mirrors `task` ("readiness is COMPUTED
  from deps").
- No `fog` state — fog is prose (§3).
- No exclusive claim column — see §7 (per-node run-lock + `in_progress` soft
  signal).

### 4.2 `map_node_dependency`
```
blocker_id TEXT NOT NULL REFERENCES map_node(id)
blocked_id TEXT NOT NULL REFERENCES map_node(id)
PRIMARY KEY (blocker_id, blocked_id)
```

### 4.3 `node_session` (resume handle, node-scoped)
```
node_id            TEXT PRIMARY KEY REFERENCES map_node(id)
harness_session_id TEXT            -- native resume handle; NULL until first run
status             TEXT NOT NULL   -- active | complete
created_at INTEGER NOT NULL
updated_at INTEGER NOT NULL
```
Replaces `planning_session`'s `(epic_id, phase)` keying. AFK/no-engine nodes may
never create a row.

### 4.4 `node_message` (multi-party transcript, node-scoped)
```
id            TEXT PRIMARY KEY
node_id       TEXT NOT NULL REFERENCES map_node(id)
role          TEXT NOT NULL       -- user | agent | tool | system
actor_user_id TEXT REFERENCES user(id)   -- which human posted (NULL for agent/tool/system)
content       TEXT NOT NULL       -- text or serialized RunEvent
seq           INTEGER NOT NULL    -- monotonic per node
created_at    INTEGER NOT NULL
```
Any user may `POST /nodes/:id/messages`; the per-node run-lock serializes agent
replies (§7). Replaces `transcript_message`.

### 4.5 `document` + `document_version`
```
document(epic_id PK REFERENCES epic(id), html TEXT NOT NULL, version INTEGER NOT NULL,
         last_edited_by TEXT REFERENCES user(id), updated_at INTEGER NOT NULL)
document_version(epic_id, version, html, editor_user_id, node_id, created_at,
                 PRIMARY KEY(epic_id, version))
```
- One HTML blob per epic, source of truth for its prose. `version` powers the
  `vNN` lineage + diffs. Sections are delimited by stable HTML `id`/`data-`
  attributes; `document_section` (below) keys on those for anchoring/provenance.
- **Last-writer-wins**: agents *evolve* the document with surgical edits, they do
  not regenerate-and-clobber, so LWW is safe.

### 4.6 `document_section` (anchor/provenance index over the HTML)
```
epic_id     TEXT NOT NULL REFERENCES epic(id)
section_id  TEXT NOT NULL          -- matches an id= attribute in document.html
title       TEXT
provenance  TEXT                   -- node_id(s) that wrote/touched it (many→one)
last_edited_by TEXT REFERENCES user(id)
version     INTEGER
PRIMARY KEY (epic_id, section_id)
```

### 4.7 `node_asset` (prototype artifacts, linked not inlined)
```
id       TEXT PRIMARY KEY
node_id  TEXT NOT NULL REFERENCES map_node(id)
mime     TEXT NOT NULL
bytes    BLOB NOT NULL             -- (or a path; reuse evidence.rs blob store if it fits)
label    TEXT
created_at INTEGER NOT NULL
```
Check `evidence.rs` for an existing blob store to reuse before adding this;
reuse only if it genuinely fits.

### 4.8 `comment` (overhaul)
Replace the flat `comment` table:
```
id            TEXT PRIMARY KEY
epic_id       TEXT NOT NULL REFERENCES epic(id)
thread_id     TEXT NOT NULL              -- threading
anchor_kind   TEXT NOT NULL             -- node | section
anchor_id     TEXT NOT NULL             -- map_node.id or document_section.section_id
author_user_id TEXT REFERENCES user(id)  -- NULL when author is the agent
is_agent      INTEGER NOT NULL DEFAULT 0
body          TEXT NOT NULL
resolved      INTEGER NOT NULL DEFAULT 0
promoted_node_id TEXT REFERENCES map_node(id)  -- set when a thread is promoted
created_at    INTEGER NOT NULL
```

### 4.9 `activity` (attribution / provenance feed)
```
id INTEGER PK, epic_id, node_id (nullable), actor_user_id, action, detail, created_at
```
Append-only. Powers "assembled from N resolved nodes", participants avatars
(derived = distinct actors), and history. Per-row `created_by`/`last_edited_by`
cover inline attribution; `activity` covers the feed.

### 4.10 `epic` changes
- **Add:** `destination`, `notes`, `not_yet_specified`, `out_of_scope`.
- **Drop (clean cutover):** `product_context`, `technical_context`.
- `status` unchanged (`Planning|Ready|InProgress|…`); breakdown still owns
  `Planning → Ready` (ARCHITECTURE §11).

---

## 5. Node types & engines

Two run engines, reused **opportunistically** (a full engine rebuild is
acceptable where reuse doesn't help). Both already exist:
`planning.rs` (interactive: resume, multi-turn, live `RunEvent`→WS, one-run-in-
flight) and `breakdown.rs` (one-shot: no resume, no transcript, live→WS).

| Kind | HITL/AFK | Engine | May reshape map? | Notes |
|---|---|---|---|---|
| **grilling** | HITL | interactive, node-scoped | **yes** | Primary frontier-builder (§6). Seed node is a normal grilling node. |
| **prototype** | HITL | interactive + scratch workspace | yes | Builds a throwaway artifact → `node_asset`, linked from node, rendered in sandboxed iframe. |
| **research** | AFK | one-shot | **no** — reports facts only | Fired in parallel; unattended. |
| **task** | AFK **or** HITL (fixed at creation) | AFK: one-shot; HITL: **no engine** (human checklist) | no | Manual work unblocking a decision; facts recorded in `gist`. |

**Methodologies are prompts, not Skill-tool calls.** Each kind's system prompt
carries its method (grilling+domain-modeling, research, prototype), adapted from
`matt-pocock-skills` as *source material*. This keeps behavior harness-agnostic
(identical on Claude and `pi`) and Dearborn-owned/versioned. Same pattern as
today's per-phase `PlanningConfig`. Charting is **not** a kind (§8).

**Determinism seam:** mirror the existing `PlanningAgent` / `BreakdownAgent`
traits so the new engines accept scripted-fake doubles under `cargo test`.

---

## 6. Grilling: the map-building act

A grilling resolution does up to five things (via CLI allow-list + native file
edits):
1. **Record decision** — set `gist`, mark node `resolved`.
2. **Edit the Document** — surgical HTML edits folding in the decision.
3. **Graduate fog → new nodes** — create the next frontier layer (`create_node`
   + `link_dependency`), trim `not_yet_specified`. *This is how the map grows.*
4. **Rule out of scope** — create+close an `out_of_scope` node + one-line prose.
5. **Invalidate/update** other nodes if the decision changed the map.

Grilling = **charting's map-authoring capability + node resolution**; prototype
shares the map-authoring bundle. **AFK types (research, AFK-task) never reshape
the map** — map mutations are HITL-only, so an unattended agent can't silently
redraw the frontier overnight.

---

## 7. Sessions & concurrency

- **Per-node sessions.** Each node owns its resume handle (`node_session`),
  transcript (`node_message`), and WS topic `node:<id>`. The epic's single linear
  transcript is retired; the epic surface is the map + the union of node sessions.
- **Parallel by default.** The "one run in flight" lock moves from **per-epic to
  per-node** — unblocked frontier nodes are worked concurrently.
- **Multi-party node sessions.** Any user posts into a node's conversation
  (attributed via `node_message.actor_user_id`); the per-node run-lock serializes
  the agent's replies. `state=in_progress` is a soft "being worked" signal, not a
  lock. **No exclusive claim.**
- **Document write = per-epic semaphore.** An in-process `tokio::Mutex` keyed by
  `epic_id` in `AppState` (same pattern as today's in-flight set). Single server
  process (no horizontal scaling) makes the in-process lock sufficient; SQLite
  already serializes writers.
- **Edits confined to the resolution step.** A session grills/thinks unlocked;
  only its *resolution edit* takes the semaphore — a bounded read→edit→commit —
  so siblings never stall. Base-`version` check on commit; anchor-based edits
  make a moved anchor a clean re-read/retry rather than a bad write.

---

## 8. Lifecycle

1. **Create epic** — user types `title` + `destination` (+ optional `notes`).
   No agent "charting" session. System auto-seeds **one grilling node** (`open`),
   using the **standard grilling prompt** (it infers "I'm first" from the empty
   map — kept identical; revisit only if it fails in practice). Document starts
   empty.
2. **Work the frontier** — one node per session (§5/§6). Research nodes fire in
   parallel, unattended. Comments/promotions flow throughout (§9).
3. **Completion** — eligible when **no open nodes ∧ `not_yet_specified` empty**.
   UI surfaces "the way is clear — ready to break down"; a **human explicitly
   triggers** breakdown (preserves today's approve gate). Disagree it's done? Add
   nodes (directly or via promote).
4. **Breakdown** — the existing one-shot engine reads the **settled Document**
   (not transcripts), emits the task DAG via **CLI** tools, flips
   `Planning → Ready`. Executor unchanged from here.

---

## 9. Multi-user, comments, promotion

- **Flat permissions.** Every authenticated user can do everything on every
  epic. No driver, roles, or membership. Participants = derived distinct actors.
- **Attribution everywhere** — `created_by`/`last_edited_by` per row + the
  `activity` feed.
- **Comments** — threaded, anchored to a **node** or a **document section**,
  user-attributed, agent may reply, `resolved` flag.
- **Promote-to-node** — a comment thread becomes a new frontier node of a chosen
  kind (grilling/research/prototype), carrying optional extra context; records
  `promoted_node_id`.

---

## 10. Agent tooling — the `dearborn` CLI

- **One binary, additional subcommands.** `dearborn serve` (today) plus
  agent-facing verbs. A thin **authenticated REST client** over the existing HTTP
  API, scoped by the existing **capability-token** mechanism (`mcp.rs`), reused
  as the CLI bearer.
- **Harness-agnostic** (bash → works on Claude, `pi`, future agents). **MCP is
  retired entirely** — planning *and* breakdown move to the CLI.
- **Map/state operations via CLI** (structured, small payloads):
  `dearborn node create|resolve|link`, `dearborn map set-destination|set-notes|
  set-fog|set-out-of-scope`, `dearborn comment post`, `dearborn task create|link`
  (breakdown), etc.
- **Document editing via native file tools** — the HTML doc is written to a
  scratch workspace file; the agent edits it with its harness's native
  Edit/Write; **`dearborn document sync`** (folded into `node resolve`) takes the
  per-epic semaphore, checks base `version`, and persists a new version (WS
  `document_updated` on `epic:<id>` re-renders the client). Big HTML through file
  tools, not tool-args.
- **Prototype build** — scratch workspace (not a target-repo clone); artifact →
  `node_asset`, linked from the node.

---

## 11. Client (v1)

Reuse the WS live-update *plumbing* pattern (`dag/stream.ts` reducer +
`useDagStream` transport) but **build the Map graph fresh** — the current
`DagEditorView` layout is not the desired look. Design the new Map graph
component **generic enough that the executor task DAG can adopt it later**
(intended follow-up, not v1).

- **Map view** — *fresh* graph: nodes colored by kind + computed readiness,
  edges, click-to-open. Live via `map_updated` frames on `epic:<id>`.
- **Node session view** — **refactor `PlanningView`** into a multi-party
  `node:<id>` chat + resolve affordance.
- **Document view** — *net-new*: render `document.html` **inline** (no
  sanitization in v1 — trusted single-team/self-hosted; **sanitize in v2**),
  TOC, section provenance chips, section-anchored comments, live updates.
- **Comment panel** — *net-new*: threads, node/section anchors, attribution,
  promote-to-node.
- **Prototype artifact** — rendered in a **sandboxed iframe** (isolation/
  functionality: it's a standalone HTML app), linked from its node.
- **Deferred:** presence avatars, animations, prototype browser-chrome dressing.

---

## 12. Migration — clean cutover (pre-production)

- No feature flag, no coexistence. **Delete** the product/technical planning path
  and breakdown-from-context.
- Additive schema; **no data backfill**. Drop `product_context`/
  `technical_context`; retire `planning_session`/`transcript_message` in favor of
  `node_session`/`node_message`.
- Existing `Planning` epics are re-created under the new flow (no synthesized
  map). Epics already `Ready`/`InProgress` are **untouched** — the executor half
  is unchanged, so in-flight execution keeps working.

---

## 13. Settled residual defaults

(Confirmed, no further grilling.)
- Node session/transcript = **new node-scoped tables** (§4.3/4.4), not re-keyed
  legacy tables.
- CLI auth = **existing capability-token** scoping.
- Attribution = per-row columns **+** `activity` log.
- Determinism = **`PlanningAgent`/`BreakdownAgent`-style trait doubles**.
- Document HTML **sanitization = v2**.

---

## 14. Implementation sequence

**Phase 0 — Schema & cutover.** Migrations for §4; drop old columns/tables;
delete old planning/breakdown-from-context code paths. Epic create form gains
`destination`/`notes`.

**Phase 1 — CLI + capability tokens.** `dearborn` subcommands over REST; reuse
capability-token scoping; retire MCP. Unit-test the client against the API.

**Phase 2 — Node engines.** Re-scope the interactive engine to per-node sessions
(grilling/prototype) and the one-shot engine to research/AFK-task; per-kind
system prompts (from `matt-pocock-skills`); trait doubles. Per-node run-lock.

**Phase 3 — Document.** `document`/`document_version`/`document_section`; scratch
file round-trip; per-epic write semaphore; `document sync`; WS
`document_updated`.

**Phase 4 — Map lifecycle.** Seed grilling node on epic create; fog graduation;
out-of-scope; completion eligibility; breakdown reads the Document + writes the
task DAG via CLI.

**Phase 5 — Comments, promotion, attribution.** `comment` overhaul, threads,
anchors, promote-to-node, `activity` feed.

**Phase 6 — Client.** Fresh Map graph; `PlanningView`→node-chat refactor;
Document view; comment panel; prototype sandbox iframe.

**Phase 7 — Prototype + task nodes.** `node_asset` (reuse `evidence.rs` blob
store if it fits); prototype scratch workspace; AFK/HITL task flavors.

---

## 15. Open v2 items

Document HTML sanitization · multi-human membership/roles/driver/presence ·
Dearborn-internal git versioning of the Document · refactor executor task DAG to
share the Map component · richer prototype-pane chrome/live-reload.
