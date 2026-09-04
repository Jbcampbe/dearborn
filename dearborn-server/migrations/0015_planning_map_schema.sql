-- Planning-map & living-document data model (epic "Wayfinder-Inspired
-- Planning", plan §4). Additive: no data backfill, no feature flag.
--
-- The new tables mirror the *shape* of task/task_dependency but stay
-- semantically distinct — planning nodes and implementation tasks evolve
-- separately, so the executor's `task` tables, readiness computation, and
-- lease/claim path are untouched here.
--
-- `epic` cutover: the four wayfinder prose columns (destination, notes,
-- not_yet_specified, out_of_scope) replace the vestigial `product_context` /
-- `technical_context` pair, which is DROPPED outright (clean cutover, §12).
-- Epics already Ready/InProgress keep every executor column intact; nothing
-- is backfilled.
--
-- `planning_session` / `transcript_message` are superseded by
-- `node_session` / `node_message` for all new code. Their tables are retired
-- together with the old linear-planning code paths that still read and write
-- them, not here.

-- §4.1 map_node: one decision/investigation per agent session. Readiness is
-- COMPUTED from dependencies, never stored; there is no fog state (fog is
-- epic-level prose) and no exclusive-claim column.
CREATE TABLE map_node (
  id                  TEXT PRIMARY KEY,
  epic_id             TEXT NOT NULL REFERENCES epic(id),
  kind                TEXT NOT NULL,  -- grilling | research | prototype | task
  task_mode           TEXT,           -- kind=task only: afk | hitl (fixed at creation)
  state               TEXT NOT NULL,  -- open | in_progress | resolved | out_of_scope
  title               TEXT NOT NULL,
  question            TEXT,           -- the decision/investigation this node resolves
  gist                TEXT,           -- one-line resolution answer (set on resolve)
  out_of_scope_reason TEXT,
  created_by          TEXT REFERENCES user(id),
  resolved_by         TEXT REFERENCES user(id),
  position_x          REAL,           -- graph layout (nullable; auto-layout may own this)
  position_y          REAL,
  created_at          INTEGER NOT NULL,
  updated_at          INTEGER NOT NULL
);

CREATE INDEX idx_map_node_epic ON map_node(epic_id);

-- §4.2 map_node_dependency: `blocker` blocks `blocked` (mirrors task_dependency).
CREATE TABLE map_node_dependency (
  blocker_id TEXT NOT NULL REFERENCES map_node(id),
  blocked_id TEXT NOT NULL REFERENCES map_node(id),
  PRIMARY KEY (blocker_id, blocked_id)
);

-- §4.3 node_session: the per-node durable resume handle (replaces
-- planning_session's (epic_id, phase) keying). AFK/no-engine nodes may never
-- create a row.
CREATE TABLE node_session (
  node_id            TEXT PRIMARY KEY REFERENCES map_node(id),
  harness_session_id TEXT,                       -- native resume handle; NULL until first run
  status             TEXT NOT NULL,              -- active | complete
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
);

-- §4.4 node_message: multi-party node transcript (replaces transcript_message).
-- Any user may post into a node's conversation; `actor_user_id` attributes the
-- human (NULL for agent/tool/system); `seq` is monotonic per node.
CREATE TABLE node_message (
  id            TEXT PRIMARY KEY,
  node_id       TEXT NOT NULL REFERENCES map_node(id),
  role          TEXT NOT NULL,                    -- user | agent | tool | system
  actor_user_id TEXT REFERENCES user(id),         -- which human posted (NULL for agent/tool/system)
  content       TEXT NOT NULL,                    -- text or serialized RunEvent
  seq           INTEGER NOT NULL,                 -- monotonic per node
  created_at    INTEGER NOT NULL
);

CREATE INDEX idx_node_message_node ON node_message(node_id);

-- §4.5 document: one living HTML blob per epic — the settled-decisions spec,
-- source of truth for its prose and what breakdown reads. `version` powers
-- the vNN lineage; sections are delimited by stable HTML id/data- attributes
-- that document_section keys on.
CREATE TABLE document (
  epic_id        TEXT PRIMARY KEY REFERENCES epic(id),
  html           TEXT NOT NULL,
  version        INTEGER NOT NULL,
  last_edited_by TEXT REFERENCES user(id),
  updated_at     INTEGER NOT NULL
);

CREATE TABLE document_version (
  epic_id        TEXT NOT NULL,
  version        INTEGER NOT NULL,
  html           TEXT NOT NULL,
  editor_user_id TEXT REFERENCES user(id),
  node_id        TEXT,
  created_at     INTEGER NOT NULL,
  PRIMARY KEY (epic_id, version)
);

-- §4.6 document_section: anchor/provenance index over document.html.
CREATE TABLE document_section (
  epic_id        TEXT NOT NULL REFERENCES epic(id),
  section_id     TEXT NOT NULL,                   -- matches an id= attribute in document.html
  title          TEXT,
  provenance     TEXT,                            -- node_id(s) that wrote/touched it (many→one)
  last_edited_by TEXT REFERENCES user(id),
  version        INTEGER,
  PRIMARY KEY (epic_id, section_id)
);

-- §4.7 node_asset: prototype artifacts, linked not inlined.
CREATE TABLE node_asset (
  id         TEXT PRIMARY KEY,
  node_id    TEXT NOT NULL REFERENCES map_node(id),
  mime       TEXT NOT NULL,
  bytes      BLOB NOT NULL,
  label      TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_node_asset_node ON node_asset(node_id);

-- §4.8 comment overhaul: threaded, anchored to a map node or a document
-- section, user- or agent-attributed, promotable to a frontier node. Replaces
-- the (unused) flat comment table in place.
DROP TABLE comment;
CREATE TABLE comment (
  id               TEXT PRIMARY KEY,
  epic_id          TEXT NOT NULL REFERENCES epic(id),
  thread_id        TEXT NOT NULL,                 -- threading
  anchor_kind      TEXT NOT NULL,                 -- node | section
  anchor_id        TEXT NOT NULL,                 -- map_node.id or document_section.section_id
  author_user_id   TEXT REFERENCES user(id),      -- NULL when author is the agent
  is_agent         INTEGER NOT NULL DEFAULT 0,
  body             TEXT NOT NULL,
  resolved         INTEGER NOT NULL DEFAULT 0,
  promoted_node_id TEXT REFERENCES map_node(id),  -- set when a thread is promoted
  created_at       INTEGER NOT NULL
);

CREATE INDEX idx_comment_epic ON comment(epic_id);

-- §4.9 activity: append-only attribution/provenance feed. Per-row
-- created_by/last_edited_by columns cover inline attribution; this covers the
-- feed (participants, history, "assembled from N resolved nodes").
CREATE TABLE activity (
  id            INTEGER PRIMARY KEY,
  epic_id       TEXT NOT NULL REFERENCES epic(id),
  node_id       TEXT,
  actor_user_id TEXT REFERENCES user(id),
  action        TEXT NOT NULL,
  detail        TEXT,
  created_at    INTEGER NOT NULL
);

CREATE INDEX idx_activity_epic ON activity(epic_id);

-- §4.10 epic changes: add the four wayfinder prose columns…
ALTER TABLE epic ADD COLUMN destination TEXT;
ALTER TABLE epic ADD COLUMN notes TEXT;
ALTER TABLE epic ADD COLUMN not_yet_specified TEXT;
ALTER TABLE epic ADD COLUMN out_of_scope TEXT;

-- …and drop the two planning-context columns they replace.
ALTER TABLE epic DROP COLUMN product_context;
ALTER TABLE epic DROP COLUMN technical_context;
