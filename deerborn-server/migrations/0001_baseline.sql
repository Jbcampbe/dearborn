-- Projects: one repo, one KB (KB deferred to v2), optional build/test/run cmds.
CREATE TABLE project (
  id            TEXT PRIMARY KEY,          -- ulid/uuid
  name          TEXT NOT NULL,
  repo_url      TEXT NOT NULL,
  pat_encrypted BLOB,                      -- AES-256-GCM; never returned by API
  setup_cmd     TEXT,                      -- optional
  test_cmd      TEXT,                      -- optional; NULL => Half 2 skips test gate
  run_cmd       TEXT,                      -- optional
  clone_path    TEXT,                      -- canonical read-only checkout on disk
  clone_status  TEXT NOT NULL DEFAULT 'pending', -- pending|ready|error
  clone_error   TEXT,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

-- Epic: unit of planning AND of concurrency/leasing (Half 2).
CREATE TABLE epic (
  id                TEXT PRIMARY KEY,
  project_id        TEXT NOT NULL REFERENCES project(id),
  title             TEXT NOT NULL,
  product_context   TEXT,                  -- maintained live by product-planning agent
  technical_context TEXT,                  -- maintained live by technical-planning agent
  status            TEXT NOT NULL DEFAULT 'Planning',
                    -- Planning|Ready|InProgress|Completed|Blocked|Cancelled
  branch_name       TEXT,                  -- deerborn/<project key>-<id> (Half 2)
  -- queue/lease columns: written by Half 1's enqueue, read by Half 2's claim
  lease_owner       TEXT,
  lease_expires_at  INTEGER,               -- unix ms
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL
);

-- Task: a vertical slice; the executor's unit of work.
CREATE TABLE task (
  id             TEXT PRIMARY KEY,
  epic_id        TEXT REFERENCES epic(id), -- NULL => standalone task
  project_id     TEXT NOT NULL REFERENCES project(id),
  title          TEXT NOT NULL,
  description    TEXT,                      -- end-to-end behavior, not layer-by-layer
  acceptance     TEXT,                      -- acceptance_criteria
  status         TEXT NOT NULL DEFAULT 'Todo',
                 -- Todo|InProgress|Done|Failed|Cancelled  (readiness is COMPUTED from deps)
  failure_reason TEXT,                      -- Half 2: test_gate_exhausted|review_not_converged|blocked|agent_error
  agent_session_id TEXT,                    -- Half 2: the session that implemented it
  position       INTEGER,                   -- ordering hint within an epic/lane
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL
);

-- Dependency edge: `blocker` blocks `blocked` (matches to-tasks `blocks:`).
CREATE TABLE task_dependency (
  blocker_id TEXT NOT NULL REFERENCES task(id),
  blocked_id TEXT NOT NULL REFERENCES task(id),
  PRIMARY KEY (blocker_id, blocked_id)
);

-- Durable planning transcript (source of truth, resumable, backend-portable).
CREATE TABLE transcript_message (
  id         TEXT PRIMARY KEY,
  epic_id    TEXT NOT NULL REFERENCES epic(id),
  phase      TEXT NOT NULL,                 -- product|technical
  role       TEXT NOT NULL,                 -- user|agent|tool|system
  content    TEXT NOT NULL,                 -- text or serialized RunEvent
  seq        INTEGER NOT NULL,              -- monotonic per epic
  created_at INTEGER NOT NULL
);

-- Per-run/per-stage evidence (mostly written by Half 2; table exists now).
CREATE TABLE agent_run (
  id         TEXT PRIMARY KEY,
  task_id    TEXT REFERENCES task(id),
  epic_id    TEXT REFERENCES epic(id),
  stage      TEXT NOT NULL,                 -- planning|breakdown|implement|review|judge|fix|...
  session_id TEXT,
  log        TEXT,
  created_at INTEGER NOT NULL
);

CREATE TABLE comment (
  id         TEXT PRIMARY KEY,
  task_id    TEXT REFERENCES task(id),
  epic_id    TEXT REFERENCES epic(id),
  author     TEXT NOT NULL,                 -- user|agent
  body       TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
