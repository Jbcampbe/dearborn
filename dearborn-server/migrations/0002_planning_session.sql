-- Planning-session lifecycle (T-201).
--
-- One row per (epic, phase). A row is created when planning starts for a phase
-- (the `product` phase is created with the epic; `technical` is added when the
-- user advances the epic in T-205). It holds the durable resume handle for the
-- native agent-harness session so a planning conversation survives a server
-- restart: T-202 writes `harness_session_id` (the `session_id` surfaced by the
-- harness at run time) and resumes from it on the next turn. The transcript
-- itself lives in `transcript_message` and remains the source of truth.
CREATE TABLE planning_session (
  epic_id            TEXT NOT NULL REFERENCES epic(id),
  phase              TEXT NOT NULL,                    -- product|technical
  harness_session_id TEXT,                             -- harness resume id; NULL until T-202's first run
  status             TEXT NOT NULL DEFAULT 'active',   -- active|complete
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL,
  PRIMARY KEY (epic_id, phase)
);
