-- Agent & project settings (docs/design/agent-and-project-settings.md, T1).
--
-- global_settings: singleton row (id = 1) holding the global layer of the
-- agent-config resolution chain — which harnesses are selectable anywhere,
-- the default harness, and the default model PER HARNESS (model ids are
-- harness-specific, so a map, not a scalar; see design §3). Seeded to
-- byte-for-byte today's behavior: Claude enabled and default, no models
-- (every CLI runs at its own configured default until a model is set).
-- JSON maps are stored as TEXT; typed access lives in agent_settings.rs.
CREATE TABLE global_settings (
  id                INTEGER PRIMARY KEY CHECK (id = 1),
  default_harness   TEXT NOT NULL DEFAULT 'claude',
  default_models    TEXT NOT NULL DEFAULT '{}',        -- JSON: {harness: model|null}
  enabled_harnesses TEXT NOT NULL DEFAULT '["claude"]', -- JSON: [harness, ...]
  updated_at        INTEGER NOT NULL
);

-- Seed the singleton. updated_at 0 marks "never user-edited"; the first write
-- bumps it like every other table.
INSERT INTO global_settings (id, default_harness, default_models,
                             enabled_harnesses, updated_at)
VALUES (1, 'claude', '{"claude": null}', '["claude"]', 0);

-- Per-project per-slot overrides (design §6): absent row = inherit globals
-- everywhere. Every column is nullable so a row can override just one facet;
-- "reset" clears columns / deletes the row — defaults are NEVER copied in
-- (design §6), so built-in prompt improvements keep reaching non-overridden
-- slots after an upgrade.
CREATE TABLE agent_setting (
  project_id    TEXT NOT NULL REFERENCES project(id),
  slot          TEXT NOT NULL,   -- agent_slot enum key (snake_case)
  harness       TEXT,
  model         TEXT,
  system_prompt TEXT,
  updated_at    INTEGER NOT NULL,
  PRIMARY KEY (project_id, slot)
);

-- Base branch (design §5): project-level default for new epics; NULL → repo
-- default branch. The epic column is the point of commitment — snapshotted at
-- provision from `epic.base_branch ?? project.base_branch ?? repo default`
-- (§5's resolution chain); pre-existing epics stay NULL and behave unchanged.
ALTER TABLE project ADD COLUMN base_branch TEXT;
ALTER TABLE epic    ADD COLUMN base_branch TEXT;
