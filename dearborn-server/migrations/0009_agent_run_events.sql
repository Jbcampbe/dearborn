-- Per-tool-call evidence rows for agent stages.
-- Each ToolStart/ToolEnd event the harness emits during a stage becomes one
-- row here, linked to its parent agent_run row via agent_run_id. Callers can
-- reconstruct the exact tool-call timeline by ordering on the
-- (agent_run_id, created_at) index: pair each tool_end row with the earlier
-- tool_start row sharing its tool_call_id.
CREATE TABLE agent_run_events (
  id           TEXT    PRIMARY KEY,          -- ulid/uuid
  agent_run_id TEXT    NOT NULL REFERENCES agent_run(id),
  kind         TEXT    NOT NULL,             -- "tool_start" | "tool_end"
  tool_call_id TEXT    NOT NULL,             -- pairs a tool_start with its tool_end
  name         TEXT    NOT NULL,             -- tool name ("tool_start" only; "" for "tool_end")
  ok           INTEGER,                      -- NULL for "tool_start"; 0/1 for "tool_end"
  created_at   INTEGER NOT NULL              -- unix ms
);

CREATE INDEX idx_agent_run_events_run
  ON agent_run_events(agent_run_id, created_at);
