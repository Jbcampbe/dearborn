-- Per-tool-call evidence rows for agent stages (tool-events epic).
-- Each ToolStart/ToolEnd event the harness emits during a stage becomes one
-- row here, linked to its parent agent_run row via run_id.  `seq` preserves
-- arrival order within a run so callers can reconstruct the exact tool-call
-- timeline without relying on insertion rowid.
CREATE TABLE agent_run_events (
  id           TEXT    PRIMARY KEY,
  run_id       TEXT    NOT NULL REFERENCES agent_run(id),
  seq          INTEGER NOT NULL,   -- 0-based arrival order within the run
  kind         TEXT    NOT NULL,   -- "tool_start" | "tool_end"
  tool_call_id TEXT    NOT NULL,   -- pairs a tool_start with its tool_end
  name         TEXT    NOT NULL,   -- tool name ("tool_start" only; "" for "tool_end")
  ok           INTEGER             -- NULL for "tool_start"; 0/1 for "tool_end"
);
