-- Epic: PR identity + why it blocked.
ALTER TABLE epic ADD COLUMN pr_url         TEXT;
ALTER TABLE epic ADD COLUMN pr_number      INTEGER;
ALTER TABLE epic ADD COLUMN blocked_reason TEXT;

-- Task: standalone tasks are leasable work items with their own branch + PR.
ALTER TABLE task ADD COLUMN lease_owner      TEXT;
ALTER TABLE task ADD COLUMN lease_expires_at INTEGER;   -- unix ms
ALTER TABLE task ADD COLUMN branch_name      TEXT;
ALTER TABLE task ADD COLUMN pr_url           TEXT;
ALTER TABLE task ADD COLUMN pr_number        INTEGER;
ALTER TABLE task ADD COLUMN base_sha         TEXT;      -- set at claim; review diffs against it

-- agent_run becomes the per-stage evidence table (agent AND non-agent stages).
ALTER TABLE agent_run ADD COLUMN attempt    INTEGER NOT NULL DEFAULT 1;
ALTER TABLE agent_run ADD COLUMN status     TEXT NOT NULL DEFAULT 'running';
                     -- running|ok|error|timeout|cancelled
ALTER TABLE agent_run ADD COLUMN verdict    TEXT;       -- PASS|NEEDS_CHANGES|BLOCKED (review only)
ALTER TABLE agent_run ADD COLUMN started_at INTEGER;
ALTER TABLE agent_run ADD COLUMN ended_at   INTEGER;
ALTER TABLE agent_run ADD COLUMN exit_code  INTEGER;

-- Claim-path indexes.
CREATE INDEX IF NOT EXISTS idx_epic_claim ON epic(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_task_claim ON task(status, epic_id, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_agent_run_task ON agent_run(task_id, created_at);
