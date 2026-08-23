-- agent_run evidence for configurable agents (T8): which harness/model produced
-- a stage, and a hash of the resolved instruction prompt — so live-read agent
-- settings (design §9) never make historical runs unauditable. NULL = the row
-- predates this feature; no backfill.
ALTER TABLE agent_run ADD COLUMN harness     TEXT;
ALTER TABLE agent_run ADD COLUMN model       TEXT;
ALTER TABLE agent_run ADD COLUMN prompt_hash TEXT;
