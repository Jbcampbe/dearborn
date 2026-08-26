-- Token columns for cost graphs. NULL = predates this feature or non-agent stage.
-- (Spec named this migration "0009", but 0009/0010 were already taken in this
-- tree by agent_run_events / actual_model; slotted here as 0011 instead.)
ALTER TABLE agent_run ADD COLUMN input_tokens  INTEGER;
ALTER TABLE agent_run ADD COLUMN output_tokens INTEGER;
