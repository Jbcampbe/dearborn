-- actual model used by a stage run (harness-reported, vs. configured `model`).
ALTER TABLE agent_run ADD COLUMN actual_model TEXT;
