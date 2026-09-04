-- The model's reasoning stream for a stage, persisted alongside `log` so the
-- task pipeline view can render thinking for completed runs (and reconcile a
-- running run's live tail against a flushed prefix), not just live over the WS.
-- Unlike `log`, thinking is NOT part of the harness's assembled reply; it is
-- captured from `RunEvent::Thinking` deltas. NOT NULL DEFAULT '' so pre-feature
-- rows and non-agent stages read as "no thinking" rather than NULL.
ALTER TABLE agent_run ADD COLUMN thinking TEXT NOT NULL DEFAULT '';
