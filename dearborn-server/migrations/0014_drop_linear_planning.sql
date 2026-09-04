-- Clean cutover (epic "Wayfinder-Inspired Planning", §12): the linear
-- product/technical planning flow is gone — no feature flag, no coexistence.
--
-- `planning_session` keyed planning by (epic_id, phase) and `transcript_message`
-- held the epic-level transcript those phases shared. Their per-node
-- replacements (`node_session` / `node_message`, migration 0015) are the only
-- session/transcript stores from here on; planning history lives on map nodes.
-- No data backfill: existing Planning epics are re-created under the new flow.

DROP TABLE transcript_message;
DROP TABLE planning_session;
