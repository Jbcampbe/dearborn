-- Recommendation 5 (failure triage): a redacted, length-capped copy of the
-- human-readable failure message alongside `failure_reason`, so a Failed task
-- / Blocked epic can be triaged from the API or the board frames without DB
-- spelunking. Written by `worker::fail_item` — the single failure router — on
-- every failure that has a message; cleared by `POST /tasks/{id}/retry` so a
-- fresh attempt never inherits stale detail.

ALTER TABLE task ADD COLUMN failure_detail TEXT;
ALTER TABLE epic ADD COLUMN failure_detail TEXT;
