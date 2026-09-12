-- Actual client attempt start. Historical request_ts remains the bundle/version key.
-- No backfill: old batch/post-response timestamps cannot prove HTTP timing.
ALTER TABLE execution_quote_canary_events ADD COLUMN http_request_started_ts TEXT;
ALTER TABLE execution_quote_canary_provider_samples ADD COLUMN http_request_started_ts TEXT;
ALTER TABLE execution_canary_build_plan_metadata ADD COLUMN http_request_started_ts TEXT;
