-- Local full successful response availability; no legacy backfill or elapsed synthesis.
ALTER TABLE execution_quote_canary_events ADD COLUMN quote_response_available_ts TEXT;
ALTER TABLE execution_quote_canary_provider_samples ADD COLUMN quote_response_available_ts TEXT;
ALTER TABLE execution_canary_build_plan_metadata ADD COLUMN quote_response_available_ts TEXT;
