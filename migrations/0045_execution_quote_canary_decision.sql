ALTER TABLE execution_quote_canary_events
    ADD COLUMN decision_status TEXT;

ALTER TABLE execution_quote_canary_events
    ADD COLUMN decision_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_decision_request_ts
    ON execution_quote_canary_events(decision_status, request_ts);
