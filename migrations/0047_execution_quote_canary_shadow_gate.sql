CREATE TABLE IF NOT EXISTS execution_quote_canary_shadow_gate_events (
    signal_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
    status TEXT NOT NULL CHECK(status IN ('shadow_recorded', 'shadow_dropped')),
    reason TEXT,
    recorded_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_shadow_gate_request
    ON execution_quote_canary_shadow_gate_events(side, recorded_ts);
