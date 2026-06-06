CREATE TABLE IF NOT EXISTS execution_quote_canary_provider_samples (
    event_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
    quote_status TEXT NOT NULL,
    request_ts TEXT NOT NULL,
    quote_latency_ms INTEGER,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_response_json TEXT,
    quote_price_sol REAL,
    shadow_price_sol REAL,
    slippage_bps REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    decision_status TEXT,
    decision_reason TEXT,
    error TEXT,
    PRIMARY KEY(event_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_provider_samples_request
    ON execution_quote_canary_provider_samples(provider, request_ts);
