CREATE TABLE IF NOT EXISTS execution_quote_canary_events (
    event_id TEXT PRIMARY KEY,
    signal_id TEXT,
    shadow_closed_trade_id INTEGER,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
    quote_status TEXT NOT NULL,
    request_ts TEXT NOT NULL,
    signal_ts TEXT,
    decision_delay_ms INTEGER,
    quote_latency_ms INTEGER,
    leader_notional_sol REAL,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_price_sol REAL,
    shadow_price_sol REAL,
    slippage_bps REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    priority_fee_status TEXT,
    priority_fee_lamports INTEGER,
    priority_fee_json TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_signal_side
    ON execution_quote_canary_events(signal_id, side);

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_closed_side
    ON execution_quote_canary_events(shadow_closed_trade_id, side);

CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_side_request_ts
    ON execution_quote_canary_events(side, request_ts);
