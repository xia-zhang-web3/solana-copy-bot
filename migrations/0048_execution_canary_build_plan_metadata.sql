CREATE TABLE IF NOT EXISTS execution_canary_build_plan_metadata (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    recorded_ts TEXT NOT NULL,
    quote_source TEXT,
    quote_event_id TEXT,
    quote_status TEXT,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_response_json TEXT,
    quote_price_sol REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    priority_fee_source TEXT,
    priority_fee_status TEXT,
    priority_fee_lamports INTEGER,
    priority_fee_json TEXT,
    slippage_bps REAL,
    decision_status TEXT,
    decision_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_canary_build_plan_signal
    ON execution_canary_build_plan_metadata(signal_id);

CREATE INDEX IF NOT EXISTS idx_execution_canary_build_plan_recorded_ts
    ON execution_canary_build_plan_metadata(recorded_ts);
