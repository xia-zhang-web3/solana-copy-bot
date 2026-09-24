-- Additive receipt classification. Historical fills and positions retain their cash basis.
CREATE TABLE execution_receipt_cash_components (
    order_id TEXT PRIMARY KEY NOT NULL REFERENCES execution_canary_receipt_facts(order_id) ON DELETE RESTRICT,
    tx_signature TEXT NOT NULL,
    components_json TEXT NOT NULL CHECK(length(components_json) BETWEEN 2 AND 8192),
    recorded_at TEXT NOT NULL
);
-- Derived only inside the confirmed SELL settlement transaction. Historical
-- BUY cash basis remains immutable; missing components remain explicit nulls.
CREATE TABLE execution_receipt_trade_cycles (
    sell_order_id TEXT PRIMARY KEY NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT,
    position_id TEXT NOT NULL REFERENCES positions(position_id) ON DELETE RESTRICT,
    accounting_json TEXT NOT NULL CHECK(length(accounting_json) BETWEEN 2 AND 8192),
    recorded_at TEXT NOT NULL
);
