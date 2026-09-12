-- Exact receipt observations only; no historical backfill, debit or decomposition.
CREATE TABLE execution_receipt_native_observations (
    order_id TEXT PRIMARY KEY REFERENCES execution_canary_receipt_facts(order_id) ON DELETE RESTRICT,
    tx_signature TEXT NOT NULL UNIQUE,
    observations_json TEXT NOT NULL CHECK(length(observations_json) <= 262144),
    conflict_reason TEXT,
    recorded_at TEXT NOT NULL
);
