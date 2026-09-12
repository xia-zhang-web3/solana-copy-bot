-- New evidence only. An empty ledger does not backfill or certify historical costs.
CREATE TABLE execution_failed_expense_tasks (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    tx_signature TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt >= 1),
    route TEXT NOT NULL,
    wallet TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    operation_at TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    failure_source TEXT NOT NULL CHECK(failure_source IN ('signature_status','receipt_meta')),
    failure_error_json TEXT NOT NULL,
    commitment TEXT NOT NULL CHECK(commitment IN ('confirmed','finalized')),
    slot TEXT,
    status TEXT NOT NULL CHECK(status IN ('pending','complete','conflict')),
    reason TEXT NOT NULL,
    attempt_seq INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT
);
CREATE INDEX execution_failed_expense_signature ON execution_failed_expense_tasks(tx_signature);
CREATE INDEX execution_failed_expense_recovery ON execution_failed_expense_tasks(route,status,attempt_seq,order_id);
CREATE INDEX execution_failed_expense_window ON execution_failed_expense_tasks(operation_at);
CREATE TABLE execution_failed_expense_facts (
    order_id TEXT PRIMARY KEY REFERENCES execution_failed_expense_tasks(order_id) ON DELETE RESTRICT,
    facts_json TEXT NOT NULL
);
-- This is the single expense marker, unique by actual transaction; all amounts are
-- canonical integer TEXT, validated by Rust, never SQLite arithmetic or REAL.
CREATE TABLE execution_failed_expense_ledger (
    tx_signature TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES execution_failed_expense_tasks(order_id) ON DELETE RESTRICT,
    wallet_fee_lamports TEXT NOT NULL,
    transaction_fee_lamports TEXT NOT NULL,
    payer TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE execution_failed_expense_cursor (
    route TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL CHECK(sequence >= 0)
);
