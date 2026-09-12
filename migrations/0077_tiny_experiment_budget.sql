-- Variant A only. No activation, balance credit, historical backfill or config reset.
CREATE TABLE execution_tiny_experiment (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 experiment_id TEXT NOT NULL UNIQUE CHECK(length(trim(experiment_id)) BETWEEN 1 AND 128),
 wallet TEXT NOT NULL CHECK(length(trim(wallet))>0),
 activated_at TEXT NOT NULL,
 deadline TEXT NOT NULL,
 last_decision_at TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','stopped','completed')),
 stop_reason TEXT,
 buy_order_id TEXT UNIQUE,
 token TEXT,
 position_id TEXT UNIQUE
);
CREATE TABLE execution_tiny_reservations (
 order_id TEXT PRIMARY KEY REFERENCES execution_canary_dispatch(order_id),
 experiment_id TEXT NOT NULL REFERENCES execution_tiny_experiment(experiment_id),
 tx_signature TEXT NOT NULL UNIQUE,
 wallet TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('buy','sell')),
 message_sha256 TEXT NOT NULL,
 transaction_sha256 TEXT NOT NULL,
 buy_lamports INTEGER NOT NULL CHECK(buy_lamports BETWEEN 0 AND 10000000),
 fee_bound INTEGER NOT NULL CHECK(fee_bound BETWEEN 0 AND 100000),
 priority_fee INTEGER NOT NULL CHECK(priority_fee BETWEEN 0 AND 50000 AND priority_fee<=fee_bound),
 fee_slot TEXT NOT NULL,
 actual_fee INTEGER CHECK(actual_fee>=0),
 outcome TEXT CHECK(outcome IN ('successful','failed')),
 reserved_at TEXT NOT NULL,
 reconciled_at TEXT,
 CHECK((actual_fee IS NULL AND outcome IS NULL AND reconciled_at IS NULL)
    OR (actual_fee IS NOT NULL AND outcome IS NOT NULL AND reconciled_at IS NOT NULL))
);
