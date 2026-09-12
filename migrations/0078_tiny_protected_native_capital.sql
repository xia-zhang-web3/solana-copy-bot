-- Explicit opt-in only. Preserve historical experiments, reservations and receipts.
ALTER TABLE execution_tiny_experiment ADD COLUMN policy_mode TEXT NOT NULL
 DEFAULT 'decoded_amount' CHECK(policy_mode IN ('decoded_amount','protected_native_capital'));
CREATE TABLE execution_tiny_native_policy (
 experiment_id TEXT PRIMARY KEY REFERENCES execution_tiny_experiment(experiment_id),
 wallet TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version=1),
 initial_lamports TEXT NOT NULL,
 original_reserve TEXT NOT NULL,
 floor_lamports TEXT NOT NULL,
 allowance INTEGER NOT NULL CHECK(allowance=15000000),
 observation_slot TEXT NOT NULL,
 observed_at TEXT NOT NULL
);
ALTER TABLE execution_tiny_reservations RENAME TO execution_tiny_reservations_0077;
CREATE TABLE execution_tiny_reservations (
 order_id TEXT PRIMARY KEY REFERENCES execution_canary_dispatch(order_id),
 experiment_id TEXT NOT NULL REFERENCES execution_tiny_experiment(experiment_id),
 tx_signature TEXT NOT NULL UNIQUE,
 wallet TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('buy','sell')),
 message_sha256 TEXT NOT NULL,
 transaction_sha256 TEXT NOT NULL,
 buy_lamports INTEGER CHECK(buy_lamports BETWEEN 0 AND 10000000),
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
INSERT INTO execution_tiny_reservations SELECT * FROM execution_tiny_reservations_0077;
DROP TABLE execution_tiny_reservations_0077;
-- The parent reservation binds message, transaction, signature, wallet and experiment.
-- NULL decoded amount plus this row records a different guarantee, never Known DEX input.
CREATE TABLE execution_tiny_capital_evidence (
 order_id TEXT PRIMARY KEY REFERENCES execution_tiny_reservations(order_id),
 requested_lamports INTEGER NOT NULL CHECK(requested_lamports BETWEEN 1 AND 10000000),
 floor_lamports TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64)
);
