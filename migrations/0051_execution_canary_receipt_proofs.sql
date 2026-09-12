-- Additive only: old confirmed orders and historical fills are never rewritten.
CREATE TABLE execution_canary_receipt_proofs (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id),
    tx_signature TEXT NOT NULL,
    wallet_pubkey TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    confirmation_status TEXT NOT NULL,
    slot TEXT,
    confirmed_at TEXT NOT NULL,
    last_attempt_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
