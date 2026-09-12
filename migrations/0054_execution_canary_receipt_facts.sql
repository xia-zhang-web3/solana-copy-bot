-- Additive observation only. No historical backfill or accounting/status mutation.
-- Raw integers are canonical decimal TEXT, validated by the typed storage API.
CREATE TABLE execution_canary_receipt_facts (
    order_id TEXT PRIMARY KEY REFERENCES execution_canary_receipt_proofs(order_id),
    tx_signature TEXT NOT NULL,
    wallet_pubkey TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    slot TEXT NOT NULL,
    wallet_native_pre TEXT NOT NULL,
    wallet_native_post TEXT NOT NULL,
    wallet_native_delta TEXT NOT NULL,
    transaction_fee TEXT,
    fee_coverage TEXT NOT NULL CHECK (fee_coverage IN ('known', 'missing', 'invalid')),
    fee_payer TEXT,
    token_delta_raw TEXT,
    token_decimals INTEGER CHECK (token_decimals BETWEEN 0 AND 255),
    token_coverage TEXT NOT NULL CHECK (token_coverage IN ('paired_balances', 'proven_lifecycle', 'unresolved')),
    token_coverage_reason TEXT,
    wsol_coverage TEXT NOT NULL CHECK (wsol_coverage IN ('observed', 'unresolved')),
    block_time TEXT,
    decomposition TEXT NOT NULL CHECK (decomposition = 'unresolved'),
    recorded_at TEXT NOT NULL,
    CHECK ((transaction_fee IS NOT NULL) = (fee_coverage = 'known')),
    CHECK ((token_delta_raw IS NOT NULL) = (token_decimals IS NOT NULL)),
    CHECK ((token_delta_raw IS NOT NULL) = (token_coverage != 'unresolved')),
    CHECK ((token_coverage_reason IS NOT NULL) = (token_coverage = 'unresolved'))
);
