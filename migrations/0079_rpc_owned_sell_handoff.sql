-- No backfill or inventory write. A preparation reservation is never paid cash.
-- Existing quote rows expire and cannot own an unsigned financial continuation.
CREATE TABLE rpc_owned_sell_handoffs (
    intent_id TEXT PRIMARY KEY REFERENCES ordered_source_sell_intents(intent_id),
    signature TEXT NOT NULL UNIQUE REFERENCES source_sell_signature_claims(signature),
    position_id TEXT NOT NULL UNIQUE REFERENCES positions(position_id),
    order_id TEXT NOT NULL UNIQUE,
    owner TEXT NOT NULL UNIQUE,
    experiment_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    config_sha256 TEXT NOT NULL CHECK(length(config_sha256)=64),
    snapshot TEXT NOT NULL,
    authority TEXT NOT NULL,
    quote TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    deadline TEXT NOT NULL,
    fee_reserve INTEGER NOT NULL CHECK(fee_reserve=100000),
    state TEXT NOT NULL CHECK(state IN ('preparing','unsigned_prepared')),
    unsigned_payload TEXT,
    message_sha256 TEXT,
    total_fee INTEGER,
    priority_fee INTEGER,
    CHECK((state='preparing' AND unsigned_payload IS NULL) OR
          (state='unsigned_prepared' AND unsigned_payload IS NOT NULL AND length(message_sha256)=64
           AND total_fee BETWEEN 0 AND 100000 AND priority_fee BETWEEN 0 AND 50000
           AND priority_fee<=total_fee))
);
