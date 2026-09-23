-- Immutable producer-to-native sizing evidence, not a second financial ledger.
CREATE TABLE fractional_sell_decisions (
    intent_id TEXT PRIMARY KEY REFERENCES ordered_source_sell_intents(intent_id),
    base_binding TEXT NOT NULL CHECK(length(base_binding) <= 131072),
    claim_owner TEXT NOT NULL,
    decision_id TEXT NOT NULL UNIQUE,
    producer_identity TEXT NOT NULL CHECK(length(producer_identity) = 64),
    state TEXT NOT NULL CHECK(state IN ('collecting','proven','zero')),
    decision TEXT,
    evidence TEXT CHECK(length(evidence) <= 33554432)
);
