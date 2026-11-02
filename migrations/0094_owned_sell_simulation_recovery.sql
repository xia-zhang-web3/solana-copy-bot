-- Audit only: no backfill, cash movement, authority renewal or reservation release.
CREATE TABLE owned_sell_simulation_refusals (
    intent_id TEXT NOT NULL REFERENCES rpc_owned_sell_handoffs(intent_id),
    owner TEXT NOT NULL UNIQUE,
    handoff TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64),
    simulation_slot INTEGER NOT NULL CHECK(simulation_slot>=0),
    refused_at TEXT NOT NULL,
    PRIMARY KEY(intent_id,owner)
);
CREATE TABLE owned_sell_preparation_recoveries (
    old_owner TEXT PRIMARY KEY REFERENCES owned_sell_simulation_refusals(owner),
    new_owner TEXT NOT NULL UNIQUE,
    handoff TEXT NOT NULL,
    recovered_at TEXT NOT NULL
);
