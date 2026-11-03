-- A replacement preserves the old prepared generation and post-sign proof.
-- It neither releases the existing handoff fee hold nor creates dispatch permission.
CREATE TABLE owned_sell_quote_expiry_recoveries (
    intent_id TEXT PRIMARY KEY REFERENCES rpc_owned_sell_handoffs(intent_id),
    old_owner TEXT NOT NULL UNIQUE,
    new_owner TEXT NOT NULL UNIQUE,
    prepared_json TEXT NOT NULL CHECK(json_valid(prepared_json)),
    build_plan_json TEXT CHECK(build_plan_json IS NULL OR json_valid(build_plan_json)),
    recovered_at TEXT NOT NULL,
    CHECK(old_owner != new_owner)
);
CREATE TRIGGER owned_sell_quote_expiry_history_no_update
BEFORE UPDATE ON owned_sell_quote_expiry_recoveries
BEGIN SELECT RAISE(ABORT,'immutable owned SELL expiry history'); END;
CREATE TRIGGER owned_sell_quote_expiry_history_no_delete
BEFORE DELETE ON owned_sell_quote_expiry_recoveries
BEGIN SELECT RAISE(ABORT,'immutable owned SELL expiry history'); END;
