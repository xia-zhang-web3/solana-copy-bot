-- Only the protected migration runner may widen execution_order_sources.
SELECT enabled FROM temp.owner_technical_buy_rebuild_guard;
CREATE TABLE owner_technical_buy_intents (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK(length(intent_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL UNIQUE CHECK(length(run_id) BETWEEN 1 AND 128),
    wallet TEXT NOT NULL CHECK(length(wallet)>0),
    signer TEXT NOT NULL CHECK(signer=wallet),
    genesis_hash TEXT NOT NULL CHECK(length(genesis_hash)>0),
    mint TEXT NOT NULL CHECK(length(mint)>0),
    amount_lamports INTEGER NOT NULL CHECK(amount_lamports BETWEEN 1 AND 10000000),
    route TEXT NOT NULL CHECK(length(route)>0),
    activated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    authority_sha256 TEXT NOT NULL CHECK(length(authority_sha256)=64),
    max_priority_fee_lamports INTEGER NOT NULL CHECK(max_priority_fee_lamports BETWEEN 0 AND 50000),
    min_reserve_lamports INTEGER NOT NULL CHECK(min_reserve_lamports>0),
    max_slippage_bps INTEGER NOT NULL CHECK(max_slippage_bps BETWEEN 0 AND 10000),
    max_daily_loss_lamports INTEGER NOT NULL CHECK(max_daily_loss_lamports>=0),
    max_open_positions INTEGER NOT NULL CHECK(max_open_positions=1),
    max_buy_count INTEGER NOT NULL CHECK(max_buy_count=1)
);
CREATE TRIGGER owner_technical_buy_intent_no_update BEFORE UPDATE ON owner_technical_buy_intents
BEGIN SELECT RAISE(ABORT, 'owner technical BUY intent immutable'); END;
CREATE TRIGGER owner_technical_buy_intent_no_delete BEFORE DELETE ON owner_technical_buy_intents
BEGIN SELECT RAISE(ABORT, 'owner technical BUY intent immutable'); END;
-- A distinct source record makes owner origin explicit in every order chain.
-- One intent can own at most one canonical order even if a caller invents an ID.
CREATE UNIQUE INDEX owner_technical_buy_order_once ON orders(signal_id)
WHERE signal_id LIKE 'owner-buy:%';
