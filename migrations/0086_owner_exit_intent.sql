-- Protected source rebuild; historical owner BUY and copy orders are retained.
SELECT enabled FROM temp.owner_exit_rebuild_guard;
CREATE TABLE owner_exit_intents (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK(length(intent_id) BETWEEN 1 AND 128),
    run_id TEXT NOT NULL UNIQUE CHECK(length(run_id) BETWEEN 1 AND 128),
    buy_order_id TEXT NOT NULL UNIQUE REFERENCES orders(order_id) ON DELETE RESTRICT,
    buy_receipt_signature TEXT NOT NULL CHECK(length(buy_receipt_signature)>0),
    position_id TEXT NOT NULL UNIQUE REFERENCES positions(position_id) ON DELETE RESTRICT,
    wallet TEXT NOT NULL CHECK(length(wallet)>0),
    signer TEXT NOT NULL CHECK(signer=wallet),
    genesis_hash TEXT NOT NULL CHECK(length(genesis_hash)>0),
    mint TEXT NOT NULL CHECK(mint='EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
    amount_raw INTEGER NOT NULL CHECK(amount_raw=1167085),
    decimals INTEGER NOT NULL CHECK(decimals=6),
    route TEXT NOT NULL CHECK(length(route)>0),
    activated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    authority_sha256 TEXT NOT NULL CHECK(length(authority_sha256)=64),
    max_priority_fee_lamports INTEGER NOT NULL CHECK(max_priority_fee_lamports BETWEEN 0 AND 50000),
    min_reserve_lamports INTEGER NOT NULL CHECK(min_reserve_lamports>0),
    max_slippage_bps INTEGER NOT NULL CHECK(max_slippage_bps BETWEEN 0 AND 10000),
    max_daily_loss_lamports INTEGER NOT NULL CHECK(max_daily_loss_lamports>=0)
);
CREATE TRIGGER owner_exit_intent_no_update BEFORE UPDATE ON owner_exit_intents
BEGIN SELECT RAISE(ABORT, 'owner exit intent immutable'); END;
CREATE TRIGGER owner_exit_intent_no_delete BEFORE DELETE ON owner_exit_intents
BEGIN SELECT RAISE(ABORT, 'owner exit intent immutable'); END;
-- One fresh-quote retry is allowed only before a durable dispatch claim.
CREATE TABLE owner_exit_pre_dispatch_rearms (
    intent_id TEXT PRIMARY KEY REFERENCES owner_exit_intents(intent_id) ON DELETE RESTRICT,
    count INTEGER NOT NULL CHECK(count BETWEEN 1 AND 1)
);
-- The signed transaction commits a fee obligation before any network send.
-- UNKNOWN keeps the bound amount reserved until receipt reconciliation.
CREATE TABLE owner_exit_fee_reservations (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    run_id TEXT NOT NULL UNIQUE REFERENCES owner_exit_intents(run_id) ON DELETE RESTRICT,
    tx_signature TEXT NOT NULL UNIQUE CHECK(length(tx_signature)>0),
    wallet TEXT NOT NULL CHECK(length(wallet)>0),
    fee_bound INTEGER NOT NULL CHECK(fee_bound BETWEEN 0 AND 100000),
    priority_fee INTEGER NOT NULL CHECK(priority_fee BETWEEN 0 AND 50000),
    fee_slot TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','successful','failed')),
    actual_fee INTEGER CHECK(actual_fee>=0),
    reconciled_at TEXT,
    CHECK ((state='pending' AND actual_fee IS NULL AND reconciled_at IS NULL)
       OR (state!='pending' AND actual_fee IS NOT NULL AND reconciled_at IS NOT NULL))
);
CREATE TRIGGER owner_exit_fee_no_delete BEFORE DELETE ON owner_exit_fee_reservations
BEGIN SELECT RAISE(ABORT, 'owner exit fee reservation immutable'); END;
CREATE TRIGGER owner_exit_fee_identity_immutable BEFORE UPDATE OF
    order_id,run_id,tx_signature,wallet,fee_bound,priority_fee,fee_slot
    ON owner_exit_fee_reservations
BEGIN SELECT RAISE(ABORT, 'owner exit fee identity immutable'); END;
CREATE UNIQUE INDEX owner_exit_order_once ON orders(signal_id)
WHERE signal_id LIKE 'owner-exit:%';
