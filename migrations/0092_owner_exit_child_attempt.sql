-- A child is an explicit owner authority, never rearming or replacing its parent.
SELECT enabled FROM temp.owner_exit_child_rebuild_guard;
CREATE TABLE owner_exit_child_attempts (
    child_intent_id TEXT PRIMARY KEY NOT NULL CHECK(length(child_intent_id) BETWEEN 1 AND 128
        AND child_intent_id='source-child:'||substr(parent_intent_id,17)),
    parent_intent_id TEXT NOT NULL UNIQUE REFERENCES owner_exit_intents(intent_id) ON DELETE RESTRICT,
    parent_failed_order_id TEXT NOT NULL UNIQUE REFERENCES orders(order_id) ON DELETE RESTRICT,
    parent_failed_order_json TEXT NOT NULL CHECK(json_valid(parent_failed_order_json)),
    parent_failed_order_sha256 TEXT NOT NULL CHECK(length(parent_failed_order_sha256)=64),
    intent_json TEXT NOT NULL CHECK(json_valid(intent_json)
        AND json_extract(intent_json,'$.intent_id')=child_intent_id),
    registered_at TEXT NOT NULL
);
CREATE TRIGGER owner_exit_child_no_update BEFORE UPDATE ON owner_exit_child_attempts
BEGIN SELECT RAISE(ABORT,'owner exit child immutable'); END;
CREATE TRIGGER owner_exit_child_no_delete BEFORE DELETE ON owner_exit_child_attempts
BEGIN SELECT RAISE(ABORT,'owner exit child immutable'); END;
CREATE VIEW owner_exit_effective_intents AS
SELECT intent_id,run_id,buy_order_id,buy_receipt_signature,position_id,wallet,signer,
       genesis_hash,mint,amount_raw,decimals,route,activated_at,expires_at,authority_sha256,
       max_priority_fee_lamports,min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports,
       NULL AS child_origin_id
FROM owner_exit_intents
UNION ALL
SELECT child_intent_id,
       json_extract(intent_json,'$.run_id'),json_extract(intent_json,'$.buy_order_id'),
       json_extract(intent_json,'$.buy_receipt_signature'),json_extract(intent_json,'$.position_id'),
       json_extract(intent_json,'$.wallet'),json_extract(intent_json,'$.signer'),
       json_extract(intent_json,'$.genesis_hash'),json_extract(intent_json,'$.mint'),
       json_extract(intent_json,'$.amount_raw'),json_extract(intent_json,'$.decimals'),
       json_extract(intent_json,'$.route'),json_extract(intent_json,'$.activated_at'),
       json_extract(intent_json,'$.expires_at'),json_extract(intent_json,'$.authority_sha256'),
       json_extract(intent_json,'$.max_priority_fee_lamports'),json_extract(intent_json,'$.min_reserve_lamports'),
       json_extract(intent_json,'$.max_slippage_bps'),json_extract(intent_json,'$.max_daily_loss_lamports'),
       child_intent_id
FROM owner_exit_child_attempts;
