-- Preserve the SOURCE rule; only the exact BOT native receipt exit gains zero reserve.
DROP TRIGGER owner_exit_intents_zero_scope;
CREATE TRIGGER owner_exit_intents_zero_scope BEFORE INSERT ON owner_exit_intents
WHEN NEW.min_reserve_lamports=0 AND NOT EXISTS(
    SELECT 1 FROM controlled_receipt_lot_scope
    WHERE wallet=NEW.wallet AND run_id=NEW.run_id AND (
        (json_extract(scope_json,'$.source_wallet')=NEW.wallet
         AND json_extract(scope_json,'$.source_run_id')=NEW.run_id)
        OR (json_extract(scope_json,'$.bot_wallet')=NEW.wallet
            AND json_extract(scope_json,'$.bot_run_id')=NEW.run_id
            AND substr(NEW.intent_id,1,17)='bot-receipt-exit:'
            AND length(NEW.intent_id)>17
            AND NEW.buy_order_id='exec-canary:native-buy-v1:'||substr(NEW.intent_id,18))
    )
)
BEGIN SELECT RAISE(ABORT,'receipt lot zero reserve requires immutable scope'); END;
