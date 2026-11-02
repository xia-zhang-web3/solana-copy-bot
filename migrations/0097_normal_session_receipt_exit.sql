-- Preserve legacy SOURCE/BOT scope rules and bind the normal BOT to its real BUY.
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
) AND NOT EXISTS(
    SELECT 1 FROM real_session_sessions a
    JOIN real_session_cycles c ON c.session_id=a.session_id
    JOIN real_session_orders b ON b.cycle_id=c.cycle_id AND b.order_id=c.buy_order_id AND b.side='buy'
    JOIN orders o ON o.order_id=b.order_id
    JOIN execution_canary_receipt_facts r ON r.order_id=o.order_id
    JOIN fills f ON f.order_id=o.order_id AND f.position_id=c.position_id
    WHERE a.singleton=1 AND json_extract(a.identity_json,'$.policy')='real_bounded_usdc_session_v1'
      AND json_extract(a.identity_json,'$.max_cycles')=1
      AND NEW.run_id=a.session_id AND NEW.wallet=json_extract(a.scope_json,'$.bot_wallet')
      AND NEW.signer=NEW.wallet AND NEW.genesis_hash=json_extract(a.identity_json,'$.genesis')
      AND NEW.intent_id='bot-receipt-exit:'||c.source_buy_signature
      AND NEW.buy_order_id='exec-canary:native-buy-v1:'||c.source_buy_signature
      AND NEW.buy_order_id=c.buy_order_id AND NEW.position_id=c.position_id
      AND NEW.mint=c.token AND NEW.mint=r.token AND NEW.mint=f.token
      AND NEW.buy_receipt_signature=o.tx_signature AND NEW.buy_receipt_signature=r.tx_signature
      AND r.wallet_pubkey=NEW.wallet AND r.side='buy' AND o.status='execution_canary_confirmed'
      AND r.token_delta_raw=CAST(NEW.amount_raw AS TEXT) AND f.qty_raw=r.token_delta_raw
      AND r.token_decimals=6 AND f.qty_decimals=6 AND NEW.decimals=6
)
BEGIN SELECT RAISE(ABORT,'receipt lot zero reserve requires immutable scope'); END;
