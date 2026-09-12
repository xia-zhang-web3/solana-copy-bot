-- Lookup only: preserve all historical duplicate claims and monetary rows.
CREATE INDEX IF NOT EXISTS idx_buy_receipt_proofs_signature
    ON execution_canary_receipt_proofs(tx_signature, order_id, wallet_pubkey);
CREATE INDEX IF NOT EXISTS idx_buy_receipt_facts_signature
    ON execution_canary_receipt_facts(tx_signature, order_id, wallet_pubkey);
CREATE INDEX IF NOT EXISTS idx_buy_receipt_orders_signature
    ON orders(tx_signature, order_id);
