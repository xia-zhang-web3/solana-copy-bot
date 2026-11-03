-- Preserve an expired unsigned generation; transfer its hold only with a
-- canonical separately authorized owner-exit dispatch. Never rewrite history.
CREATE TABLE unsigned_owner_exit_transfers (
    intent_id TEXT PRIMARY KEY REFERENCES rpc_owned_sell_handoffs(intent_id),
    old_owner TEXT NOT NULL UNIQUE,
    exit_intent_id TEXT NOT NULL UNIQUE REFERENCES owner_exit_intents(intent_id),
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(order_id),
    held_fee INTEGER NOT NULL CHECK(held_fee=100000),
    handoff_json TEXT NOT NULL CHECK(json_valid(handoff_json)),
    build_plan_json TEXT CHECK(build_plan_json IS NULL OR json_valid(build_plan_json)),
    transferred_at TEXT NOT NULL
);
CREATE TRIGGER unsigned_owner_exit_no_update BEFORE UPDATE ON unsigned_owner_exit_transfers
BEGIN SELECT RAISE(ABORT,'immutable unsigned owner exit transfer'); END;
CREATE TRIGGER unsigned_owner_exit_no_delete BEFORE DELETE ON unsigned_owner_exit_transfers
BEGIN SELECT RAISE(ABORT,'immutable unsigned owner exit transfer'); END;
CREATE VIEW rpc_owned_sell_pending_obligations AS
SELECT h.* FROM rpc_owned_sell_handoffs h WHERE NOT EXISTS (
    SELECT 1 FROM unsigned_owner_exit_transfers t
    JOIN owner_exit_intents i ON i.intent_id=t.exit_intent_id
    JOIN orders o ON o.order_id=t.order_id
    JOIN execution_order_sources s ON s.identity_id=o.signal_id AND s.owner_exit_intent_id=i.intent_id
    JOIN execution_canary_dispatch d ON d.order_id=o.order_id
    JOIN owner_exit_fee_reservations f ON f.order_id=d.order_id
    WHERE t.intent_id=h.intent_id AND t.old_owner=h.owner AND t.held_fee=h.fee_reserve
      AND i.intent_id LIKE 'bot-receipt-exit:%' AND i.position_id=h.position_id
      AND i.wallet=h.wallet AND i.run_id=h.experiment_id
      AND i.mint=json_extract(h.snapshot,'$.quote.mint')
      AND i.amount_raw=CAST(json_extract(h.snapshot,'$.quote.raw') AS INTEGER)
      AND i.decimals=6 AND o.order_id='exec-canary:owner-exit:'||i.intent_id
      AND o.signal_id='owner-exit:'||i.intent_id AND o.client_order_id=d.client_order_id
      AND d.signal_id=o.signal_id AND d.side='sell' AND d.attempt=1
      AND d.wallet=i.wallet AND d.token=i.mint AND d.route=i.route AND o.route=d.route
      AND o.tx_signature=d.tx_signature AND f.tx_signature=d.tx_signature
      AND f.run_id=i.run_id AND f.wallet=i.wallet AND f.fee_bound=t.held_fee
);
