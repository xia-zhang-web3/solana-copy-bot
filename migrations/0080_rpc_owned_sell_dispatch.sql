-- Only the protected migration runner may rebuild orders while preserving incoming FKs.
SELECT enabled FROM temp.rpc_owned_sell_rebuild_guard;
CREATE TABLE execution_order_sources (
    identity_id TEXT PRIMARY KEY,
    copy_signal_id TEXT UNIQUE REFERENCES copy_signals(signal_id) ON DELETE CASCADE,
    owned_sell_intent_id TEXT UNIQUE REFERENCES rpc_owned_sell_handoffs(intent_id) ON DELETE RESTRICT,
    CHECK ((copy_signal_id IS NOT NULL AND owned_sell_intent_id IS NULL AND identity_id=copy_signal_id)
        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NOT NULL AND identity_id=owned_sell_intent_id))
);
INSERT INTO execution_order_sources(identity_id,copy_signal_id) SELECT signal_id,signal_id FROM copy_signals;
CREATE TRIGGER execution_order_source_insert AFTER INSERT ON copy_signals
BEGIN
    INSERT INTO execution_order_sources(identity_id,copy_signal_id) VALUES(NEW.signal_id,NEW.signal_id);
END;
CREATE TRIGGER execution_order_source_update AFTER UPDATE OF signal_id ON copy_signals
WHEN OLD.signal_id != NEW.signal_id
BEGIN
    UPDATE execution_order_sources SET identity_id=NEW.signal_id,copy_signal_id=NEW.signal_id WHERE copy_signal_id=OLD.signal_id;
END;
-- Transfer one existing unsigned owner into canonical execution. Retain its evidence.
CREATE TABLE rpc_owned_sell_dispatches (
    intent_id TEXT PRIMARY KEY REFERENCES rpc_owned_sell_handoffs(intent_id),
    handoff_owner TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL UNIQUE REFERENCES execution_canary_dispatch(order_id),
    prepared TEXT NOT NULL,
    dispatch TEXT NOT NULL,
    consumed_at TEXT NOT NULL
);
