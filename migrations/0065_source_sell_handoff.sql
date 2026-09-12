-- object source_sell_handoffs
CREATE TABLE source_sell_handoffs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT NOT NULL UNIQUE,
    wallet_id TEXT NOT NULL, dex TEXT NOT NULL,
    token_in TEXT NOT NULL, token_out TEXT NOT NULL,
    qty_in REAL NOT NULL, qty_out REAL NOT NULL,
    slot INTEGER NOT NULL, ts TEXT NOT NULL,
    qty_in_raw TEXT, qty_in_decimals INTEGER,
    qty_out_raw TEXT, qty_out_decimals INTEGER,
    original_position_id TEXT,
    disposition TEXT NOT NULL CHECK(disposition IN ('pending','unknown','staged','refused')),
    reason TEXT NOT NULL CHECK(length(reason)<=64),
    CHECK((original_position_id IS NULL AND disposition='unknown') OR
          (length(original_position_id)>0 AND disposition IN ('pending','staged','refused')))
);
-- object source_sell_handoff_cursor
CREATE TABLE source_sell_handoff_cursor (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    last_sequence INTEGER CHECK(last_sequence IS NULL OR last_sequence>0)
);
-- object idx_source_sell_handoff_pending
CREATE INDEX idx_source_sell_handoff_pending ON source_sell_handoffs(disposition,sequence);
-- object source_sell_handoff_before_observed
CREATE TRIGGER source_sell_handoff_before_observed BEFORE INSERT ON observed_swaps
BEGIN
    INSERT OR IGNORE INTO source_sell_handoffs(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,disposition,reason)
        SELECT signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,'unknown','original_generation_unknown' FROM observed_swaps
        WHERE signature=NEW.signature AND token_out='So11111111111111111111111111111111111111112'
        AND token_in!='So11111111111111111111111111111111111111112'
        AND NOT EXISTS(SELECT 1 FROM source_sell_handoffs WHERE signature=NEW.signature);
    SELECT CASE WHEN EXISTS(SELECT 1 FROM source_sell_handoffs h WHERE h.signature=NEW.signature
        AND NOT (h.signature IS NEW.signature AND h.wallet_id IS NEW.wallet_id AND h.dex IS NEW.dex AND h.token_in IS NEW.token_in AND h.token_out IS NEW.token_out AND h.qty_in IS NEW.qty_in AND h.qty_out IS NEW.qty_out AND h.slot IS NEW.slot AND h.ts IS NEW.ts AND h.qty_in_raw IS NEW.qty_in_raw AND h.qty_in_decimals IS NEW.qty_in_decimals AND h.qty_out_raw IS NEW.qty_out_raw AND h.qty_out_decimals IS NEW.qty_out_decimals)) THEN RAISE(IGNORE) END;
END;
-- object source_sell_handoff_after_observed
CREATE TRIGGER source_sell_handoff_after_observed AFTER INSERT ON observed_swaps
WHEN NEW.token_out='So11111111111111111111111111111111111111112'
    AND NEW.token_in!='So11111111111111111111111111111111111111112'
BEGIN
    INSERT OR IGNORE INTO source_sell_handoffs(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,disposition,reason)
        SELECT NEW.signature,NEW.wallet_id,NEW.dex,NEW.token_in,NEW.token_out,NEW.qty_in,NEW.qty_out,NEW.slot,NEW.ts,NEW.qty_in_raw,NEW.qty_in_decimals,NEW.qty_out_raw,NEW.qty_out_decimals,'unknown','original_generation_unknown'
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_handoffs WHERE signature=NEW.signature);
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_handoffs WHERE signature=NEW.signature)
        THEN RAISE(ABORT,'source_sell_handoff_missing') END;
END;
-- object source_sell_handoff_before_retention
CREATE TRIGGER source_sell_handoff_before_retention BEFORE DELETE ON observed_swaps
WHEN OLD.token_out='So11111111111111111111111111111111111111112'
    AND OLD.token_in!='So11111111111111111111111111111111111111112'
BEGIN
    INSERT OR IGNORE INTO source_sell_handoffs(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,disposition,reason)
        SELECT OLD.signature,OLD.wallet_id,OLD.dex,OLD.token_in,OLD.token_out,OLD.qty_in,OLD.qty_out,OLD.slot,OLD.ts,OLD.qty_in_raw,OLD.qty_in_decimals,OLD.qty_out_raw,OLD.qty_out_decimals,'unknown','original_generation_unknown'
        WHERE NOT EXISTS(SELECT 1 FROM source_sell_handoffs WHERE signature=OLD.signature);
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM source_sell_handoffs WHERE signature=OLD.signature)
        THEN RAISE(ABORT,'source_sell_handoff_retention_missing') END;
END;
