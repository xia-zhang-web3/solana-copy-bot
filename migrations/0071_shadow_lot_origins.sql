-- Origins outlive closed/deleted lots; AUTOINCREMENT lot ids are never reassigned.
-- No FK to live lots: accepted FIFO deletes fully consumed lots.
-- object shadow_lot_origins
CREATE TABLE shadow_lot_origins (
    lot_id INTEGER PRIMARY KEY,
    signal_id TEXT NOT NULL UNIQUE,
    origin TEXT NOT NULL CHECK(length(CAST(origin AS BLOB)) <= 16384)
);
-- object shadow_lot_origin_no_replace
CREATE TRIGGER shadow_lot_origin_no_replace BEFORE INSERT ON shadow_lot_origins
WHEN EXISTS(SELECT 1 FROM shadow_lot_origins WHERE lot_id=NEW.lot_id OR signal_id=NEW.signal_id)
BEGIN SELECT RAISE(ABORT, 'immutable shadow lot origin'); END;
-- object shadow_lot_origin_no_update
CREATE TRIGGER shadow_lot_origin_no_update BEFORE UPDATE ON shadow_lot_origins
BEGIN SELECT RAISE(ABORT, 'immutable shadow lot origin'); END;
-- object shadow_lot_origin_no_delete
CREATE TRIGGER shadow_lot_origin_no_delete BEFORE DELETE ON shadow_lot_origins
BEGIN SELECT RAISE(ABORT, 'immutable shadow lot origin'); END;
-- object idx_shadow_lots_pair_qty_id
CREATE INDEX idx_shadow_lots_pair_qty_id ON shadow_lots(wallet_id, token, qty, id);
