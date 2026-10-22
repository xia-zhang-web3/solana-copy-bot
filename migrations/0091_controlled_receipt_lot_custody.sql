SELECT enabled FROM temp.receipt_lot_rebuild_guard;
-- Explicit controlled shared-wallet scope only. Existing generic experiments unchanged.
CREATE TABLE controlled_receipt_lot_scope (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 wallet TEXT NOT NULL,
 run_id TEXT NOT NULL,
 scope_json TEXT NOT NULL CHECK(length(CAST(scope_json AS BLOB)) BETWEEN 1 AND 8192)
);

CREATE TRIGGER controlled_receipt_lot_scope_no_update BEFORE UPDATE ON controlled_receipt_lot_scope BEGIN SELECT RAISE(ABORT,'receipt lot scope immutable');END;
CREATE TRIGGER controlled_receipt_lot_scope_no_delete BEFORE DELETE ON controlled_receipt_lot_scope BEGIN SELECT RAISE(ABORT,'receipt lot scope immutable');END;
