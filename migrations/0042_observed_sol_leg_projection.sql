CREATE TABLE IF NOT EXISTS observed_sol_leg_swaps (
    signature TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    is_buy INTEGER NOT NULL CHECK(is_buy IN (0, 1)),
    token_mint TEXT NOT NULL,
    token_qty REAL NOT NULL,
    sol_notional REAL NOT NULL,
    slot INTEGER NOT NULL,
    ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observed_sol_leg_swaps_ts_slot_signature
    ON observed_sol_leg_swaps(ts, slot, signature);

CREATE TABLE IF NOT EXISTS observed_sol_leg_coverage (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    covered_from_ts TEXT,
    covered_through_ts TEXT,
    updated_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS observed_swaps_sol_leg_projection_insert
AFTER INSERT ON observed_swaps
WHEN NEW.token_in = 'So11111111111111111111111111111111111111112'
  OR NEW.token_out = 'So11111111111111111111111111111111111111112'
BEGIN
    INSERT OR REPLACE INTO observed_sol_leg_swaps(
        signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts
    ) VALUES (
        NEW.signature,
        NEW.wallet_id,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.token_out ELSE NEW.token_in END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.qty_out ELSE NEW.qty_in END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.qty_in ELSE NEW.qty_out END,
        NEW.slot,
        NEW.ts
    );
    UPDATE observed_sol_leg_coverage
       SET covered_through_ts = CASE
               WHEN covered_through_ts IS NULL OR NEW.ts > covered_through_ts THEN NEW.ts
               ELSE covered_through_ts
           END,
           updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
     WHERE id = 1;
END;

CREATE TRIGGER IF NOT EXISTS observed_swaps_sol_leg_projection_delete
AFTER DELETE ON observed_swaps
BEGIN
    DELETE FROM observed_sol_leg_swaps WHERE signature = OLD.signature;
END;
