ALTER TABLE shadow_lots ADD COLUMN live_eligible INTEGER NOT NULL DEFAULT 0;
ALTER TABLE shadow_closed_trades ADD COLUMN live_eligible INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_shadow_lots_live_opened_open
    ON shadow_lots(live_eligible, opened_ts)
    WHERE qty > 0;

CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_live_closed
    ON shadow_closed_trades(live_eligible, closed_ts);
