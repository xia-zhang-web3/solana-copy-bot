ALTER TABLE shadow_closed_trades
    ADD COLUMN close_context TEXT NOT NULL DEFAULT 'market';

CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_close_context_closed_ts
    ON shadow_closed_trades(close_context, closed_ts);
