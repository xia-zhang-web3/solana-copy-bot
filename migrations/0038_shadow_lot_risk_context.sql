ALTER TABLE shadow_lots
    ADD COLUMN risk_context TEXT NOT NULL DEFAULT 'market';

CREATE INDEX IF NOT EXISTS idx_shadow_lots_risk_context_opened_ts
    ON shadow_lots(risk_context, opened_ts);
