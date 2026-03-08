ALTER TABLE shadow_lots
    ADD COLUMN accounting_bucket TEXT NOT NULL DEFAULT 'legacy_pre_cutover';

ALTER TABLE shadow_closed_trades
    ADD COLUMN accounting_bucket TEXT NOT NULL DEFAULT 'legacy_pre_cutover';
