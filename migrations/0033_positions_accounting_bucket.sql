ALTER TABLE positions
    ADD COLUMN accounting_bucket TEXT NOT NULL DEFAULT 'legacy_pre_cutover';

DROP INDEX IF EXISTS idx_positions_one_open_token;

CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_one_open_token_bucket
ON positions(token, accounting_bucket)
WHERE state = 'open';
