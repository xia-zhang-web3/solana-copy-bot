CREATE INDEX IF NOT EXISTS idx_wallet_metrics_wallet_id_id
    ON wallet_metrics(wallet_id, id DESC);
