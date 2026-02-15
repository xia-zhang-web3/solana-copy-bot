CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_in_out_ts
    ON observed_swaps(token_in, token_out, ts);

CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_out_in_ts
    ON observed_swaps(token_out, token_in, ts);

CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_wallet_closed_ts
    ON shadow_closed_trades(wallet_id, closed_ts);
