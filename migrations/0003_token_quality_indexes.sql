CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_in_ts
    ON observed_swaps(token_in, ts);

CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_out_ts
    ON observed_swaps(token_out, ts);

