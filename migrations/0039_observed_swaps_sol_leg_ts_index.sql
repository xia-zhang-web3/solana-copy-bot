DROP INDEX IF EXISTS idx_observed_swaps_sol_leg_ts_slot_signature;

CREATE INDEX IF NOT EXISTS idx_observed_swaps_sol_leg_ts_slot_signature
    ON observed_swaps(ts, slot, signature)
    WHERE token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112';
