CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
    ON observed_swaps(ts, slot, signature);
