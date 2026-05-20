CREATE INDEX IF NOT EXISTS idx_observed_sol_leg_swaps_scan_covering
    ON observed_sol_leg_swaps(
        ts,
        slot,
        signature,
        wallet_id,
        is_buy,
        token_mint,
        token_qty,
        sol_notional
    );
