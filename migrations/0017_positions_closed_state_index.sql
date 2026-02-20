CREATE INDEX IF NOT EXISTS idx_positions_state_closed_ts
    ON positions(state, closed_ts);
