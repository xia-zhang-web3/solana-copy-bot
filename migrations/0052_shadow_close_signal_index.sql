-- Atomic FIFO replay checks must not scan a wallet's full close history.
CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_signal_wallet_token
    ON shadow_closed_trades(signal_id, wallet_id, token);
