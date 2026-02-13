ALTER TABLE wallet_metrics ADD COLUMN closed_trades INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS shadow_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    cost_sol REAL NOT NULL,
    opened_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shadow_lots_wallet_token ON shadow_lots(wallet_id, token, id);

CREATE TABLE IF NOT EXISTS shadow_closed_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    entry_cost_sol REAL NOT NULL,
    exit_value_sol REAL NOT NULL,
    pnl_sol REAL NOT NULL,
    opened_ts TEXT NOT NULL,
    closed_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shadow_closed_trades_closed_ts
    ON shadow_closed_trades(closed_ts);
