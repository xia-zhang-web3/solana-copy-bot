CREATE TABLE IF NOT EXISTS wallets (
    wallet_id TEXT PRIMARY KEY,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS wallet_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    pnl REAL NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL DEFAULT 0,
    trades INTEGER NOT NULL DEFAULT 0,
    hold_median_seconds INTEGER NOT NULL DEFAULT 0,
    score REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS followlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL,
    added_at TEXT NOT NULL,
    removed_at TEXT,
    reason TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS observed_swaps (
    signature TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    dex TEXT NOT NULL,
    token_in TEXT NOT NULL,
    token_out TEXT NOT NULL,
    qty_in REAL NOT NULL,
    qty_out REAL NOT NULL,
    slot INTEGER NOT NULL,
    ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS copy_signals (
    signal_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    side TEXT NOT NULL,
    token TEXT NOT NULL,
    notional_sol REAL NOT NULL,
    ts TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    route TEXT NOT NULL,
    submit_ts TEXT NOT NULL,
    confirm_ts TEXT,
    status TEXT NOT NULL,
    err_code TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    fee REAL NOT NULL DEFAULT 0,
    slippage_bps REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    cost_sol REAL NOT NULL,
    opened_ts TEXT NOT NULL,
    closed_ts TEXT,
    pnl_sol REAL,
    state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_events (
    event_id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    severity TEXT NOT NULL,
    ts TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS system_heartbeat (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    ts TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observed_swaps_wallet_ts ON observed_swaps(wallet_id, ts);
CREATE INDEX IF NOT EXISTS idx_wallet_metrics_score_window ON wallet_metrics(score, window_start);
CREATE INDEX IF NOT EXISTS idx_positions_state_token ON positions(state, token);
CREATE INDEX IF NOT EXISTS idx_orders_status_submit_ts ON orders(status, submit_ts);
