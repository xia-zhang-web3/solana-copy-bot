CREATE TABLE IF NOT EXISTS wallet_scoring_days (
    wallet_id TEXT NOT NULL,
    activity_day TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    trades INTEGER NOT NULL DEFAULT 0,
    spent_sol REAL NOT NULL DEFAULT 0,
    max_buy_notional_sol REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (wallet_id, activity_day)
);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_days_day_wallet
    ON wallet_scoring_days(activity_day, wallet_id);

CREATE TABLE IF NOT EXISTS wallet_scoring_tx_minutes (
    wallet_id TEXT NOT NULL,
    minute_bucket INTEGER NOT NULL,
    tx_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (wallet_id, minute_bucket)
);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_tx_minutes_bucket_wallet
    ON wallet_scoring_tx_minutes(minute_bucket, wallet_id);

CREATE TABLE IF NOT EXISTS wallet_scoring_open_lots (
    buy_signature TEXT NOT NULL PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    cost_sol REAL NOT NULL,
    opened_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_open_lots_wallet_token_ts
    ON wallet_scoring_open_lots(wallet_id, token, opened_ts, buy_signature);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_open_lots_opened_ts
    ON wallet_scoring_open_lots(opened_ts);

CREATE TABLE IF NOT EXISTS wallet_scoring_carryover_lots (
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    cost_sol REAL NOT NULL,
    oldest_opened_ts TEXT NOT NULL,
    PRIMARY KEY (wallet_id, token)
);

CREATE TABLE IF NOT EXISTS wallet_scoring_buy_facts (
    buy_signature TEXT NOT NULL PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    ts TEXT NOT NULL,
    activity_day TEXT NOT NULL,
    notional_sol REAL NOT NULL,
    market_volume_5m_sol REAL NOT NULL,
    market_unique_traders_5m INTEGER NOT NULL,
    market_liquidity_proxy_sol REAL NOT NULL,
    quality_source TEXT NOT NULL,
    quality_token_age_seconds INTEGER,
    quality_holders INTEGER,
    quality_liquidity_sol REAL,
    rug_check_after_ts TEXT NOT NULL,
    rug_volume_lookahead_sol REAL,
    rug_unique_traders_lookahead INTEGER
);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_buy_facts_wallet_ts
    ON wallet_scoring_buy_facts(wallet_id, ts);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_buy_facts_pending_rug
    ON wallet_scoring_buy_facts(rug_check_after_ts, ts)
    WHERE rug_volume_lookahead_sol IS NULL;

CREATE TABLE IF NOT EXISTS wallet_scoring_close_facts (
    sell_signature TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    closed_ts TEXT NOT NULL,
    activity_day TEXT NOT NULL,
    pnl_sol REAL NOT NULL,
    hold_seconds INTEGER NOT NULL,
    win INTEGER NOT NULL,
    PRIMARY KEY (sell_signature, segment_index)
);

CREATE INDEX IF NOT EXISTS idx_wallet_scoring_close_facts_wallet_ts
    ON wallet_scoring_close_facts(wallet_id, closed_ts);

CREATE TABLE IF NOT EXISTS discovery_scoring_state (
    state_key TEXT NOT NULL PRIMARY KEY,
    state_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
