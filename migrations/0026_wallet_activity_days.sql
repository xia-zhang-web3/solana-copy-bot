CREATE TABLE IF NOT EXISTS wallet_activity_days (
    wallet_id TEXT NOT NULL,
    activity_day TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY (wallet_id, activity_day)
);

CREATE INDEX IF NOT EXISTS idx_wallet_activity_days_day_wallet
    ON wallet_activity_days(activity_day, wallet_id);
