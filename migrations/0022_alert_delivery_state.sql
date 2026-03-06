CREATE TABLE IF NOT EXISTS alert_delivery_state (
    channel TEXT PRIMARY KEY,
    last_rowid INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
