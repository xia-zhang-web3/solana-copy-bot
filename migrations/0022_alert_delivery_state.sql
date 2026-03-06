CREATE TABLE IF NOT EXISTS alert_delivery_state (
    channel TEXT PRIMARY KEY,
    last_ts TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
