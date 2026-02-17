CREATE TABLE IF NOT EXISTS ui_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    stage TEXT,
    reason TEXT,
    wallet_id TEXT,
    token TEXT,
    signature TEXT,
    payload_json TEXT,
    ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ui_events_ts
    ON ui_events(ts);

CREATE INDEX IF NOT EXISTS idx_ui_events_type_ts
    ON ui_events(event_type, ts);
