CREATE TABLE IF NOT EXISTS discovery_v2_status_snapshot (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    policy_fingerprint TEXT NOT NULL,
    status_now TEXT NOT NULL,
    status_window_start TEXT NOT NULL,
    runtime_cursor_ts TEXT,
    runtime_cursor_slot INTEGER,
    runtime_cursor_signature TEXT,
    status_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
