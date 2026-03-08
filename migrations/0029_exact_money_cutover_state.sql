CREATE TABLE IF NOT EXISTS exact_money_cutover_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cutover_ts TEXT NOT NULL,
    recorded_ts TEXT NOT NULL,
    note TEXT
);
