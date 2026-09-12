-- Traversal progress is separate from signal time, quotes, orders and fills.
-- Keep the key even if its signal leaves the candidate set; wrap on the next page.
CREATE TABLE IF NOT EXISTS execution_owned_sell_cursor (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    signal_ts TEXT NOT NULL,
    signal_id TEXT NOT NULL
);
