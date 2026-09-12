-- Traversal only: never an order, generation witness or write-off authority.
CREATE TABLE execution_failed_sell_sweep_cursors (
    route TEXT PRIMARY KEY NOT NULL CHECK(length(trim(route)) > 0),
    last_submit_ts TEXT,
    last_rowid INTEGER,
    CHECK ((last_submit_ts IS NULL AND last_rowid IS NULL)
        OR (last_submit_ts IS NOT NULL AND length(last_submit_ts) > 0 AND last_rowid IS NOT NULL))
);
