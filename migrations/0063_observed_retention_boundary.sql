-- A retained old source row is not evidence of coverage across a deleted range.
CREATE TABLE IF NOT EXISTS observed_retention_boundary (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    floor_ts TEXT CHECK(floor_ts IS NULL OR typeof(floor_ts) = 'text')
);
INSERT OR IGNORE INTO observed_retention_boundary(id, floor_ts) VALUES(1, NULL);
