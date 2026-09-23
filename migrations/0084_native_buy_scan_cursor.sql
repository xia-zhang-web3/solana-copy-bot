-- Bounded keyset progress across invalid historical admissions and restarts.
CREATE TABLE native_buy_scan_cursor (
    kind TEXT PRIMARY KEY CHECK(kind IN ('pending','finalized')),
    admitted_at TEXT NOT NULL,
    signature TEXT NOT NULL
);
