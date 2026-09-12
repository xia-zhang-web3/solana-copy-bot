-- Bounded last observation and lease per strict intent; never a legacy quote or order.
-- object ordered_sell_quote_results
CREATE TABLE ordered_sell_quote_results (
    intent_id TEXT PRIMARY KEY REFERENCES ordered_source_sell_intents(intent_id),
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    owner TEXT NOT NULL,
    lease_until TEXT,
    binding TEXT,
    record TEXT,
    binding_attempt INTEGER NOT NULL CHECK(binding_attempt BETWEEN 1 AND 3),
    CHECK(length(CAST(binding AS BLOB)) <= 131072),
    CHECK(length(CAST(record AS BLOB)) <= 131072)
);
-- object ordered_sell_quote_cursor
CREATE TABLE ordered_sell_quote_cursor (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    intent_id TEXT NOT NULL
);
