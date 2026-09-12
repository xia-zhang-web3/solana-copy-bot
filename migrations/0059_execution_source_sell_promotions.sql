-- Immutable provenance for canonical signals; no FK/cascade into retained history.
CREATE TABLE execution_source_sell_promotions (
    signal_id TEXT PRIMARY KEY NOT NULL CHECK(length(trim(signal_id)) > 0),
    intent_id TEXT NOT NULL UNIQUE CHECK(length(trim(intent_id)) > 0),
    promoted_at TEXT NOT NULL CHECK(length(trim(promoted_at)) > 0)
);
