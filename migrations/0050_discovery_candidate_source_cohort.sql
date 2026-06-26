CREATE TABLE IF NOT EXISTS discovery_candidate_sources (
    wallet_id TEXT PRIMARY KEY,
    source_cohort TEXT NOT NULL,
    window_start TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

ALTER TABLE execution_quote_canary_events ADD COLUMN source_cohort TEXT;
