CREATE TABLE IF NOT EXISTS token_quality_cache (
    mint TEXT PRIMARY KEY,
    holders INTEGER,
    liquidity_sol REAL,
    token_age_seconds INTEGER,
    fetched_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_token_quality_cache_fetched_at
    ON token_quality_cache(fetched_at);
