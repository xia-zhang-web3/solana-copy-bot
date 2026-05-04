use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};

pub fn ensure_discovery_v2_schema(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(SCHEMA)
        .context("failed ensuring discovery v2 storage-core schema")
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS observed_swaps (
    signature TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    dex TEXT NOT NULL,
    token_in TEXT NOT NULL,
    token_out TEXT NOT NULL,
    qty_in REAL NOT NULL,
    qty_out REAL NOT NULL,
    qty_in_raw TEXT,
    qty_in_decimals INTEGER,
    qty_out_raw TEXT,
    qty_out_decimals INTEGER,
    slot INTEGER NOT NULL,
    ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
    ON observed_swaps(ts, slot, signature);

CREATE TABLE IF NOT EXISTS token_quality_cache (
    mint TEXT PRIMARY KEY,
    holders INTEGER,
    liquidity_sol REAL,
    token_age_seconds INTEGER,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallets (
    wallet_id TEXT PRIMARY KEY,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS wallet_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    pnl REAL NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL DEFAULT 0,
    trades INTEGER NOT NULL DEFAULT 0,
    closed_trades INTEGER NOT NULL DEFAULT 0,
    hold_median_seconds INTEGER NOT NULL DEFAULT 0,
    score REAL NOT NULL DEFAULT 0,
    buy_total INTEGER NOT NULL DEFAULT 0,
    tradable_ratio REAL NOT NULL DEFAULT 0.0,
    rug_ratio REAL NOT NULL DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS followlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id TEXT NOT NULL,
    added_at TEXT NOT NULL,
    removed_at TEXT,
    reason TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS discovery_strategy_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
    publication_reason TEXT NOT NULL DEFAULT '',
    publication_last_published_at TEXT,
    publication_last_published_window_start TEXT,
    publication_scoring_source TEXT,
    publication_wallet_ids_json TEXT,
    publication_policy_fingerprint TEXT,
    updated_at TEXT NOT NULL
);
";
