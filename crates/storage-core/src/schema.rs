use crate::observed_timestamp::{
    ensure_observed_swaps_timestamp_validation_index_empty_safe,
    observed_swaps_non_utc_timestamp_index_is_valid,
};
use crate::quality::ensure_discovery_v2_quality_prepare_tables;
use crate::schema_indexes::{
    ensure_observed_swaps_read_indexes_empty_safe, followlist_active_wallet_index_is_valid,
    validate_observed_swaps_read_indexes,
};
use crate::status_snapshot::ensure_discovery_v2_status_snapshot_table;
use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};

pub fn ensure_discovery_v2_schema(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(SCHEMA)
        .context("failed ensuring discovery v2 storage-core schema")?;
    ensure_observed_swaps_timestamp_validation_index_empty_safe(&store.conn)?;
    ensure_observed_swaps_read_indexes_empty_safe(store)?;
    ensure_discovery_strategy_state_table(store)?;
    ensure_discovery_runtime_state_table(store)?;
    ensure_discovery_v2_status_snapshot_table(store)?;
    ensure_discovery_v2_quality_prepare_tables(store)?;
    Ok(())
}

pub(crate) fn ensure_discovery_strategy_state_table(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS discovery_strategy_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                trusted_selection_bootstrap_required INTEGER NOT NULL DEFAULT 0,
                trusted_selection_reason TEXT NOT NULL DEFAULT '',
                trusted_selection_state TEXT NOT NULL DEFAULT 'invalid',
                active_trusted_snapshot_id TEXT,
                active_trusted_snapshot_window_start TEXT,
                last_trusted_bootstrap_source_kind TEXT,
                last_trusted_bootstrap_at TEXT,
                bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0,
                bootstrap_degraded_reason TEXT,
                bootstrap_degraded_armed_at TEXT,
                publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
                publication_reason TEXT NOT NULL DEFAULT '',
                publication_last_published_at TEXT,
                publication_last_published_window_start TEXT,
                publication_scoring_source TEXT,
                publication_wallet_ids_json TEXT,
                publication_policy_fingerprint TEXT,
                publication_runtime_cursor_ts TEXT,
                publication_runtime_cursor_slot INTEGER,
                publication_runtime_cursor_signature TEXT,
                updated_at TEXT NOT NULL
            )",
        )
        .context("failed ensuring discovery_strategy_state table")?;
    for (column, definition) in [
        (
            "trusted_selection_bootstrap_required",
            "INTEGER NOT NULL DEFAULT 0",
        ),
        ("trusted_selection_reason", "TEXT NOT NULL DEFAULT ''"),
        ("trusted_selection_state", "TEXT NOT NULL DEFAULT 'invalid'"),
        ("active_trusted_snapshot_id", "TEXT"),
        ("active_trusted_snapshot_window_start", "TEXT"),
        ("last_trusted_bootstrap_source_kind", "TEXT"),
        ("last_trusted_bootstrap_at", "TEXT"),
        ("bootstrap_degraded_active", "INTEGER NOT NULL DEFAULT 0"),
        ("bootstrap_degraded_reason", "TEXT"),
        ("bootstrap_degraded_armed_at", "TEXT"),
        (
            "publication_runtime_mode",
            "TEXT NOT NULL DEFAULT 'fail_closed'",
        ),
        ("publication_reason", "TEXT NOT NULL DEFAULT ''"),
        ("publication_last_published_at", "TEXT"),
        ("publication_last_published_window_start", "TEXT"),
        ("publication_scoring_source", "TEXT"),
        ("publication_wallet_ids_json", "TEXT"),
        ("publication_policy_fingerprint", "TEXT"),
        ("publication_runtime_cursor_ts", "TEXT"),
        ("publication_runtime_cursor_slot", "INTEGER"),
        ("publication_runtime_cursor_signature", "TEXT"),
        (
            "updated_at",
            "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        ),
    ] {
        ensure_column(store, "discovery_strategy_state", column, definition)?;
    }
    Ok(())
}

pub(crate) fn ensure_discovery_runtime_state_table(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS discovery_runtime_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                cursor_ts TEXT NOT NULL,
                cursor_slot INTEGER NOT NULL,
                cursor_signature TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
        )
        .context("failed ensuring discovery_runtime_state table")?;
    Ok(())
}

pub fn validate_discovery_v2_schema_read_only(store: &SqliteDiscoveryStore) -> Result<()> {
    validate_discovery_v2_schema_read_only_inner(store, true)
}

pub fn validate_discovery_v2_status_schema_read_only(store: &SqliteDiscoveryStore) -> Result<()> {
    validate_discovery_v2_schema_read_only_inner(store, false)
}

fn validate_discovery_v2_schema_read_only_inner(
    store: &SqliteDiscoveryStore,
    require_runtime_state: bool,
) -> Result<()> {
    for table in [
        "observed_swaps",
        "token_quality_cache",
        "wallets",
        "wallet_metrics",
        "followlist",
        "discovery_strategy_state",
    ] {
        if !store.sqlite_table_exists(table)? {
            anyhow::bail!("discovery v2 schema missing required table: {table}");
        }
    }
    if require_runtime_state && !store.sqlite_table_exists("discovery_runtime_state")? {
        anyhow::bail!("discovery v2 schema missing required table: discovery_runtime_state");
    }
    for (table, column) in [
        ("observed_swaps", "signature"),
        ("observed_swaps", "wallet_id"),
        ("observed_swaps", "dex"),
        ("observed_swaps", "token_in"),
        ("observed_swaps", "token_out"),
        ("observed_swaps", "qty_in"),
        ("observed_swaps", "qty_out"),
        ("observed_swaps", "qty_in_raw"),
        ("observed_swaps", "qty_in_decimals"),
        ("observed_swaps", "qty_out_raw"),
        ("observed_swaps", "qty_out_decimals"),
        ("observed_swaps", "slot"),
        ("observed_swaps", "ts"),
        ("token_quality_cache", "mint"),
        ("token_quality_cache", "holders"),
        ("token_quality_cache", "liquidity_sol"),
        ("token_quality_cache", "token_age_seconds"),
        ("token_quality_cache", "fetched_at"),
        ("wallets", "wallet_id"),
        ("wallets", "first_seen"),
        ("wallets", "last_seen"),
        ("wallets", "status"),
        ("wallet_metrics", "id"),
        ("wallet_metrics", "wallet_id"),
        ("wallet_metrics", "window_start"),
        ("wallet_metrics", "pnl"),
        ("wallet_metrics", "win_rate"),
        ("wallet_metrics", "trades"),
        ("wallet_metrics", "closed_trades"),
        ("wallet_metrics", "hold_median_seconds"),
        ("wallet_metrics", "score"),
        ("wallet_metrics", "buy_total"),
        ("wallet_metrics", "tradable_ratio"),
        ("wallet_metrics", "rug_ratio"),
        ("followlist", "id"),
        ("followlist", "wallet_id"),
        ("followlist", "added_at"),
        ("followlist", "removed_at"),
        ("followlist", "reason"),
        ("followlist", "active"),
        ("discovery_strategy_state", "id"),
        ("discovery_strategy_state", "publication_runtime_mode"),
        ("discovery_strategy_state", "publication_reason"),
        ("discovery_strategy_state", "publication_last_published_at"),
        (
            "discovery_strategy_state",
            "publication_last_published_window_start",
        ),
        ("discovery_strategy_state", "publication_scoring_source"),
        ("discovery_strategy_state", "publication_wallet_ids_json"),
        ("discovery_strategy_state", "publication_policy_fingerprint"),
        ("discovery_strategy_state", "publication_runtime_cursor_ts"),
        (
            "discovery_strategy_state",
            "publication_runtime_cursor_slot",
        ),
        (
            "discovery_strategy_state",
            "publication_runtime_cursor_signature",
        ),
        ("discovery_strategy_state", "updated_at"),
    ] {
        if !column_exists(store, table, column)? {
            anyhow::bail!("discovery v2 schema missing required column: {table}.{column}");
        }
    }
    if require_runtime_state {
        for column in [
            "id",
            "cursor_ts",
            "cursor_slot",
            "cursor_signature",
            "updated_at",
        ] {
            if !column_exists(store, "discovery_runtime_state", column)? {
                anyhow::bail!(
                    "discovery v2 schema missing required column: discovery_runtime_state.{column}"
                );
            }
        }
    }
    if !followlist_active_wallet_index_is_valid(store)? {
        anyhow::bail!(
            "discovery v2 schema missing or malformed required index: idx_followlist_one_active_wallet"
        );
    }
    if !observed_swaps_non_utc_timestamp_index_is_valid(store)? {
        anyhow::bail!(
            "discovery v2 schema missing or malformed required index: idx_observed_swaps_non_utc_ts"
        );
    }
    validate_observed_swaps_read_indexes(store)?;
    Ok(())
}

pub(crate) fn ensure_column(
    store: &SqliteDiscoveryStore,
    table: &str,
    column: &str,
    column_definition: &str,
) -> Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = store.conn.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let existing: String = row.get(1)?;
        if existing == column {
            return Ok(());
        }
    }
    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {column_definition}");
    store
        .conn
        .execute(&alter, [])
        .with_context(|| format!("failed adding {table}.{column}"))?;
    Ok(())
}

pub(crate) fn column_exists(
    store: &SqliteDiscoveryStore,
    table: &str,
    column: &str,
) -> Result<bool> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = store.conn.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let existing: String = row.get(1)?;
        if existing == column {
            return Ok(true);
        }
    }
    Ok(false)
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_followlist_one_active_wallet
    ON followlist(wallet_id)
    WHERE active = 1;

CREATE TABLE IF NOT EXISTS discovery_strategy_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    trusted_selection_bootstrap_required INTEGER NOT NULL DEFAULT 0,
    trusted_selection_reason TEXT NOT NULL DEFAULT '',
    trusted_selection_state TEXT NOT NULL DEFAULT 'invalid',
    active_trusted_snapshot_id TEXT,
    active_trusted_snapshot_window_start TEXT,
    last_trusted_bootstrap_source_kind TEXT,
    last_trusted_bootstrap_at TEXT,
    bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0,
    bootstrap_degraded_reason TEXT,
    bootstrap_degraded_armed_at TEXT,
    publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
    publication_reason TEXT NOT NULL DEFAULT '',
    publication_last_published_at TEXT,
    publication_last_published_window_start TEXT,
    publication_scoring_source TEXT,
    publication_wallet_ids_json TEXT,
    publication_policy_fingerprint TEXT,
    publication_runtime_cursor_ts TEXT,
    publication_runtime_cursor_slot INTEGER,
    publication_runtime_cursor_signature TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS discovery_runtime_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    cursor_ts TEXT NOT NULL,
    cursor_slot INTEGER NOT NULL,
    cursor_signature TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS discovery_v2_status_snapshot (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    policy_fingerprint TEXT NOT NULL,
    status_now TEXT NOT NULL,
    status_window_start TEXT NOT NULL,
    runtime_cursor_ts TEXT,
    runtime_cursor_slot INTEGER,
    runtime_cursor_signature TEXT,
    status_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
";
