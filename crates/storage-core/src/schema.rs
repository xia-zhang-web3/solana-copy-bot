use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};

pub fn ensure_discovery_v2_schema(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(SCHEMA)
        .context("failed ensuring discovery v2 storage-core schema")?;
    ensure_column(
        store,
        "discovery_strategy_state",
        "publication_policy_fingerprint",
        "TEXT",
    )?;
    ensure_column(
        store,
        "discovery_strategy_state",
        "publication_runtime_cursor_ts",
        "TEXT",
    )?;
    ensure_column(
        store,
        "discovery_strategy_state",
        "publication_runtime_cursor_slot",
        "INTEGER",
    )?;
    ensure_column(
        store,
        "discovery_strategy_state",
        "publication_runtime_cursor_signature",
        "TEXT",
    )?;
    Ok(())
}

pub fn validate_discovery_v2_schema_read_only(store: &SqliteDiscoveryStore) -> Result<()> {
    for table in [
        "observed_swaps",
        "token_quality_cache",
        "wallets",
        "wallet_metrics",
        "followlist",
        "discovery_strategy_state",
        "discovery_runtime_state",
    ] {
        if !store.sqlite_table_exists(table)? {
            anyhow::bail!("discovery v2 schema missing required table: {table}");
        }
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
        ("discovery_runtime_state", "id"),
        ("discovery_runtime_state", "cursor_ts"),
        ("discovery_runtime_state", "cursor_slot"),
        ("discovery_runtime_state", "cursor_signature"),
        ("discovery_runtime_state", "updated_at"),
    ] {
        if !column_exists(store, table, column)? {
            anyhow::bail!("discovery v2 schema missing required column: {table}.{column}");
        }
    }
    if !followlist_active_wallet_index_is_valid(store)? {
        anyhow::bail!(
            "discovery v2 schema missing or malformed required index: idx_followlist_one_active_wallet"
        );
    }
    Ok(())
}

fn ensure_column(
    store: &SqliteDiscoveryStore,
    table: &str,
    column: &str,
    column_type: &str,
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
    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {column_type}");
    store
        .conn
        .execute(&alter, [])
        .with_context(|| format!("failed adding {table}.{column}"))?;
    Ok(())
}

fn column_exists(store: &SqliteDiscoveryStore, table: &str, column: &str) -> Result<bool> {
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

fn followlist_active_wallet_index_is_valid(store: &SqliteDiscoveryStore) -> Result<bool> {
    use rusqlite::OptionalExtension;

    let index_flags = store
        .conn
        .query_row(
            "SELECT [unique], partial FROM pragma_index_list('followlist') WHERE name = ?1",
            ["idx_followlist_one_active_wallet"],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed checking followlist active wallet index flags")?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 1 || partial != 1 {
        return Ok(false);
    }

    let mut stmt = store
        .conn
        .prepare(
            "SELECT name FROM pragma_index_info('idx_followlist_one_active_wallet') ORDER BY seqno",
        )
        .context("failed preparing followlist active wallet index column introspection")?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("failed querying followlist active wallet index columns")?
        .collect::<rusqlite::Result<Vec<String>>>()
        .context("failed collecting followlist active wallet index columns")?;
    if columns.as_slice() != ["wallet_id"] {
        return Ok(false);
    }

    let index_sql: Option<String> = store
        .conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1 LIMIT 1",
            ["idx_followlist_one_active_wallet"],
            |row| row.get(0),
        )
        .optional()
        .context("failed reading followlist active wallet index sql")?;
    let Some(index_sql) = index_sql else {
        return Ok(false);
    };
    Ok(followlist_active_wallet_index_predicate_is_valid(
        &index_sql,
    ))
}

fn followlist_active_wallet_index_predicate_is_valid(index_sql: &str) -> bool {
    let compact_sql: String = index_sql
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '"' && *ch != '`' && *ch != '[' && *ch != ']')
        .flat_map(char::to_lowercase)
        .collect();
    let Some((_, predicate)) = compact_sql.split_once("where") else {
        return false;
    };
    let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
    matches!(
        predicate,
        "active=1" | "(active=1)" | "1=active" | "(1=active)"
    )
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_followlist_one_active_wallet
    ON followlist(wallet_id)
    WHERE active = 1;

CREATE TABLE IF NOT EXISTS discovery_strategy_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
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
";
