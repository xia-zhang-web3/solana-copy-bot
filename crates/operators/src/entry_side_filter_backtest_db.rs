use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use std::time::Duration as StdDuration;

#[derive(Debug, Clone)]
pub(crate) struct ClosedTrade {
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) entry_cost_sol: f64,
    pub(crate) pnl_sol: f64,
    pub(crate) opened_ts: DateTime<Utc>,
    pub(crate) closed_ts: DateTime<Utc>,
    pub(crate) close_context: String,
}

pub(crate) fn open_read_only_db(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)
        .with_context(|| format!("failed opening read-only sqlite db {}", path.display()))?;
    conn.busy_timeout(StdDuration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "query_only", true)?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "cache_size", -262_144)?;
    conn.pragma_update(None, "mmap_size", 1_073_741_824_i64)?;
    Ok(conn)
}

pub(crate) fn load_closed_trades(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ClosedTrade>> {
    let mut stmt = conn
        .prepare(
            "SELECT wallet_id, token, entry_cost_sol, pnl_sol, opened_ts, closed_ts,
                    COALESCE(close_context, 'market') AS close_context
             FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_closed_ts
             WHERE closed_ts >= ?1
               AND closed_ts < ?2
             ORDER BY closed_ts ASC, id ASC
             LIMIT ?3",
        )
        .context("failed preparing closed trade query")?;
    let rows = stmt.query_map(
        params![since.to_rfc3339(), until.to_rfc3339(), i64::from(limit)],
        read_trade,
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading closed trades")
}

pub(crate) fn load_history_trades(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ClosedTrade>> {
    load_closed_trades(conn, since, until, limit)
}

pub(crate) fn token_first_seen_before(
    conn: &Connection,
    token: &str,
    before: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let left = first_seen_by_column(
        conn,
        "idx_observed_swaps_token_in_ts",
        "token_in",
        token,
        before,
    )?;
    let right = first_seen_by_column(
        conn,
        "idx_observed_swaps_token_out_ts",
        "token_out",
        token,
        before,
    )?;
    Ok(match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    })
}

pub(crate) fn latest_leader_buy_before(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
    before: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let mut stmt = conn
        .prepare(
            "SELECT ts
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE wallet_id = ?1
               AND token_out = ?2
               AND ts <= ?3
             ORDER BY ts DESC
             LIMIT 1",
        )
        .context("failed preparing leader buy lookup")?;
    optional_ts(
        stmt.query_row(params![wallet_id, token, before.to_rfc3339()], |row| {
            row.get::<_, String>(0)
        }),
    )
}

fn first_seen_by_column(
    conn: &Connection,
    index: &str,
    column: &str,
    token: &str,
    before: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let sql = format!(
        "SELECT ts FROM observed_swaps INDEXED BY {index}
         WHERE {column} = ?1 AND ts <= ?2
         ORDER BY ts ASC LIMIT 1"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("failed preparing token first-seen lookup")?;
    optional_ts(stmt.query_row(params![token, before.to_rfc3339()], |row| {
        row.get::<_, String>(0)
    }))
}

fn read_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClosedTrade> {
    let opened_raw: String = row.get(4)?;
    let closed_raw: String = row.get(5)?;
    Ok(ClosedTrade {
        wallet_id: row.get(0)?,
        token: row.get(1)?,
        entry_cost_sol: row.get(2)?,
        pnl_sol: row.get(3)?,
        opened_ts: parse_ts(&opened_raw, 4)?,
        closed_ts: parse_ts(&closed_raw, 5)?,
        close_context: row.get(6)?,
    })
}

fn optional_ts(result: rusqlite::Result<String>) -> Result<Option<DateTime<Utc>>> {
    match result {
        Ok(raw) => Ok(Some(
            DateTime::parse_from_rfc3339(&raw)
                .context("invalid observed_swaps ts")?
                .with_timezone(&Utc),
        )),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(error).context("failed reading timestamp"),
    }
}

fn parse_ts(raw: &str, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
