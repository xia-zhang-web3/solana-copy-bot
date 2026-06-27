use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use serde::Serialize;
use std::path::Path;
use std::time::{Duration as StdDuration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct ObservedSolLegRow {
    pub(crate) dex: String,
    pub(crate) wallet_id: String,
    pub(crate) is_buy: bool,
    pub(crate) token_mint: String,
    pub(crate) token_qty: f64,
    pub(crate) sol_notional: f64,
    pub(crate) ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuerySafety {
    pub read_only: bool,
    pub query_only: bool,
    pub rows_seen: usize,
    pub max_rows_exhausted: bool,
    pub deadline_exhausted: bool,
    pub query_plan: Vec<String>,
}

pub(crate) struct SolLegScan {
    pub(crate) rows: Vec<ObservedSolLegRow>,
    pub(crate) safety: QuerySafety,
}

pub(crate) fn open_read_only_db(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)
        .with_context(|| format!("failed opening read-only sqlite db {}", path.display()))?;
    conn.busy_timeout(StdDuration::from_secs(5))?;
    conn.pragma_update(None, "query_only", true)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "cache_size", -131_072)?;
    Ok(conn)
}

pub(crate) fn load_sol_leg_rows(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    max_rows: usize,
    deadline: Instant,
) -> Result<SolLegScan> {
    let plan = explain_sol_leg_scan(conn, since, until, max_rows.saturating_add(1))?;
    let mut stmt = conn
        .prepare(SOL_LEG_SCAN_SQL)
        .context("failed preparing universe de-risk SOL-leg scan")?;
    let mut rows = stmt
        .query(params![
            since.to_rfc3339(),
            until.to_rfc3339(),
            max_rows.saturating_add(1).min(i64::MAX as usize) as i64,
        ])
        .context("failed querying universe de-risk SOL-leg scan")?;
    let mut out = Vec::new();
    let mut deadline_exhausted = false;
    while let Some(row) = rows.next().context("failed reading SOL-leg scan row")? {
        if Instant::now() >= deadline {
            deadline_exhausted = true;
            break;
        }
        if out.len() >= max_rows {
            break;
        }
        let is_buy_raw: i64 = row.get(2)?;
        let ts_raw: String = row.get(6)?;
        out.push(ObservedSolLegRow {
            dex: row.get(0)?,
            wallet_id: row.get(1)?,
            is_buy: is_buy_raw == 1,
            token_mint: row.get(3)?,
            token_qty: row.get(4)?,
            sol_notional: row.get(5)?,
            ts: parse_ts(&ts_raw, "observed_swaps.ts")?,
        });
    }
    let max_rows_exhausted = !deadline_exhausted && out.len() >= max_rows;
    Ok(SolLegScan {
        safety: QuerySafety {
            read_only: conn.is_readonly(rusqlite::DatabaseName::Main)?,
            query_only: query_only_enabled(conn)?,
            rows_seen: out.len(),
            max_rows_exhausted,
            deadline_exhausted,
            query_plan: plan,
        },
        rows: out,
    })
}

fn explain_sol_leg_scan(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {SOL_LEG_SCAN_SQL}"))
        .context("failed preparing universe de-risk query plan")?;
    let rows = stmt.query_map(
        params![
            since.to_rfc3339(),
            until.to_rfc3339(),
            limit.min(i64::MAX as usize) as i64,
        ],
        |row| row.get::<_, String>(3),
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading universe de-risk query plan")
}

fn query_only_enabled(conn: &Connection) -> Result<bool> {
    let raw: i64 = conn.query_row("PRAGMA query_only", [], |row| row.get(0))?;
    Ok(raw == 1)
}

fn parse_ts(raw: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {label}: {raw}"))
}

const SOL_LEG_SCAN_SQL: &str = "
SELECT dex, wallet_id,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END AS is_buy,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN token_out ELSE token_in END AS token_mint,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_out ELSE qty_in END AS token_qty,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_in ELSE qty_out END AS sol_notional,
       ts
FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
WHERE ts >= ?1
  AND ts < ?2
  AND (token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112')
ORDER BY ts ASC, slot ASC, signature ASC
LIMIT ?3";
