use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use std::time::Duration as StdDuration;

const DIAG_PREFIX: &str = "quote:entry-shadow-diag:%";
const MARKET_EXIT_DIAG_PREFIX: &str = "quote:market-exit-shadow-diag:";

#[derive(Debug, Clone)]
pub(crate) struct EntryQuoteEvent {
    pub(crate) request_ts: DateTime<Utc>,
    pub(crate) signal_ts: Option<DateTime<Utc>>,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) quote_status: String,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) shadow_price_sol: Option<f64>,
    pub(crate) price_impact_pct: Option<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CloseOutcome {
    pub(crate) close_id: i64,
    pub(crate) close_context: String,
    pub(crate) entry_cost_sol: f64,
    pub(crate) exit_value_sol: f64,
    pub(crate) pnl_sol: f64,
    pub(crate) market_exit_quote: Option<MarketExitQuote>,
}

#[derive(Debug, Clone)]
pub(crate) struct MarketExitQuote {
    pub(crate) quote_status: String,
    pub(crate) error: Option<String>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) shadow_price_sol: Option<f64>,
    pub(crate) decision_delay_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct EntryQuoteOutcome {
    pub(crate) event: EntryQuoteEvent,
    pub(crate) closes: Vec<CloseOutcome>,
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

pub(crate) fn load_entry_quote_outcomes(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    close_match_limit: u32,
) -> Result<Vec<EntryQuoteOutcome>> {
    let events = load_entry_quote_events(conn, since, until, limit)?;
    let mut out = Vec::with_capacity(events.len());
    for event in events {
        let closes = load_matching_closes(conn, &event, until, close_match_limit)?;
        out.push(EntryQuoteOutcome { event, closes });
    }
    Ok(out)
}

fn load_entry_quote_events(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<EntryQuoteEvent>> {
    let mut stmt = conn
        .prepare(
            "SELECT request_ts, signal_ts, wallet_id, token,
                    quote_status, quote_price_sol, shadow_price_sol, price_impact_pct
             FROM execution_quote_canary_events
             INDEXED BY idx_execution_quote_canary_events_side_request_ts
             WHERE side = 'buy'
               AND request_ts >= ?1
               AND request_ts < ?2
               AND event_id LIKE ?3
             ORDER BY request_ts ASC, event_id ASC
             LIMIT ?4",
        )
        .context("failed preparing Track-B entry quote event query")?;
    let rows = stmt.query_map(
        params![
            since.to_rfc3339(),
            until.to_rfc3339(),
            DIAG_PREFIX,
            i64::from(limit)
        ],
        |row| {
            let request_raw: String = row.get(0)?;
            let signal_raw: Option<String> = row.get(1)?;
            Ok(EntryQuoteEvent {
                request_ts: parse_ts(&request_raw, "execution_quote_canary_events.request_ts")?,
                signal_ts: signal_raw
                    .as_deref()
                    .map(|raw| parse_ts(raw, "execution_quote_canary_events.signal_ts"))
                    .transpose()?,
                wallet_id: row.get(2)?,
                token: row.get(3)?,
                quote_status: row.get(4)?,
                quote_price_sol: row.get(5)?,
                shadow_price_sol: row.get(6)?,
                price_impact_pct: row.get(7)?,
            })
        },
    )?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading Track-B entry quote events")
}

fn load_matching_closes(
    conn: &Connection,
    event: &EntryQuoteEvent,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CloseOutcome>> {
    let Some(opened_ts) = event.signal_ts.as_ref() else {
        return Ok(Vec::new());
    };
    let mut stmt = conn
        .prepare(
            "SELECT id,
                    COALESCE(close_context, 'market') AS close_context,
                    entry_cost_sol, exit_value_sol, pnl_sol
             FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_wallet_closed_ts
             WHERE wallet_id = ?1
               AND closed_ts >= ?2
               AND closed_ts < ?3
               AND token = ?4
               AND opened_ts = ?5
             ORDER BY closed_ts ASC, id ASC
             LIMIT ?6",
        )
        .context("failed preparing Track-B close outcome query")?;
    let rows = stmt.query_map(
        params![
            &event.wallet_id,
            opened_ts.to_rfc3339(),
            until.to_rfc3339(),
            &event.token,
            opened_ts.to_rfc3339(),
            i64::from(limit),
        ],
        |row| {
            Ok(CloseOutcome {
                close_id: row.get(0)?,
                close_context: row.get(1)?,
                entry_cost_sol: row.get(2)?,
                exit_value_sol: row.get(3)?,
                pnl_sol: row.get(4)?,
                market_exit_quote: None,
            })
        },
    )?;
    let mut closes = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading Track-B close outcomes")?;
    for close in &mut closes {
        close.market_exit_quote = load_market_exit_quote(conn, close.close_id)?;
    }
    Ok(closes)
}

fn load_market_exit_quote(conn: &Connection, close_id: i64) -> Result<Option<MarketExitQuote>> {
    let event_id = format!("{MARKET_EXIT_DIAG_PREFIX}{close_id}");
    let mut stmt = conn
        .prepare(
            "SELECT quote_status, error, quote_price_sol, shadow_price_sol, decision_delay_ms
             FROM execution_quote_canary_events
             WHERE event_id = ?1",
        )
        .context("failed preparing Track-B market-exit quote lookup")?;
    let mut rows = stmt
        .query(params![event_id])
        .context("failed querying Track-B market-exit quote")?;
    let Some(row) = rows
        .next()
        .context("failed iterating Track-B market-exit quote")?
    else {
        return Ok(None);
    };
    Ok(Some(MarketExitQuote {
        quote_status: row.get(0)?,
        error: row.get(1)?,
        quote_price_sol: row.get(2)?,
        shadow_price_sol: row.get(3)?,
        decision_delay_ms: row.get(4)?,
    }))
}

fn parse_ts(raw: &str, label: &str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                label.len(),
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
