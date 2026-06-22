use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use std::time::Duration as StdDuration;

const EVENT_PREFIX_HEAD: &str = "quote:exit-policy:backstop";

#[derive(Debug, Clone)]
pub(crate) struct DiagnosticEvent {
    pub(crate) event_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) quote_status: String,
    pub(crate) error: Option<String>,
    pub(crate) request_ts: DateTime<Utc>,
    pub(crate) signal_ts: DateTime<Utc>,
    pub(crate) decision_delay_ms: Option<u64>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) shadow_price_sol: Option<f64>,
    pub(crate) close_matches: Vec<CloseMatch>,
}

#[derive(Debug, Clone)]
pub(crate) struct CloseMatch {
    pub(crate) close_context: String,
    pub(crate) qty: f64,
    pub(crate) exit_value_sol: f64,
}

pub(crate) fn open_read_only_db(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)
        .with_context(|| format!("failed opening read-only sqlite db {}", path.display()))?;
    conn.busy_timeout(StdDuration::from_secs(5))
        .context("failed setting sqlite busy timeout")?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("failed enabling sqlite foreign_keys")?;
    conn.pragma_update(None, "query_only", true)
        .context("failed setting sqlite query_only")?;
    conn.pragma_update(None, "temp_store", "MEMORY")
        .context("failed setting temp_store=MEMORY")?;
    conn.pragma_update(None, "cache_size", -262_144)
        .context("failed setting operator cache_size")?;
    conn.pragma_update(None, "mmap_size", 1_073_741_824_i64)
        .context("failed setting operator mmap_size")?;
    Ok(conn)
}

pub(crate) fn load_diagnostic_events(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    hold_minutes: i64,
    close_match_limit: u32,
) -> Result<Vec<DiagnosticEvent>> {
    let event_prefix = format!("{EVENT_PREFIX_HEAD}{hold_minutes}m:");
    let mut stmt = conn
        .prepare(
            "SELECT
                event_id,
                wallet_id,
                token,
                quote_status,
                error,
                request_ts,
                signal_ts,
                decision_delay_ms,
                quote_price_sol,
                shadow_price_sol
             FROM execution_quote_canary_events INDEXED BY idx_execution_quote_canary_events_side_request_ts
             WHERE side = 'sell'
               AND request_ts >= ?1
               AND request_ts < ?2
               AND event_id LIKE ?3
             ORDER BY request_ts ASC, event_id ASC
             LIMIT ?4",
        )
        .context("failed to prepare exit policy diagnostic event query")?;
    let rows = stmt
        .query_map(
            params![
                since.to_rfc3339(),
                until.to_rfc3339(),
                format!("{event_prefix}%"),
                i64::from(limit.max(1)),
            ],
            read_event_row,
        )
        .context("failed querying exit policy diagnostic events")?;

    let mut events = Vec::new();
    for row in rows {
        let mut event = row.context("failed reading exit policy diagnostic event")?;
        event.close_matches = load_close_matches(conn, &event, hold_minutes, close_match_limit)
            .with_context(|| {
                format!(
                    "failed loading close matches for event_id={}",
                    event.event_id
                )
            })?;
        events.push(event);
    }
    Ok(events)
}

fn read_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DiagnosticEvent> {
    let request_raw: String = row.get(5)?;
    let signal_raw: String = row.get(6)?;
    Ok(DiagnosticEvent {
        event_id: row.get(0)?,
        wallet_id: row.get(1)?,
        token: row.get(2)?,
        quote_status: row.get(3)?,
        error: row.get(4)?,
        request_ts: parse_sql_ts(&request_raw, 5)?,
        signal_ts: parse_sql_ts(&signal_raw, 6)?,
        decision_delay_ms: optional_i64_to_u64(row.get(7)?, 7)?,
        quote_price_sol: row.get(8)?,
        shadow_price_sol: row.get(9)?,
        close_matches: Vec::new(),
    })
}

fn load_close_matches(
    conn: &Connection,
    event: &DiagnosticEvent,
    hold_minutes: i64,
    close_match_limit: u32,
) -> Result<Vec<CloseMatch>> {
    let opened_ts = event.signal_ts - Duration::minutes(hold_minutes);
    let mut stmt = conn
        .prepare(
            "SELECT
                COALESCE(close_context, 'market') AS close_context,
                qty,
                exit_value_sol
             FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_wallet_closed_ts
             WHERE wallet_id = ?1
               AND closed_ts >= ?2
               AND token = ?3
               AND opened_ts = ?4
             ORDER BY closed_ts ASC, id ASC
             LIMIT ?5",
        )
        .context("failed to prepare exit policy close match query")?;
    let rows = stmt
        .query_map(
            params![
                &event.wallet_id,
                event.request_ts.to_rfc3339(),
                &event.token,
                opened_ts.to_rfc3339(),
                i64::from(close_match_limit.max(1)),
            ],
            |row| {
                Ok(CloseMatch {
                    close_context: row.get(0)?,
                    qty: row.get(1)?,
                    exit_value_sol: row.get(2)?,
                })
            },
        )
        .context("failed querying exit policy close matches")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading exit policy close matches")
}

fn parse_sql_ts(raw: &str, index: usize) -> rusqlite::Result<DateTime<Utc>> {
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

fn optional_i64_to_u64(value: Option<i64>, index: usize) -> rusqlite::Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })
        })
        .transpose()
}
