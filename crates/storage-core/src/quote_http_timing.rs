//! Actual HTTP provenance is separate from the historical quote/version timestamp.
use anyhow::{bail, Context, Result};
use rusqlite::Connection;

pub const QUOTE_HTTP_TIMING_MIGRATION: &str = "0066_quote_http_timing.sql";
const TABLES: [&str; 3] = [
    "execution_quote_canary_events",
    "execution_quote_canary_provider_samples",
    "execution_canary_build_plan_metadata",
];

/// Missing columns are supported only before 0066. A damaged applied schema is an error.
pub fn quote_http_timing_available(conn: &Connection, table: &str) -> Result<bool> {
    if !TABLES.contains(&table) {
        bail!("unsupported quote timing table");
    }
    let has_migrations: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')", [], |r| r.get(0))?;
    let applied = has_migrations
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [QUOTE_HTTP_TIMING_MIGRATION],
            |r| r.get::<_, bool>(0),
        )?;
    let mut selected = false;
    for name in if applied {
        TABLES.to_vec()
    } else {
        vec![table]
    } {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({name})"))?;
        let names = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let present = names.iter().any(|c| c == "http_request_started_ts");
        if applied && !present {
            bail!("applied 0066 quote timing schema is incomplete: {name}.http_request_started_ts");
        }
        if name == table {
            selected = present;
        }
    }
    Ok(selected)
}

/// Qualified SQL expression for existing report readers; never substitutes batch time.
pub fn quote_http_started_expr(conn: &Connection, table: &str, alias: &str) -> Result<String> {
    if !alias.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("invalid quote timing alias");
    }
    Ok(if quote_http_timing_available(conn, table)? {
        if alias.is_empty() {
            "http_request_started_ts".into()
        } else {
            format!("{alias}.http_request_started_ts")
        }
    } else {
        "NULL".into()
    })
}

pub(crate) fn read_http_started(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let raw: Option<String> = row
        .get(index)
        .context("failed reading actual quote HTTP start")?;
    raw.map(|s| crate::observed_timestamp::parse_rfc3339_utc(&s, "http_request_started_ts"))
        .transpose()
}

pub(crate) fn timing_write_available(
    conn: &Connection,
    table: &str,
    actual: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<bool> {
    let available = quote_http_timing_available(conn, table)?;
    if actual.is_some() && !available {
        bail!("0066 is required to persist actual quote HTTP timing");
    }
    Ok(available)
}

pub(crate) fn actual_delay_ms(
    source: Option<chrono::DateTime<chrono::Utc>>,
    actual: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<u64> {
    let (source, actual) = (source?, actual?);
    if actual < source {
        return None;
    }
    u64::try_from((actual - source).num_milliseconds()).ok()
}
