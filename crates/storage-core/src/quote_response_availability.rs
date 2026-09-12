//! Additive local completion provenance; readonly compatibility never writes schema.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::Connection;

pub const MIGRATION: &str = "0076_quote_response_availability.sql";
const COLUMN: &str = "quote_response_available_ts";
const TABLES: [&str; 3] = [
    "execution_quote_canary_events",
    "execution_quote_canary_provider_samples",
    "execution_canary_build_plan_metadata",
];

pub fn available(conn: &Connection, table: &str) -> Result<bool> {
    if !TABLES.contains(&table) {
        bail!("unsupported quote availability table");
    }
    let migrations: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')", [], |r| r.get(0))?;
    let applied = migrations
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0),
        )?;
    let mut selected = false;
    for name in if applied {
        TABLES.to_vec()
    } else {
        vec![table]
    } {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({name})"))?;
        let cols = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let col = cols.iter().find(|c| c.0 == COLUMN);
        if applied && col.is_none() {
            bail!("applied 0076 quote availability schema is incomplete: {name}.{COLUMN}");
        }
        if let Some((_, ty, required)) = col {
            if !ty.eq_ignore_ascii_case("TEXT") || *required {
                bail!("invalid 0076 quote availability column: {name}.{COLUMN}");
            }
        }
        if name == table {
            selected = col.is_some();
        }
    }
    Ok(selected)
}

pub(crate) fn expr(conn: &Connection, table: &str) -> Result<&'static str> {
    Ok(if available(conn, table)? {
        COLUMN
    } else {
        "NULL"
    })
}
pub(crate) fn read(row: &rusqlite::Row<'_>, index: usize) -> Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = row
        .get(index)
        .context("failed reading quote response availability")?;
    raw.map(|s| crate::observed_timestamp::parse_rfc3339_utc(&s, COLUMN))
        .transpose()
}

pub(crate) struct Write {
    pub column: &'static str,
    pub placeholder: &'static str,
    pub update: &'static str,
    pub value: Option<String>,
    pub enabled: bool,
}
impl Write {
    pub fn new(
        conn: &Connection,
        table: &str,
        actual: Option<DateTime<Utc>>,
        success: bool,
    ) -> Result<Self> {
        let enabled = available(conn, table)?;
        if actual.is_some() && !enabled {
            bail!("0076 is required to persist quote response availability");
        }
        Ok(Self {
            enabled,
            column: if enabled {
                ", quote_response_available_ts"
            } else {
                ""
            },
            placeholder: if enabled { ", ?" } else { "" },
            update: if enabled {
                "quote_response_available_ts=excluded.quote_response_available_ts,"
            } else {
                ""
            },
            // Replacement with an error cannot retain a former successful response time.
            value: actual.filter(|_| success).map(|t| t.to_rfc3339()),
        })
    }
}
