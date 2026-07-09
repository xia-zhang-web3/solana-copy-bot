use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn collect_bounded<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
    limit: u32,
    label: &str,
) -> Result<(Vec<T>, bool)> {
    let rows = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("failed reading {label}"))?;
    Ok(bounded_vec(rows, limit))
}

pub(crate) fn bounded_vec<T>(mut rows: Vec<T>, limit: u32) -> (Vec<T>, bool) {
    let hit = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    (rows, hit)
}

pub(crate) fn collect_optional<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Option<T>>>,
) -> rusqlite::Result<Vec<T>> {
    let mut out = Vec::new();
    for row in rows {
        if let Some(value) = row? {
            out.push(value);
        }
    }
    Ok(out)
}

pub(crate) fn optional_entry_metadata_expr(conn: &Connection, column: &str) -> Result<String> {
    Ok(
        if table_has_column(conn, "execution_quote_canary_events", column)? {
            format!("COALESCE(entry.{column}, diag.{column})")
        } else {
            format!("NULL AS {column}")
        },
    )
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            &format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name = ?1 LIMIT 1"),
            params![column],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

pub(crate) fn optional_i64_to_u64(
    value: Option<i64>,
    column: usize,
) -> rusqlite::Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    column,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })
        })
        .transpose()
}

pub(crate) fn parse_ts(raw: &str, label: &str) -> rusqlite::Result<DateTime<Utc>> {
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
