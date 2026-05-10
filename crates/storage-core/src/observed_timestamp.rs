use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;

use crate::SqliteDiscoveryStore;

pub(crate) const OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX: &str = "idx_observed_swaps_non_utc_ts";

pub(crate) fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    if !raw.ends_with("+00:00") {
        return Err(anyhow!(
            "{field} must use canonical UTC offset +00:00: {raw}"
        ));
    }
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err(anyhow!("{field} must use UTC offset +00:00: {raw}"));
    }
    Ok(parsed.with_timezone(&Utc))
}

pub(crate) fn ensure_observed_swaps_timestamps_canonical_utc_read_only(
    conn: &rusqlite::Connection,
) -> Result<()> {
    if !observed_swaps_non_utc_timestamp_index_is_valid_on_conn(conn)? {
        anyhow::bail!(
            "observed_swaps timestamp validation index {OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX} \
             is missing or malformed; run migration 0040 before timestamp-sensitive reads"
        );
    }

    let sql = format!(
        "SELECT ts FROM observed_swaps WHERE {} LIMIT 1",
        observed_swaps_non_utc_timestamp_index_predicate_sql()
    );
    let raw = conn
        .query_row(&sql, [], |row| row.get::<_, String>(0))
        .optional()
        .context("failed validating observed_swaps timestamp offsets")?;
    if let Some(raw) = raw {
        anyhow::bail!("observed_swaps.ts is not canonical UTC: {raw}");
    }
    Ok(())
}

pub(crate) fn ensure_observed_swaps_timestamp_validation_index_empty_safe(
    conn: &rusqlite::Connection,
) -> Result<()> {
    if observed_swaps_non_utc_timestamp_index_is_valid_on_conn(conn)? {
        return Ok(());
    }
    let has_rows = conn
        .query_row("SELECT 1 FROM observed_swaps LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .context("failed checking observed_swaps row presence before timestamp index ensure")?
        .is_some();
    if has_rows {
        anyhow::bail!(
            "observed_swaps timestamp validation index {OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX} \
             is missing or malformed on a non-empty table; run migration 0040 offline"
        );
    }
    let sql = format!(
        "DROP INDEX IF EXISTS {OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX};
         {}",
        observed_swaps_non_utc_timestamp_index_sql()
    );
    conn.execute_batch(&sql)
        .context("failed preparing empty observed_swaps timestamp validation index")?;
    Ok(())
}

pub(crate) fn observed_swaps_non_utc_timestamp_index_is_valid(
    store: &SqliteDiscoveryStore,
) -> Result<bool> {
    observed_swaps_non_utc_timestamp_index_is_valid_on_conn(&store.conn)
}

fn observed_swaps_non_utc_timestamp_index_is_valid_on_conn(
    conn: &rusqlite::Connection,
) -> Result<bool> {
    let index_flags = conn
        .query_row(
            "SELECT [unique], partial FROM pragma_index_list('observed_swaps') WHERE name = ?1",
            [OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed checking observed_swaps non-UTC timestamp index flags")?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 0 || partial != 1 {
        return Ok(false);
    }

    let mut stmt = conn
        .prepare(&format!(
            "SELECT name, [desc], coll, key
             FROM pragma_index_xinfo('{OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX}')
             ORDER BY seqno"
        ))
        .context("failed preparing observed_swaps non-UTC index column introspection")?;
    let key_columns = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .context("failed querying observed_swaps non-UTC index columns")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed collecting observed_swaps non-UTC index columns")?
        .into_iter()
        .filter(|(_, _, _, key)| *key != 0)
        .collect::<Vec<_>>();
    if key_columns.len() != 1 {
        return Ok(false);
    }
    let (name, desc, coll, _) = &key_columns[0];
    if name.as_deref() != Some("ts") || *desc != 0 || coll.as_deref() != Some("BINARY") {
        return Ok(false);
    }

    let index_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1 LIMIT 1",
            [OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX],
            |row| row.get(0),
        )
        .optional()
        .context("failed reading observed_swaps non-UTC index sql")?;
    let Some(index_sql) = index_sql else {
        return Ok(false);
    };
    Ok(observed_swaps_non_utc_timestamp_index_predicate_is_valid(
        &index_sql,
    ))
}

fn observed_swaps_non_utc_timestamp_index_predicate_is_valid(index_sql: &str) -> bool {
    let compact_index_sql = compact_sql(index_sql);
    let Some((_, predicate)) = compact_index_sql.split_once("where") else {
        return false;
    };
    let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
    predicate == compact_sql(observed_swaps_non_utc_timestamp_index_predicate_sql())
}

pub(crate) fn observed_swaps_non_utc_timestamp_index_sql() -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX}
            ON observed_swaps(ts)
            WHERE {};",
        observed_swaps_non_utc_timestamp_index_predicate_sql()
    )
}

fn compact_sql(sql: &str) -> String {
    let mut compact = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if in_string {
            compact.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    compact.push(chars.next().expect("peeked escaped quote"));
                } else {
                    in_string = false;
                }
            }
            continue;
        }
        if ch == '\'' {
            in_string = true;
            compact.push(ch);
            continue;
        }
        if ch.is_whitespace() || matches!(ch, '"' | '`' | '[' | ']') {
            continue;
        }
        compact.extend(ch.to_lowercase());
    }
    compact
}

pub(crate) fn observed_swaps_non_utc_timestamp_index_predicate_sql() -> &'static str {
    "NOT (
        (
            (length(ts) = 25 AND substr(ts, 20, 6) = '+00:00')
            OR (
                length(ts) BETWEEN 27 AND 35
                AND substr(ts, 20, 1) = '.'
                AND substr(ts, -6) = '+00:00'
                AND substr(ts, 21, length(ts) - 26) GLOB '[0-9]*'
                AND substr(ts, 21, length(ts) - 26) NOT GLOB '*[^0-9]*'
            )
        )
        AND substr(ts, 5, 1) = '-'
        AND substr(ts, 8, 1) = '-'
        AND substr(ts, 11, 1) = 'T'
        AND substr(ts, 14, 1) = ':'
        AND substr(ts, 17, 1) = ':'
        AND substr(ts, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
        AND substr(ts, 6, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 6, 2) AS INTEGER) BETWEEN 1 AND 12
        AND substr(ts, 9, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 9, 2) AS INTEGER) BETWEEN 1 AND 31
        AND substr(ts, 12, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 12, 2) AS INTEGER) BETWEEN 0 AND 23
        AND substr(ts, 15, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 15, 2) AS INTEGER) BETWEEN 0 AND 59
        AND substr(ts, 18, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 18, 2) AS INTEGER) BETWEEN 0 AND 59
        AND julianday(ts) IS NOT NULL
        AND strftime('%Y-%m-%dT%H:%M:%S', ts) = substr(ts, 1, 19)
    )"
}
