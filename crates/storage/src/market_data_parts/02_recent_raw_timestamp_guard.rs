use super::*;

pub(super) const OBSERVED_SWAPS_NON_UTC_TIMESTAMP_PREDICATE: &str = "NOT (
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
)";
pub(super) const OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX: &str = "idx_observed_swaps_non_utc_ts";

pub(super) fn ensure_recent_raw_timestamp_guard_index_valid(conn: &Connection) -> Result<()> {
    if recent_raw_timestamp_guard_index_valid(conn)? {
        return Ok(());
    }
    bail!(
        "observed_swaps timestamp validation requires valid {OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX}; run offline migration 0040 before recent_raw state/prune"
    );
}

fn recent_raw_timestamp_guard_index_valid(conn: &Connection) -> Result<bool> {
    let index_flags = conn
        .query_row(
            "SELECT [unique], partial
             FROM pragma_index_list('observed_swaps')
             WHERE name = ?1",
            params![OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed checking recent_raw timestamp guard index flags")?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 0 || partial == 0 {
        return Ok(false);
    }

    let mut stmt = conn
        .prepare(&format!(
            "SELECT name, [desc], coll, key
             FROM pragma_index_xinfo('{OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX}')
             ORDER BY seqno"
        ))
        .context("failed preparing recent_raw timestamp guard index introspection")?;
    let key_columns = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
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
            params![OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX],
            |row| row.get(0),
        )
        .optional()
        .context("failed reading recent_raw timestamp guard index sql")?;
    let Some(index_sql) = index_sql else {
        return Ok(false);
    };
    let compact_index_sql = compact_sql_preserving_strings(&index_sql);
    let Some((_, predicate)) = compact_index_sql.split_once("where") else {
        return Ok(false);
    };
    let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
    Ok(predicate == compact_sql_preserving_strings(OBSERVED_SWAPS_NON_UTC_TIMESTAMP_PREDICATE))
}

fn compact_sql_preserving_strings(sql: &str) -> String {
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
