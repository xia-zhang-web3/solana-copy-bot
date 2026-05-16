use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};

const OBSERVED_SWAPS_SOL_LEG_PREDICATE: &str =
    "token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112'";

const OBSERVED_SWAPS_REQUIRED_READ_INDEXES: &[(&str, &[&str], bool, Option<&str>)] = &[
    (
        "idx_observed_swaps_ts_slot_signature",
        &["ts", "slot", "signature"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_token_in_ts",
        &["token_in", "ts"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_token_out_ts",
        &["token_out", "ts"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_token_in_out_ts",
        &["token_in", "token_out", "ts"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_token_out_in_ts",
        &["token_out", "token_in", "ts"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_wallet_ts",
        &["wallet_id", "ts"],
        false,
        None,
    ),
    (
        "idx_observed_swaps_sol_leg_ts_slot_signature",
        &["ts", "slot", "signature"],
        true,
        Some(OBSERVED_SWAPS_SOL_LEG_PREDICATE),
    ),
];

const OBSERVED_SWAPS_READ_INDEX_SQL: &str = "
CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
    ON observed_swaps(ts, slot, signature);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_in_ts
    ON observed_swaps(token_in, ts);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_out_ts
    ON observed_swaps(token_out, ts);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_in_out_ts
    ON observed_swaps(token_in, token_out, ts);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_token_out_in_ts
    ON observed_swaps(token_out, token_in, ts);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_wallet_ts
    ON observed_swaps(wallet_id, ts);
CREATE INDEX IF NOT EXISTS idx_observed_swaps_sol_leg_ts_slot_signature
    ON observed_swaps(ts, slot, signature)
    WHERE token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112';
";

pub(crate) fn ensure_observed_swaps_read_indexes_empty_safe(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    ensure_observed_swaps_read_indexes_empty_safe_on_conn(&store.conn)
}

pub(crate) fn ensure_observed_swaps_read_indexes_empty_safe_on_conn(
    conn: &Connection,
) -> Result<()> {
    let mut missing = Vec::new();
    for (index_name, columns, partial, predicate) in OBSERVED_SWAPS_REQUIRED_READ_INDEXES {
        if !observed_swaps_index_is_valid_on_conn(conn, index_name, columns, *partial, *predicate)?
        {
            missing.push(*index_name);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let has_rows = conn
        .query_row("SELECT 1 FROM observed_swaps LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .context("failed checking observed_swaps row presence before read index ensure")?
        .is_some();
    if has_rows {
        anyhow::bail!(
            "observed_swaps required read indexes are missing or malformed on a non-empty table; \
             run migrations offline: {}",
            missing.join(", ")
        );
    }
    for index_name in missing {
        let drop_sql = format!("DROP INDEX IF EXISTS {index_name}");
        conn.execute(&drop_sql, []).with_context(|| {
            format!("failed dropping malformed empty observed_swaps index {index_name}")
        })?;
    }
    conn.execute_batch(OBSERVED_SWAPS_READ_INDEX_SQL)
        .context("failed preparing empty observed_swaps read indexes")?;
    Ok(())
}

pub(crate) fn observed_swaps_sol_leg_index_is_valid(store: &SqliteDiscoveryStore) -> Result<bool> {
    observed_swaps_index_is_valid(
        store,
        "idx_observed_swaps_sol_leg_ts_slot_signature",
        &["ts", "slot", "signature"],
        true,
        Some(OBSERVED_SWAPS_SOL_LEG_PREDICATE),
    )
}

pub(crate) fn observed_swaps_read_index_is_valid(
    store: &SqliteDiscoveryStore,
    index_name: &str,
) -> Result<bool> {
    let Some((_, columns, partial, predicate)) = OBSERVED_SWAPS_REQUIRED_READ_INDEXES
        .iter()
        .find(|(required_name, _, _, _)| *required_name == index_name)
    else {
        anyhow::bail!("unknown observed_swaps read index: {index_name}");
    };
    observed_swaps_index_is_valid(store, index_name, columns, *partial, *predicate)
}

pub(crate) fn validate_observed_swaps_read_indexes(store: &SqliteDiscoveryStore) -> Result<()> {
    for (index_name, columns, partial, predicate) in OBSERVED_SWAPS_REQUIRED_READ_INDEXES {
        if !observed_swaps_index_is_valid(store, index_name, columns, *partial, *predicate)? {
            anyhow::bail!("discovery v2 schema missing or malformed required index: {index_name}");
        }
    }
    Ok(())
}

fn observed_swaps_index_is_valid(
    store: &SqliteDiscoveryStore,
    index_name: &str,
    expected_columns: &[&str],
    expected_partial: bool,
    expected_predicate: Option<&str>,
) -> Result<bool> {
    observed_swaps_index_is_valid_on_conn(
        &store.conn,
        index_name,
        expected_columns,
        expected_partial,
        expected_predicate,
    )
}

fn observed_swaps_index_is_valid_on_conn(
    conn: &Connection,
    index_name: &str,
    expected_columns: &[&str],
    expected_partial: bool,
    expected_predicate: Option<&str>,
) -> Result<bool> {
    let index_flags = conn
        .query_row(
            "SELECT [unique], partial FROM pragma_index_list('observed_swaps') WHERE name = ?1",
            [index_name],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .with_context(|| format!("failed checking observed_swaps index flags for {index_name}"))?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 0 || (partial != 0) != expected_partial {
        return Ok(false);
    }

    let pragma = format!(
        "SELECT name, [desc], coll, key FROM pragma_index_xinfo('{index_name}') ORDER BY seqno"
    );
    let mut stmt = conn.prepare(&pragma).with_context(|| {
        format!("failed preparing observed_swaps index introspection for {index_name}")
    })?;
    let columns = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .with_context(|| format!("failed querying observed_swaps index columns for {index_name}"))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| {
            format!("failed collecting observed_swaps index columns for {index_name}")
        })?;
    let key_columns = columns
        .into_iter()
        .filter(|(_, _, _, key)| *key != 0)
        .collect::<Vec<_>>();
    if key_columns.len() != expected_columns.len() {
        return Ok(false);
    }
    for ((name, desc, coll, _), expected_column) in key_columns.iter().zip(expected_columns) {
        if name.as_deref() != Some(*expected_column) {
            return Ok(false);
        }
        if *desc != 0 || coll.as_deref() != Some("BINARY") {
            return Ok(false);
        }
    }
    let Some(expected_predicate) = expected_predicate else {
        return Ok(true);
    };
    let index_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1 LIMIT 1",
            [index_name],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("failed reading observed_swaps index sql for {index_name}"))?;
    let Some(index_sql) = index_sql else {
        return Ok(false);
    };
    let compact_index_sql = compact_sql(&index_sql);
    let Some((_, predicate)) = compact_index_sql.split_once("where") else {
        return Ok(false);
    };
    let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
    Ok(predicate == compact_sql(expected_predicate))
}

pub(crate) fn followlist_active_wallet_index_is_valid(
    store: &SqliteDiscoveryStore,
) -> Result<bool> {
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
    let compact_index_sql = compact_sql(index_sql);
    let Some((_, predicate)) = compact_index_sql.split_once("where") else {
        return false;
    };
    let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
    matches!(
        predicate,
        "active=1" | "(active=1)" | "1=active" | "(1=active)"
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
