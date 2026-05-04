fn recent_raw_journal_coverage_snapshot_on_conn(
    conn: &Connection,
) -> Result<(usize, Option<DateTime<Utc>>, Option<DiscoveryRuntimeCursor>)> {
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))
        .context("failed counting recent raw journal observed_swaps rows")?;
    let covered_since_raw: Option<String> = conn
        .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
        .optional()
        .context("failed loading recent raw journal covered_since timestamp")?
        .flatten();
    let covered_since = parse_optional_rfc3339_utc(
        covered_since_raw,
        "recent_raw_journal_state.covered_since_ts",
    )?;
    let covered_through_cursor_raw = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed loading recent raw journal covered_through cursor")?;
    let covered_through_cursor = covered_through_cursor_raw
        .map(
            |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                Ok(DiscoveryRuntimeCursor {
                    ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
                    slot: slot_raw.max(0) as u64,
                    signature,
                })
            },
        )
        .transpose()?;
    Ok((
        row_count.max(0) as usize,
        covered_since,
        covered_through_cursor,
    ))
}

fn recent_raw_journal_state_query(conn: &Connection) -> Result<RecentRawJournalStateRow> {
    let (row_count, covered_since, covered_through_cursor) =
        recent_raw_journal_coverage_snapshot_on_conn(conn)?;
    let row = conn
        .query_row(
            "SELECT
                last_batch_rows,
                last_batch_completed_at,
                last_pruned_rows,
                last_pruned_at,
                updated_at
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()
        .context("failed reading recent raw journal state")?;
    let Some((
        last_batch_rows,
        last_batch_completed_at_raw,
        last_pruned_rows,
        last_pruned_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(RecentRawJournalStateRow {
            covered_since,
            covered_through_cursor,
            row_count,
            ..RecentRawJournalStateRow::default()
        });
    };
    Ok(RecentRawJournalStateRow {
        covered_since,
        covered_through_cursor,
        row_count,
        last_batch_rows: last_batch_rows.max(0) as usize,
        last_batch_completed_at: parse_optional_rfc3339_utc(
            last_batch_completed_at_raw,
            "recent_raw_journal_state.last_batch_completed_at",
        )?,
        last_pruned_rows: last_pruned_rows.max(0) as usize,
        last_pruned_at: parse_optional_rfc3339_utc(
            last_pruned_at_raw,
            "recent_raw_journal_state.last_pruned_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "recent_raw_journal_state.updated_at",
        )?,
    })
}

fn recent_raw_journal_state_cached_query(conn: &Connection) -> Result<RecentRawJournalStateRow> {
    let row = conn
        .query_row(
            "SELECT
                covered_since_ts,
                covered_through_cursor_ts,
                covered_through_cursor_slot,
                covered_through_cursor_signature,
                row_count,
                last_batch_rows,
                last_batch_completed_at,
                last_pruned_rows,
                last_pruned_at,
                updated_at
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()
        .context("failed reading cached recent raw journal state")?;
    let Some((
        covered_since_raw,
        covered_through_ts_raw,
        covered_through_slot_raw,
        covered_through_signature,
        row_count,
        last_batch_rows,
        last_batch_completed_at_raw,
        last_pruned_rows,
        last_pruned_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(RecentRawJournalStateRow::default());
    };
    let covered_through_cursor = match (
        covered_through_ts_raw,
        covered_through_slot_raw,
        covered_through_signature,
    ) {
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Some(DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(
                &ts_raw,
                "recent_raw_journal_state.covered_through_cursor_ts",
            )?,
            slot: slot_raw.max(0) as u64,
            signature,
        }),
        _ => None,
    };
    Ok(RecentRawJournalStateRow {
        covered_since: parse_optional_rfc3339_utc(
            covered_since_raw,
            "recent_raw_journal_state.covered_since_ts",
        )?,
        covered_through_cursor,
        row_count: row_count.max(0) as usize,
        last_batch_rows: last_batch_rows.max(0) as usize,
        last_batch_completed_at: parse_optional_rfc3339_utc(
            last_batch_completed_at_raw,
            "recent_raw_journal_state.last_batch_completed_at",
        )?,
        last_pruned_rows: last_pruned_rows.max(0) as usize,
        last_pruned_at: parse_optional_rfc3339_utc(
            last_pruned_at_raw,
            "recent_raw_journal_state.last_pruned_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "recent_raw_journal_state.updated_at",
        )?,
    })
}

fn recent_raw_journal_state_row_exists(conn: &Connection) -> Result<bool> {
    let row = conn
        .query_row(
            "SELECT 1
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("failed checking cached recent raw journal state row presence")?;
    Ok(row.is_some())
}

fn upsert_recent_raw_journal_state_on_conn(
    conn: &Connection,
    state: &RecentRawJournalStateRow,
) -> Result<()> {
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id,
            covered_since_ts,
            covered_through_cursor_ts,
            covered_through_cursor_slot,
            covered_through_cursor_signature,
            row_count,
            last_batch_rows,
            last_batch_completed_at,
            last_pruned_rows,
            last_pruned_at,
            updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(id) DO UPDATE SET
            covered_since_ts = excluded.covered_since_ts,
            covered_through_cursor_ts = excluded.covered_through_cursor_ts,
            covered_through_cursor_slot = excluded.covered_through_cursor_slot,
            covered_through_cursor_signature = excluded.covered_through_cursor_signature,
            row_count = excluded.row_count,
            last_batch_rows = excluded.last_batch_rows,
            last_batch_completed_at = excluded.last_batch_completed_at,
            last_pruned_rows = excluded.last_pruned_rows,
            last_pruned_at = excluded.last_pruned_at,
            updated_at = excluded.updated_at",
        params![
            1_i64,
            state.covered_since.map(|ts| ts.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.slot as i64),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            state.row_count as i64,
            state.last_batch_rows as i64,
            state.last_batch_completed_at.map(|ts| ts.to_rfc3339()),
            state.last_pruned_rows as i64,
            state.last_pruned_at.map(|ts| ts.to_rfc3339()),
            state.updated_at.map(|ts| ts.to_rfc3339()),
        ],
    )
    .context("failed upserting recent raw journal state")?;
    Ok(())
}
