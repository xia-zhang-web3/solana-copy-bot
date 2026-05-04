impl DiscoveryService {
    fn load_recent_raw_source_state_read_only(
        runtime_db_path: &Path,
    ) -> Result<RecentRawJournalStateRow> {
        let conn = Connection::open_with_flags(runtime_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("failed opening {}", runtime_db_path.display()))?;
        let table_exists = conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'recent_raw_journal_state'
                 LIMIT 1",
                [],
                |_| Ok(true),
            )
            .optional()
            .context("failed checking recent_raw_journal_state table")?
            .unwrap_or(false);
        if !table_exists {
            anyhow::bail!(
                "cached recent raw journal state table recent_raw_journal_state is missing"
            );
        }
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
            anyhow::bail!("cached recent raw journal state row id=1 is missing");
        };
        let covered_through_cursor = match (
            covered_through_ts_raw,
            covered_through_slot_raw,
            covered_through_signature,
        ) {
            (Some(ts_raw), Some(slot_raw), Some(signature)) => Some(DiscoveryRuntimeCursor {
                ts_utc: DateTime::parse_from_rfc3339(&ts_raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| {
                        format!(
                            "invalid recent_raw_journal_state.covered_through_cursor_ts timestamp value: {ts_raw}"
                        )
                    })?,
                slot: slot_raw.max(0) as u64,
                signature,
            }),
            _ => None,
        };
        Ok(RecentRawJournalStateRow {
            covered_since: Self::parse_recent_raw_optional_rfc3339_utc(
                covered_since_raw,
                "recent_raw_journal_state.covered_since_ts",
            )?,
            covered_through_cursor,
            row_count: row_count.max(0) as usize,
            last_batch_rows: last_batch_rows.max(0) as usize,
            last_batch_completed_at: Self::parse_recent_raw_optional_rfc3339_utc(
                last_batch_completed_at_raw,
                "recent_raw_journal_state.last_batch_completed_at",
            )?,
            last_pruned_rows: last_pruned_rows.max(0) as usize,
            last_pruned_at: Self::parse_recent_raw_optional_rfc3339_utc(
                last_pruned_at_raw,
                "recent_raw_journal_state.last_pruned_at",
            )?,
            updated_at: Self::parse_recent_raw_optional_rfc3339_utc(
                updated_at_raw,
                "recent_raw_journal_state.updated_at",
            )?,
        })
    }
}
