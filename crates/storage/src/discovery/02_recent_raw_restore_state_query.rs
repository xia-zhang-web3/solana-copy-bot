fn discovery_recent_raw_restore_state_query(
    conn: &Connection,
) -> Result<DiscoveryRecentRawRestoreStateRow> {
    let row = conn
        .query_row(
            "SELECT
                journal_available,
                journal_replayed,
                required_window_start,
                journal_covered_since,
                journal_covered_through_cursor_ts,
                journal_covered_through_cursor_slot,
                journal_covered_through_cursor_signature,
                gap_fill_replayed,
                gap_fill_covered_since,
                gap_fill_covered_through_cursor_ts,
                gap_fill_covered_through_cursor_slot,
                gap_fill_covered_through_cursor_signature,
                effective_covered_since,
                effective_covered_through_cursor_ts,
                effective_covered_through_cursor_slot,
                effective_covered_through_cursor_signature,
                artifact_runtime_cursor_ts,
                artifact_runtime_cursor_slot,
                artifact_runtime_cursor_signature,
                journal_covers_artifact_cursor,
                raw_coverage_satisfied,
                gap_fill_replayed_rows,
                replayed_rows,
                reason,
                replay_started_at,
                replay_completed_at,
                updated_at
             FROM discovery_recent_raw_restore_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<i64>>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<i64>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, i64>(19)?,
                    row.get::<_, i64>(20)?,
                    row.get::<_, i64>(21)?,
                    row.get::<_, i64>(22)?,
                    row.get::<_, Option<String>>(23)?,
                    row.get::<_, Option<String>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<String>>(26)?,
                ))
            },
        )
        .optional()
        .context("failed reading discovery recent raw restore state")?;
    let Some((
        journal_available,
        journal_replayed,
        required_window_start_raw,
        journal_covered_since_raw,
        journal_covered_through_cursor_ts_raw,
        journal_covered_through_cursor_slot_raw,
        journal_covered_through_cursor_signature,
        gap_fill_replayed,
        gap_fill_covered_since_raw,
        gap_fill_covered_through_cursor_ts_raw,
        gap_fill_covered_through_cursor_slot_raw,
        gap_fill_covered_through_cursor_signature,
        effective_covered_since_raw,
        effective_covered_through_cursor_ts_raw,
        effective_covered_through_cursor_slot_raw,
        effective_covered_through_cursor_signature,
        artifact_runtime_cursor_ts_raw,
        artifact_runtime_cursor_slot_raw,
        artifact_runtime_cursor_signature,
        journal_covers_artifact_cursor,
        raw_coverage_satisfied,
        gap_fill_replayed_rows,
        replayed_rows,
        reason,
        replay_started_at_raw,
        replay_completed_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(DiscoveryRecentRawRestoreStateRow::default());
    };

    Ok(DiscoveryRecentRawRestoreStateRow {
        journal_available: journal_available != 0,
        journal_replayed: journal_replayed != 0,
        required_window_start: parse_optional_rfc3339_utc(
            required_window_start_raw,
            "discovery_recent_raw_restore_state.required_window_start",
        )?,
        journal_covered_since: parse_optional_rfc3339_utc(
            journal_covered_since_raw,
            "discovery_recent_raw_restore_state.journal_covered_since",
        )?,
        journal_covered_through_cursor: parse_optional_runtime_cursor(
            journal_covered_through_cursor_ts_raw,
            journal_covered_through_cursor_slot_raw,
            journal_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.journal_covered_through_cursor",
        )?,
        gap_fill_replayed: gap_fill_replayed != 0,
        gap_fill_covered_since: parse_optional_rfc3339_utc(
            gap_fill_covered_since_raw,
            "discovery_recent_raw_restore_state.gap_fill_covered_since",
        )?,
        gap_fill_covered_through_cursor: parse_optional_runtime_cursor(
            gap_fill_covered_through_cursor_ts_raw,
            gap_fill_covered_through_cursor_slot_raw,
            gap_fill_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.gap_fill_covered_through_cursor",
        )?,
        effective_covered_since: parse_optional_rfc3339_utc(
            effective_covered_since_raw,
            "discovery_recent_raw_restore_state.effective_covered_since",
        )?,
        effective_covered_through_cursor: parse_optional_runtime_cursor(
            effective_covered_through_cursor_ts_raw,
            effective_covered_through_cursor_slot_raw,
            effective_covered_through_cursor_signature,
            "discovery_recent_raw_restore_state.effective_covered_through_cursor",
        )?,
        artifact_runtime_cursor: parse_optional_runtime_cursor(
            artifact_runtime_cursor_ts_raw,
            artifact_runtime_cursor_slot_raw,
            artifact_runtime_cursor_signature,
            "discovery_recent_raw_restore_state.artifact_runtime_cursor",
        )?,
        journal_covers_artifact_cursor: journal_covers_artifact_cursor != 0,
        raw_coverage_satisfied: raw_coverage_satisfied != 0,
        gap_fill_replayed_rows: gap_fill_replayed_rows.max(0) as usize,
        replayed_rows: replayed_rows.max(0) as usize,
        reason,
        replay_started_at: parse_optional_rfc3339_utc(
            replay_started_at_raw,
            "discovery_recent_raw_restore_state.replay_started_at",
        )?,
        replay_completed_at: parse_optional_rfc3339_utc(
            replay_completed_at_raw,
            "discovery_recent_raw_restore_state.replay_completed_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "discovery_recent_raw_restore_state.updated_at",
        )?,
    })
}
