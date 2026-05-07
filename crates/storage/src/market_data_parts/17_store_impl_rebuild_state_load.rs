use super::*;

impl SqliteStore {
    pub(super) fn load_discovery_persisted_rebuild_state_query(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    phase,
                    window_start,
                    horizon_end,
                    metrics_window_start,
                    phase_cursor_ts,
                    phase_cursor_slot,
                    phase_cursor_signature,
                    prepass_rows_processed,
                    prepass_pages_processed,
                    replay_rows_processed,
                    replay_pages_processed,
                    chunks_completed,
                    state_json,
                    started_at,
                    updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, String>(14)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery persisted rebuild state")?;

        raw.map(
            |(
                phase_raw,
                window_start_raw,
                horizon_end_raw,
                metrics_window_start_raw,
                cursor_ts_raw,
                cursor_slot_raw,
                cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                state_json,
                started_at_raw,
                updated_at_raw,
            )| {
                let phase_cursor = match (cursor_ts_raw, cursor_slot_raw, cursor_signature) {
                    (None, None, None) => None,
                    (Some(ts_raw), Some(slot_raw), Some(signature)) => {
                        Some(DiscoveryRuntimeCursor {
                            ts_utc: parse_rfc3339_utc(
                                &ts_raw,
                                "discovery_persisted_rebuild_state.phase_cursor_ts",
                            )?,
                            slot: slot_raw.max(0) as u64,
                            signature,
                        })
                    }
                    _ => {
                        return Err(anyhow!(
                            "discovery_persisted_rebuild_state contains partial phase cursor state"
                        ));
                    }
                };
                Ok(DiscoveryPersistedRebuildStateRow {
                    phase: DiscoveryPersistedRebuildPhase::parse(&phase_raw)?,
                    window_start: parse_rfc3339_utc(
                        &window_start_raw,
                        "discovery_persisted_rebuild_state.window_start",
                    )?,
                    horizon_end: parse_rfc3339_utc(
                        &horizon_end_raw,
                        "discovery_persisted_rebuild_state.horizon_end",
                    )?,
                    metrics_window_start: parse_rfc3339_utc(
                        &metrics_window_start_raw,
                        "discovery_persisted_rebuild_state.metrics_window_start",
                    )?,
                    phase_cursor,
                    prepass_rows_processed: prepass_rows_processed.max(0) as usize,
                    prepass_pages_processed: prepass_pages_processed.max(0) as usize,
                    replay_rows_processed: replay_rows_processed.max(0) as usize,
                    replay_pages_processed: replay_pages_processed.max(0) as usize,
                    chunks_completed: chunks_completed.max(0) as usize,
                    state_json,
                    started_at: parse_rfc3339_utc(
                        &started_at_raw,
                        "discovery_persisted_rebuild_state.started_at",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_persisted_rebuild_state.updated_at",
                    )?,
                })
            },
        )
        .transpose()
    }
}
