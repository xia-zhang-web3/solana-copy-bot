impl SqliteStore {
    pub fn upsert_discovery_persisted_rebuild_state(
        &self,
        state: &DiscoveryPersistedRebuildStateRow,
    ) -> Result<()> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_persisted_rebuild_state(
                    id,
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
                 ) VALUES (
                    1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    phase = excluded.phase,
                    window_start = excluded.window_start,
                    horizon_end = excluded.horizon_end,
                    metrics_window_start = excluded.metrics_window_start,
                    phase_cursor_ts = excluded.phase_cursor_ts,
                    phase_cursor_slot = excluded.phase_cursor_slot,
                    phase_cursor_signature = excluded.phase_cursor_signature,
                    prepass_rows_processed = excluded.prepass_rows_processed,
                    prepass_pages_processed = excluded.prepass_pages_processed,
                    replay_rows_processed = excluded.replay_rows_processed,
                    replay_pages_processed = excluded.replay_pages_processed,
                    chunks_completed = excluded.chunks_completed,
                    state_json = excluded.state_json,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at",
                params![
                    state.phase.as_str(),
                    state.window_start.to_rfc3339(),
                    state.horizon_end.to_rfc3339(),
                    state.metrics_window_start.to_rfc3339(),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    state.phase_cursor.as_ref().map(|cursor| cursor.slot as i64),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    state.prepass_rows_processed as i64,
                    state.prepass_pages_processed as i64,
                    state.replay_rows_processed as i64,
                    state.replay_pages_processed as i64,
                    state.chunks_completed as i64,
                    &state.state_json,
                    state.started_at.to_rfc3339(),
                    state.updated_at.to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery persisted rebuild state")?;
        Ok(())
    }

    pub fn clear_discovery_persisted_rebuild_state(&self) -> Result<()> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "DELETE FROM discovery_persisted_rebuild_state WHERE id = 1",
                [],
            )
        })
        .context("failed clearing discovery persisted rebuild state")?;
        Ok(())
    }
}
