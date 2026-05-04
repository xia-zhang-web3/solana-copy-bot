impl SqliteStore {
    pub fn clear_discovery_scoring_backfill_progress(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'backfill_progress_start_ts',
                    'backfill_progress_cursor_ts',
                    'backfill_progress_cursor_slot',
                    'backfill_progress_cursor_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery scoring backfill progress")?;
                clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn load_discovery_scoring_covered_since(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_since_ts")
    }

    pub fn load_discovery_scoring_backfill_progress(
        &self,
    ) -> Result<Option<(DateTime<Utc>, DiscoveryRuntimeCursor)>> {
        let Some(start_ts) = self.load_discovery_scoring_state_ts("backfill_progress_start_ts")?
        else {
            return Ok(None);
        };
        let cursor_ts = self.load_discovery_scoring_state_ts("backfill_progress_cursor_ts")?;
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.backfill_progress_cursor_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.backfill_progress_cursor_signature",
            )?;
        match (cursor_ts, slot_raw, signature) {
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.backfill_progress_cursor_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some((
                    start_ts,
                    DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    },
                )))
            }
            _ => Ok(None),
        }
    }

    pub fn load_discovery_scoring_seed_boundary_install_marker(
        &self,
    ) -> Result<Option<DiscoveryScoringSeedBoundaryInstallMarker>> {
        let Some(boundary_start_ts) =
            self.load_discovery_scoring_state_ts("seed_boundary_install_start_ts")?
        else {
            return Ok(None);
        };
        let boundary_cursor_ts =
            self.load_discovery_scoring_state_ts("seed_boundary_install_cursor_ts")?;
        let boundary_cursor_slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'seed_boundary_install_cursor_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.seed_boundary_install_cursor_slot")?;
        let boundary_cursor_signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'seed_boundary_install_cursor_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.seed_boundary_install_cursor_signature",
            )?;
        match (
            boundary_cursor_ts,
            boundary_cursor_slot_raw,
            boundary_cursor_signature,
        ) {
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.seed_boundary_install_cursor_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some(DiscoveryScoringSeedBoundaryInstallMarker {
                    boundary_start_ts,
                    boundary_cursor: DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    },
                }))
            }
            (None, None, None) => Ok(None),
            _ => anyhow::bail!(
                "discovery_scoring_state.seed_boundary_install marker is partially populated"
            ),
        }
    }
}
