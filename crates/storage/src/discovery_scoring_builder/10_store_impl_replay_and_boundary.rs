use super::super::*;

impl SqliteStore {
    pub fn begin_discovery_scoring_replay_builder(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        self.begin_discovery_scoring_replay_builder_with_lot_bootstrap(
            starting_cursor_ts,
            starting_cursor_slot,
            starting_cursor_signature,
            false,
        )
    }

    pub fn begin_discovery_scoring_replay_builder_lazy_open_lots(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        self.begin_discovery_scoring_replay_builder_with_lot_bootstrap(
            starting_cursor_ts,
            starting_cursor_slot,
            starting_cursor_signature,
            true,
        )
    }

    fn begin_discovery_scoring_replay_builder_with_lot_bootstrap(
        &self,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
        lazy_open_lot_loading: bool,
    ) -> Result<DiscoveryScoringReplayBuilder> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for builder init")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "builder replay does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringReplayBuilder {
            open_lots: if lazy_open_lot_loading {
                HashMap::new()
            } else {
                load_all_open_lots(&self.conn)?
            },
            loaded_open_lot_keys: HashSet::new(),
            lazy_open_lot_loading,
            market_windows: seed_market_windows_from_lookback(
                self,
                CursorRef {
                    ts: starting_cursor_ts,
                    slot: starting_cursor_slot,
                    signature: starting_cursor_signature,
                },
            )?,
            quality_cache: HashMap::new(),
        })
    }

    pub fn export_discovery_scoring_builder_boundary_seed_snapshot(
        &self,
        builder: &DiscoveryScoringReplayBuilder,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for builder boundary export")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "builder boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
        })
    }

    pub fn begin_discovery_scoring_boundary_lot_builder(
        &self,
        progress_start_ts: DateTime<Utc>,
        starting_cursor_ts: DateTime<Utc>,
        starting_cursor_slot: u64,
        starting_cursor_signature: &str,
    ) -> Result<DiscoveryScoringBoundaryLotBuilder> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context(
                "failed counting wallet_scoring_carryover_lots for boundary lot builder init",
            )?;
        if carryover_count != 0 {
            anyhow::bail!(
                "boundary lot builder does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        if let Some(state) = load_persisted_boundary_lot_builder_state_on_conn(&self.conn)? {
            let expected_cursor = DiscoveryRuntimeCursor {
                ts_utc: starting_cursor_ts,
                slot: starting_cursor_slot,
                signature: starting_cursor_signature.to_string(),
            };
            if state.progress_start_ts == progress_start_ts && state.cursor == expected_cursor {
                return Ok(DiscoveryScoringBoundaryLotBuilder {
                    open_lots: load_boundary_seed_lots_into_open_lots(&state.open_lots),
                });
            }
        }
        Ok(DiscoveryScoringBoundaryLotBuilder {
            open_lots: load_all_open_lots(&self.conn)?,
        })
    }

    pub fn export_discovery_scoring_boundary_lot_seed_snapshot(
        &self,
        builder: &DiscoveryScoringBoundaryLotBuilder,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
                [],
                |row| row.get(0),
            )
            .context("failed counting wallet_scoring_carryover_lots for boundary lot export")?;
        if carryover_count != 0 {
            anyhow::bail!(
                "boundary lot export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
        })
    }

    pub fn advance_discovery_scoring_boundary_lot_builder_in_memory_with_timings(
        &self,
        builder: &mut DiscoveryScoringBoundaryLotBuilder,
        swaps: &[SwapEvent],
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let prepare_ms = prepare_discovery_scoring_boundary_lot_batch(builder, swaps)?;
        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms: 0,
            rug_finalize_ms: 0,
        })
    }

    pub fn advance_discovery_scoring_boundary_lot_builder_and_checkpoint_with_timings(
        &self,
        builder: &mut DiscoveryScoringBoundaryLotBuilder,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringCheckpointedBatchTimings> {
        let prepare_ms = prepare_discovery_scoring_boundary_lot_batch(builder, swaps)?;

        let progress_update_ms = self.with_immediate_transaction_retry(
            "discovery scoring boundary lot batch with checkpoint",
            |conn| {
                crate::discovery_scoring::maybe_fail_after_materialization_before_checkpoint()?;

                let progress_started_at = Instant::now();
                let updated_at = Utc::now().to_rfc3339();
                crate::discovery_scoring::upsert_discovery_scoring_backfill_progress_on_conn(
                    conn,
                    progress_start_ts,
                    progress_cursor,
                    &updated_at,
                )?;
                upsert_persisted_boundary_lot_builder_state_on_conn(
                    conn,
                    progress_start_ts,
                    progress_cursor,
                    builder,
                    &updated_at,
                )?;
                Ok(progress_started_at.elapsed().as_millis() as u64)
            },
        )?;

        Ok(DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms: 0,
            progress_update_ms,
        })
    }
}
