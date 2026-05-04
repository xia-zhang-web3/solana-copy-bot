impl SqliteStore {
    pub fn apply_discovery_scoring_batch_with_timings(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let prepare_started_at = Instant::now();
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

        let apply_started_at = Instant::now();
        self.with_immediate_transaction_retry("discovery scoring batch", |conn| {
            apply_discovery_scoring_swaps_on_conn(conn, &prepared)
        })?;
        let apply_ms = apply_started_at.elapsed().as_millis() as u64;

        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_batch_and_checkpoint_with_timings(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        self.apply_discovery_scoring_batch_and_checkpoint_with_timings_and_diagnostics(
            swaps,
            config,
            progress_start_ts,
            progress_cursor,
            |_| {},
            |_, _, _, _| {},
            |_| {},
            None,
        )
    }

    pub fn apply_discovery_scoring_batch_and_checkpoint_with_timings_and_diagnostics(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
        mut stage_start: impl FnMut(&str),
        mut stage_end: impl FnMut(&str, usize, u64, &str),
        mut prepare_event: impl FnMut(String),
        prepare_deadline: Option<Instant>,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        stage_start("prepare_discovery_scoring_swaps");
        let prepare_started_at = Instant::now();
        let prepared = match prepare_discovery_scoring_swaps_with_diagnostics(
            &self.conn,
            swaps,
            config,
            &mut prepare_event,
            prepare_deadline,
        ) {
            Ok(prepared) => {
                stage_end(
                    "prepare_discovery_scoring_swaps",
                    prepared.len(),
                    prepare_started_at.elapsed().as_millis() as u64,
                    "completed",
                );
                prepared
            }
            Err(error) => {
                let outcome = if prepare_error_is_runtime_budget(&error) {
                    "runtime_budget_exhausted"
                } else {
                    "failed"
                };
                stage_end(
                    "prepare_discovery_scoring_swaps",
                    swaps.len(),
                    prepare_started_at.elapsed().as_millis() as u64,
                    outcome,
                );
                return Err(error);
            }
        };
        let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring batch with checkpoint",
            |conn| {
                apply_discovery_scoring_swaps_and_checkpoint_on_conn(
                    conn,
                    &prepared,
                    progress_start_ts,
                    progress_cursor,
                    &mut stage_start,
                    &mut stage_end,
                )
            },
        )?;

        Ok(super::DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms,
            progress_update_ms,
        })
    }

    pub fn apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings(
        &self,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        self.apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings_and_diagnostics(
            swaps,
            progress_start_ts,
            progress_cursor,
            |_| {},
            |_, _, _, _| {},
        )
    }

    pub fn apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings_and_diagnostics(
        &self,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
        mut stage_start: impl FnMut(&str),
        mut stage_end: impl FnMut(&str, usize, u64, &str),
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring boundary lot sql batch with checkpoint",
            |conn| {
                apply_discovery_scoring_boundary_lot_swaps_and_checkpoint_on_conn(
                    conn,
                    swaps,
                    progress_start_ts,
                    progress_cursor,
                    &mut stage_start,
                    &mut stage_end,
                )
            },
        )?;

        Ok(super::DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms: 0,
            apply_ms,
            progress_update_ms,
        })
    }

    pub fn finalize_discovery_scoring_rug_facts(&self, watermark_ts: DateTime<Utc>) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring rug finalize", |conn| {
            finalize_mature_rug_facts_on_conn(conn, watermark_ts)?;
            Ok(0usize)
        })?;
        Ok(())
    }

    pub fn finalize_discovery_scoring_rug_facts_with_timing(
        &self,
        watermark_ts: DateTime<Utc>,
    ) -> Result<u64> {
        let started_at = Instant::now();
        self.finalize_discovery_scoring_rug_facts(watermark_ts)?;
        Ok(started_at.elapsed().as_millis() as u64)
    }
}
