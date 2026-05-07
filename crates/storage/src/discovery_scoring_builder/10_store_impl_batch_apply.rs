use super::super::*;

impl SqliteStore {
    pub fn advance_discovery_scoring_builder_batch_in_memory_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let (_prepared, prepare_ms) =
            prepare_discovery_scoring_builder_batch(self, builder, swaps, config)?;
        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms: 0,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_builder_batch_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let (prepared, prepare_ms) =
            prepare_discovery_scoring_builder_batch(self, builder, swaps, config)?;
        let apply_started_at = Instant::now();
        self.with_immediate_transaction_retry("discovery scoring builder batch", |conn| {
            flush_prepared_discovery_scoring_builder_batch_on_conn(conn, &prepared)?;
            Ok(())
        })?;
        let apply_ms = apply_started_at.elapsed().as_millis() as u64;

        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_builder_batch_and_checkpoint_with_timings(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringCheckpointedBatchTimings> {
        self.apply_discovery_scoring_builder_batch_and_checkpoint_with_timings_and_diagnostics(
            builder,
            swaps,
            config,
            progress_start_ts,
            progress_cursor,
            |_| {},
            |_, _, _, _| {},
        )
    }

    pub fn apply_discovery_scoring_builder_batch_and_checkpoint_with_timings_and_diagnostics(
        &self,
        builder: &mut DiscoveryScoringReplayBuilder,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
        mut stage_start: impl FnMut(&str),
        mut stage_end: impl FnMut(&str, usize, u64, &str),
    ) -> Result<DiscoveryScoringCheckpointedBatchTimings> {
        stage_start("prepare_discovery_scoring_builder_batch");
        let prepare_started_at = Instant::now();
        let (prepared, prepare_ms) =
            match prepare_discovery_scoring_builder_batch(self, builder, swaps, config) {
                Ok((prepared, prepare_ms)) => {
                    stage_end(
                        "prepare_discovery_scoring_builder_batch",
                        swaps.len(),
                        prepare_ms.max(prepare_started_at.elapsed().as_millis() as u64),
                        "completed",
                    );
                    (prepared, prepare_ms)
                }
                Err(error) => {
                    stage_end(
                        "prepare_discovery_scoring_builder_batch",
                        swaps.len(),
                        prepare_started_at.elapsed().as_millis() as u64,
                        "failed",
                    );
                    return Err(error);
                }
            };

        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring builder batch with checkpoint",
            |conn| {
                stage_start("aggregate_batch_apply");
                let apply_started_at = Instant::now();
                if let Err(error) =
                    flush_prepared_discovery_scoring_builder_batch_on_conn(conn, &prepared)
                {
                    stage_end(
                        "aggregate_batch_apply",
                        swaps.len(),
                        apply_started_at.elapsed().as_millis() as u64,
                        "failed",
                    );
                    return Err(error);
                }
                let apply_ms = apply_started_at.elapsed().as_millis() as u64;
                stage_end("aggregate_batch_apply", swaps.len(), apply_ms, "completed");

                crate::discovery_scoring::maybe_fail_after_materialization_before_checkpoint()?;

                stage_start("progress_checkpoint");
                let progress_started_at = Instant::now();
                let updated_at = Utc::now().to_rfc3339();
                if let Err(error) =
                    crate::discovery_scoring::upsert_discovery_scoring_backfill_progress_on_conn(
                        conn,
                        progress_start_ts,
                        progress_cursor,
                        &updated_at,
                    )
                {
                    stage_end(
                        "progress_checkpoint",
                        swaps.len(),
                        progress_started_at.elapsed().as_millis() as u64,
                        "failed",
                    );
                    return Err(error);
                }
                let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
                stage_end(
                    "progress_checkpoint",
                    swaps.len(),
                    progress_update_ms,
                    "completed",
                );
                Ok((apply_ms, progress_update_ms))
            },
        )?;

        Ok(DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms,
            progress_update_ms,
        })
    }
}
