fn finalize_aggregate_replay_rug_facts(
    store: &SqliteStore,
    page: &[SwapEvent],
    last_swap: &SwapEvent,
    cursor: &DiscoveryRuntimeCursor,
    gap_cursor: Option<&DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
    gap_cursor_observed_before: bool,
    gap_cursor_observed: bool,
    max_pages: Option<usize>,
    reached_repair_target: bool,
    progress: &mut AggregateReplayProgress,
) -> Result<bool> {
    let rug_finalize_started = Instant::now();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_START,
        None,
        gap_cursor,
        None,
        repair_target_cursor,
        Some(cursor),
        None,
        gap_cursor_observed_before,
        max_pages,
        Some(page.len()),
        progress.page_count,
        progress.last_replay_cursor.as_ref(),
        reached_repair_target,
        progress.caught_up_to_tail,
        false,
        0,
    );
    let rug_finalize_result = run_discovery_scoring_replay_stage_with_sqlite_lock_retry(
        "finalize_discovery_scoring_rug_facts",
        OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
        || store.finalize_discovery_scoring_rug_facts(last_swap.ts_utc),
        observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable,
    );
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_END,
        rug_finalize_result.as_ref().err().and_then(|error| {
            observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(error).then_some(
                OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
            )
        }),
        gap_cursor,
        None,
        repair_target_cursor,
        Some(cursor),
        None,
        gap_cursor_observed_before,
        max_pages,
        Some(page.len()),
        progress.page_count,
        progress.last_replay_cursor.as_ref(),
        reached_repair_target,
        progress.caught_up_to_tail,
        false,
        elapsed_ms_ceil(rug_finalize_started.elapsed()),
    );
    if let Err(error) = rug_finalize_result {
        if observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(&error) {
            if let Some(first_gap_swap) = page.iter().min_by(|a, b| {
                a.ts_utc
                    .cmp(&b.ts_utc)
                    .then_with(|| a.slot.cmp(&b.slot))
                    .then_with(|| a.signature.cmp(&b.signature))
            }) {
                if let Err(gap_error) =
                    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
                        ts_utc: first_gap_swap.ts_utc,
                        slot: first_gap_swap.slot,
                        signature: first_gap_swap.signature.clone(),
                    })
                {
                    if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                        return Err(gap_error).context(
                            "observed swap writer startup replay stopping after fatal discovery scoring rug finalize gap cursor failure",
                        );
                    }
                    warn!(
                        reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
                        error = %gap_error,
                        gap_since = %first_gap_swap.ts_utc,
                        "failed to latch discovery scoring materialization gap after retryable rug finalize failure",
                    );
                }
            }
            warn!(
                reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
                error = %error,
                watermark_ts = %last_swap.ts_utc,
                "discovery scoring rug finalize hit retryable sqlite lock; leaving coverage watermark unchanged",
            );
            progress.gap_cursor_observed = gap_cursor_observed;
            return Ok(true);
        }
        return Err(error).context("failed to run discovery scoring rug finalize");
    }
    Ok(false)
}
