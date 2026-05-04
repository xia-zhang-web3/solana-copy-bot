fn update_aggregate_replay_covered_through(
    store: &SqliteStore,
    page: &[SwapEvent],
    cursor: &DiscoveryRuntimeCursor,
    gap_cursor: Option<&DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
    gap_cursor_observed_before: bool,
    gap_cursor_observed: bool,
    max_pages: Option<usize>,
    reached_repair_target: bool,
    progress: &mut AggregateReplayProgress,
) -> Result<bool> {
    let covered_through_update_started = Instant::now();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_START,
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
    let covered_through_update_result = run_discovery_scoring_replay_stage_with_sqlite_lock_retry(
        "set_discovery_scoring_covered_through_cursor",
        OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
        || store.set_discovery_scoring_covered_through_cursor(cursor),
        observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable,
    );
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_END,
        covered_through_update_result
            .as_ref()
            .err()
            .and_then(|error| {
                observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                    error,
                )
                .then_some(
                    OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
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
        elapsed_ms_ceil(covered_through_update_started.elapsed()),
    );
    if let Err(error) = covered_through_update_result {
        if observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(&error)
        {
            if let Err(gap_error) =
                latch_discovery_scoring_materialization_gap_from_swaps(store, page)
            {
                if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                    return Err(gap_error).context(
                        "observed swap writer startup replay stopping after fatal discovery scoring covered_through retry gap cursor failure",
                    );
                }
                warn!(
                    reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                    error = %gap_error,
                    "failed to latch discovery scoring materialization gap after retryable covered_through cursor update failure",
                );
            }
            warn!(
                reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                error = %error,
                covered_through = %cursor.ts_utc,
                "discovery scoring covered_through cursor update hit retryable sqlite lock during replay; leaving coverage watermark unchanged",
            );
            progress.gap_cursor_observed = gap_cursor_observed;
            return Ok(true);
        }
        return Err(error)
            .context("failed to run discovery scoring covered_through cursor update");
    }
    Ok(false)
}
