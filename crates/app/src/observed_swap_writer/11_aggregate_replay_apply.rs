fn apply_aggregate_replay_page(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
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
    let apply_started = Instant::now();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_APPLY_START,
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
    let apply_result = run_discovery_scoring_replay_stage_with_sqlite_lock_retry(
        "apply_discovery_scoring_batch",
        OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE,
        || store.apply_discovery_scoring_batch(page, &config.aggregate_write_config),
        observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable,
    );
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_APPLY_END,
        apply_result.as_ref().err().and_then(|error| {
            observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(error).then_some(
                OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE,
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
        elapsed_ms_ceil(apply_started.elapsed()),
    );
    if let Err(error) = apply_result {
        let retryable_replay_apply_lock =
            observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(&error);
        let mut retryable_gap_latched = false;
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
                        "observed swap writer startup replay stopping after fatal discovery scoring gap cursor failure",
                    );
                }
                if retryable_replay_apply_lock && progress.gap_cursor_loaded {
                    warn!(
                        reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE,
                        error = %gap_error,
                        "failed to refresh discovery scoring materialization gap after retryable replay apply lock; keeping existing gap evidence",
                    );
                    retryable_gap_latched = true;
                } else if retryable_replay_apply_lock {
                    return Err(gap_error).context(
                        "observed swap writer startup replay stopping after retryable discovery scoring apply failure could not latch gap cursor",
                    );
                } else {
                    warn!(
                        error = %gap_error,
                        gap_since = %first_gap_swap.ts_utc,
                        "failed to latch discovery scoring materialization gap during aggregate-writer startup replay",
                    );
                }
            } else {
                retryable_gap_latched = true;
            }
        }
        if retryable_replay_apply_lock {
            warn!(
                reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE,
                error = %error,
                gap_latched = retryable_gap_latched,
                "discovery scoring replay apply hit retryable sqlite lock; leaving coverage watermark unchanged",
            );
            progress.gap_cursor_loaded |= retryable_gap_latched;
            progress.gap_cursor_observed = gap_cursor_observed;
            return Ok(true);
        }
        return Err(error).context(
            "failed replaying discovery scoring rows during aggregate-writer startup catch-up",
        );
    }
    Ok(false)
}
