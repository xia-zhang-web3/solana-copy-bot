fn log_discovery_aggregate_phase(
    current_phase: &'static str,
    reason: Option<&'static str>,
    current_gap_cursor: Option<&DiscoveryRuntimeCursor>,
    persisted_covered_through_cursor: Option<&DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
    resume_cursor: Option<&DiscoveryRuntimeCursor>,
    repair_resume_source: Option<&'static str>,
    reconstructed_gap_row_observed: bool,
    repair_page_limit: Option<usize>,
    page_rows: Option<usize>,
    page_count: usize,
    last_replay_cursor: Option<&DiscoveryRuntimeCursor>,
    reached_target: bool,
    caught_up_to_tail: bool,
    latch_cleared: bool,
    elapsed_ms: u64,
) {
    #[cfg(test)]
    if let Ok(mut events) = DISCOVERY_AGGREGATE_PHASE_EVENTS_FOR_TEST.lock() {
        events.push(current_phase);
    }
    info!(
        current_phase,
        reason,
        materialization_gap_ts = current_gap_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        materialization_gap_slot = current_gap_cursor.map(|cursor| cursor.slot),
        materialization_gap_signature = current_gap_cursor.map(|cursor| cursor.signature.as_str()),
        persisted_covered_through_ts =
            persisted_covered_through_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        persisted_covered_through_slot = persisted_covered_through_cursor.map(|cursor| cursor.slot),
        persisted_covered_through_signature =
            persisted_covered_through_cursor.map(|cursor| cursor.signature.as_str()),
        repair_target_ts = repair_target_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        repair_target_slot = repair_target_cursor.map(|cursor| cursor.slot),
        repair_target_signature = repair_target_cursor.map(|cursor| cursor.signature.as_str()),
        resume_cursor_ts = resume_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        resume_cursor_slot = resume_cursor.map(|cursor| cursor.slot),
        resume_cursor_signature = resume_cursor.map(|cursor| cursor.signature.as_str()),
        repair_resume_source,
        reconstructed_gap_row_observed,
        repair_page_limit,
        page_rows,
        page_count,
        last_replay_cursor_ts = last_replay_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        last_replay_cursor_slot = last_replay_cursor.map(|cursor| cursor.slot),
        last_replay_cursor_signature = last_replay_cursor.map(|cursor| cursor.signature.as_str()),
        reached_target,
        caught_up_to_tail,
        latch_cleared,
        elapsed_ms,
        "discovery aggregate materialization repair phase"
    );
}

#[cfg(test)]
fn clear_discovery_aggregate_phase_events_for_test() {
    if let Ok(mut events) = DISCOVERY_AGGREGATE_PHASE_EVENTS_FOR_TEST.lock() {
        events.clear();
    }
}

#[cfg(test)]
fn discovery_aggregate_phase_events_for_test() -> Vec<&'static str> {
    DISCOVERY_AGGREGATE_PHASE_EVENTS_FOR_TEST
        .lock()
        .map(|events| events.clone())
        .unwrap_or_default()
}

fn process_discovery_aggregate_write_request(
    store: &SqliteStore,
    inserted_swaps: &[SwapEvent],
    batch_started: Instant,
    aggregate_write_config: &DiscoveryAggregateWriteConfig,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    if inserted_swaps.is_empty() {
        telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
        return Ok(());
    }

    let aggregate_started = Instant::now();
    let aggregate_result =
        store.apply_discovery_scoring_batch(inserted_swaps, aggregate_write_config);
    telemetry.note_discovery_scoring_completed(elapsed_ms_ceil(aggregate_started.elapsed()));
    if let Err(error) = aggregate_result {
        if let Some(first_gap_swap) = inserted_swaps.iter().min_by(|a, b| {
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
                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    return Err(gap_error).context(
                        "observed swap writer stopping after fatal discovery scoring gap cursor failure",
                    );
                }
                warn!(
                    error = %gap_error,
                    gap_since = %first_gap_swap.ts_utc,
                    "failed to latch discovery scoring materialization gap after aggregate batch failure"
                );
            }
        }
        if observed_swap_writer_discovery_scoring_error_requires_abort(&error) {
            telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
            return Err(error).context(
                "observed swap writer stopping after fatal discovery scoring materialization failure",
            );
        }
        warn!(
            error = %error,
            inserted_swaps = inserted_swaps.len(),
            "observed swap batch inserted raw rows but discovery scoring materialization failed"
        );
    } else if let Some(max_swap) = inserted_swaps.iter().max_by(|a, b| {
        a.ts_utc
            .cmp(&b.ts_utc)
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.signature.cmp(&b.signature))
    }) {
        if let Err(error) =
            store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
                ts_utc: max_swap.ts_utc,
                slot: max_swap.slot,
                signature: max_swap.signature.clone(),
            })
        {
            if observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &error,
            ) {
                if let Err(gap_error) =
                    latch_discovery_scoring_materialization_gap_from_swaps(store, inserted_swaps)
                {
                    if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                        telemetry
                            .note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                        return Err(gap_error).context(
                            "observed swap writer stopping after fatal discovery scoring covered_through retry gap cursor failure",
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
                    covered_through = %max_swap.ts_utc,
                    "discovery scoring covered_through cursor update hit retryable sqlite lock; leaving coverage watermark unchanged",
                );
            } else {
                telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                return Err(error).context(
                    "observed swap writer stopping after fatal discovery scoring coverage watermark failure",
                );
            }
        }
    }

    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
    Ok(())
}

fn set_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
    message: String,
) {
    if let Ok(mut guard) = terminal_failure_message.lock() {
        if guard.is_none() {
            *guard = Some(message);
        }
    }
}

fn load_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    terminal_failure_message
        .lock()
        .ok()
        .and_then(|message| message.clone())
}
