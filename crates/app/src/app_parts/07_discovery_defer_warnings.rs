fn warn_discovery_deferred_due_to_recent_raw_journal_persisted_lag(
    trigger: &'static str,
    persisted_lag_gate: &DiscoveryRecentRawJournalPersistedLagGate,
) {
    let observed_tail_cursor_ts = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.ts_utc.to_rfc3339());
    let observed_tail_cursor_slot = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.slot);
    let observed_tail_cursor_signature = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.signature.as_str());
    let journal_cursor_ts = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.ts_utc.to_rfc3339());
    let journal_cursor_slot = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.slot);
    let journal_cursor_signature = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.signature.as_str());
    warn!(
        reason = persisted_lag_gate
            .reason
            .unwrap_or(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_LAG_UNPROVEN),
        discovery_task_trigger = trigger,
        observed_swaps_tail_ts_utc = observed_tail_cursor_ts.as_deref(),
        observed_swaps_tail_slot = observed_tail_cursor_slot,
        observed_swaps_tail_signature = observed_tail_cursor_signature,
        recent_raw_journal_covered_through_ts_utc = journal_cursor_ts.as_deref(),
        recent_raw_journal_covered_through_slot = journal_cursor_slot,
        recent_raw_journal_covered_through_signature = journal_cursor_signature,
        recent_raw_journal_lag_seconds = persisted_lag_gate.lag_seconds,
        recent_raw_journal_lag_check_error = persisted_lag_gate.error.as_deref(),
        "discovery scheduled/background cycle deferred because recent_raw journal persisted coverage is not proven current"
    );
}

fn warn_discovery_deferred_due_to_recent_raw_journal_settle_window(
    trigger: &'static str,
    settle_gate: &DiscoveryRecentRawJournalSafetySettleGate,
) {
    warn!(
        reason = settle_gate
            .reason
            .unwrap_or(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_SETTLE_WINDOW),
        discovery_task_trigger = trigger,
        recent_raw_journal_clean_consecutive_checks = settle_gate.consecutive_clean_checks,
        recent_raw_journal_required_consecutive_checks =
            DISCOVERY_RECENT_RAW_JOURNAL_SETTLE_CONSECUTIVE_CHECKS,
        recent_raw_journal_stable_for_seconds = settle_gate
            .stable_for
            .map(|duration| duration.as_secs()),
        recent_raw_journal_required_stable_seconds =
            DISCOVERY_RECENT_RAW_JOURNAL_SETTLE_WINDOW.as_secs(),
        recent_raw_journal_clean_since_known = settle_gate.clean_since.is_some(),
        "discovery scheduled/background cycle deferred until recent_raw journal safety remains stable"
    );
}

fn warn_discovery_deferred_due_to_recent_raw_journal_backlog(
    trigger: &'static str,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
) {
    warn!(
        reason = DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG,
        discovery_task_trigger = trigger,
        observed_swap_writer_journal_queue_depth_batches =
            observed_swap_writer_snapshot.journal_queue_depth_batches,
        observed_swap_writer_journal_queue_capacity_batches =
            observed_swap_writer_snapshot.journal_queue_capacity_batches,
        observed_swap_writer_journal_overflow_depth_batches =
            observed_swap_writer_snapshot.journal_overflow_depth_batches,
        observed_swap_writer_journal_overflow_capacity_batches =
            observed_swap_writer_snapshot.journal_overflow_capacity_batches,
        observed_swap_writer_journal_queue_row_debt =
            observed_swap_writer_snapshot.journal_queue_row_debt,
        observed_swap_writer_journal_overflow_row_debt =
            observed_swap_writer_snapshot.journal_overflow_row_debt,
        observed_swap_writer_journal_overflow_row_debt_capacity =
            observed_swap_writer_snapshot.journal_overflow_row_debt_capacity,
        observed_swap_writer_journal_writer_inflight_rows =
            observed_swap_writer_snapshot.journal_writer_inflight_rows,
        "discovery scheduled/background cycle deferred because recent_raw journal backlog is unsafe"
    );
}

fn warn_discovery_deferred_due_to_runtime_memory_pressure(
    trigger: &'static str,
    memory_gate: &DiscoveryRuntimeMemoryPressureGate,
) {
    warn!(
        reason = DISCOVERY_CYCLE_DEFERRED_DUE_TO_RUNTIME_MEMORY_PRESSURE,
        discovery_task_trigger = trigger,
        runtime_memory_pressure_reason = memory_gate.pressure_reason,
        process_rss_bytes = memory_gate.process_rss_bytes,
        process_rss_threshold_bytes = memory_gate.process_rss_threshold_bytes,
        system_available_bytes = memory_gate.system_available_bytes,
        system_available_threshold_bytes = memory_gate.system_available_threshold_bytes,
        system_total_bytes = memory_gate.system_total_bytes,
        system_available_bps = memory_gate.system_available_bps,
        system_available_min_bps = memory_gate.system_available_min_bps,
        runtime_memory_pressure_error = memory_gate.error.as_deref(),
        "discovery scheduled/background cycle deferred because runtime memory pressure is unsafe"
    );
}

fn warn_discovery_aborted_due_to_recent_raw_journal_persisted_lag(
    trigger: &'static str,
    persisted_lag_gate: &DiscoveryRecentRawJournalPersistedLagGate,
) {
    let observed_tail_cursor_ts = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.ts_utc.to_rfc3339());
    let observed_tail_cursor_slot = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.slot);
    let observed_tail_cursor_signature = persisted_lag_gate
        .observed_tail_cursor
        .as_ref()
        .map(|cursor| cursor.signature.as_str());
    let journal_cursor_ts = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.ts_utc.to_rfc3339());
    let journal_cursor_slot = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.slot);
    let journal_cursor_signature = persisted_lag_gate
        .journal_covered_through_cursor
        .as_ref()
        .map(|cursor| cursor.signature.as_str());
    warn!(
        reason = DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG,
        discovery_task_trigger = trigger,
        observed_swaps_tail_ts_utc = observed_tail_cursor_ts.as_deref(),
        observed_swaps_tail_slot = observed_tail_cursor_slot,
        observed_swaps_tail_signature = observed_tail_cursor_signature,
        recent_raw_journal_covered_through_ts_utc = journal_cursor_ts.as_deref(),
        recent_raw_journal_covered_through_slot = journal_cursor_slot,
        recent_raw_journal_covered_through_signature = journal_cursor_signature,
        recent_raw_journal_lag_seconds = persisted_lag_gate.lag_seconds,
        recent_raw_journal_lag_check_error = persisted_lag_gate.error.as_deref(),
        "discovery task aborted because recent_raw journal persisted coverage became unsafe while discovery was running"
    );
}

fn warn_discovery_aborted_due_to_runtime_memory_pressure(
    trigger: &'static str,
    memory_gate: &DiscoveryRuntimeMemoryPressureGate,
) {
    warn!(
        reason = DISCOVERY_CYCLE_ABORTED_DUE_TO_RUNTIME_MEMORY_PRESSURE,
        discovery_task_trigger = trigger,
        runtime_memory_pressure_reason = memory_gate.pressure_reason,
        process_rss_bytes = memory_gate.process_rss_bytes,
        process_rss_threshold_bytes = memory_gate.process_rss_threshold_bytes,
        system_available_bytes = memory_gate.system_available_bytes,
        system_available_threshold_bytes = memory_gate.system_available_threshold_bytes,
        system_total_bytes = memory_gate.system_total_bytes,
        system_available_bps = memory_gate.system_available_bps,
        system_available_min_bps = memory_gate.system_available_min_bps,
        runtime_memory_pressure_error = memory_gate.error.as_deref(),
        "discovery task aborted because runtime memory pressure became unsafe while discovery was running"
    );
}

fn warn_discovery_aborted_due_to_recent_raw_journal_backlog(
    trigger: &'static str,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
) {
    warn!(
        reason = DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG,
        discovery_task_trigger = trigger,
        observed_swap_writer_journal_queue_depth_batches =
            observed_swap_writer_snapshot.journal_queue_depth_batches,
        observed_swap_writer_journal_queue_capacity_batches =
            observed_swap_writer_snapshot.journal_queue_capacity_batches,
        observed_swap_writer_journal_overflow_depth_batches =
            observed_swap_writer_snapshot.journal_overflow_depth_batches,
        observed_swap_writer_journal_overflow_capacity_batches =
            observed_swap_writer_snapshot.journal_overflow_capacity_batches,
        observed_swap_writer_journal_queue_row_debt =
            observed_swap_writer_snapshot.journal_queue_row_debt,
        observed_swap_writer_journal_overflow_row_debt =
            observed_swap_writer_snapshot.journal_overflow_row_debt,
        observed_swap_writer_journal_overflow_row_debt_capacity =
            observed_swap_writer_snapshot.journal_overflow_row_debt_capacity,
        observed_swap_writer_journal_writer_inflight_rows =
            observed_swap_writer_snapshot.journal_writer_inflight_rows,
        "discovery task aborted because recent_raw journal backlog became unsafe while discovery was running"
    );
}

fn discovery_catch_up_has_ingestion_pressure(
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    ingestion_runtime_snapshot.is_some_and(|snapshot| {
        snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio > 0.0
    })
}

fn discovery_catch_up_pending_requests_only_pressure(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    observed_swap_writer_snapshot.pending_requests
        >= DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD
        && !discovery_catch_up_has_writer_queue_pressure(observed_swap_writer_snapshot)
        && !discovery_catch_up_has_ingestion_pressure(ingestion_runtime_snapshot)
}

fn discovery_catch_up_pending_requests_is_only_raw_plateau(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    observed_swap_writer_snapshot.pending_requests
        == DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD
        && !discovery_catch_up_has_writer_queue_pressure(observed_swap_writer_snapshot)
        && !discovery_catch_up_has_ingestion_pressure(ingestion_runtime_snapshot)
}
