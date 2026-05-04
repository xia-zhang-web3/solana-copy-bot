fn run_discovery_aggregate_gap_repair_slice(
    sqlite_path: &str,
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    require_gap_cursor: bool,
    repair_epoch: &mut DiscoveryAggregateGapRepairEpoch,
) -> Result<bool> {
    let slice_started = Instant::now();
    if !config.aggregate_gap_fallback_enabled || config.aggregate_idle_replay_max_pages == 0 {
        return Ok(false);
    }

    let current_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    if current_gap_cursor.is_none() {
        repair_epoch.reset();
        if require_gap_cursor {
            telemetry.set_aggregate_gap_active(false);
            return Ok(false);
        }
    }

    if let Some(current_gap_cursor) = current_gap_cursor.as_ref() {
        if !repair_epoch.matches_gap(current_gap_cursor) {
            let (repair_target_cursor, repair_target_source) =
                discovery_aggregate_repair_target_for_gap(
                    sqlite_path,
                    store,
                    Some(current_gap_cursor),
                )?;
            let resume_decision = discovery_aggregate_repair_resume_decision(
                sqlite_path,
                store,
                Some(current_gap_cursor),
            )?;
            info!(
                materialization_gap_ts = %current_gap_cursor.ts_utc,
                materialization_gap_slot = current_gap_cursor.slot,
                materialization_gap_signature = %current_gap_cursor.signature,
                persisted_covered_through_ts = resume_decision
                    .persisted_covered_through_cursor
                    .as_ref()
                    .map(|cursor| cursor.ts_utc.to_rfc3339()),
                persisted_covered_through_slot = resume_decision
                    .persisted_covered_through_cursor
                    .as_ref()
                    .map(|cursor| cursor.slot),
                persisted_covered_through_signature = resume_decision
                    .persisted_covered_through_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str()),
                repair_resume_source = resume_decision.repair_resume_source,
                reconstructed_gap_row_observed = resume_decision.reconstructed_gap_row_observed,
                repair_page_limit = config.aggregate_idle_replay_max_pages,
                repair_target_ts = repair_target_cursor.as_ref().map(|cursor| cursor.ts_utc.to_rfc3339()),
                repair_target_slot = repair_target_cursor.as_ref().map(|cursor| cursor.slot),
                repair_target_signature = repair_target_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                repair_target_source,
                "discovery aggregate materialization gap repair epoch started"
            );
            repair_epoch.reset_for_gap(
                current_gap_cursor.clone(),
                repair_target_cursor,
                resume_decision,
            );
        }
    }

    let resume_after_cursor = repair_epoch.resume_after_cursor.as_ref();
    let repair_target_cursor = current_gap_cursor
        .as_ref()
        .and_then(|_| repair_epoch.repair_target_cursor.as_ref());
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_START,
        Some(DISCOVERY_AGGREGATE_REASON_MATERIALIZATION_GAP_LATCHED),
        current_gap_cursor.as_ref(),
        repair_epoch.persisted_covered_through_cursor.as_ref(),
        repair_target_cursor,
        resume_after_cursor,
        repair_epoch.repair_resume_source,
        repair_epoch.reconstructed_gap_row_observed,
        Some(config.aggregate_idle_replay_max_pages),
        None,
        0,
        None,
        false,
        false,
        false,
        0,
    );

    telemetry.set_aggregate_worker_busy(true);
    let result = run_aggregate_gap_replay_with_resume(
        store,
        config,
        Some(config.aggregate_idle_replay_max_pages),
        resume_after_cursor,
        repair_epoch.gap_cursor_observed,
        repair_target_cursor,
    );
    telemetry.set_aggregate_worker_busy(false);
    let replay_progress = result?;

    repair_epoch.gap_cursor_observed |= replay_progress.gap_cursor_observed;
    let remaining_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let latch_cleared = remaining_gap_cursor.is_none() && current_gap_cursor.is_some();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_END,
        Some(DISCOVERY_AGGREGATE_REASON_MATERIALIZATION_GAP_LATCHED),
        current_gap_cursor.as_ref(),
        repair_epoch.persisted_covered_through_cursor.as_ref(),
        repair_target_cursor,
        resume_after_cursor,
        repair_epoch.repair_resume_source,
        repair_epoch.reconstructed_gap_row_observed,
        Some(config.aggregate_idle_replay_max_pages),
        Some(replay_progress.last_page_rows),
        replay_progress.page_count,
        replay_progress.last_replay_cursor.as_ref(),
        replay_progress.reached_repair_target,
        replay_progress.caught_up_to_tail,
        latch_cleared,
        elapsed_ms_ceil(slice_started.elapsed()),
    );
    if let Some(remaining_gap_cursor) = remaining_gap_cursor {
        if let Some(last_replay_cursor) = replay_progress.last_replay_cursor {
            if repair_epoch.matches_gap(&remaining_gap_cursor) {
                repair_epoch.resume_after_cursor = Some(last_replay_cursor);
            } else {
                repair_epoch.reset();
            }
        }
    } else {
        repair_epoch.reset();
    }
    telemetry.set_aggregate_gap_active(
        store
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some(),
    );
    Ok(true)
}
