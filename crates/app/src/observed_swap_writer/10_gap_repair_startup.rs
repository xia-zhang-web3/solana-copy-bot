fn run_aggregate_startup_replay(
    sqlite_path: &str,
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
) -> Result<AggregateReplayProgress> {
    let startup_started = Instant::now();
    if !config.aggregate_writes_enabled {
        log_discovery_aggregate_phase(
            DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_SKIPPED,
            Some(DISCOVERY_AGGREGATE_REASON_AGGREGATE_WRITES_DISABLED),
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            0,
            None,
            false,
            false,
            false,
            0,
        );
        return Ok(AggregateReplayProgress::default());
    }

    let current_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let resume_decision = discovery_aggregate_repair_resume_decision(
        sqlite_path,
        store,
        current_gap_cursor.as_ref(),
    )?;
    let (repair_target_cursor, repair_target_source) =
        discovery_aggregate_repair_target_for_gap(sqlite_path, store, current_gap_cursor.as_ref())?;
    if let Some(current_gap_cursor) = current_gap_cursor.as_ref() {
        info!(
            materialization_gap_ts = %current_gap_cursor.ts_utc,
            materialization_gap_slot = current_gap_cursor.slot,
            materialization_gap_signature = %current_gap_cursor.signature,
            repair_target_ts = repair_target_cursor.as_ref().map(|cursor| cursor.ts_utc.to_rfc3339()),
            repair_target_slot = repair_target_cursor.as_ref().map(|cursor| cursor.slot),
            repair_target_signature = repair_target_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
            repair_target_source,
            "discovery aggregate materialization repair target selected during startup replay"
        );
    }
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_START,
        current_gap_cursor
            .as_ref()
            .map(|_| DISCOVERY_AGGREGATE_REASON_MATERIALIZATION_GAP_LATCHED),
        current_gap_cursor.as_ref(),
        resume_decision.persisted_covered_through_cursor.as_ref(),
        repair_target_cursor.as_ref(),
        resume_decision.resume_after_cursor.as_ref(),
        current_gap_cursor
            .as_ref()
            .map(|_| resume_decision.repair_resume_source),
        resume_decision.reconstructed_gap_row_observed,
        current_gap_cursor
            .as_ref()
            .map(|_| config.aggregate_idle_replay_max_pages.max(1)),
        None,
        0,
        None,
        false,
        false,
        false,
        0,
    );

    let max_pages = current_gap_cursor
        .as_ref()
        .map(|_| config.aggregate_idle_replay_max_pages.max(1));
    let progress = run_aggregate_gap_replay_with_resume(
        store,
        config,
        max_pages,
        resume_decision.resume_after_cursor.as_ref(),
        resume_decision.gap_cursor_observed,
        repair_target_cursor.as_ref(),
    )?;
    let remaining_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let latch_cleared = current_gap_cursor.is_some() && remaining_gap_cursor.is_none();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_END,
        current_gap_cursor
            .as_ref()
            .map(|_| DISCOVERY_AGGREGATE_REASON_MATERIALIZATION_GAP_LATCHED),
        current_gap_cursor.as_ref(),
        resume_decision.persisted_covered_through_cursor.as_ref(),
        repair_target_cursor.as_ref(),
        resume_decision.resume_after_cursor.as_ref(),
        current_gap_cursor
            .as_ref()
            .map(|_| resume_decision.repair_resume_source),
        resume_decision.reconstructed_gap_row_observed,
        current_gap_cursor
            .as_ref()
            .map(|_| config.aggregate_idle_replay_max_pages.max(1)),
        Some(progress.last_page_rows),
        progress.page_count,
        progress.last_replay_cursor.as_ref(),
        progress.reached_repair_target,
        progress.caught_up_to_tail,
        latch_cleared,
        elapsed_ms_ceil(startup_started.elapsed()),
    );
    Ok(progress)
}
