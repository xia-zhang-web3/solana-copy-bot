use anyhow::Result;

use super::*;

pub(crate) fn handle_discovery_join_result(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    ingestion: &IngestionService,
    recent_raw_journal_path: &str,
    discovery_result: Option<
        std::result::Result<Result<DiscoveryTaskOutput>, tokio::task::JoinError>,
    >,
    follow_snapshot: &mut Arc<FollowSnapshot>,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_strategy_fail_closed: &mut bool,
    shadow_risk_guard: &mut ShadowRiskGuard,
    follow_event_retention: Duration,
    shadow_queue_full: bool,
    discovery_handle: &mut Option<JoinHandle<Result<DiscoveryTaskOutput>>>,
    discovery_running_trigger: &mut Option<&'static str>,
    discovery_catch_up_pending: &mut bool,
    discovery_catch_up_recent_raw_journal_defer_retry_at: &mut Option<StdInstant>,
    discovery_recent_raw_journal_abort_reason: &mut Option<&'static str>,
    discovery_runtime_memory_abort_reason: &mut Option<&'static str>,
    discovery_recent_raw_journal_safety_settle: &mut DiscoveryRecentRawJournalSafetySettleState,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
    discovery_critical_target_buy_mints_backpressure_refresh_state: &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
) -> Result<()> {
    *discovery_handle = None;
    let discovery_abort_reason = discovery_recent_raw_journal_abort_reason
        .take()
        .or_else(|| discovery_runtime_memory_abort_reason.take());
    let discovery_task_trigger = discovery_running_trigger.take().unwrap_or("unknown");
    if discovery_task_output_should_be_ignored_due_to_abort_reason(discovery_abort_reason) {
        let reason = discovery_abort_reason.expect("checked abort reason");
        warn!(
        reason,
        discovery_task_trigger,
        "discovery task result ignored because runtime safety gate became unsafe while the task was running"
    );
        if discovery_task_abort_preserves_catch_up_pending(discovery_task_trigger) {
            *discovery_catch_up_pending = true;
            *discovery_catch_up_recent_raw_journal_defer_retry_at = Some(
                StdInstant::now() + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL,
            );
        }
        return Ok(());
    }
    match discovery_result.expect("guard ensures discovery task exists") {
        Ok(Ok(discovery_output)) => {
            let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
            let persisted_lag_gate = load_discovery_recent_raw_journal_persisted_lag_gate(
                store,
                recent_raw_journal_path,
            );
            if let Some(reason) = discovery_recent_raw_journal_abort_reason_from_gates(
                &observed_swap_writer_snapshot,
                &persisted_lag_gate,
            ) {
                discovery_recent_raw_journal_safety_settle.reset();
                warn!(
                reason,
                discovery_task_trigger,
                "discovery task success output ignored because recent_raw journal is unsafe at join"
            );
                if discovery_task_trigger == "catch_up_retrigger" {
                    *discovery_catch_up_pending = true;
                    *discovery_catch_up_recent_raw_journal_defer_retry_at = Some(
                        StdInstant::now()
                            + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL,
                    );
                }
                return Ok(());
            }
            let memory_gate = load_discovery_runtime_memory_pressure_gate();
            if discovery_runtime_memory_pressure_defer_reason(&memory_gate).is_some() {
                warn_discovery_aborted_due_to_runtime_memory_pressure(
                    discovery_task_trigger,
                    &memory_gate,
                );
                if discovery_task_abort_preserves_catch_up_pending(discovery_task_trigger) {
                    *discovery_catch_up_pending = true;
                    *discovery_catch_up_recent_raw_journal_defer_retry_at = Some(
                        StdInstant::now()
                            + DISCOVERY_CATCH_UP_RECENT_RAW_JOURNAL_BACKLOG_RETRY_INTERVAL,
                    );
                }
                return Ok(());
            }
            info!(
                discovery_task_join_result = "success",
                discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                discovery_scoring_source = discovery_output.scoring_source,
                discovery_published = discovery_output.published,
                "discovery task joined successfully"
            );
            let active_follow_wallets_before_clear = follow_snapshot.active.len();
            let fail_closed_runtime_surface = apply_fail_closed_runtime_follow_surface_if_needed(
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                &discovery_output,
            );
            if fail_closed_runtime_surface && !discovery_output.published {
                warn!(
                discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                discovery_scoring_source = discovery_output.scoring_source,
                active_follow_wallets_before_clear,
                "clearing runtime follow snapshot because discovery no longer has a publishable universe to keep in memory"
            );
            }
            if discovery_output.published {
                if !fail_closed_runtime_surface {
                    let mut snapshot = follow_snapshot.as_ref().clone();
                    apply_follow_snapshot_update(
                        &mut snapshot,
                        discovery_output.active_wallets.clone(),
                        discovery_output.cycle_ts,
                        follow_event_retention,
                    );
                    *follow_snapshot = Arc::new(snapshot);
                    if *shadow_strategy_fail_closed {
                        refresh_shadow_open_lot_index_or_warn(store, open_shadow_lots)?;
                    }
                    *shadow_strategy_fail_closed = false;
                }
                shadow_risk_guard
                    .observe_discovery_cycle(
                        store,
                        discovery_output.cycle_ts,
                        discovery_output.eligible_wallets,
                        discovery_output.active_follow_wallets,
                        Some(&discovery_output),
                    )
                    .context(
                        "shadow risk discovery-cycle universe event failed with fatal sqlite I/O",
                    )?;
            }
            let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
            let ingestion_snapshot = ingestion.runtime_snapshot();
            let catch_up_block_reason = discovery_catch_up_block_reason(
                &discovery_output,
                shadow_queue_full,
                &observed_swap_writer_snapshot,
                ingestion_snapshot.as_ref(),
            );
            let pending_requests_only_pressure = discovery_catch_up_pending_requests_only_pressure(
                &observed_swap_writer_snapshot,
                ingestion_snapshot.as_ref(),
            );
            let pending_requests_only_raw_plateau =
                discovery_catch_up_pending_requests_is_only_raw_plateau(
                    &observed_swap_writer_snapshot,
                    ingestion_snapshot.as_ref(),
                );
            *discovery_catch_up_pending = should_schedule_discovery_catch_up(
                &discovery_output,
                shadow_queue_full,
                &observed_swap_writer_snapshot,
                ingestion_snapshot.as_ref(),
            );
            *discovery_catch_up_recent_raw_journal_defer_retry_at = None;
            if *discovery_catch_up_pending {
                let pressure_override =
                    discovery_output.persisted_stream_catch_up_pressure_override_requested;
                if pressure_override {
                    info!(
                    discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                    discovery_scoring_source = discovery_output.scoring_source,
                    discovery_persisted_stream_catch_up_pressure_override_requested =
                        pressure_override,
                    shadow_queue_full,
                    writer_pending_requests =
                        observed_swap_writer_snapshot.pending_requests,
                    writer_pending_requests_threshold =
                        DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                    writer_aggregate_queue_depth_batches =
                        observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                    writer_journal_queue_depth_batches =
                        observed_swap_writer_snapshot.journal_queue_depth_batches,
                    discovery_catch_up_pending_requests_only_pressure_bypassed =
                        pending_requests_only_pressure,
                    discovery_catch_up_pending_requests_only_raw_plateau_bypassed =
                        pending_requests_only_raw_plateau,
                    yellowstone_output_queue_fill_ratio =
                        ingestion_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                    "bounded partial discovery rebuild requested immediate catch-up cycle and overrode the runtime pressure gate because fail-closed publication recovery is on a constrained priority path"
                );
                } else {
                    info!(
                        discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                        discovery_scoring_source = discovery_output.scoring_source,
                        discovery_persisted_stream_catch_up_pressure_override_requested =
                            pressure_override,
                        shadow_queue_full,
                        writer_pending_requests = observed_swap_writer_snapshot.pending_requests,
                        writer_pending_requests_threshold =
                            DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                        writer_aggregate_queue_depth_batches =
                            observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                        writer_journal_queue_depth_batches =
                            observed_swap_writer_snapshot.journal_queue_depth_batches,
                        discovery_catch_up_pending_requests_only_raw_plateau_bypassed =
                            pending_requests_only_raw_plateau,
                        yellowstone_output_queue_fill_ratio = ingestion_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                        "bounded partial discovery rebuild requested immediate catch-up cycle"
                    );
                }
            } else if discovery_output.persisted_stream_catch_up_requested {
                info!(
                discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                discovery_scoring_source = discovery_output.scoring_source,
                discovery_persisted_stream_catch_up_pressure_override_requested =
                    discovery_output
                        .persisted_stream_catch_up_pressure_override_requested,
                shadow_queue_full,
                writer_pending_requests =
                    observed_swap_writer_snapshot.pending_requests,
                writer_pending_requests_threshold =
                    DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                writer_aggregate_queue_depth_batches =
                    observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                writer_journal_queue_depth_batches =
                    observed_swap_writer_snapshot.journal_queue_depth_batches,
                discovery_catch_up_block_reason = catch_up_block_reason
                    .map(DiscoveryCatchUpBlockReason::as_str),
                discovery_catch_up_pending_requests_only_blocker =
                    pending_requests_only_pressure,
                discovery_catch_up_pending_requests_only_raw_plateau =
                    pending_requests_only_raw_plateau,
                yellowstone_output_queue_fill_ratio =
                    ingestion_snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                "bounded partial discovery rebuild requested catch-up, but runtime pressure gate deferred immediate retrigger"
            );
            }
            refresh_discovery_critical_target_buy_mints_or_warn(
                store,
                discovery_critical_target_buy_mints,
            )?;
            discovery_critical_target_buy_mints_backpressure_refresh_state
                .note_refresh_attempt(StdInstant::now());
        }
        Ok(Err(error)) => {
            *discovery_running_trigger = None;
            *discovery_catch_up_pending = false;
            *discovery_catch_up_recent_raw_journal_defer_retry_at = None;
            warn!(
                discovery_task_join_result = "task_error",
                error = %error,
                "discovery task joined with task error"
            );
            if discovery_task_error_requires_restart(&error) {
                return Err(error).context("discovery cycle failed with fatal sqlite I/O");
            }
            warn!(error = %error, "discovery cycle failed");
        }
        Err(error) => {
            *discovery_running_trigger = None;
            *discovery_catch_up_pending = false;
            *discovery_catch_up_recent_raw_journal_defer_retry_at = None;
            warn!(
                discovery_task_join_result = "join_error",
                error = %error,
                "discovery task join failed"
            );
        }
    }
    Ok(())
}
