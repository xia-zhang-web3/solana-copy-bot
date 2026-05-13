use anyhow::{Context, Result};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use super::*;

pub(super) async fn handle_irrelevant_observed_swap(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    swap: SwapEvent,
    source_branch: IrrelevantObservedSwapBackpressureSourceBranch,
    ingestion_snapshot: Option<IngestionRuntimeSnapshot>,
    follow_snapshot: &Arc<FollowSnapshot>,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
    discovery_critical_target_buy_mints_backpressure_refresh_state: &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    zero_universe_empty_target_noncritical_best_effort: &mut ZeroUniverseEmptyTargetNoncriticalBestEffortState,
    pending_irrelevant_swaps: &mut VecDeque<PendingIrrelevantObservedSwap>,
    recent_swap_signatures: &mut HashSet<String>,
    recent_swap_signature_order: &mut VecDeque<String>,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
    swap_processing_started_at: StdInstant,
) -> Result<()> {
    let discovery_critical_irrelevant_persistence =
        irrelevant_observed_swap_requires_discovery_critical_persistence(
            &swap,
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        );
    zero_universe_empty_target_noncritical_best_effort.refresh_after_writer_pressure_clears(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
        &observed_swap_writer.snapshot(),
        StdInstant::now(),
    );
    if should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
        discovery_critical_irrelevant_persistence,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
        zero_universe_empty_target_noncritical_best_effort.exhausted(),
    ) {
        forget_recent_swap_signature(
            recent_swap_signatures,
            recent_swap_signature_order,
            &swap.signature,
        );
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        debug!(
            signature = %swap.signature,
            "dropping non-critical irrelevant observed swap after the empty-target zero-universe best-effort writer budget was already exhausted"
        );
        return Ok(());
    }
    if should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
        discovery_critical_irrelevant_persistence,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        ingestion_snapshot,
    ) {
        forget_recent_swap_signature(
            recent_swap_signatures,
            recent_swap_signature_order,
            &swap.signature,
        );
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        debug!(
            signature = %swap.signature,
            yellowstone_output_queue_depth = ingestion_snapshot
                .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                .unwrap_or(0),
            yellowstone_output_queue_capacity = ingestion_snapshot
                .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                .unwrap_or(0),
            yellowstone_output_queue_fill_ratio = ingestion_snapshot
                .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                .unwrap_or(0.0),
            "dropping non-critical irrelevant observed swap under Yellowstone output pressure"
        );
        return Ok(());
    }
    match persist_irrelevant_observed_swap(
        observed_swap_writer,
        recent_swap_signatures,
        recent_swap_signature_order,
        &swap,
        discovery_critical_irrelevant_persistence,
    )
    .await
    {
        Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued) => {
            app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
            return Ok(());
        }
        Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure) => {
            let writer_snapshot = observed_swap_writer.snapshot();
            let discovery_critical_irrelevant_persistence =
                refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                    store,
                    &swap,
                    follow_snapshot,
                    open_shadow_lots,
                    shadow_strategy_fail_closed,
                    discovery_critical_target_buy_mints,
                    discovery_critical_target_buy_mints_backpressure_refresh_state,
                )?;
            reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
                zero_universe_empty_target_noncritical_best_effort,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
            );
            if !should_buffer_backpressured_irrelevant_observed_swap(
                discovery_critical_irrelevant_persistence,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
            ) {
                forget_recent_swap_signature(
                    recent_swap_signatures,
                    recent_swap_signature_order,
                    &swap.signature,
                );
                app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
                debug!(
                    signature = %swap.signature,
                    "dropping non-critical irrelevant observed swap under writer backpressure"
                );
                if zero_universe_empty_target_noncritical_irrelevant_context(
                    discovery_critical_irrelevant_persistence,
                    follow_snapshot,
                    open_shadow_lots,
                    shadow_strategy_fail_closed,
                    discovery_critical_target_buy_mints,
                ) {
                    zero_universe_empty_target_noncritical_best_effort
                        .mark_exhausted(StdInstant::now());
                }
                return Ok(());
            }
            warn_irrelevant_observed_swap_writer_backpressure(
                &swap,
                source_branch,
                discovery_critical_irrelevant_persistence,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
                pending_irrelevant_swaps,
                &writer_snapshot,
                ingestion_snapshot,
            );
            let pending_queue_was_full =
                pending_irrelevant_swap_queue_is_full(pending_irrelevant_swaps);
            if should_drop_backpressured_discovery_critical_irrelevant_observed_swap(
                pending_irrelevant_swaps,
            ) {
                forget_recent_swap_signature(
                    recent_swap_signatures,
                    recent_swap_signature_order,
                    &swap.signature,
                );
                app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
                warn!(
                    signature = %swap.signature,
                    pending_irrelevant_swap_queue_depth =
                        pending_irrelevant_swaps.len(),
                    observed_swap_writer_pending_requests =
                        writer_snapshot.pending_requests,
                    yellowstone_output_queue_depth = ingestion_snapshot
                        .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                        .unwrap_or(0),
                    yellowstone_output_queue_capacity = ingestion_snapshot
                        .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                        .unwrap_or(0),
                    yellowstone_output_queue_fill_ratio = ingestion_snapshot
                        .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                        .unwrap_or(0.0),
                    "dropping discovery-critical irrelevant observed swap because the bounded in-memory backlog is full; continuing ingestion polling so the upstream output queue can drain"
                );
                return Ok(());
            }
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap,
                discovery_critical: discovery_critical_irrelevant_persistence,
                processing_started_at: swap_processing_started_at,
                backpressure_started_at: StdInstant::now(),
                last_backpressure_log_at: Some(StdInstant::now()),
            });
            if discovery_critical_irrelevant_persistence
                && !pending_queue_was_full
                && pending_irrelevant_swap_queue_is_full(pending_irrelevant_swaps)
            {
                warn!(
                pending_irrelevant_swap_queue_depth =
                    pending_irrelevant_swaps.len(),
                "discovery-critical irrelevant observed swap backlog reached the bounded in-memory limit; continuing ingestion polling and dropping additional backpressured discovery-critical swaps until writer capacity recovers"
            );
            }
            return Ok(());
        }
        Err(error) => {
            app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
            let error_chain = format_error_chain(&error);
            if observed_swap_writer_error_requires_restart(&error) {
                return Err(error).context(
                "observed swap writer is no longer running; restarting app to avoid silent stale ingestion",
            );
            }
            warn!(
                error = %error,
                error_chain = %error_chain,
                signature = %swap.signature,
                "failed enqueueing observed swap"
            );
            return Ok(());
        }
    }
}
