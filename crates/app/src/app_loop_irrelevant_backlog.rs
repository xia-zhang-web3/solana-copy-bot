use anyhow::{Context, Result};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use super::*;

pub(super) async fn retry_pending_irrelevant_swaps(
    store: &SqliteStore,
    ingestion: &IngestionService,
    observed_swap_writer: &ObservedSwapWriter,
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
) -> Result<()> {
    let _ = refresh_discovery_critical_target_buy_mints_for_backpressure_if_due(
        store,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
        discovery_critical_target_buy_mints_backpressure_refresh_state,
        StdInstant::now(),
    )?;
    reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
        zero_universe_empty_target_noncritical_best_effort,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
    );
    let dropped_noncritical_pending = prune_noncritical_zero_universe_pending_irrelevant_swaps(
        pending_irrelevant_swaps,
        recent_swap_signatures,
        recent_swap_signature_order,
        app_consumer_loop_telemetry,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
    );
    if dropped_noncritical_pending > 0 {
        info!(
            dropped_noncritical_pending_irrelevant_swaps =
                dropped_noncritical_pending,
            pending_irrelevant_swap_queue_depth_after_prune =
                pending_irrelevant_swaps.len(),
            "dropped stale non-target irrelevant swap backlog after refreshing the exact rebuild target-mint set"
        );
    }
    loop {
        let Some(mut pending) = pending_irrelevant_swaps.pop_front() else {
            break;
        };
        match enqueue_irrelevant_observed_swap(
            observed_swap_writer,
            recent_swap_signatures,
            recent_swap_signature_order,
            &pending.swap,
            pending.discovery_critical,
        )
        .await
        {
            Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued) => {
                info!(
                    signature = %pending.swap.signature,
                    writer_backpressure_elapsed_ms =
                        pending.backpressure_started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    pending_irrelevant_swap_queue_depth_after_dequeue =
                        pending_irrelevant_swaps.len(),
                    "observed swap writer backpressure cleared; queued pending irrelevant observed swap"
                );
                app_consumer_loop_telemetry
                    .note_processing_started_at(pending.processing_started_at);
            }
            Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure) => {
                let should_log = pending
                    .last_backpressure_log_at
                    .map(|last| last.elapsed() >= OBSERVED_SWAP_WRITER_BACKPRESSURE_LOG_THROTTLE)
                    .unwrap_or(true);
                if should_log {
                    let writer_snapshot = observed_swap_writer.snapshot();
                    let ingestion_snapshot = ingestion.runtime_snapshot();
                    warn!(
                        signature = %pending.swap.signature,
                        writer_backpressure_elapsed_ms =
                            pending.backpressure_started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        pending_irrelevant_swap_queue_depth =
                            pending_irrelevant_swaps.len().saturating_add(1),
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
                        "observed swap writer backpressure is delaying irrelevant observed swap persistence"
                    );
                    pending.last_backpressure_log_at = Some(StdInstant::now());
                }
                pending_irrelevant_swaps.push_front(pending);
                break;
            }
            Err(error) => {
                app_consumer_loop_telemetry
                    .note_processing_started_at(pending.processing_started_at);
                let error_chain = format_error_chain(&error);
                if observed_swap_writer_error_requires_restart(&error) {
                    return Err(error).context(
                        "observed swap writer is no longer running; restarting app to avoid silent stale ingestion",
                    );
                }
                warn!(
                    error = %error,
                    error_chain = %error_chain,
                    signature = %pending.swap.signature,
                    "failed retrying pending irrelevant observed swap"
                );
            }
        }
    }
    Ok(())
}
