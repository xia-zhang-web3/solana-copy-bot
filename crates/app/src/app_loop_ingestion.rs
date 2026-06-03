use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::time::{self, Duration};
use tracing::{debug, info, warn};

use super::*;
use crate::app_loop_irrelevant_swap::handle_irrelevant_observed_swap;
use crate::app_loop_relevant_swap::handle_relevant_observed_swap;

pub(super) async fn handle_ingestion_swap_poll(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    execution_canary_runner: &ExecutionCanaryRunner,
    shadow: &ShadowService,
    sqlite_path: &str,
    maybe_swap: Result<Option<SwapEvent>>,
    ingestion_snapshot: Option<IngestionRuntimeSnapshot>,
    follow_snapshot: &Arc<FollowSnapshot>,
    shadow_scheduler: &mut ShadowScheduler,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    shadow_risk_guard: &mut ShadowRiskGuard,
    operator_emergency_stop: &OperatorEmergencyStop,
    pause_new_trades_on_outage: bool,
    shadow_queue_full: bool,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
    discovery_critical_target_buy_mints_backpressure_refresh_state: &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    zero_universe_empty_target_noncritical_best_effort: &mut ZeroUniverseEmptyTargetNoncriticalBestEffortState,
    pending_irrelevant_swaps: &mut VecDeque<PendingIrrelevantObservedSwap>,
    recent_swap_signatures: &mut HashSet<String>,
    recent_swap_signature_order: &mut VecDeque<String>,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
    ingestion_error_streak: &mut u32,
    ingestion_backoff_until: &mut Option<time::Instant>,
) -> Result<()> {
    let now = Utc::now();
    reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
        zero_universe_empty_target_noncritical_best_effort,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
    );
    shadow_risk_guard
        .observe_ingestion_snapshot(store, now, ingestion_snapshot)
        .context("shadow risk ingestion snapshot infra event failed with fatal sqlite I/O")?;
    let swap = match maybe_swap {
        Ok(Some(swap)) => {
            if *ingestion_error_streak > 0 {
                info!(
                    consecutive_errors = *ingestion_error_streak,
                    "ingestion stream recovered"
                );
            }
            *ingestion_error_streak = 0;
            swap
        }
        Ok(None) => {
            *ingestion_error_streak = 0;
            debug!("ingestion emitted no swap");
            return Ok(());
        }
        Err(error) => {
            *ingestion_error_streak = ingestion_error_streak.saturating_add(1);
            let backoff_ms = ingestion_error_backoff_ms(*ingestion_error_streak);
            *ingestion_backoff_until =
                Some(time::Instant::now() + Duration::from_millis(backoff_ms));
            warn!(
                error = %error,
                consecutive_errors = *ingestion_error_streak,
                backoff_ms,
                "ingestion error; applying backoff before next poll"
            );
            return Ok(());
        }
    };
    let swap_processing_started_at = StdInstant::now();
    app_consumer_loop_telemetry.note_swap_seen();

    if let Err(error) = observed_swap_writer.ensure_running() {
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        return Err(error).context(
            "observed swap writer is no longer running; restarting app to avoid silent stale ingestion",
        );
    }

    if !note_recent_swap_signature(
        recent_swap_signatures,
        recent_swap_signature_order,
        &swap.signature,
    ) {
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        debug!(signature = %swap.signature, "duplicate swap ignored by recent signature dedupe");
        return Ok(());
    }

    let side = match classify_observed_swap_shadow_relevance(
        &swap,
        follow_snapshot,
        shadow_scheduler,
        open_shadow_lots,
    ) {
        ObservedSwapShadowRelevance::IrrelevantUnclassified => {
            handle_irrelevant_observed_swap(
                store,
                observed_swap_writer,
                swap,
                IrrelevantObservedSwapBackpressureSourceBranch::Unclassified,
                ingestion_snapshot,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
                discovery_critical_target_buy_mints_backpressure_refresh_state,
                zero_universe_empty_target_noncritical_best_effort,
                pending_irrelevant_swaps,
                recent_swap_signatures,
                recent_swap_signature_order,
                app_consumer_loop_telemetry,
                swap_processing_started_at,
            )
            .await?;
            return Ok(());
        }
        ObservedSwapShadowRelevance::IrrelevantNotFollowed(side) => {
            app_consumer_loop_telemetry.note_follow_rejected();
            let reason = "not_followed";
            *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
            *shadow_drop_stage_counts.entry("follow").or_insert(0) += 1;
            debug!(
                stage = "follow",
                reason,
                side = if matches!(side, ShadowSwapSide::Buy) {
                    "buy"
                } else {
                    "sell"
                },
                wallet = %swap.wallet,
                signature = %swap.signature,
                "shadow gate dropped"
            );
            handle_irrelevant_observed_swap(
                store,
                observed_swap_writer,
                swap,
                IrrelevantObservedSwapBackpressureSourceBranch::NotFollowed,
                ingestion_snapshot,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
                discovery_critical_target_buy_mints_backpressure_refresh_state,
                zero_universe_empty_target_noncritical_best_effort,
                pending_irrelevant_swaps,
                recent_swap_signatures,
                recent_swap_signature_order,
                app_consumer_loop_telemetry,
                swap_processing_started_at,
            )
            .await?;
            return Ok(());
        }
        ObservedSwapShadowRelevance::Relevant(side) => side,
    };

    handle_relevant_observed_swap(
        store,
        observed_swap_writer,
        execution_canary_runner,
        shadow,
        sqlite_path,
        swap,
        side,
        now,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        shadow_risk_guard,
        operator_emergency_stop,
        pause_new_trades_on_outage,
        shadow_scheduler,
        shadow_queue_full,
        shadow_causal_holdback_enabled,
        shadow_causal_holdback_ms,
        shadow_drop_reason_counts,
        shadow_drop_stage_counts,
        shadow_queue_full_outcome_counts,
        recent_swap_signatures,
        recent_swap_signature_order,
        app_consumer_loop_telemetry,
        swap_processing_started_at,
    )
    .await
}
