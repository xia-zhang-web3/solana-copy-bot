use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use super::*;

pub(super) async fn handle_relevant_observed_swap(
    store: &SqliteStore,
    observed_swap_writer: &ObservedSwapWriter,
    shadow: &ShadowService,
    sqlite_path: &str,
    swap: SwapEvent,
    side: ShadowSwapSide,
    now: DateTime<Utc>,
    follow_snapshot: &Arc<FollowSnapshot>,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    shadow_risk_guard: &mut ShadowRiskGuard,
    operator_emergency_stop: &OperatorEmergencyStop,
    pause_new_trades_on_outage: bool,
    shadow_scheduler: &mut ShadowScheduler,
    shadow_queue_full: bool,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
    recent_swap_signatures: &mut HashSet<String>,
    recent_swap_signature_order: &mut VecDeque<String>,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
    swap_processing_started_at: StdInstant,
) -> Result<()> {
    let relevant_persistence = match persist_relevant_observed_swap(
        observed_swap_writer,
        recent_swap_signatures,
        recent_swap_signature_order,
        &swap,
    )
    .await
    {
        Ok(inserted) => relevant_observed_swap_persistence(inserted),
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
                "failed persisting relevant observed swap"
            );
            return Ok(());
        }
    };
    if matches!(
        relevant_persistence,
        RelevantObservedSwapPersistence::Duplicate
    ) {
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        debug!(
            signature = %swap.signature,
            "duplicate swap ignored by observed swap store"
        );
        return Ok(());
    }

    if shadow_strategy_fail_closed {
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        let reason = "runtime_follow_universe_unavailable";
        *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
        *shadow_drop_stage_counts.entry("selection").or_insert(0) += 1;
        debug!(
            stage = "selection",
            reason,
            wallet = %swap.wallet,
            signature = %swap.signature,
            "shadow gate dropped"
        );
        return Ok(());
    }

    if matches!(side, ShadowSwapSide::Buy) {
        if operator_emergency_stop.is_active() {
            app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
            let reason_key = BuyRiskBlockReason::OperatorEmergencyStop.as_str();
            *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
            *shadow_drop_stage_counts.entry("risk").or_insert(0) += 1;
            debug!(
                stage = "risk",
                reason = reason_key,
                detail = %operator_emergency_stop.detail(),
                side = "buy",
                wallet = %swap.wallet,
                signature = %swap.signature,
                "shadow gate dropped"
            );
            return Ok(());
        }

        match shadow_risk_guard.can_open_buy(store, now, pause_new_trades_on_outage) {
            BuyRiskDecision::Allow => {}
            BuyRiskDecision::Blocked { reason, detail } => {
                app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
                let reason_key = reason.as_str();
                *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
                *shadow_drop_stage_counts.entry("risk").or_insert(0) += 1;
                debug!(
                    stage = "risk",
                    reason = reason_key,
                    detail = %detail,
                    side = "buy",
                    wallet = %swap.wallet,
                    signature = %swap.signature,
                    "shadow gate dropped"
                );
                return Ok(());
            }
        }
    }

    let task_key = shadow_task_key_for_swap(&swap, side);
    let task_input = ShadowTaskInput {
        swap,
        follow_snapshot: Arc::clone(&follow_snapshot),
        key: task_key,
    };
    if shadow_scheduler.should_hold_sell_for_causality(
        shadow_causal_holdback_enabled,
        shadow_causal_holdback_ms,
        side,
        &task_input.key,
        open_shadow_lots,
    ) {
        match shadow_scheduler.hold_sell_for_causality(
            SHADOW_PENDING_TASK_CAPACITY,
            task_input,
            shadow_causal_holdback_ms,
            Utc::now(),
        ) {
            Ok(()) => {}
            Err(overflow_task) => shadow_scheduler.handle_held_sell_overflow(
                overflow_task,
                SHADOW_PENDING_TASK_CAPACITY,
                shadow_causal_holdback_ms,
                Utc::now(),
                shadow_drop_reason_counts,
                shadow_drop_stage_counts,
                shadow_queue_full_outcome_counts,
            ),
        }
        app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
        return Ok(());
    }
    if shadow_scheduler.should_process_shadow_inline(
        shadow_queue_full,
        shadow_scheduler.shadow_scheduler_needs_reset,
        shadow_scheduler.shadow_workers.len(),
        &task_input.key,
    ) {
        if shadow_scheduler
            .inflight_shadow_keys
            .insert(task_input.key.clone())
        {
            spawn_shadow_worker_task(
                &mut shadow_scheduler.shadow_workers,
                shadow,
                sqlite_path,
                task_input,
            );
        } else if let Err(dropped_task) =
            shadow_scheduler.enqueue_shadow_task(SHADOW_PENDING_TASK_CAPACITY, task_input)
        {
            shadow_scheduler.handle_shadow_enqueue_overflow(
                side,
                dropped_task,
                SHADOW_PENDING_TASK_CAPACITY,
                shadow_drop_reason_counts,
                shadow_drop_stage_counts,
                shadow_queue_full_outcome_counts,
            );
        }
    } else if let Err(dropped_task) =
        shadow_scheduler.enqueue_shadow_task(SHADOW_PENDING_TASK_CAPACITY, task_input)
    {
        shadow_scheduler.handle_shadow_enqueue_overflow(
            side,
            dropped_task,
            SHADOW_PENDING_TASK_CAPACITY,
            shadow_drop_reason_counts,
            shadow_drop_stage_counts,
            shadow_queue_full_outcome_counts,
        );
    }
    app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
    Ok(())
}
