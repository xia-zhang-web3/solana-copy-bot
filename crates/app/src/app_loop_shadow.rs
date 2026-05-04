use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashSet};

use super::*;
use crate::shadow_scheduler::ShadowTaskOutput;

pub(super) fn prepare_shadow_scheduler_before_select(
    store: &SqliteStore,
    sqlite_path: &str,
    shadow: &ShadowService,
    shadow_scheduler: &mut ShadowScheduler,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<bool> {
    if shadow_scheduler.shadow_scheduler_needs_reset {
        if shadow_scheduler.shadow_workers.is_empty() {
            shadow_scheduler.inflight_shadow_keys.clear();
            shadow_scheduler.rebuild_ready_queue();
            shadow_scheduler.shadow_scheduler_needs_reset = false;
            warn!("shadow scheduler recovered after worker join error");
        }
    } else {
        shadow_scheduler.spawn_shadow_tasks_up_to_limit(
            sqlite_path,
            shadow,
            SHADOW_WORKER_POOL_SIZE,
        );
    }
    shadow_scheduler.release_held_shadow_sells(
        open_shadow_lots,
        shadow_drop_reason_counts,
        shadow_drop_stage_counts,
        shadow_queue_full_outcome_counts,
        SHADOW_PENDING_TASK_CAPACITY,
        Utc::now(),
    );

    let shadow_buffered_task_count = shadow_scheduler.buffered_shadow_task_count();
    let shadow_queue_full = shadow_buffered_task_count >= SHADOW_PENDING_TASK_CAPACITY;
    if shadow_queue_full && !shadow_scheduler.shadow_queue_backpressure_active {
        shadow_scheduler.shadow_queue_backpressure_active = true;
        warn!(
            shadow_scheduler.pending_shadow_task_count,
            shadow_held_task_count = shadow_scheduler.held_shadow_sell_count(),
            shadow_buffered_task_count,
            shadow_pending_capacity = SHADOW_PENDING_TASK_CAPACITY,
            "shadow queue backpressure active; switching to inline shadow processing"
        );
        record_shadow_queue_backpressure_risk_event(
            store,
            shadow_scheduler.pending_shadow_task_count,
            shadow_scheduler.held_shadow_sell_count(),
            shadow_buffered_task_count,
            SHADOW_PENDING_TASK_CAPACITY,
            Utc::now(),
        )?;
    } else if !shadow_queue_full && shadow_scheduler.shadow_queue_backpressure_active {
        shadow_scheduler.shadow_queue_backpressure_active = false;
        info!(
            shadow_scheduler.pending_shadow_task_count,
            shadow_held_task_count = shadow_scheduler.held_shadow_sell_count(),
            shadow_buffered_task_count,
            shadow_pending_capacity = SHADOW_PENDING_TASK_CAPACITY,
            "shadow queue backpressure cleared"
        );
    }

    Ok(shadow_queue_full)
}

pub(super) fn handle_shadow_worker_join(
    shadow_result: Option<std::result::Result<ShadowTaskOutput, tokio::task::JoinError>>,
    shadow_scheduler: &mut ShadowScheduler,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<()> {
    match shadow_result {
        Some(Ok(task_output)) => {
            shadow_scheduler.mark_task_complete(&task_output.key);
            handle_shadow_task_output(
                task_output,
                open_shadow_lots,
                shadow_drop_reason_counts,
                shadow_drop_stage_counts,
            )?;
        }
        Some(Err(error)) => {
            warn!(error = %error, "shadow task join failed");
            shadow_scheduler.shadow_scheduler_needs_reset = true;
        }
        None => {}
    }
    Ok(())
}

pub(super) fn handle_shadow_snapshot_join(
    store: &SqliteStore,
    snapshot_result: Option<
        std::result::Result<Result<copybot_shadow::ShadowSnapshot>, tokio::task::JoinError>,
    >,
    shadow_scheduler: &mut ShadowScheduler,
    shadow_strategy_fail_closed: bool,
    open_shadow_lots: &mut HashSet<(String, String)>,
    follow_snapshot: &FollowSnapshot,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<()> {
    shadow_scheduler.shadow_snapshot_handle = None;
    match snapshot_result.expect("guard ensures shadow snapshot task exists") {
        Ok(Ok(snapshot)) => {
            if !shadow_strategy_fail_closed {
                refresh_shadow_open_lot_index_or_warn(store, open_shadow_lots)?;
            }
            info!(
                closed_trades_24h = snapshot.closed_trades_24h,
                realized_pnl_sol_24h = snapshot.realized_pnl_sol_24h,
                open_lots = snapshot.open_lots,
                active_follow_wallets = follow_snapshot.active.len(),
                "shadow snapshot"
            );
            if !shadow_drop_reason_counts.is_empty() {
                info!(
                    drop_counts = ?shadow_drop_reason_counts,
                    drop_stage_counts = ?shadow_drop_stage_counts,
                    "shadow drop reasons"
                );
                shadow_drop_reason_counts.clear();
                shadow_drop_stage_counts.clear();
            }
            if !shadow_queue_full_outcome_counts.is_empty() {
                info!(
                    queue_full_outcomes = ?shadow_queue_full_outcome_counts,
                    "shadow queue_full outcomes"
                );
                shadow_queue_full_outcome_counts.clear();
            }
            if !shadow_scheduler.shadow_holdback_counts.is_empty() {
                info!(
                    holdback_outcomes = ?shadow_scheduler.shadow_holdback_counts,
                    "shadow causal holdback outcomes"
                );
                shadow_scheduler.shadow_holdback_counts.clear();
            }
        }
        Ok(Err(error)) => {
            if shadow_snapshot_error_requires_restart(&error) {
                return Err(error).context("shadow snapshot failed with fatal sqlite I/O");
            }
            warn!(error = %error, "shadow snapshot failed");
        }
        Err(error) => {
            warn!(error = %error, "shadow snapshot task join failed");
        }
    }
    Ok(())
}

pub(super) fn handle_shadow_interval_tick(
    store: &SqliteStore,
    sqlite_path: &str,
    shadow: &ShadowService,
    shadow_scheduler: &mut ShadowScheduler,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    stale_lot_max_hold_hours: u32,
    stale_lot_terminal_zero_price_hours: u32,
    stale_lot_recovery_zero_price_enabled: bool,
) -> Result<()> {
    if shadow_strategy_fail_closed {
        return Ok(());
    }
    let cleanup_now = Utc::now();
    match close_stale_shadow_lots(
        store,
        open_shadow_lots,
        stale_lot_max_hold_hours,
        stale_lot_terminal_zero_price_hours,
        stale_lot_recovery_zero_price_enabled,
        cleanup_now,
    ) {
        Ok(stats)
            if stats.closed_priced > 0
                || stats.recovery_zero_closed > 0
                || stats.terminal_zero_closed > 0
                || stats.skipped_unpriced > 0 =>
        {
            info!(
                closed_priced = stats.closed_priced,
                recovery_zero_closed = stats.recovery_zero_closed,
                terminal_zero_closed = stats.terminal_zero_closed,
                skipped_unpriced = stats.skipped_unpriced,
                max_hold_hours = stale_lot_max_hold_hours,
                terminal_zero_price_hours = stale_lot_terminal_zero_price_hours,
                recovery_zero_price_enabled = stale_lot_recovery_zero_price_enabled,
                "stale lot cleanup tick"
            );
        }
        Ok(_) => {}
        Err(error) => {
            if stale_lot_cleanup_error_requires_restart(&error) {
                return Err(error).context("stale lot cleanup failed with fatal sqlite I/O");
            }
            warn!(error = %error, "stale lot cleanup failed");
        }
    }
    if shadow_scheduler.shadow_snapshot_handle.is_some() {
        warn!("shadow snapshot still running, skipping scheduled trigger");
        return Ok(());
    }
    shadow_scheduler.shadow_snapshot_handle = Some(tokio::task::spawn_blocking(
        spawn_shadow_snapshot_task(sqlite_path.to_string(), shadow.clone(), Utc::now()),
    ));
    Ok(())
}
