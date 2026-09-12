use anyhow::{Context, Result};
use chrono::Utc;
use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error,
    EXECUTION_QUOTE_CANARY_SHADOW_GATE_DROPPED, EXECUTION_QUOTE_CANARY_SHADOW_GATE_RECORDED,
};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::Path;
use std::time::Duration as StdDuration;
use tokio::task::JoinSet;
use tracing::{info, warn};

use super::{ShadowService, SqliteStore};
use crate::shadow_scheduler::{ShadowSwapSide, ShadowTaskInput, ShadowTaskOutput};
use crate::swap_classification::classify_swap_side;
use crate::telemetry::{reason_to_key, reason_to_stage};
use copybot_shadow::{ShadowDropReason, ShadowProcessOutcome, ShadowSignalResult};

#[path = "owned_sell_intent.rs"]
mod owned_sell_intent;

const SHADOW_TASK_SQLITE_BUSY_TIMEOUT_SECS: u64 = 15;
const SHADOW_TASK_RETRY_BACKOFF_MS: [u64; 4] = [100, 250, 500, 1_000];

pub(crate) fn spawn_shadow_worker_task(
    shadow_workers: &mut JoinSet<ShadowTaskOutput>,
    shadow: &ShadowService,
    sqlite_path: &str,
    task_input: ShadowTaskInput,
) {
    let shadow = shadow.clone();
    let sqlite_path = sqlite_path.to_string();
    let fallback_signature = task_input.swap.signature.clone();
    let fallback_key = task_input.key.clone();
    let fallback_side = classify_swap_side(&task_input.swap);
    let fallback_signal_id =
        fallback_side.map(|side| shadow_signal_id(&fallback_signature, &fallback_key, side));
    shadow_workers.spawn_blocking(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shadow_task(shadow, &sqlite_path, task_input)
        }));
        match result {
            Ok(output) => output,
            Err(payload) => ShadowTaskOutput {
                signature: fallback_signature,
                key: fallback_key,
                signal_id: fallback_signal_id,
                side: fallback_side,
                buy_receipt: None,
                owned_sell_reject: None,
                outcome: Err(anyhow::anyhow!(
                    "shadow worker task panicked: {}",
                    panic_payload_to_string(payload.as_ref())
                )),
            },
        }
    });
}

pub(crate) fn find_last_pending_buy_index(queue: &VecDeque<ShadowTaskInput>) -> Option<usize> {
    (0..queue.len()).rev().find(|index| {
        queue
            .get(*index)
            .and_then(|task| classify_swap_side(&task.swap))
            .is_some_and(|side| matches!(side, ShadowSwapSide::Buy))
    })
}

pub(crate) fn handle_shadow_task_output(
    store: Option<&SqliteStore>,
    task_output: ShadowTaskOutput,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<Option<ShadowSignalResult>> {
    match task_output.outcome {
        Ok(ShadowProcessOutcome::Recorded(result)) => {
            // The shared delivery shape does not turn an execution intent into a shadow fill.
            if let Some(store) = store {
                if store
                    .load_copy_signal_by_signal_id(&result.signal_id)?
                    .is_some_and(|s| s.status == copybot_storage_core::EXECUTION_SELL_INTENT_STATUS)
                {
                    return Ok(Some(result));
                }
            }
            record_shadow_gate_outcome_for_recorded(store, &result);
            info!(
                signal_id = %result.signal_id,
                wallet = %result.wallet_id,
                side = %result.side,
                token = %result.token,
                notional_sol = result.notional_sol,
                latency_ms = result.latency_ms,
                closed_qty = result.closed_qty,
                realized_pnl_sol = result.realized_pnl_sol,
                "shadow signal recorded"
            );
            let key = (result.wallet_id.clone(), result.token.clone());
            if result.side == "buy" {
                open_shadow_lots.insert(key);
            } else if result.side == "sell" {
                if result.has_open_lots_after_signal.unwrap_or(false) {
                    open_shadow_lots.insert(key);
                } else {
                    open_shadow_lots.remove(&key);
                }
            }
            Ok(Some(result))
        }
        Ok(ShadowProcessOutcome::Dropped(reason)) => {
            let reason_key = task_output
                .owned_sell_reject
                .map(|reject| reject.as_str())
                .unwrap_or_else(|| reason_to_key(reason));
            record_shadow_gate_outcome_for_drop(store, &task_output, reason_key);
            let stage_key = if task_output.owned_sell_reject.is_some() {
                "owned_sell_intent"
            } else {
                reason_to_stage(reason)
            };
            *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
            *shadow_drop_stage_counts.entry(stage_key).or_insert(0) += 1;
            Ok(None)
        }
        Err(error) => {
            if shadow_task_error_requires_restart(&error) {
                return Err(error).context("shadow processing failed with fatal sqlite I/O");
            }
            warn!(
                error = %error,
                signature = %task_output.signature,
                "shadow processing failed"
            );
            Ok(None)
        }
    }
}

fn record_shadow_gate_outcome_for_recorded(
    store: Option<&SqliteStore>,
    result: &ShadowSignalResult,
) {
    let Some(store) = store else {
        return;
    };
    if let Err(error) = store.record_execution_quote_canary_shadow_gate_event(
        &result.signal_id,
        &result.wallet_id,
        &result.token,
        &result.side,
        EXECUTION_QUOTE_CANARY_SHADOW_GATE_RECORDED,
        None,
        Utc::now(),
    ) {
        warn!(
            error = %error,
            signal_id = %result.signal_id,
            "failed recording execution quote canary shadow gate outcome"
        );
    }
}

fn record_shadow_gate_outcome_for_drop(
    store: Option<&SqliteStore>,
    task_output: &ShadowTaskOutput,
    reason: &'static str,
) {
    let Some(store) = store else {
        return;
    };
    let (Some(signal_id), Some(side)) = (&task_output.signal_id, task_output.side) else {
        return;
    };
    let side = shadow_side_label(side);
    if let Err(error) = store.record_execution_quote_canary_shadow_gate_event(
        signal_id,
        &task_output.key.wallet,
        &task_output.key.token,
        side,
        EXECUTION_QUOTE_CANARY_SHADOW_GATE_DROPPED,
        Some(reason),
        Utc::now(),
    ) {
        warn!(
            error = %error,
            signal_id,
            reason,
            "failed recording execution quote canary shadow gate outcome"
        );
    }
}

fn shadow_task_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    "unknown panic payload".to_string()
}

fn shadow_task(
    shadow: ShadowService,
    sqlite_path: &str,
    task_input: ShadowTaskInput,
) -> ShadowTaskOutput {
    let ShadowTaskInput {
        swap,
        follow_snapshot,
        key,
    } = task_input;
    let signature = swap.signature.clone();
    let side = classify_swap_side(&swap);
    let signal_id = side.map(|value| shadow_signal_id(&signature, &key, value));
    let processing_now = Utc::now();
    #[cfg(test)]
    crate::app_tests::b70_hooks::before_shadow(&signature);
    let mut owned_sell_reject = None;
    let mut buy_receipt = None;
    let outcome = retry_shadow_task_on_sqlite_contention(&signature, || -> Result<_> {
        let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow worker task: {sqlite_path}")
        })?;
        store.set_busy_timeout(StdDuration::from_secs(SHADOW_TASK_SQLITE_BUSY_TIMEOUT_SECS))?;
        if classify_swap_side(&swap).is_some_and(|side| matches!(side, ShadowSwapSide::Buy))
            && !store
                .was_wallet_followed_at(&swap.wallet, swap.ts_utc)
                .with_context(|| {
                    format!(
                        "failed checking queued buy temporal followlist membership for wallet {}",
                        swap.wallet
                    )
                })?
        {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotFollowed));
        }
        // Capture the actual insert identity, never infer a lot from matching data.
        buy_receipt = None;
        if matches!(side, Some(ShadowSwapSide::Buy)) {
            let (outcome, receipt) = shadow.process_swap_with_buy_receipt(
                &store,
                &swap,
                follow_snapshot.as_ref(),
                processing_now,
            )?;
            buy_receipt = receipt;
            return Ok(outcome);
        }
        owned_sell_intent::process_swap(
            &shadow,
            &store,
            &swap,
            follow_snapshot.as_ref(),
            processing_now,
            &mut owned_sell_reject,
        )
    });
    ShadowTaskOutput {
        signature,
        key,
        signal_id,
        side,
        buy_receipt,
        owned_sell_reject,
        outcome,
    }
}

fn shadow_signal_id(
    signature: &str,
    key: &crate::shadow_scheduler::ShadowTaskKey,
    side: ShadowSwapSide,
) -> String {
    format!(
        "shadow:{}:{}:{}:{}",
        signature,
        key.wallet,
        shadow_side_label(side),
        key.token
    )
}

fn shadow_side_label(side: ShadowSwapSide) -> &'static str {
    match side {
        ShadowSwapSide::Buy => "buy",
        ShadowSwapSide::Sell => "sell",
    }
}

fn retry_shadow_task_on_sqlite_contention<T, F>(signature: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    for attempt in 0..=SHADOW_TASK_RETRY_BACKOFF_MS.len() {
        match operation() {
            Ok(result) => return Ok(result),
            Err(error) => {
                if !is_retryable_sqlite_anyhow_error(&error) {
                    return Err(error);
                }
                let Some(backoff_ms) = SHADOW_TASK_RETRY_BACKOFF_MS.get(attempt) else {
                    return Err(error);
                };
                warn!(
                    error = %error,
                    signature,
                    attempt = attempt + 1,
                    retry_backoff_ms = *backoff_ms,
                    "shadow processing retry after sqlite contention"
                );
                std::thread::sleep(StdDuration::from_millis(*backoff_ms));
            }
        }
    }
    unreachable!("shadow retry loop must return on success or terminal error");
}

#[cfg(test)]
#[path = "shadow_runtime_helpers/tests.rs"]
mod tests;
