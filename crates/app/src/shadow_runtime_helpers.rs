use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage::is_fatal_sqlite_anyhow_error;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::Path;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{info, warn};

use super::{FollowSnapshot, ShadowService, SqliteStore};
use crate::shadow_scheduler::{ShadowSwapSide, ShadowTaskInput, ShadowTaskOutput};
use crate::swap_classification::classify_swap_side;
use crate::telemetry::{reason_to_key, reason_to_stage};
use copybot_shadow::ShadowProcessOutcome;

pub(crate) fn apply_follow_snapshot_update(
    follow_snapshot: &mut FollowSnapshot,
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    retention: Duration,
) {
    let promoted_wallets: Vec<String> = active_wallets
        .difference(&follow_snapshot.active)
        .cloned()
        .collect();
    let demoted_wallets: Vec<String> = follow_snapshot
        .active
        .difference(&active_wallets)
        .cloned()
        .collect();

    for wallet in promoted_wallets {
        follow_snapshot.promoted_at.insert(wallet, cycle_ts);
    }
    for wallet in demoted_wallets {
        follow_snapshot.demoted_at.insert(wallet, cycle_ts);
    }
    follow_snapshot.active = active_wallets;

    let cutoff = cycle_ts - chrono::Duration::seconds(retention.as_secs() as i64);
    follow_snapshot.promoted_at.retain(|_, ts| *ts >= cutoff);
    follow_snapshot.demoted_at.retain(|_, ts| *ts >= cutoff);
}

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
    shadow_workers.spawn_blocking(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shadow_task(shadow, &sqlite_path, task_input)
        }));
        match result {
            Ok(output) => output,
            Err(payload) => ShadowTaskOutput {
                signature: fallback_signature,
                key: fallback_key,
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
    task_output: ShadowTaskOutput,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
) -> Result<()> {
    match task_output.outcome {
        Ok(ShadowProcessOutcome::Recorded(result)) => {
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
            let key = (result.wallet_id, result.token);
            if result.side == "buy" {
                open_shadow_lots.insert(key);
            } else if result.side == "sell" {
                if result.has_open_lots_after_signal.unwrap_or(false) {
                    open_shadow_lots.insert(key);
                } else {
                    open_shadow_lots.remove(&key);
                }
            }
            Ok(())
        }
        Ok(ShadowProcessOutcome::Dropped(reason)) => {
            let reason_key = reason_to_key(reason);
            let stage_key = reason_to_stage(reason);
            *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
            *shadow_drop_stage_counts.entry(stage_key).or_insert(0) += 1;
            Ok(())
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
            Ok(())
        }
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
    let outcome = (|| -> Result<ShadowProcessOutcome> {
        let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow worker task: {sqlite_path}")
        })?;
        shadow.process_swap(&store, &swap, follow_snapshot.as_ref(), Utc::now())
    })();
    ShadowTaskOutput {
        signature,
        key,
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use anyhow::anyhow;

    use super::*;
    use crate::shadow_scheduler::{ShadowTaskKey, ShadowTaskOutput};

    fn test_task_output(error: anyhow::Error) -> ShadowTaskOutput {
        ShadowTaskOutput {
            signature: "sig-shadow-test".to_string(),
            key: ShadowTaskKey {
                wallet: "wallet-a".to_string(),
                token: "token-a".to_string(),
            },
            outcome: Err(error),
        }
    }

    #[test]
    fn handle_shadow_task_output_returns_error_on_fatal_sqlite_io() {
        let mut open_shadow_lots = HashSet::new();
        let mut shadow_drop_reason_counts = BTreeMap::new();
        let mut shadow_drop_stage_counts = BTreeMap::new();
        let task_output = test_task_output(anyhow!(
            "disk I/O error: Error code 4874: I/O error within the xShmMap method"
        ));

        let error = handle_shadow_task_output(
            task_output,
            &mut open_shadow_lots,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
        )
        .expect_err("fatal sqlite I/O must bubble out of shadow task output handler");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("shadow processing failed with fatal sqlite I/O"),
            "expected fatal shadow-processing context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(open_shadow_lots.is_empty());
        assert!(shadow_drop_reason_counts.is_empty());
        assert!(shadow_drop_stage_counts.is_empty());
    }

    #[test]
    fn handle_shadow_task_output_warns_and_continues_on_busy_lock() -> Result<()> {
        let mut open_shadow_lots = HashSet::new();
        let mut shadow_drop_reason_counts = BTreeMap::new();
        let mut shadow_drop_stage_counts = BTreeMap::new();
        let task_output = test_task_output(anyhow!("database is locked"));

        handle_shadow_task_output(
            task_output,
            &mut open_shadow_lots,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
        )?;

        assert!(open_shadow_lots.is_empty());
        assert!(shadow_drop_reason_counts.is_empty());
        assert!(shadow_drop_stage_counts.is_empty());
        Ok(())
    }
}
