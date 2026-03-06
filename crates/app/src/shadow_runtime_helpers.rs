use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::Path;
use tokio::task::{self, JoinSet};
use tokio::time::Duration;
use tracing::{info, warn};

use super::{FollowSnapshot, ShadowService, SqliteStore, SwapEvent};
use crate::shadow_scheduler::{ShadowSwapSide, ShadowTaskInput, ShadowTaskOutput};
use crate::swap_classification::classify_swap_side;
use crate::telemetry::{reason_to_key, reason_to_stage};
use copybot_shadow::ShadowProcessOutcome;

pub(crate) async fn insert_observed_swap_with_retry(
    sqlite_path: &str,
    swap: &SwapEvent,
) -> Result<bool> {
    let sqlite_path = sqlite_path.to_string();
    let swap = swap.clone();
    task::spawn_blocking(move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for observed swap write: {sqlite_path}")
        })?;
        store.insert_observed_swap(&swap)
    })
    .await
    .context("observed swap write task failed")?
}

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
) {
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
        }
        Ok(ShadowProcessOutcome::Dropped(reason)) => {
            let reason_key = reason_to_key(reason);
            let stage_key = reason_to_stage(reason);
            *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
            *shadow_drop_stage_counts.entry(stage_key).or_insert(0) += 1;
        }
        Err(error) => {
            warn!(
                error = %error,
                signature = %task_output.signature,
                "shadow processing failed"
            );
        }
    }
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
    use super::insert_observed_swap_with_retry;
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::SqliteStore;
    use rusqlite::Connection;
    use std::path::Path;
    use std::thread;
    use std::time::Duration as StdDuration;
    use tokio::runtime::Builder;
    use tokio::time::{sleep, timeout, Duration};

    #[test]
    fn insert_observed_swap_with_retry_does_not_block_runtime_under_sqlite_lock() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-async-write-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-async".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async".to_string(),
            slot: 123,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        };

        let runtime_handle = thread::spawn(move || -> Result<bool> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let sqlite_path_for_task = sqlite_path.clone();
                let swap_for_task = swap.clone();
                let insert_task = tokio::spawn(async move {
                    insert_observed_swap_with_retry(&sqlite_path_for_task, &swap_for_task).await
                });

                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context(
                        "current-thread runtime stalled while observed swap write was blocked",
                    )?;

                insert_task
                    .await
                    .context("observed swap task join failed")?
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        blocker_conn.execute_batch("COMMIT")?;

        let inserted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap write should succeed after lock release")?;
        assert!(inserted, "observed swap insert should report a fresh write");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-async");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }
}
