use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_discovery::DiscoveryService;
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage::SqliteStore;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const DISCOVERY_PUBLICATION_TRUTH_REPAIR_TIME_BUDGET_MS: u64 = 10_000;

pub(crate) fn spawn_discovery_task(
    sqlite_path: String,
    recent_raw_journal_path: String,
    recent_raw_replay_batch_size: usize,
    discovery: DiscoveryService,
    now: DateTime<Utc>,
) -> impl FnOnce() -> Result<super::DiscoveryTaskOutput> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for discovery task: {sqlite_path}")
        })?;
        match SqliteStore::open_read_only(Path::new(&recent_raw_journal_path)) {
            Ok(journal_store) => {
                let repair = discovery
                    .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                        &store,
                        &journal_store,
                        now,
                        recent_raw_replay_batch_size,
                        Instant::now()
                            + Duration::from_millis(
                                DISCOVERY_PUBLICATION_TRUTH_REPAIR_TIME_BUDGET_MS,
                            ),
                    )?;
                let required_window_start = repair.required_window_start.to_rfc3339();
                let journal_covered_since = repair.journal_covered_since.map(|ts| ts.to_rfc3339());
                let runtime_window_first_cursor_ts = repair
                    .runtime_window_first_cursor
                    .as_ref()
                    .map(|cursor| cursor.ts_utc.to_rfc3339());
                let runtime_window_first_cursor_slot = repair
                    .runtime_window_first_cursor
                    .as_ref()
                    .map(|cursor| cursor.slot);
                let runtime_window_first_cursor_signature = repair
                    .runtime_window_first_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str());
                let replay_until_cursor_ts = repair
                    .replay_until_cursor
                    .as_ref()
                    .map(|cursor| cursor.ts_utc.to_rfc3339());
                let replay_until_cursor_slot = repair
                    .replay_until_cursor
                    .as_ref()
                    .map(|cursor| cursor.slot);
                let replay_until_cursor_signature = repair
                    .replay_until_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str());
                let log_reason = repair.reason.as_deref().unwrap_or("none");
                if repair.runtime_window_complete_after {
                    info!(
                        repair_state = repair.state,
                        repair_reason = log_reason,
                        required_window_start = required_window_start.as_str(),
                        journal_path = recent_raw_journal_path.as_str(),
                        journal_covered_since = ?journal_covered_since,
                        journal_covers_runtime_cursor = repair.journal_covers_runtime_cursor,
                        publication_truth_complete_before =
                            repair.publication_truth_complete_before,
                        publication_truth_fresh_before = repair.publication_truth_fresh_before,
                        runtime_window_complete_before = repair.runtime_window_complete_before,
                        runtime_window_complete_after = repair.runtime_window_complete_after,
                        runtime_window_first_cursor_ts = ?runtime_window_first_cursor_ts,
                        runtime_window_first_cursor_slot = ?runtime_window_first_cursor_slot,
                        runtime_window_first_cursor_signature =
                            ?runtime_window_first_cursor_signature,
                        replay_until_cursor_ts = ?replay_until_cursor_ts,
                        replay_until_cursor_slot = ?replay_until_cursor_slot,
                        replay_until_cursor_signature = ?replay_until_cursor_signature,
                        replay_batches_completed = repair.replay_batches_completed,
                        replay_rows_loaded = repair.replay_rows_loaded,
                        replay_rows_inserted = repair.replay_rows_inserted,
                        replay_time_budget_exhausted = repair.replay_time_budget_exhausted,
                        "discovery publication truth repair from recent_raw journal completed"
                    );
                } else {
                    warn!(
                        repair_state = repair.state,
                        repair_reason = log_reason,
                        required_window_start = required_window_start.as_str(),
                        journal_path = recent_raw_journal_path.as_str(),
                        journal_covered_since = ?journal_covered_since,
                        journal_covers_runtime_cursor = repair.journal_covers_runtime_cursor,
                        publication_truth_complete_before =
                            repair.publication_truth_complete_before,
                        publication_truth_fresh_before = repair.publication_truth_fresh_before,
                        runtime_window_complete_before = repair.runtime_window_complete_before,
                        runtime_window_complete_after = repair.runtime_window_complete_after,
                        runtime_window_first_cursor_ts = ?runtime_window_first_cursor_ts,
                        runtime_window_first_cursor_slot = ?runtime_window_first_cursor_slot,
                        runtime_window_first_cursor_signature =
                            ?runtime_window_first_cursor_signature,
                        replay_until_cursor_ts = ?replay_until_cursor_ts,
                        replay_until_cursor_slot = ?replay_until_cursor_slot,
                        replay_until_cursor_signature = ?replay_until_cursor_signature,
                        replay_batches_completed = repair.replay_batches_completed,
                        replay_rows_loaded = repair.replay_rows_loaded,
                        replay_rows_inserted = repair.replay_rows_inserted,
                        replay_time_budget_exhausted = repair.replay_time_budget_exhausted,
                        "discovery publication truth repair from recent_raw journal completed"
                    );
                }
            }
            Err(error) => {
                warn!(
                    journal_path = recent_raw_journal_path.as_str(),
                    error = %error,
                    "discovery publication truth repair skipped because recent_raw journal could not be opened read-only"
                );
            }
        }
        let summary = discovery.run_cycle(&store, now)?;
        let active_wallets = store.list_active_follow_wallets()?;
        Ok(super::DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
            eligible_wallets: summary.eligible_wallets,
            active_follow_wallets: summary.active_follow_wallets,
            published: summary.published,
            runtime_mode: summary.runtime_mode,
            scoring_source: summary.scoring_source,
            raw_window_cap_truncated: summary.raw_window_cap_truncated,
            cap_truncation_deactivation_guard_active: summary
                .cap_truncation_deactivation_guard_active,
            cap_truncation_deactivation_guard_reason: summary
                .cap_truncation_deactivation_guard_reason,
            cap_truncation_deactivation_guard_started_at: summary
                .cap_truncation_deactivation_guard_started_at,
            cap_truncation_floor_ts_utc: summary.cap_truncation_floor_ts_utc,
            cap_truncation_floor_signature: summary.cap_truncation_floor_signature,
            persisted_stream_catch_up_requested: summary.persisted_stream_catch_up_requested,
        })
    }
}

pub(crate) fn spawn_shadow_snapshot_task(
    sqlite_path: String,
    shadow: ShadowService,
    now: DateTime<Utc>,
) -> impl FnOnce() -> Result<ShadowSnapshot> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow snapshot task: {sqlite_path}")
        })?;
        shadow.snapshot_24h(&store, now)
    }
}

pub(crate) fn spawn_execution_task(
    sqlite_path: String,
    execution_runtime: Arc<ExecutionRuntime>,
    now: DateTime<Utc>,
    buy_submit_pause_reason: Option<String>,
) -> impl FnOnce() -> Result<ExecutionBatchReport> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for execution task: {sqlite_path}")
        })?;
        execution_runtime.process_batch(&store, now, buy_submit_pause_reason.as_deref())
    }
}
