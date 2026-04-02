use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_discovery::DiscoveryService;
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage::SqliteStore;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

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
        let publication_state_before = store.discovery_publication_state_read_only()?;
        let journal_store = match SqliteStore::open_read_only(Path::new(&recent_raw_journal_path)) {
            Ok(journal_store) => Some(journal_store),
            Err(error) => {
                warn!(
                    journal_path = recent_raw_journal_path.as_str(),
                    error = %error,
                    "discovery publication truth repair could not open recent_raw journal read-only; proceeding with journal-independent repair branches"
                );
                None
            }
        };
        let repair_time_budget =
            discovery.recommended_publication_truth_repair_time_budget(&store, now)?;
        let repair_requested_time_budget_ms =
            repair_time_budget.as_millis().min(u64::MAX as u128) as u64;
        let repair = discovery
            .repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
                &store,
                journal_store.as_ref(),
                now,
                recent_raw_replay_batch_size,
                Instant::now() + repair_time_budget,
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
        let repair_effective_time_budget_ms = repair
            .publication_truth_refresh_effective_time_budget_ms
            .unwrap_or(repair_requested_time_budget_ms);
        let repair_reached_publish_ready = !repair
            .publication_truth_refresh_delegated_to_runtime_cycle
            && repair.runtime_window_complete_after
            && (!repair.publication_truth_refresh_attempted
                || repair.publication_truth_refresh_completed);
        if repair_reached_publish_ready {
            info!(
                repair_state = repair.state,
                repair_reason = log_reason,
                repair_requested_time_budget_ms,
                repair_effective_time_budget_ms,
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
                publication_truth_refresh_attempted =
                    repair.publication_truth_refresh_attempted,
                publication_truth_refresh_completed =
                    repair.publication_truth_refresh_completed,
                publication_truth_refresh_phase = ?repair
                    .publication_truth_refresh_phase,
                publication_truth_refresh_replay_subphase = ?repair
                    .publication_truth_refresh_replay_subphase,
                publication_truth_refresh_replay_wallet_stats_complete =
                    repair.publication_truth_refresh_replay_wallet_stats_complete,
                publication_truth_refresh_replay_wallet_stats_wallet_cursor = repair
                    .publication_truth_refresh_replay_wallet_stats_wallet_cursor
                    .as_deref(),
                publication_truth_refresh_delegated_to_runtime_cycle =
                    repair.publication_truth_refresh_delegated_to_runtime_cycle,
                publication_truth_refresh_priority_recovery_contract_reason =
                    repair.publication_truth_refresh_priority_recovery_contract_reason,
                publication_truth_refresh_publishable_checkpoint_blocker = repair
                    .publication_truth_refresh_publishable_checkpoint_blocker,
                publication_truth_refresh_replay_wallet_stats_phase_page_limit = repair
                    .publication_truth_refresh_replay_wallet_stats_phase_page_limit,
                publication_truth_refresh_collect_buy_mints_phase_page_limit = repair
                    .publication_truth_refresh_collect_buy_mints_phase_page_limit,
                publication_truth_refresh_observed_swaps_loaded =
                    repair.publication_truth_refresh_observed_swaps_loaded,
                publication_truth_refresh_wallets_buffered =
                    repair.publication_truth_refresh_wallets_buffered,
                publication_truth_refresh_cycle_rows_processed =
                    repair.publication_truth_refresh_cycle_rows_processed,
                publication_truth_refresh_cycle_pages_processed =
                    repair.publication_truth_refresh_cycle_pages_processed,
                publication_truth_refresh_budget_exhausted_reason = ?repair
                    .publication_truth_refresh_budget_exhausted_reason,
                "discovery publication truth repair from recent_raw journal completed"
            );
        } else {
            warn!(
                repair_state = repair.state,
                repair_reason = log_reason,
                repair_requested_time_budget_ms,
                repair_effective_time_budget_ms,
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
                publication_truth_refresh_attempted =
                    repair.publication_truth_refresh_attempted,
                publication_truth_refresh_completed =
                    repair.publication_truth_refresh_completed,
                publication_truth_refresh_phase = ?repair
                    .publication_truth_refresh_phase,
                publication_truth_refresh_replay_subphase = ?repair
                    .publication_truth_refresh_replay_subphase,
                publication_truth_refresh_replay_wallet_stats_complete =
                    repair.publication_truth_refresh_replay_wallet_stats_complete,
                publication_truth_refresh_replay_wallet_stats_wallet_cursor = repair
                    .publication_truth_refresh_replay_wallet_stats_wallet_cursor
                    .as_deref(),
                publication_truth_refresh_delegated_to_runtime_cycle =
                    repair.publication_truth_refresh_delegated_to_runtime_cycle,
                publication_truth_refresh_priority_recovery_contract_reason =
                    repair.publication_truth_refresh_priority_recovery_contract_reason,
                publication_truth_refresh_publishable_checkpoint_blocker = repair
                    .publication_truth_refresh_publishable_checkpoint_blocker,
                publication_truth_refresh_replay_wallet_stats_phase_page_limit = repair
                    .publication_truth_refresh_replay_wallet_stats_phase_page_limit,
                publication_truth_refresh_collect_buy_mints_phase_page_limit = repair
                    .publication_truth_refresh_collect_buy_mints_phase_page_limit,
                publication_truth_refresh_observed_swaps_loaded =
                    repair.publication_truth_refresh_observed_swaps_loaded,
                publication_truth_refresh_wallets_buffered =
                    repair.publication_truth_refresh_wallets_buffered,
                publication_truth_refresh_cycle_rows_processed =
                    repair.publication_truth_refresh_cycle_rows_processed,
                publication_truth_refresh_cycle_pages_processed =
                    repair.publication_truth_refresh_cycle_pages_processed,
                publication_truth_refresh_budget_exhausted_reason = ?repair
                    .publication_truth_refresh_budget_exhausted_reason,
                "discovery publication truth repair from recent_raw journal completed"
            );
        }
        let summary = discovery.run_cycle(&store, now)?;
        let publication_state_after = store.discovery_publication_state_read_only()?;
        let publication_state_updated_at_before = publication_state_before
            .as_ref()
            .map(|state| state.updated_at.to_rfc3339());
        let publication_state_updated_at_after = publication_state_after
            .as_ref()
            .map(|state| state.updated_at.to_rfc3339());
        let publication_state_refreshed = publication_state_before
            .as_ref()
            .map(|state| state.updated_at)
            != publication_state_after
                .as_ref()
                .map(|state| state.updated_at);
        info!(
            publication_state_refreshed,
            publication_state_updated_at_before = ?publication_state_updated_at_before,
            publication_state_updated_at_after = ?publication_state_updated_at_after,
            publication_last_published_at_before = ?publication_state_before
                .as_ref()
                .and_then(|state| state.last_published_at)
                .map(|ts| ts.to_rfc3339()),
            publication_last_published_at_after = ?publication_state_after
                .as_ref()
                .and_then(|state| state.last_published_at)
                .map(|ts| ts.to_rfc3339()),
            publication_published_wallet_count_before = publication_state_before
                .as_ref()
                .and_then(|state| state.published_wallet_ids.as_ref())
                .map(|wallets| wallets.len()),
            publication_published_wallet_count_after = publication_state_after
                .as_ref()
                .and_then(|state| state.published_wallet_ids.as_ref())
                .map(|wallets| wallets.len()),
            discovery_runtime_mode_after = summary.runtime_mode.as_str(),
            discovery_published_after = summary.published,
            "discovery publication state after runtime cycle"
        );
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
            persisted_stream_catch_up_pressure_override_requested: summary
                .persisted_stream_catch_up_pressure_override_requested,
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
