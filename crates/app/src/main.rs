use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{
    load_from_env_or_default, normalize_ingestion_source, ExecutionConfig, RiskConfig, ShadowConfig,
};
#[cfg(test)]
use copybot_core_types::TokenQuantity;
use copybot_core_types::{Lamports, SignedLamports, SwapEvent};
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{FollowSnapshot, ShadowService};
use copybot_storage::{
    sqlite_contention_snapshot as legacy_sqlite_contention_snapshot, SqliteStore,
};
use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, run_observed_startup_step,
    sqlite_contention_snapshot as core_sqlite_contention_snapshot, SqliteContentionSnapshot,
    StartupStepOutcome,
};
#[cfg(test)]
use copybot_storage_core::{
    StartupStepProgressReporter, StartupStepRuntimePolicy, StartupStepTimeoutBehavior,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::env;
#[cfg(test)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

fn sqlite_contention_snapshot() -> SqliteContentionSnapshot {
    let core = core_sqlite_contention_snapshot();
    let legacy = legacy_sqlite_contention_snapshot();
    SqliteContentionSnapshot {
        write_retry_total: core
            .write_retry_total
            .saturating_add(legacy.write_retry_total),
        busy_error_total: core
            .busy_error_total
            .saturating_add(legacy.busy_error_total),
    }
}

mod alerts;
mod app_loop_ingestion;
mod app_loop_irrelevant_backlog;
mod app_loop_irrelevant_swap;
mod app_loop_maintenance;
mod app_loop_relevant_swap;
mod app_loop_shadow;
mod app_loop_shutdown;
mod config_contract;
mod discovery_runtime;
mod history_retention;
mod observed_swap_writer;
mod shadow_runtime_helpers;
mod shadow_scheduler;
mod stale_close;
mod startup;
mod swap_classification;
mod task_spawns;
mod telemetry;

use crate::alerts::AlertDispatcher;
use crate::app_loop_ingestion::handle_ingestion_swap_poll;
use crate::app_loop_irrelevant_backlog::retry_pending_irrelevant_swaps;
use crate::app_loop_maintenance::{
    handle_app_heartbeat_tick, handle_observed_swap_retention_join, handle_risk_refresh_tick,
};
use crate::app_loop_shadow::{
    handle_shadow_interval_tick, handle_shadow_snapshot_join, handle_shadow_worker_join,
    prepare_shadow_scheduler_before_select,
};
use crate::app_loop_shutdown::shutdown_app_loop_tasks;
use crate::config_contract::{contains_placeholder_value, validate_execution_runtime_contract};
use crate::discovery_runtime::{DiscoveryService, RuntimePublicationTruthResolution};
use crate::history_retention::HistoryRetentionRunner;
use crate::observed_swap_writer::{
    run_observed_swap_retention_maintenance_once, ObservedSwapRecentRawJournalConfig,
    ObservedSwapRetentionConfig, ObservedSwapRetentionMaintenanceSummary,
    ObservedSwapRetentionRuntimeHealthHandle, ObservedSwapWriter, ObservedSwapWriterSnapshot,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_CAPACITY_MULTIPLIER,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
    OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL, OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL,
    OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL, OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
    OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT, OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT,
    OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT,
};
use crate::shadow_runtime_helpers::{handle_shadow_task_output, spawn_shadow_worker_task};
use crate::shadow_scheduler::{ShadowScheduler, ShadowSwapSide, ShadowTaskInput, ShadowTaskKey};
use crate::stale_close::close_stale_shadow_lots;
#[cfg(test)]
use crate::startup::STARTUP_LARGE_WAL_CHECKPOINT_TIMEOUT;
use crate::startup::{
    build_startup_progress_reporter, defer_implicit_startup_sqlite_wal_autocheckpoint,
    emit_inline_startup_progress, perform_startup_wal_checkpoint,
    restore_implicit_startup_sqlite_wal_autocheckpoint, run_inline_startup_step,
    skip_inline_startup_step, sqlite_startup_policy, startup_step_policy,
    StartupWalCheckpointOutcome, STARTUP_SQLITE_AUX_STEP_TIMEOUT,
    STARTUP_WAL_CHECKPOINT_DEFER_REASON,
};
use crate::swap_classification::{classify_swap_side, shadow_task_key_for_swap};
use crate::task_spawns::spawn_shadow_snapshot_task;
use crate::telemetry::format_error_chain;

include!("app_parts/00.rs");
include!("app_parts/00_sqlite_maintenance.rs");
include!("app_parts/01.rs");
include!("app_parts/01_irrelevant_backpressure.rs");
include!("app_parts/02.rs");
include!("app_parts/02_app_consumer_telemetry.rs");
include!("app_parts/02_operator_emergency.rs");
include!("app_parts/03.rs");
include!("app_parts/04.rs");
include!("app_parts/05.rs");
include!("app_parts/06.rs");
include!("app_parts/06_shadow_risk_infra.rs");

mod app_loop;
use crate::app_loop::run_app_loop;

#[cfg(test)]
mod app_tests;
