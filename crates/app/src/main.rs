use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{
    load_from_env_or_default, normalize_ingestion_source, ExecutionConfig, RiskConfig, ShadowConfig,
};
use copybot_core_types::SwapEvent;
#[cfg(test)]
use copybot_core_types::TokenQuantity;
use copybot_discovery::{DiscoveryService, RuntimePublicationTruthResolution};
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{FollowSnapshot, ShadowService};
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, report_startup_step_progress, run_observed_startup_step,
    sqlite_contention_snapshot, DiscoveryAggregateWriteConfig, DiscoveryRuntimeMode, Lamports,
    SignedLamports, SqliteContentionSnapshot, SqliteStartupPolicy, SqliteStore, StartupStepOutcome,
    StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy,
    StartupStepTimeoutBehavior,
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

mod alerts;
mod config_contract;
mod execution_pause_helpers;
mod execution_runtime_helpers;
mod history_retention;
mod observed_swap_writer;
mod secrets;
mod shadow_runtime_helpers;
mod shadow_scheduler;
mod stale_close;
mod swap_classification;
mod task_spawns;
mod telemetry;

use crate::alerts::AlertDispatcher;
use crate::config_contract::{contains_placeholder_value, validate_execution_runtime_contract};
use crate::execution_pause_helpers::resolve_buy_submit_pause_reason;
use crate::execution_runtime_helpers::log_execution_batch_report;
use crate::history_retention::HistoryRetentionRunner;
use crate::observed_swap_writer::{
    run_observed_swap_retention_maintenance_once, ObservedSwapRecentRawJournalConfig,
    ObservedSwapRetentionConfig, ObservedSwapRetentionMaintenanceSummary,
    ObservedSwapRetentionRuntimeHealthHandle, ObservedSwapWriter, ObservedSwapWriterSnapshot,
    OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL, OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL,
    OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL, OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
    OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT, OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT,
    OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT,
};
use crate::secrets::resolve_execution_adapter_secrets;
use crate::shadow_runtime_helpers::{
    apply_follow_snapshot_update, handle_shadow_task_output, spawn_shadow_worker_task,
};
use crate::shadow_scheduler::{ShadowScheduler, ShadowSwapSide, ShadowTaskInput, ShadowTaskKey};
use crate::stale_close::close_stale_shadow_lots;
use crate::swap_classification::{classify_swap_side, shadow_task_key_for_swap};
use crate::task_spawns::{spawn_discovery_task, spawn_execution_task, spawn_shadow_snapshot_task};
use crate::telemetry::format_error_chain;

const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";
const SHADOW_WORKER_POOL_SIZE: usize = 4;
const SHADOW_INLINE_WORKER_OVERFLOW: usize = 1;
const SHADOW_MAX_CONCURRENT_WORKERS: usize =
    SHADOW_WORKER_POOL_SIZE + SHADOW_INLINE_WORKER_OVERFLOW;
const SHADOW_PENDING_TASK_CAPACITY: usize = 256;
const INGESTION_ERROR_BACKOFF_MS: [u64; 6] = [100, 250, 500, 1_000, 2_000, 5_000];
const RISK_DB_REFRESH_MIN_SECONDS: i64 = 5;
const RISK_INFRA_SAMPLE_MIN_SECONDS: i64 = 10;
const RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS: i64 = 60;
const RISK_INFRA_EVENT_THROTTLE_SECONDS: i64 = 300;
const RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES: u64 = 5;
const RISK_INFRA_CLEAR_HEALTHY_SAMPLES: u64 = 5;
const RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY: usize = 32_768;
const APP_CONSUMER_LOOP_LATENCY_SAMPLE_CAPACITY: usize = 512;
const DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY: usize =
    OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY;
const STALE_LOT_CLEANUP_BATCH_LIMIT: u32 = 300;
const HARD_STOP_CLEAR_HEALTHY_REFRESHES: u64 = 6;
const SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO: f64 = 0.25;
const SQLITE_MAINTENANCE_PARTIAL_RETRY_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_INGESTION_OVERRIDE_PATH: &str = "state/ingestion_source_override.env";
const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
const DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS: u64 = 500;
const APP_LOG_FILTER_ENV: &str = "COPYBOT_APP_LOG_FILTER";
const LEGACY_RUST_LOG_ENV: &str = "RUST_LOG";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const OBSERVED_SWAP_WRITER_BACKPRESSURE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const OBSERVED_SWAP_WRITER_BACKPRESSURE_LOG_THROTTLE: StdDuration = StdDuration::from_secs(5);
const DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD: usize = 128;
const STARTUP_STEP_LOG_INTERVAL: StdDuration = StdDuration::from_secs(5);
const STARTUP_REQUIRED_STEP_TIMEOUT: StdDuration = StdDuration::from_secs(120);
const STARTUP_SQLITE_AUX_STEP_TIMEOUT: StdDuration = StdDuration::from_secs(30);
const STARTUP_WAL_CHECKPOINT_DEFER_REASON: &str = "deferred_off_startup_critical_path";
#[cfg(test)]
static APP_ENV_LOCK: Mutex<()> = Mutex::new(());

fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / LAMPORTS_PER_SOL
}

fn signed_lamports_to_sol(lamports: SignedLamports) -> f64 {
    lamports.as_i128() as f64 / LAMPORTS_PER_SOL
}

fn sol_to_lamports_floor(sol: f64, label: &str) -> Result<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!(
            "invalid {}={} (must be finite and >= 0)",
            label,
            sol
        ));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid {}={} (exceeds representable lamports)",
            label,
            sol
        ));
    }
    Ok(Lamports::new(scaled.floor() as u64))
}

fn sol_to_signed_lamports_conservative(sol: f64, label: &str) -> Result<SignedLamports> {
    if !sol.is_finite() {
        return Err(anyhow!("invalid {}={} (must be finite)", label, sol));
    }
    let magnitude = sol.abs() * LAMPORTS_PER_SOL;
    if !magnitude.is_finite() || magnitude > i64::MAX as f64 {
        return Err(anyhow!(
            "invalid {}={} (exceeds representable signed lamports)",
            label,
            sol
        ));
    }
    let signed = if sol >= 0.0 {
        magnitude.floor() as i128
    } else {
        -(magnitude.ceil() as i128)
    };
    Ok(SignedLamports::new(signed))
}

fn startup_step_policy(timeout: StdDuration) -> StartupStepRuntimePolicy {
    StartupStepRuntimePolicy::new(STARTUP_STEP_LOG_INTERVAL, Some(timeout))
        .with_timeout_behavior(StartupStepTimeoutBehavior::AbortProcess)
}

fn build_startup_progress_reporter() -> StartupStepProgressReporter {
    Arc::new(|event| log_startup_progress_event(&event))
}

fn log_startup_progress_event(event: &StartupStepProgress) {
    match event.outcome {
        StartupStepOutcome::Started | StartupStepOutcome::Completed => {
            info!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
        StartupStepOutcome::Waiting | StartupStepOutcome::Skipped => {
            warn!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
        StartupStepOutcome::Failed | StartupStepOutcome::TimedOut => {
            tracing::error!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
    }
}

fn emit_inline_startup_progress(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    outcome: StartupStepOutcome,
    started_at: StdInstant,
    budget: Option<StdDuration>,
    detail: Option<String>,
) {
    report_startup_step_progress(
        Some(reporter),
        stage,
        outcome,
        started_at.elapsed(),
        budget,
        detail,
    );
}

fn run_inline_startup_step<T, F>(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    budget: Option<StdDuration>,
    operation: F,
) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let started_at = StdInstant::now();
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Started,
        started_at,
        budget,
        None,
    );
    match operation() {
        Ok(value) => {
            emit_inline_startup_progress(
                reporter,
                stage,
                StartupStepOutcome::Completed,
                started_at,
                budget,
                None,
            );
            Ok(value)
        }
        Err(error) => {
            emit_inline_startup_progress(
                reporter,
                stage,
                StartupStepOutcome::Failed,
                started_at,
                budget,
                Some(format!("{error:#}")),
            );
            Err(error).with_context(|| format!("startup step {stage} failed"))
        }
    }
}

fn skip_inline_startup_step(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    detail: impl Into<String>,
) {
    let started_at = StdInstant::now();
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Started,
        started_at,
        None,
        None,
    );
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Skipped,
        started_at,
        None,
        Some(detail.into()),
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupWalCheckpointOutcome {
    Deferred,
}

fn perform_startup_wal_checkpoint(
    reporter: &StartupStepProgressReporter,
) -> StartupWalCheckpointOutcome {
    skip_inline_startup_step(
        reporter,
        "startup_sqlite_wal_checkpoint",
        STARTUP_WAL_CHECKPOINT_DEFER_REASON,
    );
    StartupWalCheckpointOutcome::Deferred
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli_config = parse_config_arg();
    let default_path = cli_config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (mut config, loaded_config_path) = load_from_env_or_default(&default_path)?;
    let (applied_source_override, invalid_source_override_error) =
        match load_ingestion_source_override() {
            Ok(source_override) => (
                apply_ingestion_source_override(&mut config.ingestion.source, source_override),
                None,
            ),
            Err(error) => (None, Some(error)),
        };
    resolve_execution_adapter_secrets(&mut config.execution, loaded_config_path.as_path())?;

    init_tracing(&config.system.log_level, config.system.log_json)?;
    info!(
        config_path = %loaded_config_path.display(),
        env = %config.system.env,
        "configuration loaded"
    );
    if let Some(source_override) = applied_source_override.as_deref() {
        info!(
            source = %source_override,
            "applying ingestion source override from failover file (override has highest priority)"
        );
    }
    if let Some(error) = invalid_source_override_error.as_ref() {
        warn!(
            error = %error,
            "ignoring invalid ingestion source override file"
        );
    }
    let migrations_dir = resolve_migrations_dir(&loaded_config_path, &config.system.migrations_dir);
    let startup_reporter = build_startup_progress_reporter();
    run_inline_startup_step(
        &startup_reporter,
        "startup_config_validation",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        || {
            validate_execution_runtime_contract(&config.execution, &config.system.env)?;
            validate_execution_risk_contract(&config.risk)?;
            validate_live_execution_policy_contract(
                &config.execution,
                &config.risk,
                &config.system.env,
            )?;
            Ok(())
        },
    )?;

    let sqlite_startup_policy = SqliteStartupPolicy {
        open_step: startup_step_policy(STARTUP_REQUIRED_STEP_TIMEOUT),
        pragma_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        schema_bootstrap_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        migrations_scan_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        migrations_apply_step: startup_step_policy(STARTUP_REQUIRED_STEP_TIMEOUT),
    };
    let bootstrap = SqliteStore::open_and_migrate_for_startup(
        Path::new(&config.sqlite.path),
        &migrations_dir,
        &sqlite_startup_policy,
        Some(&startup_reporter),
    )
    .context("failed to initialize sqlite store")?;
    let store = bootstrap.store;
    let applied = bootstrap.applied_migrations;
    let deferred_migrations = bootstrap.deferred_migrations;
    info!(applied, "sqlite migrations applied");
    if !deferred_migrations.is_empty() {
        warn!(
            deferred_migrations = ?deferred_migrations,
            detail = "deferred_off_startup_critical_path",
            "startup deferred optional sqlite performance migrations; runtime may use bounded fallback query paths until they are applied offline"
        );
    }
    match perform_startup_wal_checkpoint(&startup_reporter) {
        StartupWalCheckpointOutcome::Deferred => {
            warn!(
                reason = STARTUP_WAL_CHECKPOINT_DEFER_REASON,
                "startup sqlite wal checkpoint deferred"
            );
        }
    }

    let mut store = run_observed_startup_step(
        "startup_sqlite_heartbeat",
        startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        Some(&startup_reporter),
        move || {
            store
                .record_heartbeat("copybot-app", "startup")
                .context("failed to write startup heartbeat")?;
            Ok(store)
        },
    )?;
    let alert_dispatcher = run_inline_startup_step(
        &startup_reporter,
        "startup_alert_dispatcher_init",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        AlertDispatcher::from_env,
    )
    .context("failed to initialize alert delivery")?;
    if let Some(dispatcher) = &alert_dispatcher {
        store = run_observed_startup_step(
            "startup_alert_delivery_cursor",
            startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
            Some(&startup_reporter),
            move || {
                store
                    .ensure_alert_delivery_cursor("webhook")
                    .context("failed to initialize alert delivery cursor")?;
                Ok(store)
            },
        )?;
        if dispatcher.test_on_startup() {
            dispatcher
                .send_startup_test(&config.system.env)
                .await
                .context("failed sending startup alert delivery test")?;
            info!("startup alert delivery test sent");
        }
    } else {
        skip_inline_startup_step(
            &startup_reporter,
            "startup_alert_delivery_cursor",
            "alert_dispatcher_disabled",
        );
    }

    let (ingestion, discovery, shadow, execution_runtime) = run_inline_startup_step(
        &startup_reporter,
        "startup_runtime_service_init",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        || {
            let ingestion = IngestionService::build(&config.ingestion)
                .context("failed to initialize ingestion service")?;
            let discovery_http_url = select_role_helius_http_url(
                &config.discovery.helius_http_url,
                &config.ingestion.helius_http_url,
            );
            let shadow_http_url = select_role_helius_http_url(
                &config.shadow.helius_http_url,
                &config.ingestion.helius_http_url,
            );
            let discovery_http_url = enforce_quality_gate_http_url(
                "discovery",
                &config.system.env,
                config.shadow.quality_gates_enabled,
                discovery_http_url,
            )?;
            let shadow_http_url = enforce_quality_gate_http_url(
                "shadow",
                &config.system.env,
                config.shadow.quality_gates_enabled,
                shadow_http_url,
            )?;
            validate_shadow_quality_gate_contract(&config.shadow, &config.system.env)?;
            let discovery = DiscoveryService::new_with_helius(
                config.discovery.clone(),
                config.shadow.clone(),
                discovery_http_url.clone(),
            );
            let shadow = ShadowService::new_with_helius(config.shadow.clone(), shadow_http_url);
            let execution_runtime =
                ExecutionRuntime::from_config(config.execution.clone(), config.risk.clone());
            Ok((ingestion, discovery, shadow, execution_runtime))
        },
    )?;
    let discovery_scoring_retention_days = config
        .discovery
        .scoring_window_days
        .max(config.discovery.decay_window_days)
        .saturating_add(1);
    let discovery_aggregate_write_config = DiscoveryAggregateWriteConfig {
        max_tx_per_minute: config.discovery.max_tx_per_minute,
        rug_lookahead_seconds: config.discovery.rug_lookahead_seconds as u32,
        helius_http_url: None,
        min_token_age_hint_seconds: Some(config.shadow.min_token_age_seconds),
    };

    let app_loop_handoff_started = StdInstant::now();
    emit_inline_startup_progress(
        &startup_reporter,
        "startup_app_loop_handoff",
        StartupStepOutcome::Started,
        app_loop_handoff_started,
        None,
        None,
    );
    emit_inline_startup_progress(
        &startup_reporter,
        "startup_app_loop_handoff",
        StartupStepOutcome::Completed,
        app_loop_handoff_started,
        None,
        None,
    );
    run_app_loop(
        store,
        ingestion,
        discovery,
        shadow,
        execution_runtime,
        config.risk.clone(),
        config.sqlite.path.clone(),
        config.system.heartbeat_seconds,
        config.history_retention.clone(),
        config.recent_raw_journal.clone(),
        config.discovery.fetch_refresh_seconds,
        config.discovery.refresh_seconds,
        config.discovery.observed_swaps_retention_days,
        discovery_scoring_retention_days,
        config.discovery.scoring_aggregates_write_enabled,
        discovery_aggregate_write_config,
        config.ingestion.source.clone(),
        config.shadow.refresh_seconds,
        config.shadow.max_signal_lag_seconds,
        config.shadow.causal_holdback_enabled,
        config.shadow.causal_holdback_ms,
        config.system.pause_new_trades_on_outage,
        alert_dispatcher,
    )
    .await
}

fn parse_config_arg() -> Option<PathBuf> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().map(PathBuf::from);
        }
        if let Some(inline) = arg.strip_prefix("--config=") {
            return Some(PathBuf::from(inline));
        }
    }
    None
}

fn load_ingestion_source_override() -> Result<Option<String>> {
    let override_path = env::var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE")
        .unwrap_or_else(|_| DEFAULT_INGESTION_OVERRIDE_PATH.to_string());
    let content = match fs::read_to_string(&override_path) {
        Ok(content) => content,
        Err(_) => return Ok(None),
    };
    parse_ingestion_source_override(&content)
        .with_context(|| format!("invalid ingestion source override in {override_path}"))
}

fn apply_ingestion_source_override(
    ingestion_source: &mut String,
    source_override: Option<String>,
) -> Option<String> {
    if let Some(source_override) = source_override {
        *ingestion_source = source_override.clone();
        return Some(source_override);
    }
    None
}

fn parse_ingestion_source_override(content: &str) -> Result<Option<String>> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if key.trim() != "SOLANA_COPY_BOT_INGESTION_SOURCE" {
            continue;
        }
        let source = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if !source.is_empty() {
            return normalize_ingestion_source(&source).map(Some);
        }
    }
    Ok(None)
}

#[cfg(test)]
fn startup_sqlite_wal_checkpoint_error_requires_abort(
    primary_error: Option<&anyhow::Error>,
    fallback_error: Option<&anyhow::Error>,
) -> bool {
    primary_error.is_some_and(is_fatal_sqlite_anyhow_error)
        || fallback_error.is_some_and(is_fatal_sqlite_anyhow_error)
}

fn runtime_sqlite_write_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn history_retention_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn observed_swap_retention_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteMaintenanceTask {
    HistoryRetention,
    ObservedSwapRetention,
}

impl SqliteMaintenanceTask {
    fn as_str(self) -> &'static str {
        match self {
            Self::HistoryRetention => "history_retention",
            Self::ObservedSwapRetention => "observed_swap_retention",
        }
    }
}

fn sqlite_maintenance_block_reason(
    _task: SqliteMaintenanceTask,
    app_started_at: StdInstant,
    now: StdInstant,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    sqlite_contention_previous: SqliteContentionSnapshot,
    sqlite_contention_current: SqliteContentionSnapshot,
    ingestion_runtime_snapshot: Option<IngestionRuntimeSnapshot>,
) -> Option<String> {
    let startup_grace = Duration::from_secs(
        OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
            .as_secs()
            .max(1),
    );
    let elapsed_since_start = now.saturating_duration_since(app_started_at);
    if elapsed_since_start < startup_grace {
        return Some(format!(
            "startup_grace_remaining_ms={}",
            startup_grace
                .saturating_sub(elapsed_since_start)
                .as_millis()
                .min(u128::from(u64::MAX))
        ));
    }

    if observed_swap_writer_snapshot.pending_requests > 0 {
        return Some(format!(
            "writer_pending_requests={}",
            observed_swap_writer_snapshot.pending_requests
        ));
    }

    if observed_swap_writer_snapshot.aggregate_queue_depth_batches > 0 {
        return Some(format!(
            "aggregate_queue_depth_batches={}",
            observed_swap_writer_snapshot.aggregate_queue_depth_batches
        ));
    }

    if observed_swap_writer_snapshot.journal_queue_depth_batches > 0 {
        return Some(format!(
            "journal_queue_depth_batches={}",
            observed_swap_writer_snapshot.journal_queue_depth_batches
        ));
    }

    let sqlite_write_retry_delta = sqlite_contention_current
        .write_retry_total
        .saturating_sub(sqlite_contention_previous.write_retry_total);
    let sqlite_busy_error_delta = sqlite_contention_current
        .busy_error_total
        .saturating_sub(sqlite_contention_previous.busy_error_total);
    if sqlite_write_retry_delta > 0 || sqlite_busy_error_delta > 0 {
        return Some(format!(
            "sqlite_contention_delta write_retry_delta={} busy_error_delta={}",
            sqlite_write_retry_delta, sqlite_busy_error_delta
        ));
    }

    if let Some(snapshot) = ingestion_runtime_snapshot {
        if snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio
                >= SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO
        {
            return Some(format!(
                "yellowstone_output_queue_fill_ratio={:.4} depth={} capacity={} oldest_age_ms={}",
                snapshot.yellowstone_output_queue_fill_ratio,
                snapshot.yellowstone_output_queue_depth,
                snapshot.yellowstone_output_queue_capacity,
                snapshot.yellowstone_output_oldest_age_ms
            ));
        }
    }

    None
}

fn sqlite_maintenance_block_reason_key(reason: &str) -> &'static str {
    if reason.starts_with("startup_grace_remaining_ms=") {
        "startup_grace_remaining_ms"
    } else if reason.starts_with("writer_pending_requests=") {
        "writer_pending_requests"
    } else if reason.starts_with("aggregate_queue_depth_batches=") {
        "aggregate_queue_depth_batches"
    } else if reason.starts_with("journal_queue_depth_batches=") {
        "journal_queue_depth_batches"
    } else if reason.starts_with("sqlite_contention_delta ") {
        "sqlite_contention_delta"
    } else if reason.starts_with("yellowstone_output_queue_fill_ratio=") {
        "yellowstone_output_queue_fill_ratio"
    } else {
        "other"
    }
}

fn stale_lot_cleanup_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn alert_delivery_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn discovery_task_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn risk_event_write_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_pause_restore_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_background_refresh_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_open_lot_refresh_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn execution_task_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_snapshot_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_state_event_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn persist_runtime_risk_event_or_warn(
    store: &SqliteStore,
    event_type: &str,
    severity: &str,
    ts: DateTime<Utc>,
    details_json: Option<&str>,
    warning_message: &'static str,
    fatal_context: &'static str,
) -> Result<()> {
    if let Err(error) = store.insert_risk_event(event_type, severity, ts, details_json) {
        if risk_event_write_error_requires_restart(&error) {
            return Err(error).context(fatal_context);
        }
        warn!(
            error = %error,
            event_type,
            severity,
            warning_message,
            "failed to persist runtime risk event"
        );
    }
    Ok(())
}

fn persist_shadow_risk_fail_closed_event_or_warn(
    store: &SqliteStore,
    ts: DateTime<Utc>,
    details_json: &str,
) -> Result<()> {
    persist_runtime_risk_event_or_warn(
        store,
        "shadow_risk_fail_closed",
        "error",
        ts,
        Some(details_json),
        "failed to persist shadow risk fail-closed event",
        "failed to persist shadow risk fail-closed event with fatal sqlite I/O",
    )
}

fn discovery_scoring_source_uses_raw_window_cap_truncation_context(scoring_source: &str) -> bool {
    matches!(
        scoring_source,
        "raw_window"
            | "raw_window_empty"
            | "raw_window_persisted_stream"
            | "persisted_wallet_metrics_truncated_warm_restore"
            | "persisted_wallet_metrics_truncated_warm_restore_empty"
    )
}

fn discovery_output_has_raw_window_cap_truncation_context(
    discovery_output: &DiscoveryTaskOutput,
) -> bool {
    discovery_output.raw_window_cap_truncated
        && discovery_scoring_source_uses_raw_window_cap_truncation_context(
            discovery_output.scoring_source,
        )
}

fn observed_swap_writer_error_requires_restart(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message == OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT
            || message == OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT
            || message == OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT
    })
}

fn ingestion_error_backoff_ms(consecutive_errors: u32) -> u64 {
    let index =
        (consecutive_errors.saturating_sub(1) as usize).min(INGESTION_ERROR_BACKOFF_MS.len() - 1);
    INGESTION_ERROR_BACKOFF_MS[index]
}

fn note_recent_swap_signature(
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    signature: &str,
) -> bool {
    if !recent_signatures.insert(signature.to_string()) {
        return false;
    }
    recent_signature_order.push_back(signature.to_string());
    while recent_signature_order.len() > RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
        if let Some(evicted) = recent_signature_order.pop_front() {
            recent_signatures.remove(&evicted);
        }
    }
    true
}

fn forget_recent_swap_signature(
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    signature: &str,
) {
    if !recent_signatures.remove(signature) {
        return;
    }
    if recent_signature_order
        .back()
        .is_some_and(|queued| queued == signature)
    {
        recent_signature_order.pop_back();
        return;
    }
    if let Some(position) = recent_signature_order
        .iter()
        .position(|queued| queued == signature)
    {
        recent_signature_order.remove(position);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedSwapShadowRelevance {
    Relevant(ShadowSwapSide),
    IrrelevantUnclassified,
    IrrelevantNotFollowed(ShadowSwapSide),
}

fn classify_observed_swap_shadow_relevance(
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    shadow_scheduler: &ShadowScheduler,
    open_shadow_lots: &HashSet<(String, String)>,
) -> ObservedSwapShadowRelevance {
    let Some(side) = classify_swap_side(swap) else {
        return ObservedSwapShadowRelevance::IrrelevantUnclassified;
    };

    if matches!(side, ShadowSwapSide::Buy)
        && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
    {
        return ObservedSwapShadowRelevance::IrrelevantNotFollowed(side);
    }

    if matches!(side, ShadowSwapSide::Sell)
        && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
    {
        let wallet_has_recent_follow_history = follow_snapshot.is_active(&swap.wallet)
            || follow_snapshot.promoted_at.contains_key(&swap.wallet)
            || follow_snapshot.demoted_at.contains_key(&swap.wallet);
        let sell_key = shadow_task_key_for_swap(swap, side);
        let key_tuple = (sell_key.wallet.clone(), sell_key.token.clone());
        let has_pending_or_inflight = shadow_scheduler.key_has_pending_or_inflight(&sell_key);
        if !wallet_has_recent_follow_history
            && !has_pending_or_inflight
            && !open_shadow_lots.contains(&key_tuple)
        {
            return ObservedSwapShadowRelevance::IrrelevantNotFollowed(side);
        }
    }

    ObservedSwapShadowRelevance::Relevant(side)
}
async fn enqueue_irrelevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    enqueue_irrelevant_observed_swap_immediately(
        observed_swap_writer,
        recent_signatures,
        recent_signature_order,
        swap,
        discovery_critical,
    )
}

fn irrelevant_observed_swap_requires_discovery_critical_persistence(
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) -> bool {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return false;
    }

    match classify_swap_side(swap) {
        Some(ShadowSwapSide::Buy) => {
            discovery_critical_target_buy_mints.is_empty()
                || discovery_critical_target_buy_mints.contains(swap.token_out.as_str())
        }
        Some(ShadowSwapSide::Sell) => {
            !discovery_critical_target_buy_mints.is_empty()
                && discovery_critical_target_buy_mints.contains(swap.token_in.as_str())
        }
        None => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IrrelevantObservedSwapEnqueueOutcome {
    Enqueued,
    PendingWriterBackpressure,
}

#[derive(Debug, Clone)]
struct PendingIrrelevantObservedSwap {
    swap: SwapEvent,
    discovery_critical: bool,
    processing_started_at: StdInstant,
    backpressure_started_at: StdInstant,
    last_backpressure_log_at: Option<StdInstant>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryCriticalPersistedRebuildPayloadTargetMints {
    #[serde(default)]
    discovery_critical_target_buy_mints: Vec<String>,
    #[serde(default)]
    unique_buy_mints: Vec<String>,
}

fn zero_universe_fail_closed_discovery_market_context_mode(
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
) -> bool {
    shadow_strategy_fail_closed && follow_snapshot.active.is_empty() && open_shadow_lots.is_empty()
}

fn load_discovery_critical_target_buy_mints(store: &SqliteStore) -> Result<HashSet<String>> {
    let Some(state_row) = store.load_discovery_persisted_rebuild_state_read_only()? else {
        return Ok(HashSet::new());
    };
    let payload: DiscoveryCriticalPersistedRebuildPayloadTargetMints =
        serde_json::from_str(&state_row.state_json).context(
            "failed parsing discovery persisted rebuild payload while loading target buy mints for critical market-context persistence",
        )?;
    let target_buy_mints = if payload.discovery_critical_target_buy_mints.is_empty() {
        payload.unique_buy_mints
    } else {
        payload.discovery_critical_target_buy_mints
    };
    Ok(target_buy_mints.into_iter().collect())
}

fn refresh_discovery_critical_target_buy_mints_or_warn(
    store: &SqliteStore,
    target_buy_mints: &mut HashSet<String>,
) -> Result<()> {
    match load_discovery_critical_target_buy_mints(store) {
        Ok(loaded) => {
            *target_buy_mints = loaded;
            Ok(())
        }
        Err(error) => {
            if is_fatal_sqlite_anyhow_error(&error) {
                return Err(error).context(
                    "failed refreshing discovery-critical target buy mints with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                "failed refreshing discovery-critical target buy mints; keeping the previous target set"
            );
            Ok(())
        }
    }
}

fn refresh_discovery_critical_irrelevant_persistence_for_backpressure(
    store: &SqliteStore,
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
) -> Result<bool> {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return Ok(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                swap,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
            ),
        );
    }

    refresh_discovery_critical_target_buy_mints_or_warn(
        store,
        discovery_critical_target_buy_mints,
    )?;
    Ok(
        irrelevant_observed_swap_requires_discovery_critical_persistence(
            swap,
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        ),
    )
}

fn should_buffer_backpressured_irrelevant_observed_swap(
    discovery_critical: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
) -> bool {
    !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) || discovery_critical
}

fn pending_irrelevant_swap_backpressure_blocks_ingestion(
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
) -> bool {
    pending_irrelevant_swaps.len() >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
}

fn prune_noncritical_zero_universe_pending_irrelevant_swaps(
    pending_irrelevant_swaps: &mut VecDeque<PendingIrrelevantObservedSwap>,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) -> usize {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return 0;
    }

    let mut retained = VecDeque::with_capacity(pending_irrelevant_swaps.len());
    let mut dropped = 0usize;
    while let Some(mut pending) = pending_irrelevant_swaps.pop_front() {
        let still_discovery_critical =
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &pending.swap,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
            );
        if still_discovery_critical {
            pending.discovery_critical = true;
            retained.push_back(pending);
            continue;
        }

        forget_recent_swap_signature(
            recent_signatures,
            recent_signature_order,
            &pending.swap.signature,
        );
        app_consumer_loop_telemetry.note_processing_started_at(pending.processing_started_at);
        dropped = dropped.saturating_add(1);
    }
    *pending_irrelevant_swaps = retained;
    dropped
}

fn enqueue_irrelevant_observed_swap_immediately(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    let enqueue_result = if discovery_critical {
        observed_swap_writer.try_enqueue_discovery_critical(swap)
    } else {
        observed_swap_writer.try_enqueue(swap)
    };
    match enqueue_result {
        Ok(true) => Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued),
        Ok(false) => Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure),
        Err(error) => {
            forget_recent_swap_signature(
                recent_signatures,
                recent_signature_order,
                &swap.signature,
            );
            Err(error)
        }
    }
}

async fn persist_relevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
) -> Result<bool> {
    match observed_swap_writer.write(swap).await {
        Ok(inserted) => Ok(inserted),
        Err(error) => {
            forget_recent_swap_signature(
                recent_signatures,
                recent_signature_order,
                &swap.signature,
            );
            Err(error)
        }
    }
}

async fn persist_irrelevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    enqueue_irrelevant_observed_swap(
        observed_swap_writer,
        recent_signatures,
        recent_signature_order,
        swap,
        discovery_critical,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelevantObservedSwapPersistence {
    Inserted,
    Duplicate,
}

fn relevant_observed_swap_persistence(inserted: bool) -> RelevantObservedSwapPersistence {
    if inserted {
        RelevantObservedSwapPersistence::Inserted
    } else {
        RelevantObservedSwapPersistence::Duplicate
    }
}

#[derive(Debug, Default)]
struct AppConsumerLoopTelemetry {
    swaps_seen: u64,
    follow_rejected: u64,
    processing_ms_samples: VecDeque<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct AppConsumerLoopTelemetrySnapshot {
    swaps_seen: u64,
    follow_rejected: u64,
    follow_rejected_ratio: f64,
    processing_ms_p95: u64,
}

impl AppConsumerLoopTelemetry {
    fn note_swap_seen(&mut self) {
        self.swaps_seen = self.swaps_seen.saturating_add(1);
    }

    fn note_follow_rejected(&mut self) {
        self.follow_rejected = self.follow_rejected.saturating_add(1);
    }

    fn note_processing_duration(&mut self, duration_ms: u64) {
        if self.processing_ms_samples.len() >= APP_CONSUMER_LOOP_LATENCY_SAMPLE_CAPACITY {
            let _ = self.processing_ms_samples.pop_front();
        }
        self.processing_ms_samples.push_back(duration_ms);
    }

    fn note_processing_started_at(&mut self, started_at: StdInstant) {
        let duration_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.note_processing_duration(duration_ms);
    }

    fn snapshot_and_reset(&mut self) -> AppConsumerLoopTelemetrySnapshot {
        let swaps_seen = self.swaps_seen;
        let follow_rejected = self.follow_rejected;
        let processing_ms_p95 = percentile_u64_deque(&self.processing_ms_samples, 0.95);
        let follow_rejected_ratio = if swaps_seen == 0 {
            0.0
        } else {
            follow_rejected as f64 / swaps_seen as f64
        };
        self.swaps_seen = 0;
        self.follow_rejected = 0;
        self.processing_ms_samples.clear();
        AppConsumerLoopTelemetrySnapshot {
            swaps_seen,
            follow_rejected,
            follow_rejected_ratio,
            processing_ms_p95,
        }
    }
}

fn percentile_u64_deque(values: &VecDeque<u64>, q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

fn parse_operator_emergency_stop_reason(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}

#[derive(Debug)]
struct OperatorEmergencyStop {
    path: PathBuf,
    poll_interval: Duration,
    next_refresh_at: StdInstant,
    active: bool,
    detail: String,
}

impl OperatorEmergencyStop {
    fn from_env() -> Self {
        let path = env::var("SOLANA_COPY_BOT_EMERGENCY_STOP_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_OPERATOR_EMERGENCY_STOP_PATH));
        let poll_ms = env::var("SOLANA_COPY_BOT_EMERGENCY_STOP_POLL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS)
            .max(100);
        Self {
            path,
            poll_interval: Duration::from_millis(poll_ms),
            next_refresh_at: StdInstant::now(),
            active: false,
            detail: String::new(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn detail(&self) -> &str {
        if self.detail.is_empty() {
            "operator emergency stop file is present"
        } else {
            self.detail.as_str()
        }
    }

    fn refresh(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let instant_now = StdInstant::now();
        if instant_now < self.next_refresh_at {
            return Ok(());
        }
        self.next_refresh_at = instant_now + self.poll_interval;

        let (active, detail) = match fs::read_to_string(&self.path) {
            Ok(content) => (
                true,
                parse_operator_emergency_stop_reason(&content)
                    .unwrap_or_else(|| "operator emergency stop file is present".to_string()),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, String::new()),
            Err(error) => match fs::metadata(&self.path) {
                Ok(_) => {
                    let detail = format!("emergency stop file is unreadable: {error}");
                    warn!(
                        path = %self.path.display(),
                        error = %error,
                        "operator emergency stop file exists but cannot be read; failing closed"
                    );
                    (true, detail)
                }
                Err(_) => (false, String::new()),
            },
        };

        if active == self.active && detail == self.detail {
            return Ok(());
        }

        let path_display = self.path.display().to_string();

        if active {
            self.active = true;
            self.detail = detail;
            warn!(
                path = %path_display,
                detail = %self.detail(),
                "operator emergency stop activated"
            );
            let details_json = format!(
                "{{\"path\":\"{}\",\"detail\":\"{}\"}}",
                sanitize_json_value(&path_display),
                sanitize_json_value(self.detail())
            );
            persist_runtime_risk_event_or_warn(
                store,
                "operator_emergency_stop_activated",
                "warn",
                now,
                Some(&details_json),
                "failed to persist operator emergency stop activation event",
                "failed to persist operator emergency stop activation event with fatal sqlite I/O",
            )?;
        } else {
            info!(path = %path_display, "operator emergency stop cleared");
            let details_json = format!(
                "{{\"path\":\"{}\",\"state\":\"cleared\"}}",
                sanitize_json_value(&path_display)
            );
            persist_runtime_risk_event_or_warn(
                store,
                "operator_emergency_stop_cleared",
                "info",
                now,
                Some(&details_json),
                "failed to persist operator emergency stop clear event",
                "failed to persist operator emergency stop clear event with fatal sqlite I/O",
            )?;
            self.active = false;
            self.detail.clear();
        }
        Ok(())
    }
}

fn record_shadow_queue_backpressure_risk_event(
    store: &SqliteStore,
    pending_shadow_task_count: usize,
    held_shadow_task_count: usize,
    shadow_buffered_task_count: usize,
    shadow_pending_capacity: usize,
    now: DateTime<Utc>,
) -> Result<()> {
    let details_json = format!(
        "{{\"reason\":\"queue_backpressure\",\"pending\":{},\"held\":{},\"buffered\":{},\"capacity\":{}}}",
        pending_shadow_task_count,
        held_shadow_task_count,
        shadow_buffered_task_count,
        shadow_pending_capacity
    );
    persist_runtime_risk_event_or_warn(
        store,
        "shadow_queue_saturated",
        "warn",
        now,
        Some(&details_json),
        "failed to persist shadow queue backpressure risk event",
        "failed to persist shadow queue backpressure risk event with fatal sqlite I/O",
    )
}

fn refresh_shadow_open_lot_index_or_warn(
    store: &SqliteStore,
    open_shadow_lots: &mut HashSet<(String, String)>,
) -> Result<()> {
    match store.list_shadow_open_pairs() {
        Ok(pairs) => {
            *open_shadow_lots = pairs;
            Ok(())
        }
        Err(error) => {
            if shadow_open_lot_refresh_error_requires_restart(&error) {
                return Err(error)
                    .context("failed to refresh open shadow lot index with fatal sqlite I/O");
            }
            warn!(error = %error, "failed to refresh open shadow lot index");
            Ok(())
        }
    }
}

fn record_shadow_risk_state_event_or_warn(
    store: &SqliteStore,
    event_type: &str,
    severity: &str,
    ts: DateTime<Utc>,
    details_json: &str,
    fatal_context: &'static str,
) -> Result<()> {
    if let Err(error) = store.insert_risk_event(event_type, severity, ts, Some(details_json)) {
        if shadow_risk_state_event_error_requires_abort(&error) {
            return Err(error).context(fatal_context);
        }
        warn!(
            error = %error,
            event_type,
            severity,
            "failed to persist shadow risk event"
        );
    }
    Ok(())
}

fn select_role_helius_http_url(role_specific: &str, fallback: &str) -> Option<String> {
    let role_specific = role_specific.trim();
    if !role_specific.is_empty() && !contains_placeholder_value(role_specific) {
        return Some(role_specific.to_string());
    }

    let fallback = fallback.trim();
    if !fallback.is_empty() && !contains_placeholder_value(fallback) {
        return Some(fallback.to_string());
    }

    None
}

fn enforce_quality_gate_http_url(
    role: &str,
    env: &str,
    quality_gates_enabled: bool,
    endpoint: Option<String>,
) -> Result<Option<String>> {
    let env_norm = env.trim().to_ascii_lowercase();
    let enforce = quality_gates_enabled
        && (matches!(env_norm.as_str(), "paper" | "prod")
            || env_norm.starts_with("paper-")
            || env_norm.starts_with("prod-"));
    if enforce && endpoint.is_none() {
        return Err(anyhow!(
            "{role} requires a valid helius_http_url (role-specific or ingestion fallback) when quality gates are enabled in {env}"
        ));
    }
    Ok(endpoint)
}

fn validate_shadow_quality_gate_contract(config: &ShadowConfig, env: &str) -> Result<()> {
    if !config.quality_gates_enabled {
        return Ok(());
    }
    if !config.min_liquidity_sol.is_finite() || config.min_liquidity_sol < 0.0 {
        return Err(anyhow!(
            "shadow.min_liquidity_sol must be finite and >= 0 when quality gates are enabled in {}",
            env
        ));
    }
    if !config.min_volume_5m_sol.is_finite() || config.min_volume_5m_sol < 0.0 {
        return Err(anyhow!(
            "shadow.min_volume_5m_sol must be finite and >= 0 when quality gates are enabled in {}",
            env
        ));
    }
    let all_thresholds_zero = config.min_token_age_seconds == 0
        && config.min_holders == 0
        && config.min_liquidity_sol <= 0.0
        && config.min_volume_5m_sol <= 0.0
        && config.min_unique_traders_5m == 0;
    if all_thresholds_zero {
        return Err(anyhow!(
            "shadow.quality_gates_enabled=true but all quality thresholds are zero in {}; set at least one non-zero threshold",
            env
        ));
    }
    Ok(())
}

fn validate_execution_risk_contract(config: &RiskConfig) -> Result<()> {
    if !config.max_position_sol.is_finite() || config.max_position_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_position_sol must be finite and > 0, got {}",
            config.max_position_sol
        ));
    }
    if !config.max_total_exposure_sol.is_finite() || config.max_total_exposure_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_total_exposure_sol must be finite and > 0, got {}",
            config.max_total_exposure_sol
        ));
    }
    if !config.max_exposure_per_token_sol.is_finite() || config.max_exposure_per_token_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_exposure_per_token_sol must be finite and > 0, got {}",
            config.max_exposure_per_token_sol
        ));
    }
    if config.max_exposure_per_token_sol > config.max_total_exposure_sol {
        return Err(anyhow!(
            "risk.max_exposure_per_token_sol ({}) cannot exceed risk.max_total_exposure_sol ({})",
            config.max_exposure_per_token_sol,
            config.max_total_exposure_sol
        ));
    }
    if config.max_concurrent_positions == 0 {
        return Err(anyhow!(
            "risk.max_concurrent_positions must be >= 1, got {}",
            config.max_concurrent_positions
        ));
    }
    if config.max_copy_delay_sec == 0 {
        return Err(anyhow!(
            "risk.max_copy_delay_sec must be >= 1, got {}",
            config.max_copy_delay_sec
        ));
    }
    if config.max_hold_hours == 0 && config.shadow_stale_close_terminal_zero_price_hours > 0 {
        return Err(anyhow!(
            "risk.shadow_stale_close_terminal_zero_price_hours ({}) requires risk.max_hold_hours >= 1",
            config.shadow_stale_close_terminal_zero_price_hours
        ));
    }
    if config.max_hold_hours == 0 && config.shadow_stale_close_recovery_zero_price_enabled {
        return Err(anyhow!(
            "risk.shadow_stale_close_recovery_zero_price_enabled=true requires risk.max_hold_hours >= 1"
        ));
    }
    if config.shadow_stale_close_recovery_zero_price_enabled
        && config.shadow_stale_close_terminal_zero_price_hours <= config.max_hold_hours
    {
        return Err(anyhow!(
            "risk.shadow_stale_close_recovery_zero_price_enabled=true requires risk.shadow_stale_close_terminal_zero_price_hours ({}) > risk.max_hold_hours ({})",
            config.shadow_stale_close_terminal_zero_price_hours,
            config.max_hold_hours
        ));
    }
    if config.shadow_stale_close_terminal_zero_price_hours > 0 {
        let min_terminal_zero_price_hours = config.max_hold_hours.saturating_mul(2);
        if config.shadow_stale_close_terminal_zero_price_hours < min_terminal_zero_price_hours {
            return Err(anyhow!(
                "risk.shadow_stale_close_terminal_zero_price_hours ({}) must be 0 (disabled) or >= 2 * risk.max_hold_hours ({})",
                config.shadow_stale_close_terminal_zero_price_hours,
                min_terminal_zero_price_hours
            ));
        }
    }
    if !config.daily_loss_limit_pct.is_finite()
        || config.daily_loss_limit_pct < 0.0
        || config.daily_loss_limit_pct > 100.0
    {
        return Err(anyhow!(
            "risk.daily_loss_limit_pct must be finite and in [0, 100], got {}",
            config.daily_loss_limit_pct
        ));
    }
    if !config.max_drawdown_pct.is_finite()
        || config.max_drawdown_pct < 0.0
        || config.max_drawdown_pct > 100.0
    {
        return Err(anyhow!(
            "risk.max_drawdown_pct must be finite and in [0, 100], got {}",
            config.max_drawdown_pct
        ));
    }

    if !config.shadow_soft_exposure_cap_sol.is_finite()
        || config.shadow_soft_exposure_cap_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_cap_sol must be finite and > 0, got {}",
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if !config.shadow_soft_exposure_resume_below_sol.is_finite()
        || config.shadow_soft_exposure_resume_below_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_resume_below_sol must be finite and > 0, got {}",
            config.shadow_soft_exposure_resume_below_sol
        ));
    }
    if config.shadow_soft_exposure_resume_below_sol > config.shadow_soft_exposure_cap_sol {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_resume_below_sol ({}) must be <= risk.shadow_soft_exposure_cap_sol ({})",
            config.shadow_soft_exposure_resume_below_sol,
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if !config.shadow_hard_exposure_cap_sol.is_finite()
        || config.shadow_hard_exposure_cap_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_hard_exposure_cap_sol must be finite and > 0, got {}",
            config.shadow_hard_exposure_cap_sol
        ));
    }
    if config.shadow_hard_exposure_cap_sol < config.shadow_soft_exposure_cap_sol {
        return Err(anyhow!(
            "risk.shadow_hard_exposure_cap_sol ({}) must be >= risk.shadow_soft_exposure_cap_sol ({})",
            config.shadow_hard_exposure_cap_sol,
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if config.shadow_killswitch_enabled && config.shadow_soft_pause_minutes == 0 {
        return Err(anyhow!(
            "risk.shadow_soft_pause_minutes must be >= 1 when risk.shadow_killswitch_enabled=true"
        ));
    }

    if !config.shadow_drawdown_1h_stop_sol.is_finite()
        || !config.shadow_drawdown_6h_stop_sol.is_finite()
        || !config.shadow_drawdown_24h_stop_sol.is_finite()
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_*_stop_sol values must be finite (1h={}, 6h={}, 24h={})",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if config.shadow_drawdown_1h_stop_sol > 0.0
        || config.shadow_drawdown_6h_stop_sol > 0.0
        || config.shadow_drawdown_24h_stop_sol > 0.0
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_*_stop_sol values must be <= 0 (1h={}, 6h={}, 24h={})",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if !(config.shadow_drawdown_1h_stop_sol >= config.shadow_drawdown_6h_stop_sol
        && config.shadow_drawdown_6h_stop_sol >= config.shadow_drawdown_24h_stop_sol)
    {
        return Err(anyhow!(
            "risk shadow drawdown ordering is invalid: expected 1h >= 6h >= 24h, got 1h={}, 6h={}, 24h={}",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if config.shadow_killswitch_enabled
        && (config.shadow_drawdown_1h_pause_minutes == 0
            || config.shadow_drawdown_6h_pause_minutes == 0)
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_1h_pause_minutes and risk.shadow_drawdown_6h_pause_minutes must be >= 1 when risk.shadow_killswitch_enabled=true"
        ));
    }

    if !config.shadow_rug_loss_return_threshold.is_finite()
        || config.shadow_rug_loss_return_threshold >= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_return_threshold must be finite and < 0, got {}",
            config.shadow_rug_loss_return_threshold
        ));
    }
    if config.shadow_rug_loss_window_minutes == 0
        || config.shadow_rug_loss_count_threshold == 0
        || config.shadow_rug_loss_rate_sample_size == 0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_window_minutes/count_threshold/rate_sample_size must be >= 1"
        ));
    }
    if !config.shadow_rug_loss_rate_threshold.is_finite()
        || config.shadow_rug_loss_rate_threshold <= 0.0
        || config.shadow_rug_loss_rate_threshold > 1.0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_rate_threshold must be finite and in (0, 1], got {}",
            config.shadow_rug_loss_rate_threshold
        ));
    }

    if config.shadow_infra_window_minutes == 0 || config.shadow_infra_lag_breach_minutes == 0 {
        return Err(anyhow!(
            "risk.shadow_infra_window_minutes and risk.shadow_infra_lag_breach_minutes must be >= 1"
        ));
    }
    if config.shadow_infra_lag_p95_threshold_ms == 0 {
        return Err(anyhow!(
            "risk.shadow_infra_lag_p95_threshold_ms must be >= 1"
        ));
    }
    if !config.shadow_infra_replaced_ratio_threshold.is_finite()
        || config.shadow_infra_replaced_ratio_threshold <= 0.0
        || config.shadow_infra_replaced_ratio_threshold > 1.0
    {
        return Err(anyhow!(
            "risk.shadow_infra_replaced_ratio_threshold must be finite and in (0, 1], got {}",
            config.shadow_infra_replaced_ratio_threshold
        ));
    }

    if config.shadow_universe_min_active_follow_wallets == 0
        || config.shadow_universe_min_eligible_wallets == 0
        || config.shadow_universe_breach_cycles == 0
    {
        return Err(anyhow!(
            "risk.shadow_universe_min_active_follow_wallets/min_eligible_wallets/breach_cycles must be >= 1"
        ));
    }
    Ok(())
}

fn is_production_like_execution_env(env: &str) -> bool {
    let env_norm = env.trim().to_ascii_lowercase();
    matches!(env_norm.as_str(), "prod" | "production")
        || env_norm.starts_with("prod-")
        || env_norm.starts_with("prod_")
        || env_norm.starts_with("production-")
        || env_norm.starts_with("production_")
}

fn validate_live_execution_policy_contract(
    execution: &ExecutionConfig,
    risk: &RiskConfig,
    env: &str,
) -> Result<()> {
    if !execution.enabled || !is_production_like_execution_env(env) {
        return Ok(());
    }
    if execution.pretrade_min_sol_reserve < 0.05 {
        return Err(anyhow!(
            "execution.enabled=true in production-like env {} requires execution.pretrade_min_sol_reserve >= 0.05 SOL",
            env
        ));
    }
    if execution.pretrade_max_fee_overhead_bps == 0 {
        return Err(anyhow!(
            "execution.enabled=true in production-like env {} requires execution.pretrade_max_fee_overhead_bps > 0 to keep fee-overhead breakeven policy active",
            env
        ));
    }
    if risk.execution_buy_cooldown_seconds == 0 {
        return Err(anyhow!(
            "execution.enabled=true in production-like env {} requires risk.execution_buy_cooldown_seconds >= 1",
            env
        ));
    }
    Ok(())
}

fn resolve_migrations_dir(config_path: &Path, configured_migrations_dir: &str) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }

    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }

        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }

    configured
}

fn init_tracing(log_level: &str, json: bool) -> Result<()> {
    if env::var_os(LEGACY_RUST_LOG_ENV).is_some() && env::var_os(APP_LOG_FILTER_ENV).is_none() {
        eprintln!(
            "ignoring RUST_LOG for copybot-app; use {APP_LOG_FILTER_ENV} or system.log_level instead"
        );
    }
    let filter = parse_app_log_env_filter(log_level)?;
    if json {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .json()
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .compact()
            .init();
    }
    Ok(())
}

fn parse_app_log_env_filter(default_log_level: &str) -> Result<EnvFilter> {
    let raw = match env::var(APP_LOG_FILTER_ENV) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(EnvFilter::new(default_log_level)),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(anyhow!("{APP_LOG_FILTER_ENV} must be valid UTF-8"));
        }
    };
    EnvFilter::try_new(raw.as_str())
        .map_err(|error| anyhow!("{APP_LOG_FILTER_ENV} is invalid: {}", error))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuyRiskBlockReason {
    HardStop,
    ExposureCap,
    TimedPause,
    Infra,
    Universe,
    FailClosed,
    OperatorEmergencyStop,
}

impl BuyRiskBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::HardStop => "risk_hard_stop",
            Self::ExposureCap => "risk_exposure_hard_cap",
            Self::TimedPause => "risk_timed_pause",
            Self::Infra => "risk_infra_stop",
            Self::Universe => "risk_universe_stop",
            Self::FailClosed => "risk_fail_closed",
            Self::OperatorEmergencyStop => "operator_emergency_stop",
        }
    }
}

#[derive(Debug)]
enum BuyRiskDecision {
    Allow,
    Blocked {
        reason: BuyRiskBlockReason,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InfraBlockKey {
    LagP95,
    NoIngestionProgress,
    ParserStall,
    ReplacedRatio,
    Rpc429,
    Rpc5xx,
}

#[derive(Debug, Clone)]
struct InfraBlockSignal {
    key: InfraBlockKey,
    reason: String,
}

#[derive(Debug, Default)]
struct ShadowRiskGuard {
    config: RiskConfig,
    ingestion_source: String,
    hard_stop_reason: Option<String>,
    hard_stop_clear_healthy_streak: u64,
    last_db_refresh_error: Option<String>,
    exposure_hard_blocked: bool,
    exposure_hard_detail: Option<String>,
    pause_until: Option<DateTime<Utc>>,
    pause_reason: Option<String>,
    soft_exposure_pause_latched: bool,
    soft_exposure_pause_until: Option<DateTime<Utc>>,
    soft_exposure_pause_reason: Option<String>,
    universe_breach_streak: u64,
    universe_blocked: bool,
    infra_samples: VecDeque<IngestionRuntimeSnapshot>,
    lag_breach_since: Option<DateTime<Utc>>,
    infra_block_key: Option<InfraBlockKey>,
    infra_candidate_key: Option<InfraBlockKey>,
    infra_candidate_streak: u64,
    infra_healthy_streak: u64,
    infra_block_reason: Option<String>,
    infra_last_event_at: Option<DateTime<Utc>>,
    last_db_refresh_at: Option<DateTime<Utc>>,
    last_fail_closed_log_at: Option<DateTime<Utc>>,
}

impl ShadowRiskGuard {
    fn new(config: RiskConfig) -> Self {
        Self::new_with_ingestion_source(config, "mock")
    }

    fn new_with_ingestion_source(config: RiskConfig, ingestion_source: impl Into<String>) -> Self {
        Self {
            config,
            ingestion_source: ingestion_source.into(),
            ..Self::default()
        }
    }

    fn restore_pause_from_store(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            return Ok(());
        }
        let restore_result = (|| -> Result<Vec<(String, DateTime<Utc>, String)>> {
            let pause_events = store.list_risk_events_by_type_desc("shadow_risk_pause")?;
            if pause_events.is_empty() {
                return Ok(Vec::new());
            }
            let cleared_events =
                store.list_risk_events_by_type_desc("shadow_risk_pause_cleared")?;

            let mut latest_clear_by_type: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            let mut wildcard_clear_rowid: Option<i64> = None;
            for cleared_event in cleared_events {
                let cleared_pause_type = cleared_event
                    .details_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                    .and_then(|value| {
                        value
                            .get("pause_type")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    });
                match cleared_pause_type {
                    Some(pause_type) => {
                        latest_clear_by_type
                            .entry(pause_type)
                            .or_insert(cleared_event.rowid);
                    }
                    None => {
                        wildcard_clear_rowid.get_or_insert(cleared_event.rowid);
                    }
                }
            }

            let mut restored_pause_types = std::collections::HashSet::new();
            let mut restored_pauses = Vec::new();
            for pause_event in pause_events {
                let pause_details_json = pause_event
                    .details_json
                    .as_deref()
                    .ok_or_else(|| anyhow!("shadow_risk_pause missing details_json"))?;
                let pause_details: serde_json::Value = serde_json::from_str(pause_details_json)
                    .context("failed to parse shadow_risk_pause details_json")?;
                let pause_type = pause_details
                    .get("pause_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("timed_pause")
                    .to_string();
                if !restored_pause_types.insert(pause_type.clone()) {
                    continue;
                }
                if wildcard_clear_rowid.is_some_and(|rowid| rowid > pause_event.rowid) {
                    continue;
                }
                if latest_clear_by_type
                    .get(pause_type.as_str())
                    .copied()
                    .is_some_and(|rowid| rowid > pause_event.rowid)
                {
                    continue;
                }
                let until_raw = pause_details
                    .get("until")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow!("shadow_risk_pause missing until"))?;
                let until = DateTime::parse_from_rfc3339(until_raw)
                    .context("failed to parse shadow_risk_pause until")?
                    .with_timezone(&Utc);
                if pause_type != "exposure_soft_cap" && until <= now {
                    continue;
                }
                let detail = pause_details
                    .get("detail")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        format!("restored_from_risk_event_rowid={}", pause_event.rowid)
                    });
                restored_pauses.push((
                    pause_type.clone(),
                    until,
                    format!("{pause_type}: {detail}; until={}", until.to_rfc3339()),
                ));
            }

            Ok(restored_pauses)
        })();
        match restore_result {
            Ok(restored_pauses) => {
                for (pause_type, until, reason) in restored_pauses {
                    if pause_type == "exposure_soft_cap" {
                        self.soft_exposure_pause_latched = true;
                        self.soft_exposure_pause_until = Some(until);
                        self.soft_exposure_pause_reason = Some(reason.clone());
                    } else if self.pause_until.map(|value| until > value).unwrap_or(true) {
                        self.pause_until = Some(until);
                        self.pause_reason = Some(reason.clone());
                    }
                    info!(
                        reason = %reason,
                        until = %until.to_rfc3339(),
                        "restored shadow risk timed pause from durable state"
                    );
                }
            }
            Err(error) => {
                if shadow_risk_pause_restore_error_requires_restart(&error) {
                    return Err(error).context(
                        "failed to restore shadow risk timed pause with fatal sqlite I/O",
                    );
                }
                warn!(error = %error, "failed to restore shadow risk timed pause");
            }
        }
        Ok(())
    }

    fn shadow_soft_exposure_cap_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_soft_exposure_cap_sol,
            "risk.shadow_soft_exposure_cap_sol",
        )
    }

    fn shadow_soft_exposure_resume_below_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_soft_exposure_resume_below_sol,
            "risk.shadow_soft_exposure_resume_below_sol",
        )
    }

    fn shadow_hard_exposure_cap_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_hard_exposure_cap_sol,
            "risk.shadow_hard_exposure_cap_sol",
        )
    }

    fn shadow_drawdown_stop_lamports(&self, stop_sol: f64, label: &str) -> Result<SignedLamports> {
        sol_to_signed_lamports_conservative(stop_sol, label)
    }

    fn build_universe_stop_details_json(
        &self,
        eligible_wallets: usize,
        active_follow_wallets: usize,
        discovery_output: Option<&DiscoveryTaskOutput>,
    ) -> Result<String> {
        let mut details = serde_json::Map::new();
        details.insert(
            "active_follow_wallets".to_string(),
            serde_json::Value::from(active_follow_wallets as u64),
        );
        details.insert(
            "eligible_wallets".to_string(),
            serde_json::Value::from(eligible_wallets as u64),
        );
        details.insert(
            "streak".to_string(),
            serde_json::Value::from(self.universe_breach_streak),
        );
        details.insert(
            "min_active_follow_wallets".to_string(),
            serde_json::Value::from(self.config.shadow_universe_min_active_follow_wallets),
        );
        details.insert(
            "min_eligible_wallets".to_string(),
            serde_json::Value::from(self.config.shadow_universe_min_eligible_wallets),
        );
        if let Some(discovery_output) = discovery_output {
            details.insert(
                "discovery_runtime_mode".to_string(),
                serde_json::Value::from(discovery_output.runtime_mode.as_str()),
            );
            if discovery_output_has_raw_window_cap_truncation_context(discovery_output) {
                details.insert(
                    "raw_window_cap_truncated".to_string(),
                    serde_json::Value::Bool(true),
                );
                details.insert(
                    "cap_truncation_deactivation_guard_active".to_string(),
                    serde_json::Value::Bool(
                        discovery_output.cap_truncation_deactivation_guard_active,
                    ),
                );
                if let Some(reason) = discovery_output.cap_truncation_deactivation_guard_reason {
                    details.insert(
                        "cap_truncation_deactivation_guard_reason".to_string(),
                        serde_json::Value::from(reason),
                    );
                }
                if let Some(started_at) =
                    discovery_output.cap_truncation_deactivation_guard_started_at
                {
                    details.insert(
                        "cap_truncation_deactivation_guard_started_at".to_string(),
                        serde_json::Value::from(started_at.to_rfc3339()),
                    );
                }
                if let Some(floor_ts) = discovery_output.cap_truncation_floor_ts_utc {
                    details.insert(
                        "cap_truncation_floor_ts".to_string(),
                        serde_json::Value::from(floor_ts.to_rfc3339()),
                    );
                }
                if let Some(signature) = discovery_output.cap_truncation_floor_signature.as_deref()
                {
                    details.insert(
                        "cap_truncation_floor_signature".to_string(),
                        serde_json::Value::from(signature),
                    );
                }
            }
        }
        serde_json::to_string(&details).context("failed to serialize universe stop details")
    }

    fn observe_discovery_cycle(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        eligible_wallets: usize,
        active_follow_wallets: usize,
        discovery_output: Option<&DiscoveryTaskOutput>,
    ) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            return Ok(());
        }
        let breached = (active_follow_wallets as u64)
            < self.config.shadow_universe_min_active_follow_wallets
            || (eligible_wallets as u64) < self.config.shadow_universe_min_eligible_wallets;
        if breached {
            self.universe_breach_streak = self.universe_breach_streak.saturating_add(1);
        } else {
            self.universe_breach_streak = 0;
        }
        let should_block =
            self.universe_breach_streak >= self.config.shadow_universe_breach_cycles.max(1);
        if should_block != self.universe_blocked {
            if should_block {
                let details_json = self.build_universe_stop_details_json(
                    eligible_wallets,
                    active_follow_wallets,
                    discovery_output,
                )?;
                let raw_window_cap_truncated = discovery_output
                    .is_some_and(discovery_output_has_raw_window_cap_truncation_context);
                warn!(
                    active_follow_wallets,
                    eligible_wallets,
                    streak = self.universe_breach_streak,
                    min_active_follow_wallets =
                        self.config.shadow_universe_min_active_follow_wallets,
                    min_eligible_wallets = self.config.shadow_universe_min_eligible_wallets,
                    raw_window_cap_truncated,
                    cap_truncation_deactivation_guard_active = raw_window_cap_truncated
                        && discovery_output.is_some_and(
                            |output| output.cap_truncation_deactivation_guard_active
                        ),
                    cap_truncation_deactivation_guard_reason = raw_window_cap_truncated
                        .then(|| {
                            discovery_output
                                .and_then(|output| output.cap_truncation_deactivation_guard_reason)
                        })
                        .flatten(),
                    cap_truncation_floor_ts = ?raw_window_cap_truncated
                        .then(|| discovery_output.and_then(|output| output.cap_truncation_floor_ts_utc))
                        .flatten(),
                    "shadow risk universe stop activated"
                );
                record_shadow_risk_state_event_or_warn(
                    store,
                    "shadow_risk_universe_stop",
                    "warn",
                    now,
                    &details_json,
                    "failed to persist shadow risk universe stop event with fatal sqlite I/O",
                )?;
            } else {
                info!(
                    active_follow_wallets,
                    eligible_wallets, "shadow risk universe stop cleared"
                );
                record_shadow_risk_state_event_or_warn(
                    store,
                    "shadow_risk_universe_cleared",
                    "info",
                    now,
                    "{\"state\":\"cleared\"}",
                    "failed to persist shadow risk universe clear event with fatal sqlite I/O",
                )?;
            }
            self.universe_blocked = should_block;
        }
        Ok(())
    }

    fn observe_ingestion_snapshot(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        snapshot: Option<IngestionRuntimeSnapshot>,
    ) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            return Ok(());
        }
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let sample_ts = snapshot.ts_utc;
        let min_interval = chrono::Duration::seconds(RISK_INFRA_SAMPLE_MIN_SECONDS.max(1));
        let should_push = self
            .infra_samples
            .back()
            .map(|last| sample_ts - last.ts_utc >= min_interval)
            .unwrap_or(true);
        if should_push {
            self.infra_samples.push_back(snapshot);
        } else if let Some(last) = self.infra_samples.back_mut() {
            *last = snapshot;
        }

        let retention_minutes = self
            .config
            .shadow_infra_window_minutes
            .max(self.config.shadow_infra_lag_breach_minutes)
            .max(20)
            .saturating_mul(2);
        let cutoff = sample_ts - chrono::Duration::minutes(retention_minutes as i64);
        while self
            .infra_samples
            .front()
            .map(|sample| sample.ts_utc < cutoff)
            .unwrap_or(false)
        {
            self.infra_samples.pop_front();
        }

        if snapshot.ingestion_lag_ms_p95 > self.config.shadow_infra_lag_p95_threshold_ms {
            if self.lag_breach_since.is_none() {
                self.lag_breach_since = Some(sample_ts);
            }
        } else {
            self.lag_breach_since = None;
        }

        let new_signal = self.compute_infra_block_signal(sample_ts);
        match new_signal {
            Some(signal) => {
                self.infra_healthy_streak = 0;
                if self.infra_candidate_key == Some(signal.key) {
                    self.infra_candidate_streak = self.infra_candidate_streak.saturating_add(1);
                } else {
                    self.infra_candidate_key = Some(signal.key);
                    self.infra_candidate_streak = 1;
                }

                if self.infra_block_key == Some(signal.key) {
                    self.infra_block_reason = Some(signal.reason);
                    return Ok(());
                }

                if self.infra_block_key.is_some() {
                    if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                        self.infra_block_key = Some(signal.key);
                        self.infra_block_reason = Some(signal.reason);
                    }
                    return Ok(());
                }

                if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                    let reason = signal.reason;
                    warn!(reason = %reason, "shadow risk infra stop activated");
                    let details_json = self.build_infra_stop_details_json(&reason);
                    if self.should_emit_infra_event(now) {
                        record_shadow_risk_state_event_or_warn(
                            store,
                            "shadow_risk_infra_stop",
                            "warn",
                            now,
                            &details_json,
                            "failed to persist shadow risk infra stop event with fatal sqlite I/O",
                        )?;
                    }
                    self.infra_block_key = Some(signal.key);
                    self.infra_block_reason = Some(reason);
                }
            }
            None => {
                self.infra_candidate_key = None;
                self.infra_candidate_streak = 0;
                if self.infra_block_key.is_none() {
                    return Ok(());
                }
                self.infra_healthy_streak = self.infra_healthy_streak.saturating_add(1);
                if self.infra_healthy_streak < RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
                    return Ok(());
                }
                self.infra_healthy_streak = 0;
                info!("shadow risk infra stop cleared");
                if self.should_emit_infra_event(now) {
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_infra_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                        "failed to persist shadow risk infra clear event with fatal sqlite I/O",
                    )?;
                }
                self.infra_block_key = None;
                self.infra_block_reason = None;
            }
        }
        Ok(())
    }

    fn can_open_buy(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        pause_new_trades_on_outage: bool,
    ) -> BuyRiskDecision {
        if !self.config.shadow_killswitch_enabled {
            return BuyRiskDecision::Allow;
        }

        if let Err(error) = self.maybe_refresh_db_state(store, now) {
            let fail_closed_detail = format!("risk_check_error: {error}");
            let should_log = self.on_risk_refresh_error(now);
            if should_log {
                warn!(error = %error, "shadow risk fail-closed activated");
                let details_json = format!(
                    "{{\"error\":\"{}\"}}",
                    sanitize_json_value(&error.to_string())
                );
                if let Err(event_error) =
                    persist_shadow_risk_fail_closed_event_or_warn(store, now, &details_json)
                {
                    return BuyRiskDecision::Blocked {
                        reason: BuyRiskBlockReason::FailClosed,
                        detail: format!(
                            "{fail_closed_detail}; fail_closed_event_error: {event_error:#}"
                        ),
                    };
                }
            }
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail: fail_closed_detail,
            };
        }

        if let Some(reason) = self.hard_stop_reason.as_deref() {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                detail: reason.to_string(),
            };
        }

        if self.exposure_hard_blocked {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail: self
                    .exposure_hard_detail
                    .clone()
                    .unwrap_or_else(|| "exposure_hard_cap_active".to_string()),
            };
        }

        if let Some(until) = self.pause_until {
            if now < until {
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::TimedPause,
                    detail: self
                        .pause_reason
                        .clone()
                        .unwrap_or_else(|| format!("paused_until={}", until.to_rfc3339())),
                };
            }
            if let Err(error) = self.clear_pause(store, now) {
                let fail_closed_detail = format!("timed_pause_clear_error: {error}");
                let should_log = self.on_risk_refresh_error(now);
                if should_log {
                    warn!(error = %error, "shadow risk fail-closed activated during timed pause clear");
                    let details_json = format!(
                        "{{\"error\":\"{}\"}}",
                        sanitize_json_value(&error.to_string())
                    );
                    if let Err(event_error) =
                        persist_shadow_risk_fail_closed_event_or_warn(store, now, &details_json)
                    {
                        return BuyRiskDecision::Blocked {
                            reason: BuyRiskBlockReason::FailClosed,
                            detail: format!(
                                "{fail_closed_detail}; fail_closed_event_error: {event_error:#}"
                            ),
                        };
                    }
                }
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::FailClosed,
                    detail: fail_closed_detail,
                };
            }
        }

        if self.soft_exposure_pause_latched {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail: self.soft_exposure_pause_reason.clone().unwrap_or_else(|| {
                    self.soft_exposure_pause_until
                        .map(|until| {
                            format!(
                                "exposure_soft_cap: latched_until_recovery_below_resume_threshold; initial_until={}",
                                until.to_rfc3339()
                            )
                        })
                        .unwrap_or_else(|| {
                            "exposure_soft_cap: latched_until_recovery_below_resume_threshold"
                                .to_string()
                        })
                }),
            };
        }

        if self.universe_blocked {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                detail: format!("universe_breach_streak={}", self.universe_breach_streak),
            };
        }

        if pause_new_trades_on_outage {
            if let Some(reason) = self.infra_block_reason.as_deref() {
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::Infra,
                    detail: reason.to_string(),
                };
            }
        }

        BuyRiskDecision::Allow
    }

    fn on_risk_refresh_error(&mut self, now: DateTime<Utc>) -> bool {
        self.hard_stop_clear_healthy_streak = 0;
        let should_log = self
            .last_fail_closed_log_at
            .map(|logged_at| {
                now - logged_at
                    >= chrono::Duration::seconds(RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS.max(1))
            })
            .unwrap_or(true);
        if should_log {
            self.last_fail_closed_log_at = Some(now);
        }
        should_log
    }

    fn maybe_refresh_db_state(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            self.last_db_refresh_error = None;
            return Ok(());
        }

        if let Some(last_refresh) = self.last_db_refresh_at {
            if now - last_refresh < chrono::Duration::seconds(RISK_DB_REFRESH_MIN_SECONDS.max(1)) {
                if let Some(cached_error) = self.last_db_refresh_error.as_deref() {
                    return Err(anyhow::anyhow!(
                        "risk refresh cached error (within throttle window): {}",
                        cached_error
                    ));
                }
                return Ok(());
            }
        }
        self.last_db_refresh_at = Some(now);

        let refresh_result = (|| -> Result<()> {
            let (_, pnl_1h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(1))?;
            let (_, pnl_6h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(6))?;
            let (_, pnl_24h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(24))?;
            let drawdown_1h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_1h_stop_sol,
                "risk.shadow_drawdown_1h_stop_sol",
            )?;
            let drawdown_6h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_6h_stop_sol,
                "risk.shadow_drawdown_6h_stop_sol",
            )?;
            let drawdown_24h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_24h_stop_sol,
                "risk.shadow_drawdown_24h_stop_sol",
            )?;
            let shadow_soft_exposure_cap_lamports = self.shadow_soft_exposure_cap_lamports()?;
            let shadow_soft_exposure_resume_below_lamports =
                self.shadow_soft_exposure_resume_below_lamports()?;
            let shadow_hard_exposure_cap_lamports = self.shadow_hard_exposure_cap_lamports()?;

            let rug_window_start = now
                - chrono::Duration::minutes(
                    self.config.shadow_rug_loss_window_minutes.max(1) as i64
                );
            let rug_count_since = store.shadow_rug_loss_count_since(
                rug_window_start,
                self.config.shadow_rug_loss_return_threshold,
            )?;
            let (_, rug_sample_total, rug_rate_recent) = store.shadow_rug_loss_rate_recent(
                rug_window_start,
                self.config.shadow_rug_loss_rate_sample_size.max(1),
                self.config.shadow_rug_loss_return_threshold,
            )?;

            let hard_stop_breach = if pnl_24h_lamports <= drawdown_24h_stop_lamports {
                Some((
                    "drawdown_24h",
                    format!(
                        "pnl_24h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_24h_lamports),
                        signed_lamports_to_sol(drawdown_24h_stop_lamports)
                    ),
                ))
            } else if rug_count_since >= self.config.shadow_rug_loss_count_threshold
                || rug_rate_recent > self.config.shadow_rug_loss_rate_threshold
            {
                Some((
                    "rug_loss",
                    format!(
                        "rug_count_since={} sample_total={} rug_rate_recent={:.4}",
                        rug_count_since, rug_sample_total, rug_rate_recent
                    ),
                ))
            } else {
                None
            };

            if let Some((stop_type, detail)) = hard_stop_breach {
                self.hard_stop_clear_healthy_streak = 0;
                self.activate_hard_stop(store, now, stop_type, detail)?;
                return Ok(());
            }

            if self.hard_stop_reason.is_some() {
                self.hard_stop_clear_healthy_streak = self
                    .hard_stop_clear_healthy_streak
                    .saturating_add(1)
                    .min(HARD_STOP_CLEAR_HEALTHY_REFRESHES);
                if self.hard_stop_clear_healthy_streak < HARD_STOP_CLEAR_HEALTHY_REFRESHES {
                    return Ok(());
                }
                self.clear_hard_stop(store, now)?;
            } else {
                self.hard_stop_clear_healthy_streak = 0;
            }

            let exposure_lamports = store.shadow_risk_open_notional_lamports()?;
            let exposure_blocked_now = exposure_lamports >= shadow_hard_exposure_cap_lamports;
            let exposure_detail = format!(
                "risk_open_notional_sol={:.6} hard_cap={:.6}",
                lamports_to_sol(exposure_lamports),
                lamports_to_sol(shadow_hard_exposure_cap_lamports)
            );
            if exposure_blocked_now != self.exposure_hard_blocked {
                if exposure_blocked_now {
                    self.exposure_hard_blocked = true;
                    self.exposure_hard_detail = Some(exposure_detail.clone());
                    warn!(
                        detail = %exposure_detail,
                        "shadow risk exposure hard cap active"
                    );
                    let details_json = format!(
                        "{{\"state\":\"active\",\"detail\":\"{}\"}}",
                        sanitize_json_value(&exposure_detail)
                    );
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_exposure_hard_cap",
                        "warn",
                        now,
                        &details_json,
                        "failed to persist shadow risk exposure hard cap event with fatal sqlite I/O",
                    )?;
                } else {
                    info!("shadow risk exposure hard cap cleared");
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_exposure_hard_cap_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                        "failed to persist shadow risk exposure hard cap clear event with fatal sqlite I/O",
                    )?;
                    self.exposure_hard_blocked = false;
                    self.exposure_hard_detail = None;
                }
            } else if exposure_blocked_now {
                self.exposure_hard_detail = Some(exposure_detail);
            }

            if exposure_lamports >= shadow_hard_exposure_cap_lamports {
                return Ok(());
            }
            if self.soft_exposure_pause_latched
                && self.pause_until.is_none()
                && exposure_lamports < shadow_soft_exposure_resume_below_lamports
            {
                self.clear_soft_exposure_pause(store, now)?;
            } else if !self.soft_exposure_pause_latched
                && self.pause_until.is_none()
                && exposure_lamports >= shadow_soft_exposure_cap_lamports
            {
                self.activate_soft_exposure_pause(
                    store,
                    now,
                    chrono::Duration::minutes(self.config.shadow_soft_pause_minutes.max(1) as i64),
                    exposure_lamports,
                    shadow_soft_exposure_cap_lamports,
                    shadow_soft_exposure_resume_below_lamports,
                )?;
            }
            if pnl_6h_lamports <= drawdown_6h_stop_lamports {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_6h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_6h",
                    format!(
                        "pnl_6h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_6h_lamports),
                        signed_lamports_to_sol(drawdown_6h_stop_lamports)
                    ),
                )?;
            }
            if pnl_1h_lamports <= drawdown_1h_stop_lamports {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_1h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_1h",
                    format!(
                        "pnl_1h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_1h_lamports),
                        signed_lamports_to_sol(drawdown_1h_stop_lamports)
                    ),
                )?;
            }

            Ok(())
        })();

        match refresh_result {
            Ok(()) => {
                self.last_db_refresh_error = None;
                Ok(())
            }
            Err(error) => {
                self.last_db_refresh_error = Some(error.to_string());
                Err(error)
            }
        }
    }

    fn compute_infra_block_reason(&self, now: DateTime<Utc>) -> Option<String> {
        self.compute_infra_block_signal(now)
            .map(|signal| signal.reason)
    }

    fn yellowstone_output_queue_reason_context(
        &self,
        snapshot: &IngestionRuntimeSnapshot,
    ) -> Option<String> {
        if !self.uses_yellowstone_ingestion() || snapshot.yellowstone_output_queue_capacity == 0 {
            return None;
        }
        Some(format!(
            "yellowstone_output_queue_depth={} yellowstone_output_queue_capacity={} yellowstone_output_queue_fill_ratio={:.4} yellowstone_output_oldest_age_ms={}",
            snapshot.yellowstone_output_queue_depth,
            snapshot.yellowstone_output_queue_capacity,
            snapshot.yellowstone_output_queue_fill_ratio,
            snapshot.yellowstone_output_oldest_age_ms
        ))
    }

    fn enrich_infra_reason_with_yellowstone_queue_context(
        &self,
        snapshot: &IngestionRuntimeSnapshot,
        reason: String,
    ) -> String {
        match self.yellowstone_output_queue_reason_context(snapshot) {
            Some(context) => format!("{reason} {context}"),
            None => reason,
        }
    }

    fn build_infra_stop_details_json(&self, reason: &str) -> String {
        let mut details = serde_json::Map::new();
        details.insert(
            "reason".to_string(),
            serde_json::Value::String(reason.to_string()),
        );
        if self.uses_yellowstone_ingestion() {
            if let Some(snapshot) = self.infra_samples.back() {
                if snapshot.yellowstone_output_queue_capacity > 0 {
                    details.insert(
                        "yellowstone_output_queue_depth".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_depth),
                    );
                    details.insert(
                        "yellowstone_output_queue_capacity".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_capacity),
                    );
                    details.insert(
                        "yellowstone_output_queue_fill_ratio".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_fill_ratio),
                    );
                    details.insert(
                        "yellowstone_output_oldest_age_ms".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_oldest_age_ms),
                    );
                }
            }
        }
        serde_json::Value::Object(details).to_string()
    }

    fn compute_infra_block_signal(&self, now: DateTime<Utc>) -> Option<InfraBlockSignal> {
        if self.infra_samples.is_empty() {
            return None;
        }
        let latest = self.infra_samples.back().copied()?;

        if let Some(started_at) = self.lag_breach_since {
            if now - started_at
                >= chrono::Duration::minutes(
                    self.config.shadow_infra_lag_breach_minutes.max(1) as i64
                )
            {
                return Some(InfraBlockSignal {
                    key: InfraBlockKey::LagP95,
                    reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                        &latest,
                        format!(
                            "lag_p95_over_threshold_for={}m threshold_ms={}",
                            self.config.shadow_infra_lag_breach_minutes,
                            self.config.shadow_infra_lag_p95_threshold_ms
                        ),
                    ),
                });
            }
        }

        let window_start =
            now - chrono::Duration::minutes(self.config.shadow_infra_window_minutes.max(1) as i64);
        let has_full_window_coverage = self
            .infra_samples
            .front()
            .map(|sample| sample.ts_utc <= window_start)
            .unwrap_or(false);
        let baseline = self
            .infra_samples
            .iter()
            .copied()
            .find(|sample| sample.ts_utc >= window_start)
            .unwrap_or_else(|| self.infra_samples.front().copied().unwrap_or(latest));

        let delta_enqueued = latest
            .ws_notifications_enqueued
            .saturating_sub(baseline.ws_notifications_enqueued);
        let delta_replaced = latest
            .ws_notifications_replaced_oldest
            .saturating_sub(baseline.ws_notifications_replaced_oldest);
        let delta_grpc_transaction_updates_total = latest
            .grpc_transaction_updates_total
            .saturating_sub(baseline.grpc_transaction_updates_total);
        let delta_parse_rejected_total = latest
            .parse_rejected_total
            .saturating_sub(baseline.parse_rejected_total);
        let delta_grpc_decode_errors = latest
            .grpc_decode_errors
            .saturating_sub(baseline.grpc_decode_errors);
        let delta_rpc_429 = latest.rpc_429.saturating_sub(baseline.rpc_429);
        let delta_rpc_5xx = latest.rpc_5xx.saturating_sub(baseline.rpc_5xx);

        const INFRA_PARSER_STALL_MIN_TX_UPDATES: u64 = 25;
        const INFRA_PARSER_STALL_ERROR_RATIO_THRESHOLD: f64 = 0.95;

        if has_full_window_coverage
            && delta_enqueued == 0
            && delta_replaced == 0
            && delta_grpc_transaction_updates_total == 0
            && delta_rpc_429 == 0
            && delta_rpc_5xx == 0
        {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::NoIngestionProgress,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "no_ingestion_progress_for={}m",
                        self.config.shadow_infra_window_minutes.max(1)
                    ),
                ),
            });
        }

        if has_full_window_coverage
            && delta_enqueued == 0
            && delta_grpc_transaction_updates_total >= INFRA_PARSER_STALL_MIN_TX_UPDATES
        {
            let parser_errors_delta =
                delta_parse_rejected_total.saturating_add(delta_grpc_decode_errors);
            if parser_errors_delta > 0 {
                let parser_error_ratio =
                    parser_errors_delta as f64 / delta_grpc_transaction_updates_total as f64;
                if parser_error_ratio >= INFRA_PARSER_STALL_ERROR_RATIO_THRESHOLD {
                    return Some(InfraBlockSignal {
                        key: InfraBlockKey::ParserStall,
                        reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                            &latest,
                            format!(
                                "parser_stall_for={}m tx_updates_delta={} parser_errors_delta={} error_ratio={:.4}",
                                self.config.shadow_infra_window_minutes.max(1),
                                delta_grpc_transaction_updates_total,
                                parser_errors_delta,
                                parser_error_ratio
                            ),
                        ),
                    });
                }
            }
        }

        if delta_enqueued > 0 {
            let replaced_ratio = delta_replaced as f64 / delta_enqueued as f64;
            if replaced_ratio > self.config.shadow_infra_replaced_ratio_threshold {
                if !self.uses_yellowstone_ingestion() {
                    return Some(InfraBlockSignal {
                        key: InfraBlockKey::ReplacedRatio,
                        reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                            &latest,
                            format!(
                                "replaced_ratio={:.4} delta_replaced={} delta_enqueued={}",
                                replaced_ratio, delta_replaced, delta_enqueued
                            ),
                        ),
                    });
                }
            }
        }
        if delta_rpc_429 >= self.config.shadow_infra_rpc429_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc429,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "rpc_429_delta={} threshold={}",
                        delta_rpc_429, self.config.shadow_infra_rpc429_delta_threshold
                    ),
                ),
            });
        }
        if delta_rpc_5xx >= self.config.shadow_infra_rpc5xx_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc5xx,
                reason: self.enrich_infra_reason_with_yellowstone_queue_context(
                    &latest,
                    format!(
                        "rpc_5xx_delta={} threshold={}",
                        delta_rpc_5xx, self.config.shadow_infra_rpc5xx_delta_threshold
                    ),
                ),
            });
        }
        None
    }

    fn uses_yellowstone_ingestion(&self) -> bool {
        self.ingestion_source == "yellowstone_grpc"
    }

    fn activate_hard_stop(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        stop_type: &str,
        detail: String,
    ) -> Result<()> {
        self.hard_stop_clear_healthy_streak = 0;
        if self.hard_stop_reason.is_some() {
            return Ok(());
        }
        let reason = format!("{stop_type}: {detail}");
        self.hard_stop_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk hard stop activated");
        let details_json = format!(
            "{{\"stop_type\":\"{}\",\"detail\":\"{}\"}}",
            sanitize_json_value(stop_type),
            sanitize_json_value(&detail)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_hard_stop",
            "error",
            now,
            &details_json,
            "failed to persist shadow risk hard stop event with fatal sqlite I/O",
        )
    }

    fn clear_hard_stop(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let Some(previous_reason) = self.hard_stop_reason.clone() else {
            return Ok(());
        };
        let details_json = format!(
            "{{\"state\":\"cleared\",\"previous_reason\":\"{}\"}}",
            sanitize_json_value(&previous_reason)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_hard_stop_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk hard stop clear event with fatal sqlite I/O",
        )?;
        self.hard_stop_clear_healthy_streak = 0;
        self.hard_stop_reason = None;
        info!(
            previous_reason = %previous_reason,
            "shadow risk hard stop cleared"
        );
        Ok(())
    }

    fn activate_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        pause_type: &str,
        detail: String,
    ) -> Result<()> {
        let duration = if duration <= chrono::Duration::zero() {
            chrono::Duration::minutes(1)
        } else {
            duration
        };
        let until = now + duration;
        let should_update = self.pause_until.map(|value| until > value).unwrap_or(true);
        if !should_update {
            return Ok(());
        }
        self.pause_until = Some(until);
        let reason = format!("{pause_type}: {detail}; until={}", until.to_rfc3339());
        self.pause_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk timed pause activated");
        let details_json = format!(
            "{{\"pause_type\":\"{}\",\"detail\":\"{}\",\"until\":\"{}\"}}",
            sanitize_json_value(pause_type),
            sanitize_json_value(&detail),
            until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause",
            "warn",
            now,
            &details_json,
            "failed to persist shadow risk timed pause event with fatal sqlite I/O",
        )
    }

    fn activate_soft_exposure_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        exposure_lamports: Lamports,
        soft_cap_lamports: Lamports,
        resume_below_lamports: Lamports,
    ) -> Result<()> {
        let duration = if duration <= chrono::Duration::zero() {
            chrono::Duration::minutes(1)
        } else {
            duration
        };
        let until = now + duration;
        self.soft_exposure_pause_latched = true;
        self.soft_exposure_pause_until = Some(until);
        let detail = format!(
            "risk_open_notional_sol={:.6} >= soft_cap={:.6}; resume_below={:.6}",
            lamports_to_sol(exposure_lamports),
            lamports_to_sol(soft_cap_lamports),
            lamports_to_sol(resume_below_lamports)
        );
        let reason = format!("exposure_soft_cap: {detail}; until={}", until.to_rfc3339());
        self.soft_exposure_pause_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk soft exposure pause activated");
        let details_json = format!(
            "{{\"pause_type\":\"exposure_soft_cap\",\"detail\":\"{}\",\"until\":\"{}\",\"resume_below_sol\":{:.9}}}",
            sanitize_json_value(&detail),
            until.to_rfc3339(),
            lamports_to_sol(resume_below_lamports)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause",
            "warn",
            now,
            &details_json,
            "failed to persist shadow risk soft exposure pause event with fatal sqlite I/O",
        )
    }

    fn clear_pause(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let Some(previous_until) = self.pause_until else {
            self.pause_reason = None;
            return Ok(());
        };
        let previous_reason = self
            .pause_reason
            .clone()
            .unwrap_or_else(|| format!("paused_until={}", previous_until.to_rfc3339()));
        let cleared_pause_type = previous_reason
            .split_once(':')
            .map(|(pause_type, _)| pause_type.trim())
            .filter(|pause_type| !pause_type.is_empty())
            .unwrap_or("timed_pause");
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"{}\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(cleared_pause_type),
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk timed pause clear event with fatal sqlite I/O",
        )?;
        self.pause_until = None;
        self.pause_reason = None;
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk timed pause cleared"
        );
        Ok(())
    }

    fn clear_soft_exposure_pause(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let Some(previous_until) = self.soft_exposure_pause_until else {
            self.soft_exposure_pause_latched = false;
            self.soft_exposure_pause_reason = None;
            return Ok(());
        };
        let previous_reason = self.soft_exposure_pause_reason.clone().unwrap_or_else(|| {
            format!(
                "exposure_soft_cap: paused_until={}",
                previous_until.to_rfc3339()
            )
        });
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"exposure_soft_cap\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk soft exposure pause clear event with fatal sqlite I/O",
        )?;
        self.soft_exposure_pause_latched = false;
        self.soft_exposure_pause_until = None;
        self.soft_exposure_pause_reason = None;
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk soft exposure pause cleared"
        );
        Ok(())
    }

    fn should_emit_infra_event(&mut self, now: DateTime<Utc>) -> bool {
        let allow = self
            .infra_last_event_at
            .map(|last| {
                now - last >= chrono::Duration::seconds(RISK_INFRA_EVENT_THROTTLE_SECONDS.max(1))
            })
            .unwrap_or(true);
        if allow {
            self.infra_last_event_at = Some(now);
        }
        allow
    }
}

fn sanitize_json_value(value: &str) -> String {
    let json_string = serde_json::Value::String(value.to_string()).to_string();
    json_string[1..json_string.len().saturating_sub(1)].to_string()
}

async fn run_app_loop(
    store: SqliteStore,
    mut ingestion: IngestionService,
    discovery: DiscoveryService,
    shadow: ShadowService,
    execution_runtime: ExecutionRuntime,
    risk_config: RiskConfig,
    sqlite_path: String,
    heartbeat_seconds: u64,
    history_retention_config: copybot_config::HistoryRetentionConfig,
    recent_raw_journal_config: copybot_config::RecentRawJournalConfig,
    discovery_fetch_refresh_seconds: u64,
    discovery_refresh_seconds: u64,
    observed_swaps_retention_days: u32,
    discovery_scoring_retention_days: u32,
    discovery_scoring_writes_enabled: bool,
    discovery_aggregate_write_config: DiscoveryAggregateWriteConfig,
    ingestion_source: String,
    shadow_refresh_seconds: u64,
    shadow_max_signal_lag_seconds: u64,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
    pause_new_trades_on_outage: bool,
    alert_dispatcher: Option<AlertDispatcher>,
) -> Result<()> {
    info!(
        heartbeat_seconds,
        discovery_fetch_refresh_seconds,
        discovery_refresh_seconds,
        shadow_refresh_seconds,
        sqlite_path = %sqlite_path,
        "app runtime loop started"
    );
    let execution_runtime = Arc::new(execution_runtime);
    let mut interval = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut execution_interval = time::interval(Duration::from_millis(
        execution_runtime.poll_interval_ms().max(100),
    ));
    let mut risk_refresh_interval = time::interval(Duration::from_secs(
        RISK_DB_REFRESH_MIN_SECONDS.max(1) as u64,
    ));
    let mut discovery_interval =
        time::interval(Duration::from_secs(discovery_fetch_refresh_seconds.max(1)));
    let mut shadow_interval = time::interval(Duration::from_secs(shadow_refresh_seconds.max(10)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    execution_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    risk_refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    discovery_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    shadow_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let initial_active_wallets = store
        .list_active_follow_wallets()
        .context("failed to load active follow wallets")?;
    let startup_gate_now = Utc::now();
    validate_bootstrap_degraded_execution_contract(&store, &execution_runtime)?;
    let runtime_publication_truth =
        startup_runtime_publication_truth(&discovery, &store, startup_gate_now)
            .context("failed to load startup published follow universe")?;
    let (initial_follow_snapshot, recovered_active_wallets, mut shadow_strategy_fail_closed) =
        startup_follow_snapshot_from_publication_truth(
            initial_active_wallets,
            runtime_publication_truth.as_ref(),
        );
    let mut follow_snapshot = Arc::new(initial_follow_snapshot);
    let follow_event_retention =
        follow_event_retention_duration(shadow_max_signal_lag_seconds, discovery_refresh_seconds);
    let mut open_shadow_lots = if shadow_strategy_fail_closed {
        HashSet::new()
    } else {
        store
            .list_shadow_open_pairs()
            .context("failed to load open shadow lot index")?
    };
    let stale_lot_max_hold_hours = risk_config.max_hold_hours;
    let stale_lot_terminal_zero_price_hours =
        risk_config.shadow_stale_close_terminal_zero_price_hours;
    let stale_lot_recovery_zero_price_enabled =
        risk_config.shadow_stale_close_recovery_zero_price_enabled;
    let mut shadow_risk_guard =
        ShadowRiskGuard::new_with_ingestion_source(risk_config, ingestion_source);
    shadow_risk_guard.restore_pause_from_store(&store, Utc::now())?;
    let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut app_consumer_loop_telemetry = AppConsumerLoopTelemetry::default();
    let mut recent_swap_signatures: HashSet<String> = HashSet::new();
    let mut recent_swap_signature_order: VecDeque<String> = VecDeque::new();
    let mut pending_irrelevant_swaps: VecDeque<PendingIrrelevantObservedSwap> = VecDeque::new();
    let mut discovery_critical_target_buy_mints = HashSet::new();
    refresh_discovery_critical_target_buy_mints_or_warn(
        &store,
        &mut discovery_critical_target_buy_mints,
    )?;
    let mut discovery_handle: Option<JoinHandle<Result<DiscoveryTaskOutput>>> = None;
    let mut discovery_catch_up_pending = false;
    let mut shadow_scheduler = ShadowScheduler::new();
    let mut execution_handle: Option<JoinHandle<Result<ExecutionBatchReport>>> = None;
    let recent_raw_journal_writer_config = ObservedSwapRecentRawJournalConfig {
        sqlite_path: recent_raw_journal_config.path.clone(),
        retention_days: observed_swaps_retention_days.max(1).saturating_add(
            recent_raw_journal_config
                .retention_safety_buffer_days
                .max(1),
        ),
        writer_queue_capacity_batches: recent_raw_journal_config
            .writer_queue_capacity_batches
            .max(1),
    };
    let observed_swap_writer = ObservedSwapWriter::start_with_recent_raw_journal(
        sqlite_path.clone(),
        discovery_scoring_writes_enabled,
        discovery_aggregate_write_config,
        Some(recent_raw_journal_writer_config),
    )
    .context("failed to start observed swap writer")?;
    let latest_ingestion_runtime_snapshot = Arc::new(Mutex::new(ingestion.runtime_snapshot()));
    let observed_swap_retention_runtime_health = ObservedSwapRetentionRuntimeHealthHandle::new(
        observed_swap_writer.health_handle(),
        Arc::clone(&latest_ingestion_runtime_snapshot),
    );
    let observed_swap_retention_config = ObservedSwapRetentionConfig::production(
        observed_swaps_retention_days,
        discovery_scoring_retention_days,
        discovery_scoring_writes_enabled,
    );
    let observed_swap_retention_sweep_interval =
        Duration::from_secs(OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL.as_secs().max(1));
    let app_started_at = StdInstant::now();
    let mut last_observed_swap_retention_sweep = app_started_at;
    let history_retention = HistoryRetentionRunner::new(history_retention_config);
    let history_retention_sweep_interval = Duration::from_secs(history_retention.sweep_seconds());
    let mut last_history_retention_sweep = app_started_at;
    let mut last_sqlite_contention_snapshot = sqlite_contention_snapshot();
    let mut last_history_retention_skip_reason_key: Option<&'static str> = None;
    let mut last_observed_swap_retention_skip_reason_key: Option<&'static str> = None;
    let mut operator_emergency_stop = OperatorEmergencyStop::from_env();
    let mut execution_emergency_stop_active_logged = false;
    let mut execution_hard_stop_pause_logged = false;
    let mut execution_shadow_risk_pause_logged_reason: Option<String> = None;
    let mut execution_outage_pause_logged = false;
    let mut observed_swap_retention_handle: Option<
        JoinHandle<Result<ObservedSwapRetentionMaintenanceSummary>>,
    > = None;
    let mut ingestion_error_streak: u32 = 0;
    let mut ingestion_backoff_until: Option<time::Instant> = None;
    operator_emergency_stop.refresh(&store, Utc::now())?;
    info!(
        path = %operator_emergency_stop.path().display(),
        pause_new_trades_on_outage,
        "buy gate controls initialized"
    );
    if execution_runtime.is_enabled() {
        info!(
            poll_interval_ms = execution_runtime.poll_interval_ms(),
            "execution runtime enabled"
        );
    } else {
        info!("execution runtime disabled");
    }

    if let Some(RuntimePublicationTruthResolution::Recent(truth)) =
        runtime_publication_truth.as_ref()
    {
        info!(
            active_follow_wallets = truth.published_wallet_ids.len(),
            "recent published follow universe loaded for startup runtime"
        );
    } else if let Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) =
        runtime_publication_truth.as_ref()
    {
        info!(
            bootstrap_degraded_active_follow_wallets = truth.published_wallet_ids.len(),
            "startup loaded explicit bootstrap-degraded publication truth; shadow and live execution remain fail-closed until fresh raw truth publishes"
        );
    } else if recovered_active_wallets > 0 {
        info!(
            recovered_active_follow_wallets = recovered_active_wallets,
            "startup has no recent published follow universe; recovered historical wallets stay out of runtime until discovery publishes fresh or degraded runtime truth"
        );
    } else if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }

    loop {
        operator_emergency_stop.refresh(&store, Utc::now())?;

        if shadow_scheduler.shadow_scheduler_needs_reset {
            if shadow_scheduler.shadow_workers.is_empty() {
                shadow_scheduler.inflight_shadow_keys.clear();
                shadow_scheduler.rebuild_ready_queue();
                shadow_scheduler.shadow_scheduler_needs_reset = false;
                warn!("shadow scheduler recovered after worker join error");
            }
        } else {
            shadow_scheduler.spawn_shadow_tasks_up_to_limit(
                &sqlite_path,
                &shadow,
                SHADOW_WORKER_POOL_SIZE,
            );
        }
        shadow_scheduler.release_held_shadow_sells(
            &open_shadow_lots,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
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
                &store,
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

        tokio::select! {
            discovery_result = async {
                if let Some(handle) = &mut discovery_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if discovery_handle.is_some() => {
                discovery_handle = None;
                match discovery_result.expect("guard ensures discovery task exists") {
                    Ok(Ok(discovery_output)) => {
                        let active_follow_wallets_before_clear = follow_snapshot.active.len();
                        let fail_closed_runtime_surface =
                            apply_fail_closed_runtime_follow_surface_if_needed(
                                &mut follow_snapshot,
                                &mut open_shadow_lots,
                                &mut shadow_strategy_fail_closed,
                                &discovery_output,
                            );
                        if fail_closed_runtime_surface && !discovery_output.published {
                            warn!(
                                discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                                discovery_scoring_source = discovery_output.scoring_source,
                                active_follow_wallets_before_clear,
                                "clearing runtime follow snapshot because discovery no longer has a publishable universe to keep in memory"
                            );
                        }
                        if discovery_output.published {
                            if !fail_closed_runtime_surface {
                                let mut snapshot = (*follow_snapshot).clone();
                                apply_follow_snapshot_update(
                                    &mut snapshot,
                                    discovery_output.active_wallets.clone(),
                                    discovery_output.cycle_ts,
                                    follow_event_retention,
                                );
                                follow_snapshot = Arc::new(snapshot);
                                if shadow_strategy_fail_closed {
                                    refresh_shadow_open_lot_index_or_warn(
                                        &store,
                                        &mut open_shadow_lots,
                                    )?;
                                }
                                shadow_strategy_fail_closed = false;
                            }
                            shadow_risk_guard.observe_discovery_cycle(
                                &store,
                                discovery_output.cycle_ts,
                                discovery_output.eligible_wallets,
                                discovery_output.active_follow_wallets,
                                Some(&discovery_output),
                            ).context("shadow risk discovery-cycle universe event failed with fatal sqlite I/O")?;
                        }
                        let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
                        let ingestion_snapshot = ingestion.runtime_snapshot();
                        let catch_up_block_reason = discovery_catch_up_block_reason(
                            &discovery_output,
                            shadow_queue_full,
                            &observed_swap_writer_snapshot,
                            ingestion_snapshot.as_ref(),
                        );
                        let pending_requests_only_pressure =
                            discovery_catch_up_pending_requests_only_pressure(
                                &observed_swap_writer_snapshot,
                                ingestion_snapshot.as_ref(),
                            );
                        discovery_catch_up_pending = should_schedule_discovery_catch_up(
                            &discovery_output,
                            shadow_queue_full,
                            &observed_swap_writer_snapshot,
                            ingestion_snapshot.as_ref(),
                        );
                        if discovery_catch_up_pending {
                            let pressure_override =
                                discovery_output.persisted_stream_catch_up_pressure_override_requested;
                            if pressure_override {
                                info!(
                                    discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                                    discovery_scoring_source = discovery_output.scoring_source,
                                    discovery_persisted_stream_catch_up_pressure_override_requested =
                                        pressure_override,
                                    shadow_queue_full,
                                    writer_pending_requests =
                                        observed_swap_writer_snapshot.pending_requests,
                                    writer_pending_requests_threshold =
                                        DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                                    writer_aggregate_queue_depth_batches =
                                        observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                                    writer_journal_queue_depth_batches =
                                        observed_swap_writer_snapshot.journal_queue_depth_batches,
                                    discovery_catch_up_pending_requests_only_pressure_bypassed =
                                        pending_requests_only_pressure,
                                    yellowstone_output_queue_fill_ratio =
                                        ingestion_snapshot
                                            .as_ref()
                                            .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                                    "bounded partial discovery rebuild requested immediate catch-up cycle and overrode the runtime pressure gate because fail-closed publication recovery is on a constrained priority path"
                                );
                            } else {
                                info!(
                                    discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                                    discovery_scoring_source = discovery_output.scoring_source,
                                    discovery_persisted_stream_catch_up_pressure_override_requested =
                                        pressure_override,
                                    shadow_queue_full,
                                    writer_pending_requests =
                                        observed_swap_writer_snapshot.pending_requests,
                                    writer_pending_requests_threshold =
                                        DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                                    writer_aggregate_queue_depth_batches =
                                        observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                                    writer_journal_queue_depth_batches =
                                        observed_swap_writer_snapshot.journal_queue_depth_batches,
                                    yellowstone_output_queue_fill_ratio =
                                        ingestion_snapshot
                                            .as_ref()
                                            .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                                    "bounded partial discovery rebuild requested immediate catch-up cycle"
                                );
                            }
                        } else if discovery_output.persisted_stream_catch_up_requested {
                            info!(
                                discovery_runtime_mode = discovery_output.runtime_mode.as_str(),
                                discovery_scoring_source = discovery_output.scoring_source,
                                discovery_persisted_stream_catch_up_pressure_override_requested =
                                    discovery_output
                                        .persisted_stream_catch_up_pressure_override_requested,
                                shadow_queue_full,
                                writer_pending_requests =
                                    observed_swap_writer_snapshot.pending_requests,
                                writer_pending_requests_threshold =
                                    DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD,
                                writer_aggregate_queue_depth_batches =
                                    observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                                writer_journal_queue_depth_batches =
                                    observed_swap_writer_snapshot.journal_queue_depth_batches,
                                discovery_catch_up_block_reason = catch_up_block_reason
                                    .map(DiscoveryCatchUpBlockReason::as_str),
                                discovery_catch_up_pending_requests_only_blocker =
                                    pending_requests_only_pressure,
                                yellowstone_output_queue_fill_ratio =
                                    ingestion_snapshot
                                        .as_ref()
                                        .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio),
                                "bounded partial discovery rebuild requested catch-up, but runtime pressure gate deferred immediate retrigger"
                            );
                        }
                        refresh_discovery_critical_target_buy_mints_or_warn(
                            &store,
                            &mut discovery_critical_target_buy_mints,
                        )?;
                    }
                    Ok(Err(error)) => {
                        discovery_catch_up_pending = false;
                        if discovery_task_error_requires_restart(&error) {
                            return Err(error)
                                .context("discovery cycle failed with fatal sqlite I/O");
                        }
                        warn!(error = %error, "discovery cycle failed");
                    }
                    Err(error) => {
                        discovery_catch_up_pending = false;
                        warn!(error = %error, "discovery task join failed");
                    }
                }
            }
            _ = async {}, if discovery_catch_up_pending && discovery_handle.is_none() => {
                discovery_catch_up_pending = false;
                discovery_handle = Some(tokio::task::spawn_blocking(spawn_discovery_task(
                    sqlite_path.clone(),
                    recent_raw_journal_config.path.clone(),
                    recent_raw_journal_config.replay_batch_size,
                    discovery.clone(),
                    Utc::now(),
                )));
            }
            shadow_result = shadow_scheduler.shadow_workers.join_next(), if !shadow_scheduler.shadow_workers.is_empty() => {
                match shadow_result {
                    Some(Ok(task_output)) => {
                        shadow_scheduler.mark_task_complete(&task_output.key);
                        handle_shadow_task_output(
                            task_output,
                            &mut open_shadow_lots,
                            &mut shadow_drop_reason_counts,
                            &mut shadow_drop_stage_counts,
                        )?;
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "shadow task join failed");
                        shadow_scheduler.shadow_scheduler_needs_reset = true;
                    }
                    None => {}
                }
            }
            snapshot_result = async {
                if let Some(handle) = &mut shadow_scheduler.shadow_snapshot_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if shadow_scheduler.shadow_snapshot_handle.is_some() => {
                shadow_scheduler.shadow_snapshot_handle = None;
                match snapshot_result.expect("guard ensures shadow snapshot task exists") {
                    Ok(Ok(snapshot)) => {
                        if !shadow_strategy_fail_closed {
                            refresh_shadow_open_lot_index_or_warn(&store, &mut open_shadow_lots)?;
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
                            return Err(error)
                                .context("shadow snapshot failed with fatal sqlite I/O");
                        }
                        warn!(error = %error, "shadow snapshot failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "shadow snapshot task join failed");
                    }
                }
            }
            _ = interval.tick() => {
                if let Err(error) = store.record_heartbeat("copybot-app", "alive") {
                    if runtime_sqlite_write_error_requires_restart(&error) {
                        return Err(error)
                            .context("runtime heartbeat write failed with fatal sqlite I/O");
                    }
                    warn!(error = %error, "heartbeat write failed");
                }
                if let Some(dispatcher) = &alert_dispatcher {
                    match dispatcher.deliver_pending(&store).await {
                        Ok(delivered) if delivered > 0 => {
                            info!(delivered, "delivered pending alert webhooks");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            if alert_delivery_error_requires_restart(&error) {
                                return Err(error)
                                    .context("alert delivery poll failed with fatal sqlite I/O");
                            }
                            warn!(error = %error, "alert delivery poll failed");
                        }
                    }
                }
                let maintenance_gate_started_at = StdInstant::now();
                let maintenance_gate_sqlite_contention = sqlite_contention_snapshot();
                let maintenance_gate_ingestion_snapshot = ingestion.runtime_snapshot();
                if let Ok(mut snapshot) = latest_ingestion_runtime_snapshot.lock() {
                    *snapshot = maintenance_gate_ingestion_snapshot;
                }
                let maintenance_gate_writer_snapshot = observed_swap_writer.snapshot();
                if history_retention.enabled()
                    && last_history_retention_sweep.elapsed() >= history_retention_sweep_interval
                {
                    if let Some(reason) = sqlite_maintenance_block_reason(
                        SqliteMaintenanceTask::HistoryRetention,
                        app_started_at,
                        maintenance_gate_started_at,
                        &maintenance_gate_writer_snapshot,
                        last_sqlite_contention_snapshot,
                        maintenance_gate_sqlite_contention,
                        maintenance_gate_ingestion_snapshot,
                    ) {
                        let reason_key = sqlite_maintenance_block_reason_key(&reason);
                        if last_history_retention_skip_reason_key != Some(reason_key) {
                            warn!(
                                maintenance = SqliteMaintenanceTask::HistoryRetention.as_str(),
                                reason = %reason,
                                "sqlite maintenance blocked by runtime health gate"
                            );
                            last_history_retention_skip_reason_key = Some(reason_key);
                        }
                    } else {
                        info!(
                            maintenance = SqliteMaintenanceTask::HistoryRetention.as_str(),
                            observed_swap_writer_pending_requests =
                                maintenance_gate_writer_snapshot.pending_requests,
                            observed_swap_writer_aggregate_queue_depth_batches =
                                maintenance_gate_writer_snapshot.aggregate_queue_depth_batches,
                            observed_swap_writer_journal_queue_depth_batches =
                                maintenance_gate_writer_snapshot.journal_queue_depth_batches,
                            yellowstone_output_queue_depth = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                                .unwrap_or(0),
                            yellowstone_output_queue_capacity = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                                .unwrap_or(0),
                            yellowstone_output_queue_fill_ratio = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                                .unwrap_or(0.0),
                            sqlite_write_retry_total =
                                maintenance_gate_sqlite_contention.write_retry_total,
                            sqlite_busy_error_total =
                                maintenance_gate_sqlite_contention.busy_error_total,
                            "starting sqlite maintenance task"
                        );
                        let history_now = Utc::now();
                        let mut history_completed_full_sweep = true;
                        match history_retention.apply(
                            &store,
                            history_now,
                            alert_dispatcher.is_some(),
                        ) {
                            Ok(summary) => {
                                history_completed_full_sweep = summary.completed_full_sweep;
                                if !summary.is_empty() {
                                    info!(
                                        risk_events_deleted = summary.risk_events_deleted,
                                        risk_events_batches = summary.risk_events_batches,
                                        copy_signals_deleted = summary.copy_signals_deleted,
                                        copy_signals_batches = summary.copy_signals_batches,
                                        orders_deleted = summary.orders_deleted,
                                        execution_order_batches = summary.execution_order_batches,
                                        fills_deleted = summary.fills_deleted,
                                        shadow_closed_trades_deleted = summary.shadow_closed_trades_deleted,
                                        shadow_closed_trades_batches = summary.shadow_closed_trades_batches,
                                        completed_full_sweep = summary.completed_full_sweep,
                                        "history retention sweep applied"
                                    );
                                }
                            }
                            Err(error) => {
                                if history_retention_error_requires_restart(&error) {
                                    return Err(error)
                                        .context("history retention sweep failed with fatal sqlite I/O");
                                }
                                warn!(error = %error, "history retention sweep failed");
                            }
                        }
                        last_history_retention_skip_reason_key = None;
                        last_history_retention_sweep = if history_completed_full_sweep {
                            StdInstant::now()
                        } else {
                            StdInstant::now().checked_sub(
                                history_retention_sweep_interval
                                    .saturating_sub(SQLITE_MAINTENANCE_PARTIAL_RETRY_INTERVAL),
                            )
                            .unwrap_or_else(StdInstant::now)
                        };
                    }
                }
                if observed_swap_retention_handle.is_none()
                    && last_observed_swap_retention_sweep.elapsed()
                        >= observed_swap_retention_sweep_interval
                {
                    if let Some(reason) = sqlite_maintenance_block_reason(
                        SqliteMaintenanceTask::ObservedSwapRetention,
                        app_started_at,
                        maintenance_gate_started_at,
                        &maintenance_gate_writer_snapshot,
                        last_sqlite_contention_snapshot,
                        maintenance_gate_sqlite_contention,
                        maintenance_gate_ingestion_snapshot,
                    ) {
                        let reason_key = sqlite_maintenance_block_reason_key(&reason);
                        if last_observed_swap_retention_skip_reason_key != Some(reason_key) {
                            warn!(
                                maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                                reason = %reason,
                                "sqlite maintenance blocked by runtime health gate"
                            );
                            last_observed_swap_retention_skip_reason_key = Some(reason_key);
                        }
                    } else {
                        let sqlite_path = sqlite_path.clone();
                        let observed_swap_retention_runtime_health =
                            observed_swap_retention_runtime_health.clone();
                        info!(
                            maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                            observed_swap_writer_pending_requests =
                                maintenance_gate_writer_snapshot.pending_requests,
                            observed_swap_writer_aggregate_queue_depth_batches =
                                maintenance_gate_writer_snapshot.aggregate_queue_depth_batches,
                            observed_swap_writer_journal_queue_depth_batches =
                                maintenance_gate_writer_snapshot.journal_queue_depth_batches,
                            yellowstone_output_queue_depth = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                                .unwrap_or(0),
                            yellowstone_output_queue_capacity = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                                .unwrap_or(0),
                            yellowstone_output_queue_fill_ratio = maintenance_gate_ingestion_snapshot
                                .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                                .unwrap_or(0.0),
                            sqlite_write_retry_total =
                                maintenance_gate_sqlite_contention.write_retry_total,
                            sqlite_busy_error_total =
                                maintenance_gate_sqlite_contention.busy_error_total,
                            "starting sqlite maintenance task"
                        );
                        observed_swap_retention_handle =
                            Some(tokio::task::spawn_blocking(move || {
                                run_observed_swap_retention_maintenance_once(
                                    &sqlite_path,
                                    observed_swap_retention_config,
                                    Some(observed_swap_retention_runtime_health),
                                )
                            }));
                        last_observed_swap_retention_sweep = StdInstant::now();
                        last_observed_swap_retention_skip_reason_key = None;
                    }
                }
                let sqlite_contention = sqlite_contention_snapshot();
                let wal_size_bytes = std::fs::metadata(format!("{sqlite_path}-wal"))
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
                let app_consumer_loop_snapshot = app_consumer_loop_telemetry.snapshot_and_reset();
                info!(
                    observed_swap_writer_pending_requests =
                        observed_swap_writer_snapshot.pending_requests,
                    observed_swap_writer_write_ms_p95 =
                        observed_swap_writer_snapshot.write_latency_ms_p95,
                    observed_swap_writer_raw_batch_ms_p95 =
                        observed_swap_writer_snapshot.raw_batch_write_ms_p95,
                    observed_swap_writer_observed_swaps_insert_ms_p95 =
                        observed_swap_writer_snapshot.observed_swaps_insert_ms_p95,
                    observed_swap_writer_wallet_activity_days_ms_p95 =
                        observed_swap_writer_snapshot.wallet_activity_days_ms_p95,
                    observed_swap_writer_discovery_scoring_ms_p95 =
                        observed_swap_writer_snapshot.discovery_scoring_ms_p95,
                    observed_swap_writer_journal_enqueue_wait_ms_p95 =
                        observed_swap_writer_snapshot.journal_enqueue_wait_ms_p95,
                    observed_swap_writer_journal_batch_write_ms_p95 =
                        observed_swap_writer_snapshot.journal_batch_write_ms_p95,
                    observed_swap_writer_worker_busy_ms_p95 =
                        observed_swap_writer_snapshot.worker_busy_ms_p95,
                    observed_swap_writer_aggregate_queue_depth_batches =
                        observed_swap_writer_snapshot.aggregate_queue_depth_batches,
                    observed_swap_writer_aggregate_queue_capacity_batches =
                        observed_swap_writer_snapshot.aggregate_queue_capacity_batches,
                    observed_swap_writer_journal_queue_depth_batches =
                        observed_swap_writer_snapshot.journal_queue_depth_batches,
                    observed_swap_writer_journal_queue_capacity_batches =
                        observed_swap_writer_snapshot.journal_queue_capacity_batches,
                    observed_swap_writer_journal_sqlite_write_retry_total =
                        observed_swap_writer_snapshot.journal_sqlite_write_retry_total,
                    observed_swap_writer_journal_sqlite_busy_error_total =
                        observed_swap_writer_snapshot.journal_sqlite_busy_error_total,
                    "observed swap writer telemetry"
                );
                info!(
                    app_consumer_swaps_seen = app_consumer_loop_snapshot.swaps_seen,
                    app_follow_rejected = app_consumer_loop_snapshot.follow_rejected,
                    app_follow_rejected_ratio = app_consumer_loop_snapshot.follow_rejected_ratio,
                    app_consumer_loop_time_ms_p95 = app_consumer_loop_snapshot.processing_ms_p95,
                    "app consumer loop telemetry"
                );
                info!(
                    sqlite_write_retry_total = sqlite_contention.write_retry_total,
                    sqlite_busy_error_total = sqlite_contention.busy_error_total,
                    sqlite_wal_size_bytes = wal_size_bytes,
                    "sqlite contention counters"
                );
                last_sqlite_contention_snapshot = sqlite_contention;
            }
            _ = risk_refresh_interval.tick() => {
                let now = Utc::now();
                shadow_risk_guard.observe_ingestion_snapshot(
                    &store,
                    now,
                    ingestion.runtime_snapshot(),
                ).context("shadow risk ingestion snapshot infra event failed with fatal sqlite I/O")?;
                if !shadow_risk_guard.config.shadow_killswitch_enabled {
                    continue;
                }
                if let Err(error) = shadow_risk_guard.maybe_refresh_db_state(&store, now) {
                    if shadow_risk_background_refresh_error_requires_restart(&error) {
                        return Err(error)
                            .context("shadow risk background refresh failed with fatal sqlite I/O");
                    }
                    if shadow_risk_guard.on_risk_refresh_error(now) {
                        warn!(error = %error, "shadow risk background refresh failed");
                    }
                }
            }
            execution_join = async {
                match execution_handle.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => None,
                }
            }, if execution_handle.is_some() => {
                execution_handle = None;
                match execution_join {
                    Some(Ok(Ok(report))) => {
                        log_execution_batch_report(&report);
                    }
                    Some(Ok(Err(error))) => {
                        if execution_task_error_requires_restart(&error) {
                            return Err(error)
                                .context("execution batch failed with fatal sqlite I/O");
                        }
                        warn!(error = %error, "execution batch failed");
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "execution task join failed");
                    }
                    None => {}
                }
            }
            observed_swap_retention_join = async {
                match observed_swap_retention_handle.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => None,
                }
            }, if observed_swap_retention_handle.is_some() => {
                observed_swap_retention_handle = None;
                match observed_swap_retention_join {
                    Some(Ok(Ok(summary))) => {
                        info!(
                            maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                            nominal_observed_swap_cutoff = %summary.nominal_cutoff,
                            effective_observed_swap_cutoff = %summary.effective_cutoff,
                            aggregate_scoring_cutoff = ?summary.aggregate_cutoff,
                            deleted_observed_swap_rows = summary.raw_deleted_rows,
                            observed_swap_delete_batches = summary.raw_delete_batches,
                            deleted_scoring_rows = summary.scoring_deleted_rows,
                            discovery_scoring_delete_batches = summary.scoring_delete_batches,
                            completed_full_sweep = summary.completed_full_sweep,
                            stop_reason = ?summary.stop_reason,
                            wal_checkpoint_mode = summary.checkpoint.mode,
                            wal_checkpoint_busy = summary.checkpoint.busy,
                            wal_log_frames = summary.checkpoint.log_frames,
                            wal_checkpointed_frames = summary.checkpoint.checkpointed_frames,
                            duration_ms = summary.duration_ms,
                            "sqlite maintenance task completed"
                        );
                        if !summary.completed_full_sweep {
                            last_observed_swap_retention_sweep = StdInstant::now()
                                .checked_sub(
                                    observed_swap_retention_sweep_interval.saturating_sub(
                                        OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL,
                                    ),
                                )
                                .unwrap_or_else(StdInstant::now);
                        }
                    }
                    Some(Ok(Err(error))) => {
                        if observed_swap_retention_error_requires_restart(&error) {
                            return Err(error).context(
                                "observed swap retention maintenance failed with fatal sqlite I/O",
                            );
                        }
                        warn!(error = %error, "observed swap retention maintenance failed");
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "observed swap retention maintenance task join failed");
                    }
                    None => {}
                }
            }
            _ = execution_interval.tick(), if execution_runtime.is_enabled() => {
                let now = Utc::now();
                let buy_submit_pause_reason = resolve_buy_submit_pause_reason(
                    &operator_emergency_stop,
                    &shadow_risk_guard,
                    now,
                    pause_new_trades_on_outage,
                    &mut execution_emergency_stop_active_logged,
                    &mut execution_hard_stop_pause_logged,
                    &mut execution_shadow_risk_pause_logged_reason,
                    &mut execution_outage_pause_logged,
                );

                if execution_handle.is_some() {
                    warn!("execution batch still running, skipping scheduled trigger");
                    continue;
                }

                execution_handle = Some(tokio::task::spawn_blocking(spawn_execution_task(
                    sqlite_path.clone(),
                    Arc::clone(&execution_runtime),
                    now,
                    buy_submit_pause_reason,
                )));
            }
            _ = async {
                if let Some(until) = ingestion_backoff_until {
                    time::sleep_until(until).await;
                }
            }, if ingestion_backoff_until.is_some() => {
                ingestion_backoff_until = None;
            }
            _ = time::sleep(OBSERVED_SWAP_WRITER_BACKPRESSURE_RETRY_INTERVAL), if !pending_irrelevant_swaps.is_empty() => {
                refresh_discovery_critical_target_buy_mints_or_warn(
                    &store,
                    &mut discovery_critical_target_buy_mints,
                )?;
                let dropped_noncritical_pending =
                    prune_noncritical_zero_universe_pending_irrelevant_swaps(
                        &mut pending_irrelevant_swaps,
                        &mut recent_swap_signatures,
                        &mut recent_swap_signature_order,
                        &mut app_consumer_loop_telemetry,
                        &follow_snapshot,
                        &open_shadow_lots,
                        shadow_strategy_fail_closed,
                        &discovery_critical_target_buy_mints,
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
                        &observed_swap_writer,
                        &mut recent_swap_signatures,
                        &mut recent_swap_signature_order,
                        &pending.swap,
                        pending.discovery_critical,
                    ).await {
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
                                .map(|last| {
                                    last.elapsed() >= OBSERVED_SWAP_WRITER_BACKPRESSURE_LOG_THROTTLE
                                })
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
                                    observed_swap_writer_aggregate_queue_depth_batches =
                                        writer_snapshot.aggregate_queue_depth_batches,
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
            }
            maybe_swap = ingestion.next_swap(), if ingestion_backoff_until.is_none()
                && !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps) => {
                let now = Utc::now();
                shadow_risk_guard.observe_ingestion_snapshot(
                    &store,
                    now,
                    ingestion.runtime_snapshot(),
                ).context("shadow risk ingestion snapshot infra event failed with fatal sqlite I/O")?;
                let swap = match maybe_swap {
                    Ok(Some(swap)) => {
                        if ingestion_error_streak > 0 {
                            info!(
                                consecutive_errors = ingestion_error_streak,
                                "ingestion stream recovered"
                            );
                        }
                        ingestion_error_streak = 0;
                        swap
                    }
                    Ok(None) => {
                        ingestion_error_streak = 0;
                        debug!("ingestion emitted no swap");
                        continue;
                    }
                    Err(error) => {
                        ingestion_error_streak = ingestion_error_streak.saturating_add(1);
                        let backoff_ms = ingestion_error_backoff_ms(ingestion_error_streak);
                        ingestion_backoff_until =
                            Some(time::Instant::now() + Duration::from_millis(backoff_ms));
                        warn!(
                            error = %error,
                            consecutive_errors = ingestion_error_streak,
                            backoff_ms,
                            "ingestion error; applying backoff before next poll"
                        );
                        continue;
                    }
                };
                let swap_processing_started_at = StdInstant::now();
                app_consumer_loop_telemetry.note_swap_seen();

                if let Err(error) = observed_swap_writer.ensure_running() {
                    app_consumer_loop_telemetry
                        .note_processing_started_at(swap_processing_started_at);
                    return Err(error).context(
                        "observed swap writer is no longer running; restarting app to avoid silent stale ingestion",
                    );
                }

                if !note_recent_swap_signature(
                    &mut recent_swap_signatures,
                    &mut recent_swap_signature_order,
                    &swap.signature,
                ) {
                    app_consumer_loop_telemetry
                        .note_processing_started_at(swap_processing_started_at);
                    debug!(signature = %swap.signature, "duplicate swap ignored by recent signature dedupe");
                    continue;
                }

                let side = match classify_observed_swap_shadow_relevance(
                    &swap,
                    &follow_snapshot,
                    &shadow_scheduler,
                    &open_shadow_lots,
                ) {
                    ObservedSwapShadowRelevance::IrrelevantUnclassified => {
                        let discovery_critical_irrelevant_persistence =
                            irrelevant_observed_swap_requires_discovery_critical_persistence(
                                &swap,
                                &follow_snapshot,
                                &open_shadow_lots,
                                shadow_strategy_fail_closed,
                                &discovery_critical_target_buy_mints,
                            );
                        match persist_irrelevant_observed_swap(
                            &observed_swap_writer,
                            &mut recent_swap_signatures,
                            &mut recent_swap_signature_order,
                            &swap,
                            discovery_critical_irrelevant_persistence,
                        )
                        .await {
                            Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued) => {
                                app_consumer_loop_telemetry
                                    .note_processing_started_at(swap_processing_started_at);
                                continue;
                            }
                            Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure) => {
                                let writer_snapshot = observed_swap_writer.snapshot();
                                let ingestion_snapshot = ingestion.runtime_snapshot();
                                warn!(
                                    signature = %swap.signature,
                                    observed_swap_writer_pending_requests =
                                        writer_snapshot.pending_requests,
                                    observed_swap_writer_aggregate_queue_depth_batches =
                                        writer_snapshot.aggregate_queue_depth_batches,
                                    yellowstone_output_queue_depth = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                                        .unwrap_or(0),
                                    yellowstone_output_queue_capacity = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                                        .unwrap_or(0),
                                    yellowstone_output_queue_fill_ratio = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                                        .unwrap_or(0.0),
                                    "observed swap writer queue is saturated; deferring irrelevant observed swap persistence without restarting runtime"
                                );
                                let discovery_critical_irrelevant_persistence =
                                    refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                                        &store,
                                        &swap,
                                        &follow_snapshot,
                                        &open_shadow_lots,
                                        shadow_strategy_fail_closed,
                                        &mut discovery_critical_target_buy_mints,
                                    )?;
                                if !should_buffer_backpressured_irrelevant_observed_swap(
                                    discovery_critical_irrelevant_persistence,
                                    &follow_snapshot,
                                    &open_shadow_lots,
                                    shadow_strategy_fail_closed,
                                ) {
                                    forget_recent_swap_signature(
                                        &mut recent_swap_signatures,
                                        &mut recent_swap_signature_order,
                                        &swap.signature,
                                    );
                                    app_consumer_loop_telemetry
                                        .note_processing_started_at(swap_processing_started_at);
                                    debug!(
                                        signature = %swap.signature,
                                        "dropping non-critical irrelevant observed swap under zero-universe fail-closed writer backpressure"
                                    );
                                    continue;
                                }
                                pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                                    swap,
                                    discovery_critical: discovery_critical_irrelevant_persistence,
                                    processing_started_at: swap_processing_started_at,
                                    backpressure_started_at: StdInstant::now(),
                                    last_backpressure_log_at: Some(StdInstant::now()),
                                });
                                if discovery_critical_irrelevant_persistence
                                    && pending_irrelevant_swap_backpressure_blocks_ingestion(
                                        &pending_irrelevant_swaps,
                                    )
                                {
                                    warn!(
                                        pending_irrelevant_swap_queue_depth =
                                            pending_irrelevant_swaps.len(),
                                        "discovery-critical irrelevant observed swap backlog reached the bounded in-memory limit; pausing ingestion polling until writer capacity recovers"
                                    );
                                }
                                continue;
                            }
                            Err(error) => {
                                app_consumer_loop_telemetry
                                    .note_processing_started_at(swap_processing_started_at);
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
                                    "failed enqueueing observed swap"
                                );
                                continue;
                            }
                        }
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
                        let discovery_critical_irrelevant_persistence =
                            irrelevant_observed_swap_requires_discovery_critical_persistence(
                                &swap,
                                &follow_snapshot,
                                &open_shadow_lots,
                                shadow_strategy_fail_closed,
                                &discovery_critical_target_buy_mints,
                            );
                        match persist_irrelevant_observed_swap(
                            &observed_swap_writer,
                            &mut recent_swap_signatures,
                            &mut recent_swap_signature_order,
                            &swap,
                            discovery_critical_irrelevant_persistence,
                        )
                        .await {
                            Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued) => {
                                app_consumer_loop_telemetry
                                    .note_processing_started_at(swap_processing_started_at);
                                continue;
                            }
                            Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure) => {
                                let writer_snapshot = observed_swap_writer.snapshot();
                                let ingestion_snapshot = ingestion.runtime_snapshot();
                                warn!(
                                    signature = %swap.signature,
                                    observed_swap_writer_pending_requests =
                                        writer_snapshot.pending_requests,
                                    observed_swap_writer_aggregate_queue_depth_batches =
                                        writer_snapshot.aggregate_queue_depth_batches,
                                    yellowstone_output_queue_depth = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                                        .unwrap_or(0),
                                    yellowstone_output_queue_capacity = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                                        .unwrap_or(0),
                                    yellowstone_output_queue_fill_ratio = ingestion_snapshot
                                        .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                                        .unwrap_or(0.0),
                                    "observed swap writer queue is saturated; deferring irrelevant observed swap persistence without restarting runtime"
                                );
                                let discovery_critical_irrelevant_persistence =
                                    refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                                        &store,
                                        &swap,
                                        &follow_snapshot,
                                        &open_shadow_lots,
                                        shadow_strategy_fail_closed,
                                        &mut discovery_critical_target_buy_mints,
                                    )?;
                                if !should_buffer_backpressured_irrelevant_observed_swap(
                                    discovery_critical_irrelevant_persistence,
                                    &follow_snapshot,
                                    &open_shadow_lots,
                                    shadow_strategy_fail_closed,
                                ) {
                                    forget_recent_swap_signature(
                                        &mut recent_swap_signatures,
                                        &mut recent_swap_signature_order,
                                        &swap.signature,
                                    );
                                    app_consumer_loop_telemetry
                                        .note_processing_started_at(swap_processing_started_at);
                                    debug!(
                                        signature = %swap.signature,
                                        "dropping non-critical irrelevant observed swap under zero-universe fail-closed writer backpressure"
                                    );
                                    continue;
                                }
                                pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                                    swap,
                                    discovery_critical: discovery_critical_irrelevant_persistence,
                                    processing_started_at: swap_processing_started_at,
                                    backpressure_started_at: StdInstant::now(),
                                    last_backpressure_log_at: Some(StdInstant::now()),
                                });
                                if discovery_critical_irrelevant_persistence
                                    && pending_irrelevant_swap_backpressure_blocks_ingestion(
                                        &pending_irrelevant_swaps,
                                    )
                                {
                                    warn!(
                                        pending_irrelevant_swap_queue_depth =
                                            pending_irrelevant_swaps.len(),
                                        "discovery-critical irrelevant observed swap backlog reached the bounded in-memory limit; pausing ingestion polling until writer capacity recovers"
                                    );
                                }
                                continue;
                            }
                            Err(error) => {
                                app_consumer_loop_telemetry
                                    .note_processing_started_at(swap_processing_started_at);
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
                                    "failed enqueueing observed swap"
                                );
                                continue;
                            }
                        }
                    }
                    ObservedSwapShadowRelevance::Relevant(side) => side,
                };

                let relevant_persistence = match persist_relevant_observed_swap(
                    &observed_swap_writer,
                    &mut recent_swap_signatures,
                    &mut recent_swap_signature_order,
                    &swap,
                )
                .await
                {
                    Ok(inserted) => relevant_observed_swap_persistence(inserted),
                    Err(error) => {
                        app_consumer_loop_telemetry
                            .note_processing_started_at(swap_processing_started_at);
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
                        continue;
                    }
                };
                if matches!(
                    relevant_persistence,
                    RelevantObservedSwapPersistence::Duplicate
                ) {
                    app_consumer_loop_telemetry
                        .note_processing_started_at(swap_processing_started_at);
                    debug!(
                        signature = %swap.signature,
                        "duplicate swap ignored by observed swap store"
                    );
                    continue;
                }

                if shadow_strategy_fail_closed {
                    app_consumer_loop_telemetry
                        .note_processing_started_at(swap_processing_started_at);
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
                    continue;
                }

                if matches!(side, ShadowSwapSide::Buy) {
                    if operator_emergency_stop.is_active() {
                        app_consumer_loop_telemetry
                            .note_processing_started_at(swap_processing_started_at);
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
                        continue;
                    }

                    match shadow_risk_guard.can_open_buy(&store, now, pause_new_trades_on_outage) {
                        BuyRiskDecision::Allow => {}
                        BuyRiskDecision::Blocked { reason, detail } => {
                            app_consumer_loop_telemetry
                                .note_processing_started_at(swap_processing_started_at);
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
                            continue;
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
                    &open_shadow_lots,
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
                            &mut shadow_drop_reason_counts,
                            &mut shadow_drop_stage_counts,
                            &mut shadow_queue_full_outcome_counts,
                        ),
                    }
                    app_consumer_loop_telemetry
                        .note_processing_started_at(swap_processing_started_at);
                    continue;
                }
                if shadow_scheduler.should_process_shadow_inline(
                    shadow_queue_full,
                    shadow_scheduler.shadow_scheduler_needs_reset,
                    shadow_scheduler.shadow_workers.len(),
                    &task_input.key,
                ) {
                    if shadow_scheduler.inflight_shadow_keys.insert(task_input.key.clone()) {
                        spawn_shadow_worker_task(
                            &mut shadow_scheduler.shadow_workers,
                            &shadow,
                            &sqlite_path,
                            task_input,
                        );
                    } else if let Err(dropped_task) = shadow_scheduler
                        .enqueue_shadow_task(SHADOW_PENDING_TASK_CAPACITY, task_input)
                    {
                        shadow_scheduler.handle_shadow_enqueue_overflow(
                            side,
                            dropped_task,
                            SHADOW_PENDING_TASK_CAPACITY,
                            &mut shadow_drop_reason_counts,
                            &mut shadow_drop_stage_counts,
                            &mut shadow_queue_full_outcome_counts,
                        );
                    }
                } else if let Err(dropped_task) =
                    shadow_scheduler.enqueue_shadow_task(SHADOW_PENDING_TASK_CAPACITY, task_input)
                {
                    shadow_scheduler.handle_shadow_enqueue_overflow(
                        side,
                        dropped_task,
                        SHADOW_PENDING_TASK_CAPACITY,
                        &mut shadow_drop_reason_counts,
                        &mut shadow_drop_stage_counts,
                        &mut shadow_queue_full_outcome_counts,
                    );
                }
                app_consumer_loop_telemetry.note_processing_started_at(swap_processing_started_at);
            }
            _ = discovery_interval.tick() => {
                if discovery_handle.is_some() {
                    warn!("discovery cycle still running, skipping scheduled trigger");
                    continue;
                }
                discovery_catch_up_pending = false;
                discovery_handle = Some(tokio::task::spawn_blocking(spawn_discovery_task(
                    sqlite_path.clone(),
                    recent_raw_journal_config.path.clone(),
                    recent_raw_journal_config.replay_batch_size,
                    discovery.clone(),
                    Utc::now(),
                )));
            }
            _ = shadow_interval.tick() => {
                if shadow_strategy_fail_closed {
                    continue;
                }
                let cleanup_now = Utc::now();
                match close_stale_shadow_lots(
                    &store,
                    &mut open_shadow_lots,
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
                            return Err(error)
                                .context("stale lot cleanup failed with fatal sqlite I/O");
                        }
                        warn!(error = %error, "stale lot cleanup failed");
                    }
                }
                if shadow_scheduler.shadow_snapshot_handle.is_some() {
                    warn!("shadow snapshot still running, skipping scheduled trigger");
                    continue;
                }
                shadow_scheduler.shadow_snapshot_handle = Some(tokio::task::spawn_blocking(spawn_shadow_snapshot_task(
                    sqlite_path.clone(),
                    shadow.clone(),
                    Utc::now(),
                )));
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    if let Some(handle) = execution_handle.take() {
        handle.abort();
    }
    if let Some(handle) = observed_swap_retention_handle.take() {
        handle.abort();
    }
    if let Some(handle) = discovery_handle.take() {
        handle.abort();
    }
    if let Some(handle) = shadow_scheduler.shadow_snapshot_handle.take() {
        handle.abort();
    }
    observed_swap_writer
        .shutdown()
        .context("failed to shut down observed swap writer")?;

    store
        .record_heartbeat("copybot-app", "shutdown")
        .context("failed to write shutdown heartbeat")?;
    Ok(())
}

fn follow_event_retention_duration(
    shadow_max_signal_lag_seconds: u64,
    discovery_refresh_seconds: u64,
) -> Duration {
    Duration::from_secs(
        shadow_max_signal_lag_seconds
            .max(discovery_refresh_seconds.max(1).saturating_mul(2))
            .max(1),
    )
}

fn startup_follow_snapshot_from_publication_truth(
    initial_active_wallets: HashSet<String>,
    runtime_publication_truth: Option<&RuntimePublicationTruthResolution>,
) -> (FollowSnapshot, usize, bool) {
    if let Some(RuntimePublicationTruthResolution::Recent(truth)) = runtime_publication_truth {
        return (
            FollowSnapshot::from_active_wallets(truth.active_wallets()),
            0,
            false,
        );
    }
    if matches!(
        runtime_publication_truth,
        Some(RuntimePublicationTruthResolution::BootstrapDegraded(_))
    ) {
        return (
            FollowSnapshot::default(),
            initial_active_wallets.len(),
            true,
        );
    }
    let recovered_active_wallets = initial_active_wallets.len();
    let shadow_strategy_fail_closed = recovered_active_wallets > 0;
    if !shadow_strategy_fail_closed {
        (FollowSnapshot::default(), 0, false)
    } else {
        (FollowSnapshot::default(), recovered_active_wallets, true)
    }
}

fn startup_runtime_publication_truth(
    discovery: &DiscoveryService,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<RuntimePublicationTruthResolution>> {
    discovery.runtime_publication_truth_resolution(store, now)
}

fn apply_fail_closed_runtime_follow_surface_if_needed(
    follow_snapshot: &mut Arc<FollowSnapshot>,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_strategy_fail_closed: &mut bool,
    discovery_output: &DiscoveryTaskOutput,
) -> bool {
    if !matches!(
        discovery_output.runtime_mode,
        DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded
    ) {
        return false;
    }
    *shadow_strategy_fail_closed = true;
    *follow_snapshot = Arc::new(FollowSnapshot::default());
    open_shadow_lots.clear();
    true
}

fn validate_bootstrap_degraded_execution_contract(
    store: &SqliteStore,
    execution_runtime: &ExecutionRuntime,
) -> Result<()> {
    let bootstrap_degraded = store.discovery_bootstrap_degraded_state_read_only()?;
    if !bootstrap_degraded.active || !execution_runtime.is_enabled() {
        return Ok(());
    }
    let armed_at = bootstrap_degraded
        .armed_at
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "unknown".to_string());
    let reason = bootstrap_degraded
        .reason
        .unwrap_or_else(|| "unspecified".to_string());
    Err(anyhow!(
        "execution.enabled=true is not allowed while discovery bootstrap-degraded runtime state is active (reason={reason}, armed_at={armed_at})"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiscoveryCatchUpBlockReason {
    ShadowQueueFull,
    WriterPendingRequests,
    WriterAggregateQueueDepth,
    WriterJournalQueueDepth,
    YellowstoneOutputQueueFill,
}

impl DiscoveryCatchUpBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ShadowQueueFull => "shadow_queue_full",
            Self::WriterPendingRequests => "writer_pending_requests",
            Self::WriterAggregateQueueDepth => "writer_aggregate_queue_depth_batches",
            Self::WriterJournalQueueDepth => "writer_journal_queue_depth_batches",
            Self::YellowstoneOutputQueueFill => "yellowstone_output_queue_fill_ratio",
        }
    }
}

fn discovery_catch_up_has_writer_queue_pressure(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
) -> bool {
    observed_swap_writer_snapshot.aggregate_queue_depth_batches > 0
        || observed_swap_writer_snapshot.journal_queue_depth_batches > 0
}

fn discovery_catch_up_has_ingestion_pressure(
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    ingestion_runtime_snapshot.is_some_and(|snapshot| {
        snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio > 0.0
    })
}

fn discovery_catch_up_pending_requests_only_pressure(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    observed_swap_writer_snapshot.pending_requests
        >= DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD
        && !discovery_catch_up_has_writer_queue_pressure(observed_swap_writer_snapshot)
        && !discovery_catch_up_has_ingestion_pressure(ingestion_runtime_snapshot)
}

fn discovery_catch_up_block_reason(
    discovery_output: &DiscoveryTaskOutput,
    shadow_queue_full: bool,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> Option<DiscoveryCatchUpBlockReason> {
    if !discovery_output.persisted_stream_catch_up_requested {
        return None;
    }
    if shadow_queue_full {
        return Some(DiscoveryCatchUpBlockReason::ShadowQueueFull);
    }
    if observed_swap_writer_snapshot.aggregate_queue_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterAggregateQueueDepth);
    }
    if observed_swap_writer_snapshot.journal_queue_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterJournalQueueDepth);
    }
    if discovery_catch_up_has_ingestion_pressure(ingestion_runtime_snapshot) {
        return Some(DiscoveryCatchUpBlockReason::YellowstoneOutputQueueFill);
    }
    if discovery_output.persisted_stream_catch_up_pressure_override_requested {
        return None;
    }
    if observed_swap_writer_snapshot.pending_requests
        >= DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD
    {
        return Some(DiscoveryCatchUpBlockReason::WriterPendingRequests);
    }
    None
}

fn should_schedule_discovery_catch_up(
    discovery_output: &DiscoveryTaskOutput,
    shadow_queue_full: bool,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    if !discovery_output.persisted_stream_catch_up_requested {
        return false;
    }
    discovery_catch_up_block_reason(
        discovery_output,
        shadow_queue_full,
        observed_swap_writer_snapshot,
        ingestion_runtime_snapshot,
    )
    .is_none()
}

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    eligible_wallets: usize,
    active_follow_wallets: usize,
    published: bool,
    runtime_mode: DiscoveryRuntimeMode,
    scoring_source: &'static str,
    raw_window_cap_truncated: bool,
    cap_truncation_deactivation_guard_active: bool,
    cap_truncation_deactivation_guard_reason: Option<&'static str>,
    cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    cap_truncation_floor_signature: Option<String>,
    persisted_stream_catch_up_requested: bool,
    persisted_stream_catch_up_pressure_override_requested: bool,
}

#[cfg(test)]
mod app_tests {
    use super::*;
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode,
        DiscoveryTrustedSelectionStateUpdate, TrustedSelectionState, TrustedSnapshotSourceKind,
        WalletMetricRow,
    };
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    fn make_test_store(name: &str) -> Result<(SqliteStore, PathBuf)> {
        let unique = format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("copybot-app-{unique}.db"));
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((store, db_path))
    }

    fn write_temp_secret_file(name: &str, content: &str) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!(
            "copybot-secret-{}-{}-{}.txt",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        ));
        let mut file = std::fs::File::create(&path)?;
        file.write_all(content.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(&path, perms)?;
        }
        Ok(path)
    }

    fn seed_test_discovery_critical_target_buy_mints(
        store: &SqliteStore,
        target_buy_mints: &[&str],
        unique_buy_mints: &[&str],
    ) -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-03-14T16:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase: DiscoveryPersistedRebuildPhase::CollectBuyMints,
            window_start: now - chrono::Duration::days(5),
            horizon_end: now,
            metrics_window_start: now - chrono::Duration::hours(1),
            phase_cursor: None,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            state_json: serde_json::json!({
                "discovery_critical_target_buy_mints": target_buy_mints,
                "unique_buy_mints": unique_buy_mints,
            })
            .to_string(),
            started_at: now,
            updated_at: now,
        })?;
        Ok(())
    }

    fn test_publication_policy_fingerprint(config: &copybot_config::DiscoveryConfig) -> String {
        format!(
            concat!(
                "follow_top_n={};",
                "scoring_window_days={};",
                "decay_window_days={};",
                "min_leader_notional_sol={:.6};",
                "min_trades={};",
                "min_active_days={};",
                "min_score={:.6};",
                "max_tx_per_minute={};",
                "min_buy_count={};",
                "min_tradable_ratio={:.6};",
                "require_open_positions_for_publication={};",
                "max_rug_ratio={:.6};",
                "rug_lookahead_seconds={};",
                "thin_market_min_volume_sol={:.6};",
                "thin_market_min_unique_traders={}"
            ),
            config.follow_top_n,
            config.scoring_window_days,
            config.decay_window_days,
            config.min_leader_notional_sol,
            config.min_trades,
            config.min_active_days,
            config.min_score,
            config.max_tx_per_minute,
            config.min_buy_count,
            config.min_tradable_ratio,
            config.require_open_positions_for_publication,
            config.max_rug_ratio,
            config.rug_lookahead_seconds,
            config.thin_market_min_volume_sol,
            config.thin_market_min_unique_traders,
        )
    }

    fn write_temp_secret_dir(name: &str) -> Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "copybot-secret-dir-{}-{}-{}",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn permissive_shadow_quality() -> ShadowConfig {
        let mut config = ShadowConfig::default();
        config.min_token_age_seconds = 0;
        config.min_holders = 0;
        config.min_liquidity_sol = 0.0;
        config.min_volume_5m_sol = 0.0;
        config.min_unique_traders_5m = 0;
        config
    }

    fn error_chain_contains(error: &anyhow::Error, needle: &str) -> bool {
        error
            .chain()
            .any(|cause| cause.to_string().contains(needle))
    }

    fn maintenance_test_writer_snapshot() -> ObservedSwapWriterSnapshot {
        ObservedSwapWriterSnapshot {
            pending_requests: 0,
            write_latency_ms_p95: 0,
            raw_batch_write_ms_p95: 0,
            observed_swaps_insert_ms_p95: 0,
            wallet_activity_days_ms_p95: 0,
            discovery_scoring_ms_p95: 0,
            journal_enqueue_wait_ms_p95: 0,
            journal_batch_write_ms_p95: 0,
            worker_busy_ms_p95: 0,
            aggregate_queue_depth_batches: 0,
            aggregate_queue_capacity_batches: 32,
            journal_queue_depth_batches: 0,
            journal_queue_capacity_batches: 32,
            journal_sqlite_write_retry_total: 0,
            journal_sqlite_busy_error_total: 0,
        }
    }

    fn maintenance_test_ingestion_snapshot(fill_ratio: f64) -> IngestionRuntimeSnapshot {
        IngestionRuntimeSnapshot {
            ts_utc: Utc::now(),
            ws_notifications_enqueued: 0,
            ws_notifications_replaced_oldest: 0,
            grpc_message_total: 0,
            grpc_transaction_updates_total: 0,
            parse_rejected_total: 0,
            grpc_decode_errors: 0,
            rpc_429: 0,
            rpc_5xx: 0,
            ingestion_lag_ms_p95: 0,
            yellowstone_output_queue_depth: 25,
            yellowstone_output_queue_capacity: 100,
            yellowstone_output_queue_fill_ratio: fill_ratio,
            yellowstone_output_oldest_age_ms: 500,
        }
    }

    fn discovery_output_for_catch_up_tests(requested: bool) -> DiscoveryTaskOutput {
        DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: Utc::now(),
            eligible_wallets: 0,
            active_follow_wallets: 0,
            published: false,
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            scoring_source: "raw_window_persisted_stream",
            raw_window_cap_truncated: false,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: None,
            cap_truncation_deactivation_guard_started_at: None,
            cap_truncation_floor_ts_utc: None,
            cap_truncation_floor_signature: None,
            persisted_stream_catch_up_requested: requested,
            persisted_stream_catch_up_pressure_override_requested: false,
        }
    }

    fn live_like_published_discovery_output(
        now: DateTime<Utc>,
        active_follow_wallets: usize,
        eligible_wallets: usize,
        raw_window_cap_truncated: bool,
    ) -> DiscoveryTaskOutput {
        let active_wallets = (0..active_follow_wallets)
            .map(|idx| format!("wallet-{idx}"))
            .collect();
        DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
            eligible_wallets,
            active_follow_wallets,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Degraded,
            scoring_source: "raw_window_persisted_stream",
            raw_window_cap_truncated,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: None,
            cap_truncation_deactivation_guard_started_at: None,
            cap_truncation_floor_ts_utc: None,
            cap_truncation_floor_signature: None,
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        }
    }

    fn live_like_degraded_published_universe_output(
        now: DateTime<Utc>,
        active_follow_wallets: usize,
        eligible_wallets: usize,
    ) -> DiscoveryTaskOutput {
        let active_wallets = (0..active_follow_wallets)
            .map(|idx| format!("wallet-{idx}"))
            .collect();
        DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
            eligible_wallets,
            active_follow_wallets,
            published: false,
            runtime_mode: DiscoveryRuntimeMode::Degraded,
            scoring_source: "published_universe_raw_window_degraded",
            raw_window_cap_truncated: false,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: None,
            cap_truncation_deactivation_guard_started_at: None,
            cap_truncation_floor_ts_utc: None,
            cap_truncation_floor_signature: None,
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        }
    }

    fn live_like_fail_closed_no_recent_published_universe_output(
        now: DateTime<Utc>,
    ) -> DiscoveryTaskOutput {
        DiscoveryTaskOutput {
            active_wallets: HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 0,
            active_follow_wallets: 0,
            published: false,
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            scoring_source: "raw_window_incomplete_no_recent_published_universe",
            raw_window_cap_truncated: false,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: None,
            cap_truncation_deactivation_guard_started_at: None,
            cap_truncation_floor_ts_utc: None,
            cap_truncation_floor_signature: None,
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        }
    }

    fn test_swap(signature: &str) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-test".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T16:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    #[test]
    fn discovery_catch_up_scheduler_retriggers_only_for_safe_requested_runtime() {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let writer_snapshot = maintenance_test_writer_snapshot();
        let ingestion_snapshot = maintenance_test_ingestion_snapshot(0.0);

        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&ingestion_snapshot),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_allows_normal_writer_flow_below_threshold() {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests =
            DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD.saturating_sub(1);
        let ingestion_snapshot = maintenance_test_ingestion_snapshot(0.0);

        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&ingestion_snapshot),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_skips_when_runtime_pressure_is_present() {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let writer_snapshot = maintenance_test_writer_snapshot();

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.25)),
        ));
        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            true,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
        assert!(!should_schedule_discovery_catch_up(
            &discovery_output_for_catch_up_tests(false),
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_allows_pressure_override_when_pending_requests_is_the_only_blocker(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD;

        assert!(should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_keeps_shadow_queue_as_hard_stop_even_with_pressure_override() {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let writer_snapshot = maintenance_test_writer_snapshot();

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            true,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(1.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_keeps_yellowstone_pressure_as_hard_stop_even_with_pressure_override(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let writer_snapshot = maintenance_test_writer_snapshot();

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(1.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_keeps_writer_queue_depth_as_hard_stop_even_with_pressure_override(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD;
        writer_snapshot.aggregate_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.aggregate_queue_depth_batches = 0;
        writer_snapshot.journal_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_skips_when_writer_crosses_threshold() {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.pending_requests = 0;
        writer_snapshot.aggregate_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.aggregate_queue_depth_batches = 0;
        writer_snapshot.journal_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn sanitize_json_value_escapes_control_characters() {
        let raw = "line1\nline2\rline3\t\"\\\u{0000}";
        assert_eq!(
            sanitize_json_value(raw),
            "line1\\nline2\\rline3\\t\\\"\\\\\\u0000"
        );
    }

    #[test]
    fn resolve_execution_adapter_secrets_reads_file_sources() -> Result<()> {
        let auth_path = write_temp_secret_file("auth-token", "token-from-file\n")?;
        let hmac_path = write_temp_secret_file("hmac-secret", "hmac-from-file \n")?;
        let dynamic_api_auth_path =
            write_temp_secret_file("dynamic-api-auth", "api-token-file \n")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = auth_path.to_string_lossy().to_string();
        execution.submit_adapter_hmac_secret_file = hmac_path.to_string_lossy().to_string();
        execution.submit_dynamic_cu_price_api_auth_token_file =
            dynamic_api_auth_path.to_string_lossy().to_string();

        resolve_execution_adapter_secrets(&mut execution, config_path.as_path())?;
        assert_eq!(execution.submit_adapter_auth_token, "token-from-file");
        assert_eq!(execution.submit_adapter_hmac_secret, "hmac-from-file");
        assert_eq!(
            execution.submit_dynamic_cu_price_api_auth_token,
            "api-token-file"
        );

        let _ = std::fs::remove_file(auth_path);
        let _ = std::fs::remove_file(hmac_path);
        let _ = std::fs::remove_file(dynamic_api_auth_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_inline_and_file_conflict() -> Result<()> {
        let auth_path = write_temp_secret_file("auth-conflict", "token-from-file\n")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token = "inline-token".to_string();
        execution.submit_adapter_auth_token_file = auth_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("inline+file secret conflict must fail");
        assert!(
            error.to_string().contains("cannot be set at the same time"),
            "unexpected error: {}",
            error
        );

        let _ = std::fs::remove_file(auth_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_hmac_inline_and_file_conflict() -> Result<()> {
        let hmac_path = write_temp_secret_file("hmac-conflict", "hmac-file-secret\n")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_hmac_secret = "inline-hmac".to_string();
        execution.submit_adapter_hmac_secret_file = hmac_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("inline+file hmac secret conflict must fail");
        assert!(
            error.to_string().contains("cannot be set at the same time"),
            "unexpected error: {}",
            error
        );

        let _ = std::fs::remove_file(hmac_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_dynamic_api_inline_and_file_conflict() -> Result<()>
    {
        let api_token_path = write_temp_secret_file("dynamic-api-conflict", "api-token-file\n")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_dynamic_cu_price_api_auth_token = "inline-api-token".to_string();
        execution.submit_dynamic_cu_price_api_auth_token_file =
            api_token_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("inline+file dynamic API token conflict must fail");
        assert!(
            error.to_string().contains("cannot be set at the same time"),
            "unexpected error: {}",
            error
        );

        let _ = std::fs::remove_file(api_token_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_empty_secret_file() -> Result<()> {
        let empty_path = write_temp_secret_file("auth-empty", " \n\t")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = empty_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("empty secret file must fail");
        assert!(
            error_chain_contains(&error, "is empty"),
            "unexpected error: {}",
            error
        );

        let _ = std::fs::remove_file(empty_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_missing_secret_file() -> Result<()> {
        let missing_path = std::env::temp_dir().join(format!(
            "copybot-secret-missing-{}-{}.txt",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        ));
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = missing_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("missing secret file must fail");
        assert!(
            error_chain_contains(&error, "failed reading secret file"),
            "unexpected error: {}",
            error
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn resolve_execution_adapter_secrets_rejects_broad_permissions_secret_file() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let auth_path = write_temp_secret_file("auth-broad-perms", "token-from-file\n")?;
        let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml");

        let mut perms = std::fs::metadata(&auth_path)?.permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&auth_path, perms)?;

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = auth_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution, config_path.as_path())
            .expect_err("broad secret file permissions must fail");
        assert!(
            error_chain_contains(&error, "owner-only permissions"),
            "unexpected error: {}",
            error
        );

        let _ = std::fs::remove_file(auth_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_resolves_relative_paths_from_config_dir() -> Result<()> {
        let root = write_temp_secret_dir("relative-resolve")?;
        let config_dir = root.join("configs");
        let secrets_dir = root.join("secrets");
        std::fs::create_dir_all(&config_dir)?;
        std::fs::create_dir_all(&secrets_dir)?;

        let auth_path = secrets_dir.join("auth.txt");
        let hmac_path = secrets_dir.join("hmac.txt");
        let api_token_path = secrets_dir.join("priority_api.token");
        std::fs::write(&auth_path, "auth-rel\n")?;
        std::fs::write(&hmac_path, "hmac-rel\n")?;
        std::fs::write(&api_token_path, "priority-api-rel\n")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&auth_path, &hmac_path, &api_token_path] {
                let mut perms = std::fs::metadata(path)?.permissions();
                perms.set_mode(0o600);
                std::fs::set_permissions(path, perms)?;
            }
        }

        let loaded_config_path = config_dir.join("dev.toml");
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = "../secrets/auth.txt".to_string();
        execution.submit_adapter_hmac_secret_file = "../secrets/hmac.txt".to_string();
        execution.submit_dynamic_cu_price_api_auth_token_file =
            "../secrets/priority_api.token".to_string();

        resolve_execution_adapter_secrets(&mut execution, loaded_config_path.as_path())?;
        assert_eq!(execution.submit_adapter_auth_token, "auth-rel");
        assert_eq!(execution.submit_adapter_hmac_secret, "hmac-rel");
        assert_eq!(
            execution.submit_dynamic_cu_price_api_auth_token,
            "priority-api-rel"
        );

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_route_price_above_pretrade_cap() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.default_route = "jito".to_string();
        execution.submit_allowed_routes = vec!["jito".to_string(), "rpc".to_string()];
        execution.submit_route_max_slippage_bps =
            BTreeMap::from([("jito".to_string(), 50.0), ("rpc".to_string(), 50.0)]);
        execution.submit_route_tip_lamports =
            BTreeMap::from([("jito".to_string(), 0), ("rpc".to_string(), 0)]);
        execution.submit_route_compute_unit_limit =
            BTreeMap::from([("jito".to_string(), 300_000), ("rpc".to_string(), 300_000)]);
        execution.submit_route_compute_unit_price_micro_lamports =
            BTreeMap::from([("jito".to_string(), 500), ("rpc".to_string(), 1_500)]);
        execution.pretrade_max_priority_fee_lamports = 1_000;
        execution.slippage_bps = 50.0;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("allowed-route CU price above pretrade cap must fail");
        assert!(
            error
                .to_string()
                .contains("route rpc price (1500) cannot exceed"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_cu_price_without_pretrade_cap() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_cu_price_enabled = true;
        execution.pretrade_max_priority_fee_lamports = 0;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic CU price without pretrade cap must fail-closed");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_cu_price_enabled=true"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_cu_price_outside_adapter_mode() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "paper".to_string();
        execution.submit_dynamic_cu_price_enabled = true;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic CU price must be restricted to adapter_submit_confirm");
        assert!(
            error
                .to_string()
                .contains("only supported in execution.mode=adapter_submit_confirm"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_dynamic_cu_price_percentile() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.submit_dynamic_cu_price_percentile = 0;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic cu price percentile below range must fail");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_cu_price_percentile must be in 1..=100"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_cu_price_api_without_dynamic_policy() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_cu_price_enabled = false;
        execution.submit_dynamic_cu_price_api_primary_url =
            "https://priority.example.com/v1/fees".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic cu price api must require dynamic policy");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_cu_price_api_* settings require"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_cu_price_api_fallback_without_primary() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_cu_price_enabled = true;
        execution.pretrade_max_priority_fee_lamports = 2_000;
        execution.submit_dynamic_cu_price_api_fallback_url =
            "https://priority-fallback.example.com/v1/fees".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic cu price api fallback without primary must fail");
        assert!(
            error.to_string().contains("requires non-empty"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_duplicate_dynamic_cu_price_api_endpoints() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_cu_price_enabled = true;
        execution.pretrade_max_priority_fee_lamports = 2_000;
        execution.submit_dynamic_cu_price_api_primary_url =
            "https://priority.example.com/v1/fees".to_string();
        execution.submit_dynamic_cu_price_api_fallback_url =
            "https://priority.example.com:443/v1/fees".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("duplicate dynamic cu price api endpoint identity must fail");
        assert!(
            error
                .to_string()
                .contains("must resolve to distinct endpoints"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_tip_without_dynamic_cu_price() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_tip_lamports_enabled = true;
        execution.submit_dynamic_cu_price_enabled = false;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic tip requires dynamic cu price");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_tip_lamports_enabled=true requires execution.submit_dynamic_cu_price_enabled=true"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_dynamic_tip_outside_adapter_mode() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "paper".to_string();
        execution.submit_dynamic_tip_lamports_enabled = true;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic tip must be restricted to adapter_submit_confirm");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_tip_lamports_enabled is only supported"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_dynamic_tip_multiplier_bps() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_dynamic_cu_price_enabled = true;
        execution.pretrade_max_priority_fee_lamports = 2_000;
        execution.submit_dynamic_tip_lamports_enabled = true;
        execution.submit_dynamic_tip_lamports_multiplier_bps = 0;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("dynamic tip multiplier outside range must fail");
        assert!(
            error
                .to_string()
                .contains("submit_dynamic_tip_lamports_multiplier_bps must be in 1..=100000"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_pretrade_max_fee_overhead_bps() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.pretrade_max_fee_overhead_bps = 10_001;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("fee overhead guard outside 0..=10000 must fail");
        assert!(
            error
                .to_string()
                .contains("execution.pretrade_max_fee_overhead_bps must be in 0..=10000"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_contract_version_token() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        for contract_version in ["v1 beta", "v1/rollout"] {
            execution.submit_adapter_contract_version = contract_version.to_string();
            let error = validate_execution_runtime_contract(&execution, "paper")
                .expect_err("invalid contract version token must fail");
            assert!(
                error
                    .to_string()
                    .contains("must contain only [A-Za-z0-9._-]"),
                "unexpected error for contract_version {}: {}",
                contract_version,
                error
            );
        }
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_duplicate_allowed_routes_after_normalization() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.submit_allowed_routes = vec!["paper".to_string(), "PAPER".to_string()];
        execution.default_route = "paper".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("duplicate allowed routes must fail after normalization");
        assert!(
            error
                .to_string()
                .contains("execution.submit_allowed_routes contains duplicate route"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_duplicate_submit_route_order_after_normalization(
    ) {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.submit_allowed_routes = vec!["paper".to_string()];
        execution.submit_route_order = vec!["paper".to_string(), "PAPER".to_string()];
        execution.default_route = "paper".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("duplicate route order entries must fail after normalization");
        assert!(
            error
                .to_string()
                .contains("execution.submit_route_order contains duplicate route"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_duplicate_route_policy_map_keys_after_normalization(
    ) {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.submit_allowed_routes = vec!["paper".to_string()];
        execution.default_route = "paper".to_string();
        execution.submit_route_tip_lamports =
            BTreeMap::from([("paper".to_string(), 0), ("PAPER".to_string(), 0)]);

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("duplicate route policy map keys must fail after normalization");
        assert!(
            error
                .to_string()
                .contains("execution.submit_route_tip_lamports contains duplicate route key"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_fastlane_routes_when_feature_flag_disabled() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_fastlane_enabled = false;
        execution.default_route = "fastlane".to_string();
        execution.submit_allowed_routes = vec!["fastlane".to_string(), "rpc".to_string()];
        execution.submit_route_order = vec!["fastlane".to_string(), "rpc".to_string()];
        execution.submit_route_max_slippage_bps =
            BTreeMap::from([("fastlane".to_string(), 50.0), ("rpc".to_string(), 50.0)]);
        execution.submit_route_tip_lamports =
            BTreeMap::from([("fastlane".to_string(), 10_000), ("rpc".to_string(), 0)]);
        execution.submit_route_compute_unit_limit = BTreeMap::from([
            ("fastlane".to_string(), 300_000),
            ("rpc".to_string(), 300_000),
        ]);
        execution.submit_route_compute_unit_price_micro_lamports =
            BTreeMap::from([("fastlane".to_string(), 1_500), ("rpc".to_string(), 1_000)]);
        execution.pretrade_max_priority_fee_lamports = 2_000;

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("fastlane route policy must be blocked when feature flag is disabled");
        assert!(
            error
                .to_string()
                .contains("execution.submit_fastlane_enabled must be true"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_allows_fastlane_routes_when_feature_flag_enabled() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_fastlane_enabled = true;
        execution.default_route = "fastlane".to_string();
        execution.submit_allowed_routes = vec!["fastlane".to_string(), "rpc".to_string()];
        execution.submit_route_order = vec!["fastlane".to_string(), "rpc".to_string()];
        execution.submit_route_max_slippage_bps =
            BTreeMap::from([("fastlane".to_string(), 50.0), ("rpc".to_string(), 50.0)]);
        execution.submit_route_tip_lamports =
            BTreeMap::from([("fastlane".to_string(), 10_000), ("rpc".to_string(), 0)]);
        execution.submit_route_compute_unit_limit = BTreeMap::from([
            ("fastlane".to_string(), 300_000),
            ("rpc".to_string(), 300_000),
        ]);
        execution.submit_route_compute_unit_price_micro_lamports =
            BTreeMap::from([("fastlane".to_string(), 1_500), ("rpc".to_string(), 1_000)]);
        execution.pretrade_max_priority_fee_lamports = 2_000;

        validate_execution_runtime_contract(&execution, "paper")
            .expect("fastlane route policy should pass when feature flag is enabled");
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_duplicate_primary_and_fallback_adapter_endpoint()
    {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://adapter.local/submit".to_string();
        execution.submit_adapter_fallback_http_url = "https://ADAPTER.local:443/submit".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("duplicate primary/fallback adapter endpoints must fail");
        assert!(
            error
                .to_string()
                .contains("must resolve to distinct endpoints"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_requires_policy_echo_in_prod_adapter_mode() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_adapter_require_policy_echo = false;

        let error = validate_execution_runtime_contract(&execution, "prod")
            .expect_err("prod adapter mode must require strict policy echo");
        assert!(
            error
                .to_string()
                .contains("submit_adapter_require_policy_echo must be true in production-like"),
            "unexpected error: {}",
            error
        );

        execution.submit_adapter_require_policy_echo = true;
        validate_execution_runtime_contract(&execution, "prod")
            .expect("prod adapter mode with strict policy echo should pass");
    }

    #[test]
    fn validate_execution_runtime_contract_requires_policy_echo_in_prod_variants() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        execution.submit_adapter_require_policy_echo = false;
        for env in ["prod-eu", "PRODUCTION", "production_canary"] {
            validate_execution_runtime_contract(&execution, env).expect_err(
                "production-like adapter mode must require strict policy echo across env variants",
            );
        }

        execution.submit_adapter_require_policy_echo = true;
        for env in ["prod-eu", "PRODUCTION", "production_canary"] {
            validate_execution_runtime_contract(&execution, env)
                .expect("production-like adapter mode with strict policy echo should pass");
        }
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_adapter_endpoint_url() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("adapter endpoint without scheme must fail");
        assert!(
            error
                .to_string()
                .contains("must start with http:// or https://"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_adapter_endpoint_placeholder() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        for endpoint in ["https://REPLACE_ME.example", "https://replace_me.example"] {
            execution.submit_adapter_http_url = endpoint.to_string();
            let error = validate_execution_runtime_contract(&execution, "paper")
                .expect_err("adapter endpoint placeholder must fail contract validation");
            assert!(
                error.to_string().contains("must not contain placeholder"),
                "unexpected error for endpoint {}: {}",
                endpoint,
                error
            );
        }
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_invalid_adapter_endpoint_authority_forms() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        for endpoint in [
            "https://:443",
            "https://?x=1",
            "https://#frag",
            "https://[::1",
        ] {
            execution.submit_adapter_http_url = endpoint.to_string();
            let error = validate_execution_runtime_contract(&execution, "paper").expect_err(
                "adapter endpoint with malformed/empty host authority must fail contract validation",
            );
            assert!(
                error.to_string().contains("must be a valid http(s) URL"),
                "unexpected error for endpoint {}: {}",
                endpoint,
                error
            );
        }
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_adapter_endpoint_with_url_credentials() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://user:pass@adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        let error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("adapter endpoint with URL credentials must fail");
        assert!(
            error
                .to_string()
                .contains("must not embed credentials in URL"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_adapter_endpoint_with_query_or_fragment() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        execution.submit_adapter_http_url = "https://adapter.local?api-key=secret".to_string();
        let query_error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("adapter endpoint with query must fail");
        assert!(
            query_error
                .to_string()
                .contains("must not include query parameters"),
            "unexpected error: {}",
            query_error
        );

        execution.submit_adapter_http_url = "https://adapter.local#frag".to_string();
        let fragment_error = validate_execution_runtime_contract(&execution, "paper")
            .expect_err("adapter endpoint with fragment must fail");
        assert!(
            fragment_error
                .to_string()
                .contains("must not include URL fragment"),
            "unexpected error: {}",
            fragment_error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_fallback_adapter_endpoint_with_secret_bearing_url_forms(
    ) {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "https://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();

        for (endpoint, expected_error) in [
            (
                "https://user:pass@adapter-fallback.local",
                "must not embed credentials in URL",
            ),
            (
                "https://adapter-fallback.local?api-key=secret",
                "must not include query parameters",
            ),
            (
                "https://adapter-fallback.local#frag",
                "must not include URL fragment",
            ),
        ] {
            execution.submit_adapter_fallback_http_url = endpoint.to_string();
            let error = validate_execution_runtime_contract(&execution, "paper")
                .expect_err("fallback adapter endpoint with secret-bearing URL form must fail");
            assert!(
                error.to_string().contains(expected_error),
                "unexpected error for fallback endpoint {}: {}",
                endpoint,
                error
            );
        }
    }

    #[test]
    fn validate_execution_runtime_contract_rejects_non_loopback_http_adapter_endpoint_in_prod() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://adapter.local".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_adapter_require_policy_echo = true;

        let error = validate_execution_runtime_contract(&execution, "prod")
            .expect_err("non-loopback http adapter endpoint must fail in production-like env");
        assert!(
            error
                .to_string()
                .contains("must use https:// in production-like envs"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_runtime_contract_allows_loopback_http_adapter_endpoint_in_prod() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.rpc_http_url = "http://rpc.local".to_string();
        execution.submit_adapter_http_url = "http://127.0.0.1:8080".to_string();
        execution.execution_signer_pubkey = "signer-pubkey".to_string();
        execution.submit_adapter_require_policy_echo = true;

        validate_execution_runtime_contract(&execution, "prod")
            .expect("loopback http adapter endpoint should be allowed in production-like env");

        execution.submit_adapter_http_url = "http://[0:0:0:0:0:0:0:1]:8080".to_string();
        validate_execution_runtime_contract(&execution, "prod").expect(
            "expanded IPv6 loopback representation should be allowed in production-like env",
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_shadow_caps() {
        let mut risk = RiskConfig::default();
        risk.shadow_soft_exposure_cap_sol = f64::NAN;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("non-finite shadow soft cap must fail risk contract");
        assert!(
            error
                .to_string()
                .contains("risk.shadow_soft_exposure_cap_sol"),
            "unexpected error: {}",
            error
        );

        let mut risk = RiskConfig::default();
        risk.shadow_soft_exposure_resume_below_sol = 11.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("soft exposure resume threshold above soft cap must fail");
        assert!(
            error
                .to_string()
                .contains("risk.shadow_soft_exposure_resume_below_sol"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_live_loss_limits() {
        let mut risk = RiskConfig::default();
        risk.daily_loss_limit_pct = -0.1;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("negative daily loss limit percent must fail");
        assert!(
            error.to_string().contains("risk.daily_loss_limit_pct"),
            "unexpected error: {}",
            error
        );

        let mut risk = RiskConfig::default();
        risk.max_drawdown_pct = 150.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("max drawdown percent above 100 must fail");
        assert!(
            error.to_string().contains("risk.max_drawdown_pct"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_shadow_drawdown_order() {
        let mut risk = RiskConfig::default();
        risk.shadow_drawdown_1h_stop_sol = -4.0;
        risk.shadow_drawdown_6h_stop_sol = -1.0;
        risk.shadow_drawdown_24h_stop_sol = -5.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("invalid drawdown threshold order must fail");
        assert!(
            error.to_string().contains("drawdown ordering is invalid"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_rejects_fee_overhead_policy_disabled_in_prod_live() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.pretrade_max_fee_overhead_bps = 0;
        let mut risk = RiskConfig::default();
        risk.execution_buy_cooldown_seconds = 60;

        let error = validate_live_execution_policy_contract(&execution, &risk, "prod-live")
            .expect_err("prod-live execution must keep fee-overhead policy enabled");
        assert!(
            error
                .to_string()
                .contains("execution.pretrade_max_fee_overhead_bps > 0"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_rejects_zero_buy_cooldown_in_prod_live() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.pretrade_max_fee_overhead_bps = 1_000;
        let risk = RiskConfig::default();

        let error = validate_live_execution_policy_contract(&execution, &risk, "prod-live")
            .expect_err("prod-live execution must keep buy cooldown enabled");
        assert!(
            error
                .to_string()
                .contains("risk.execution_buy_cooldown_seconds >= 1"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_rejects_sub_floor_sol_reserve_in_prod_live() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.pretrade_min_sol_reserve = 0.049;
        execution.pretrade_max_fee_overhead_bps = 1_000;
        let mut risk = RiskConfig::default();
        risk.execution_buy_cooldown_seconds = 60;

        let error = validate_live_execution_policy_contract(&execution, &risk, "prod-live")
            .expect_err("prod-live execution must keep 0.05 SOL reserve floor");
        assert!(
            error
                .to_string()
                .contains("execution.pretrade_min_sol_reserve >= 0.05 SOL"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_allows_zeroed_guards_outside_prod_like_env() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        let risk = RiskConfig::default();

        validate_live_execution_policy_contract(&execution, &risk, "paper")
            .expect("paper env should not enforce prod-live reserve/cooldown contract");
    }

    fn infra_snapshot(
        ts_utc: DateTime<Utc>,
        ws_notifications_enqueued: u64,
        ws_notifications_replaced_oldest: u64,
    ) -> IngestionRuntimeSnapshot {
        IngestionRuntimeSnapshot {
            ts_utc,
            ws_notifications_enqueued,
            ws_notifications_replaced_oldest,
            grpc_message_total: 2_400_000 + ws_notifications_enqueued,
            grpc_transaction_updates_total: 24_000 + ws_notifications_enqueued,
            parse_rejected_total: 0,
            grpc_decode_errors: 0,
            rpc_429: 0,
            rpc_5xx: 0,
            ingestion_lag_ms_p95: 2_000,
            yellowstone_output_queue_depth: 0,
            yellowstone_output_queue_capacity: 0,
            yellowstone_output_queue_fill_ratio: 0.0,
            yellowstone_output_oldest_age_ms: 0,
        }
    }

    fn infra_snapshot_with_yellowstone_queue(
        ts_utc: DateTime<Utc>,
        ws_notifications_enqueued: u64,
        ws_notifications_replaced_oldest: u64,
        yellowstone_output_queue_depth: u64,
        yellowstone_output_queue_capacity: u64,
        yellowstone_output_oldest_age_ms: u64,
    ) -> IngestionRuntimeSnapshot {
        let mut snapshot = infra_snapshot(
            ts_utc,
            ws_notifications_enqueued,
            ws_notifications_replaced_oldest,
        );
        snapshot.yellowstone_output_queue_depth = yellowstone_output_queue_depth;
        snapshot.yellowstone_output_queue_capacity = yellowstone_output_queue_capacity;
        snapshot.yellowstone_output_queue_fill_ratio = if yellowstone_output_queue_capacity == 0 {
            0.0
        } else {
            yellowstone_output_queue_depth as f64 / yellowstone_output_queue_capacity as f64
        };
        snapshot.yellowstone_output_oldest_age_ms = yellowstone_output_oldest_age_ms;
        snapshot
    }

    #[test]
    fn risk_guard_infra_ratio_uses_window_delta_not_cumulative_with_consecutive_hysteresis(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_176, 14_400_134),
        ]);
        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "rolling delta ratio (134/176 ~= 0.76) should not trigger threshold 0.80"
        );

        for offset in 1..RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
            assert!(
                guard.infra_block_reason.is_none(),
                "infra gate should wait for consecutive breaches before activating"
            );
        }
        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(
                    20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                ),
                16_500_300 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 40),
                14_400_270 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 36),
            )),
        )?;
        assert!(
            guard.infra_block_reason.is_some(),
            "rolling delta ratio should activate after consecutive breaches"
        );

        for offset in 1..RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(400 + 20 * offset as i64),
                    16_502_000 + (offset as u64 * 200),
                    14_400_900 + (offset as u64 * 5),
                )),
            )?;
            assert!(
                guard.infra_block_reason.is_some(),
                "infra gate should require consecutive healthy samples before clearing"
            );
        }
        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
                16_502_000 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 200),
                14_400_900 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 5),
            )),
        )?;
        assert!(
            guard.infra_block_reason.is_none(),
            "infra gate should clear after consecutive healthy samples"
        );

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        let infra_clears = store.list_risk_events_by_type_desc("shadow_risk_infra_cleared")?;
        assert_eq!(
            infra_stops.len(),
            1,
            "expected one infra stop activation event"
        );
        assert_eq!(infra_clears.len(), 1, "expected one infra clear event");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_replaced_ratio_does_not_gate_yellowstone_source() -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio-yellowstone")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new_with_ingestion_source(cfg, "yellowstone_grpc");
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_300, 14_400_270),
        ]);
        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "yellowstone replaced_ratio should remain telemetry-only and not gate buys"
        );

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(
            guard.infra_block_reason.is_none(),
            "yellowstone replaced_ratio should not activate infra gate even after sustained breaches"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_returns_error_on_fatal_infra_stop_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_176, 14_400_134),
        ]);

        for offset in 1..RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(guard.infra_block_key.is_none());

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_infra_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_infra_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(
                    20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                ),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(
                        20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                    ),
                    16_500_300 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 40),
                    14_400_270 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 36),
                )),
            )
            .expect_err(
                "fatal infra-stop risk event write must abort ingestion-snapshot observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk infra stop event with fatal sqlite I/O"),
            "expected infra-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.infra_block_key.is_none(),
            "fatal infra-stop write must not flip runtime infra block state"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_infra_stop")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_returns_error_on_fatal_infra_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_176, 14_400_134),
        ]);

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(guard.infra_block_key.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_infra_stop")?, 1);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_infra_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_infra_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        for offset in 1..RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(400 + 20 * offset as i64),
                    16_502_000 + (offset as u64 * 200),
                    14_400_900 + (offset as u64 * 5),
                )),
            )?;
        }
        let error = guard
            .observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(
                        400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64,
                    ),
                    16_502_000 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 200),
                    14_400_900 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 5),
                )),
            )
            .expect_err(
                "fatal infra-clear risk event write must abort ingestion-snapshot observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk infra clear event with fatal sqlite I/O"),
            "expected infra-clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.infra_block_key.is_some(),
            "fatal infra-clear write must preserve runtime infra block state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_infra_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_stop_does_not_reemit_when_same_key_detail_changes() -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio-reemit")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_176, 14_400_134),
        ]);

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 60),
                    14_400_270 + (offset as u64 * 54),
                )),
            )?;
        }
        let first_reason = guard
            .infra_block_reason
            .clone()
            .expect("infra gate should activate after sustained replaced_ratio breaches");

        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(140),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(140),
                16_501_000,
                14_400_950,
            )),
        )?;
        let updated_reason = guard
            .infra_block_reason
            .clone()
            .expect("infra gate should remain active for same-key follow-up breach");
        assert_ne!(
            first_reason, updated_reason,
            "reason detail should still refresh even when gating identity stays stable"
        );
        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(
            infra_stops.len(),
            1,
            "dynamic replaced_ratio details must not create duplicate infra stop events"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_blocks_when_no_ingestion_progress_for_full_window() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("no progress over full infra window must trigger block");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_reason_includes_yellowstone_queue_context() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new_with_ingestion_source(cfg, "yellowstone_grpc");
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("yellowstone no-progress gate should still trigger");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_depth=9"),
            "expected queue depth context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_capacity=10"),
            "expected queue capacity context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_fill_ratio=0.9000"),
            "expected queue fill ratio context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_oldest_age_ms=360"),
            "expected queue oldest-age context, got: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_reason_omits_yellowstone_queue_context_for_non_yellowstone() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("mock-source no-progress gate should still trigger");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
        assert!(
            !reason.contains("yellowstone_output_queue_depth="),
            "non-yellowstone source must not append yellowstone queue context: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_persists_yellowstone_queue_context_in_infra_stop_details(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-yellowstone-details")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new_with_ingestion_source(cfg, "yellowstone_grpc");
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot_with_yellowstone_queue(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    10_000,
                    0,
                    9,
                    10,
                    360,
                )),
            )?;
        }

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(infra_stops.len(), 1, "expected one infra stop event");
        let details_json = infra_stops[0]
            .details_json
            .as_deref()
            .expect("infra stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse infra stop details_json")?;
        let reason = details
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .expect("infra stop details must include reason");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {reason}"
        );
        assert_eq!(
            details
                .get("yellowstone_output_queue_depth")
                .and_then(serde_json::Value::as_u64),
            Some(9)
        );
        assert_eq!(
            details
                .get("yellowstone_output_queue_capacity")
                .and_then(serde_json::Value::as_u64),
            Some(10)
        );
        assert_eq!(
            details
                .get("yellowstone_output_oldest_age_ms")
                .and_then(serde_json::Value::as_u64),
            Some(360)
        );
        let fill_ratio = details
            .get("yellowstone_output_queue_fill_ratio")
            .and_then(serde_json::Value::as_f64)
            .expect("yellowstone details must include fill ratio");
        assert!(
            (fill_ratio - 0.9).abs() < f64::EPSILON,
            "unexpected fill ratio: {fill_ratio}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_omits_yellowstone_queue_context_in_infra_stop_details_for_non_yellowstone(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-non-yellowstone-details")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot_with_yellowstone_queue(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    10_000,
                    0,
                    9,
                    10,
                    360,
                )),
            )?;
        }

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(infra_stops.len(), 1, "expected one infra stop event");
        let details_json = infra_stops[0]
            .details_json
            .as_deref()
            .expect("infra stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse infra stop details_json")?;
        assert!(
            details.get("reason").is_some(),
            "infra stop details must still include reason"
        );
        assert!(
            details.get("yellowstone_output_queue_depth").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_queue_capacity").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_queue_fill_ratio").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_oldest_age_ms").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_no_progress_does_not_block_when_grpc_transaction_updates_advance() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 910_000, // could include pings
                grpc_transaction_updates_total: 910_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 920_000, // could include pings
                grpc_transaction_updates_total: 920_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "no-ingestion-progress gate must not trigger while grpc_transaction_updates_total is advancing"
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_still_blocks_when_only_grpc_ping_total_advances() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 910_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 920_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard.compute_infra_block_reason(now).expect(
            "no-ingestion-progress gate must trigger when only ping-level grpc traffic advances",
        );
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_blocks_when_parser_stall_detected() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 240,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 430,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("parser-stall pattern over full window must trigger block");
        assert!(
            reason.contains("parser_stall_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 120,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 190,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "parser-stall gate must not trigger when error ratio is below threshold"
        );
    }

    #[test]
    fn risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 240,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 425,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("parser-stall gate must trigger at >= 0.95 ratio boundary");
        assert!(
            reason.contains("parser_stall_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_universe_stops_after_consecutive_breaches() -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        guard.observe_discovery_cycle(&store, now, 70, 14, None)?;
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(3), 70, 14, None)?;
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(6), 70, 14, None)?;
        assert!(guard.universe_blocked);

        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(9), 150, 30, None)?;
        assert!(!guard.universe_blocked);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_below_policy_minimum_blocks_shadow_buys_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-below-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 10, 10, true);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "a complete but tiny published universe must still trip the hard shadow policy minimums"
        );
        let decision = guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                detail,
            } => {
                assert!(
                    detail.contains("universe_breach_streak=3"),
                    "unexpected universe block detail: {detail}"
                );
            }
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(10));
        assert_eq!(details["eligible_wallets"], serde_json::json!(10));
        assert_eq!(details["min_active_follow_wallets"], serde_json::json!(15));
        assert_eq!(details["min_eligible_wallets"], serde_json::json!(80));
        assert_eq!(
            details["discovery_runtime_mode"],
            serde_json::json!("degraded")
        );
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_stop_is_threshold_driven_not_cap_truncation_driven_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-threshold-only")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 10, 10, false);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "the exact live stop class is driven by policy minimums, not by raw-window truncation context"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            } => {}
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "cap-truncation context should be absent here, but the stop must still arm: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_at_policy_minimum_allows_shadow_buys_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-at-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 15, 80, true);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            !guard.universe_blocked,
            "a complete published universe that meets the configured minimums must not be blocked"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("expected buy risk allow, got {other:?}"),
        }
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0,
            "policy-satisfying complete truth must not emit a universe stop"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_exact_live_nine_wallet_degraded_published_universe_blocks_under_current_policy_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-current-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_degraded_published_universe_output(now, 9, 9);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "the exact current live class must still block under the shipped 15/80 universe minimums"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            } => {}
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(9));
        assert_eq!(details["eligible_wallets"], serde_json::json!(9));
        assert_eq!(
            details["discovery_runtime_mode"],
            serde_json::json!("degraded")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_exact_live_nine_wallet_degraded_published_universe_requires_both_minimums_to_move_stage1(
    ) -> Result<()> {
        let now = Utc::now();
        let discovery_output = live_like_degraded_published_universe_output(now, 9, 9);

        let (active_only_store, active_only_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-active-only")?;
        let mut active_only_cfg = RiskConfig::default();
        active_only_cfg.shadow_universe_min_active_follow_wallets = 9;
        active_only_cfg.shadow_universe_min_eligible_wallets = 80;
        active_only_cfg.shadow_universe_breach_cycles = 3;
        let mut active_only_guard = ShadowRiskGuard::new(active_only_cfg);
        for minute in [0_i64, 3, 6] {
            active_only_guard.observe_discovery_cycle(
                &active_only_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            active_only_guard.universe_blocked,
            "lowering only min_active_follow_wallets must still block because eligible_wallets remains below the configured floor"
        );

        let (eligible_only_store, eligible_only_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-eligible-only")?;
        let mut eligible_only_cfg = RiskConfig::default();
        eligible_only_cfg.shadow_universe_min_active_follow_wallets = 15;
        eligible_only_cfg.shadow_universe_min_eligible_wallets = 9;
        eligible_only_cfg.shadow_universe_breach_cycles = 3;
        let mut eligible_only_guard = ShadowRiskGuard::new(eligible_only_cfg);
        for minute in [0_i64, 3, 6] {
            eligible_only_guard.observe_discovery_cycle(
                &eligible_only_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            eligible_only_guard.universe_blocked,
            "lowering only min_eligible_wallets must still block because active_follow_wallets remains below the configured floor"
        );

        let (both_store, both_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-both-minimums")?;
        let mut both_cfg = RiskConfig::default();
        both_cfg.shadow_universe_min_active_follow_wallets = 9;
        both_cfg.shadow_universe_min_eligible_wallets = 9;
        both_cfg.shadow_universe_breach_cycles = 3;
        let mut both_guard = ShadowRiskGuard::new(both_cfg);
        for minute in [0_i64, 3, 6] {
            both_guard.observe_discovery_cycle(
                &both_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            !both_guard.universe_blocked,
            "moving both minimums to the exact current live discovery surface is the smallest policy change that clears the universe stop"
        );
        match both_guard.can_open_buy(&both_store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("expected buy risk allow, got {other:?}"),
        }
        assert_eq!(
            both_store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0,
            "the exact-live 9/9 universe should not emit a universe stop once both minimums match that current discovery surface"
        );

        let _ = std::fs::remove_file(active_only_db_path);
        let _ = std::fs::remove_file(eligible_only_db_path);
        let _ = std::fs::remove_file(both_db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_nine_nine_policy_still_blocks_smaller_or_missing_universe_stage1() -> Result<()> {
        let now = Utc::now();

        let (small_active_store, small_active_db_path) =
            make_test_store("universe-stop-nine-nine-small-active")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 9;
        cfg.shadow_universe_min_eligible_wallets = 9;
        cfg.shadow_universe_breach_cycles = 3;
        let mut small_active_guard = ShadowRiskGuard::new(cfg.clone());
        let small_active_output = live_like_degraded_published_universe_output(now, 8, 9);
        for minute in [0_i64, 3, 6] {
            small_active_guard.observe_discovery_cycle(
                &small_active_store,
                now + chrono::Duration::minutes(minute),
                small_active_output.eligible_wallets,
                small_active_output.active_follow_wallets,
                Some(&small_active_output),
            )?;
        }
        assert!(small_active_guard.universe_blocked);
        assert!(matches!(
            small_active_guard.can_open_buy(
                &small_active_store,
                now + chrono::Duration::minutes(7),
                true
            ),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let (small_eligible_store, small_eligible_db_path) =
            make_test_store("universe-stop-nine-nine-small-eligible")?;
        let mut small_eligible_guard = ShadowRiskGuard::new(cfg.clone());
        let small_eligible_output = live_like_degraded_published_universe_output(now, 9, 8);
        for minute in [0_i64, 3, 6] {
            small_eligible_guard.observe_discovery_cycle(
                &small_eligible_store,
                now + chrono::Duration::minutes(minute),
                small_eligible_output.eligible_wallets,
                small_eligible_output.active_follow_wallets,
                Some(&small_eligible_output),
            )?;
        }
        assert!(small_eligible_guard.universe_blocked);
        assert!(matches!(
            small_eligible_guard.can_open_buy(
                &small_eligible_store,
                now + chrono::Duration::minutes(7),
                true
            ),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let (missing_store, missing_db_path) =
            make_test_store("universe-stop-nine-nine-no-published-universe")?;
        let mut missing_guard = ShadowRiskGuard::new(cfg);
        let missing_output = discovery_output_for_catch_up_tests(false);
        for minute in [0_i64, 3, 6] {
            missing_guard.observe_discovery_cycle(
                &missing_store,
                now + chrono::Duration::minutes(minute),
                missing_output.eligible_wallets,
                missing_output.active_follow_wallets,
                Some(&missing_output),
            )?;
        }
        assert!(missing_guard.universe_blocked);
        assert!(matches!(
            missing_guard.can_open_buy(&missing_store, now + chrono::Duration::minutes(7), true),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let _ = std::fs::remove_file(small_active_db_path);
        let _ = std::fs::remove_file(small_eligible_db_path);
        let _ = std::fs::remove_file(missing_db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_persists_cap_truncation_context_in_universe_stop_details(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-cap-truncation-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let guard_started_at = now - chrono::Duration::minutes(2);
        let floor_ts = now - chrono::Duration::minutes(9);
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Degraded,
            scoring_source: "raw_window",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("warm_load_truncated"),
            cap_truncation_deactivation_guard_started_at: Some(guard_started_at),
            cap_truncation_floor_ts_utc: Some(floor_ts),
            cap_truncation_floor_signature: Some("restart-noise-buy-1".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(5));
        assert_eq!(details["eligible_wallets"], serde_json::json!(5));
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));
        assert_eq!(
            details["cap_truncation_deactivation_guard_active"],
            serde_json::json!(false)
        );
        assert_eq!(
            details["cap_truncation_deactivation_guard_reason"],
            serde_json::json!("warm_load_truncated")
        );
        assert_eq!(
            details["cap_truncation_deactivation_guard_started_at"],
            serde_json::json!(guard_started_at.to_rfc3339())
        );
        assert_eq!(
            details["cap_truncation_floor_ts"],
            serde_json::json!(floor_ts.to_rfc3339())
        );
        assert_eq!(
            details["cap_truncation_floor_signature"],
            serde_json::json!("restart-noise-buy-1")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_persists_cap_truncation_context_for_persisted_stream_scoring(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-cap-truncation-context-persisted")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let guard_started_at = now - chrono::Duration::minutes(2);
        let floor_ts = now - chrono::Duration::minutes(9);
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "raw_window_persisted_stream",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("warm_load_truncated"),
            cap_truncation_deactivation_guard_started_at: Some(guard_started_at),
            cap_truncation_floor_ts_utc: Some(floor_ts),
            cap_truncation_floor_signature: Some("restart-noise-buy-1".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));
        assert_eq!(
            details["cap_truncation_floor_signature"],
            serde_json::json!("restart-noise-buy-1")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_omits_cap_truncation_context_when_raw_window_not_truncated(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-no-cap-truncation-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "aggregates",
            raw_window_cap_truncated: false,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("live_cap_eviction"),
            cap_truncation_deactivation_guard_started_at: Some(now),
            cap_truncation_floor_ts_utc: Some(now),
            cap_truncation_floor_signature: Some("cap-sig-001".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );
        assert!(
            details
                .get("cap_truncation_deactivation_guard_reason")
                .is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );
        assert!(
            details.get("cap_truncation_floor_signature").is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_omits_cap_truncation_context_for_aggregate_scoring_with_lingering_floor(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-aggregate-no-cap-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "aggregates",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: true,
            cap_truncation_deactivation_guard_reason: Some("live_cap_eviction"),
            cap_truncation_deactivation_guard_started_at: Some(now),
            cap_truncation_floor_ts_utc: Some(now),
            cap_truncation_floor_signature: Some("aggregate-floor-sig".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );
        assert!(
            details
                .get("cap_truncation_deactivation_guard_reason")
                .is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );
        assert!(
            details.get("cap_truncation_floor_signature").is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_returns_error_on_fatal_universe_stop_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_universe_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_universe_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_discovery_cycle(&store, now, 70, 14, None)
            .expect_err(
                "fatal universe-stop risk event write must abort discovery-cycle observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk universe stop event with fatal sqlite I/O"
            ),
            "expected universe-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            !guard.universe_blocked,
            "fatal universe-stop write must not flip runtime blocked state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_returns_error_on_fatal_universe_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        guard.observe_discovery_cycle(&store, now, 70, 14, None)?;
        assert!(guard.universe_blocked);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            1
        );

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_universe_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_universe_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_discovery_cycle(&store, now + chrono::Duration::minutes(3), 150, 30, None)
            .expect_err(
                "fatal universe-clear risk event write must abort discovery-cycle observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk universe clear event with fatal sqlite I/O"
            ),
            "expected universe-clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.universe_blocked,
            "fatal universe-clear write must preserve runtime blocked state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_blocks_then_auto_clears() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                ..
            } => {}
            other => panic!("expected exposure-cap block, got {other:?}"),
        }

        store.delete_shadow_lot(lot_id)?;
        let decision_after_clear = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 1).max(6)),
            true,
        );
        match decision_after_clear {
            BuyRiskDecision::Allow => {}
            other => panic!("expected auto-clear after exposure normalizes, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_ignores_dust_shadow_lots() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-dust")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        store.update_shadow_lot(lot_id, 1e-13, 1.2)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Allow => {}
            other => panic!("expected dust lot to be ignored by hard exposure cap, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_uses_lamport_normalized_cap() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-lamport-cap")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 0.1000000004;
        cfg.shadow_soft_exposure_cap_sol = 99.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.1, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail,
            } => assert!(
                detail.contains("hard_cap"),
                "expected hard cap detail, got {detail}"
            ),
            other => panic!("expected lamport-normalized exposure block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_ignores_micro_swap_outlier_price() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-cleanup")?;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 0.9,
            amount_out: 900.0,
            signature: "sig-price-1".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(20),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-price-2".to_string(),
            slot: 2,
            ts_utc: now - chrono::Duration::minutes(12),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.1,
            amount_out: 1100.0,
            signature: "sig-price-3".to_string(),
            slot: 3,
            ts_utc: now - chrono::Duration::minutes(7),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 0.001,
            amount_out: 0.000001,
            signature: "sig-price-outlier".to_string(),
            slot: 4,
            ts_utc: now - chrono::Duration::minutes(1),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)?;

        assert_eq!(stats.closed_priced, 1);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let (trades, pnl) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 1);
        assert!(pnl > 0.0, "expected positive pnl after stale cleanup close");
        assert!(
            pnl < 2.0,
            "stale-close pnl must stay in realistic band and ignore micro-swap outlier (got {})",
            pnl
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 1);
        assert!(store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            0
        );
        let (trades, _) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_returns_error_on_fatal_price_unavailable_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-risk-event-fatal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_stale_close_risk_event_insert
             BEFORE INSERT ON risk_events
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let error = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)
            .expect_err("fatal stale-close risk event write must abort cleanup");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to record stale-close price unavailable risk event with fatal sqlite I/O"
            ),
            "expected stale-close fatal risk-event context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_recovery_zero_price_closes_unpriced_lot_only_when_enabled() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-unpriced")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-recovery".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.recovery_zero_closed, 1);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_recovery_zero_price")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            0
        );
        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_close_context(&signal_id)?,
            Some(copybot_storage::SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE.to_string())
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_terminal_closes_and_records_risk_events_when_reliable_price_missing_after_terminal_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-terminal-unpriced")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-terminal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            1
        );
        let (trades, pnl) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 1);
        assert!(
            pnl < 0.0,
            "terminal zero-price stale close must realize a loss"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_recovery_zero_price_does_not_override_terminal_close_after_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-terminal-boundary")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-recovery-terminal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.recovery_zero_closed, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_close_context(&signal_id)?,
            Some(copybot_storage::SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE.to_string())
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_recovery_zero_price")?,
            0
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_terminal_zero_price_preserves_exact_qty_sidecars() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced-exact")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1.0,
            signature: "sig-exact-only-one-sample".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot_exact(
            "wallet-a",
            "token-a",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.25,
            opened_ts,
        )?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_qty_exact(&signal_id)?,
            Some(TokenQuantity::new(500_000, 6))
        );
        assert_eq!(
            store.shadow_closed_trade_accounting_bucket(&signal_id)?,
            Some("exact_post_cutover".to_string())
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_ignores_terminal_zero_price_stale_close_losses_for_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-terminal-risk-ignore")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-risk-ignore".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;
        assert_eq!(stats.terminal_zero_closed, 1);

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(all_trades, 1);
        assert!(
            all_pnl < 0.0,
            "accounting pnl must keep terminal-zero loss visible"
        );

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::days(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("terminal-zero stale close should not trip hard stop, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_ignores_recovery_zero_price_stale_close_losses_for_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-risk-ignore")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-recovery-risk-ignore".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;
        assert_eq!(stats.recovery_zero_closed, 1);

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(all_trades, 1);
        assert!(all_pnl < 0.0);

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::days(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("recovery zero-price stale close should not trip hard stop, got {other:?}")
            }
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_stale_close_terminal_zero_price_hours() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 6;
        risk.shadow_stale_close_terminal_zero_price_hours = 11;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close terminal zero threshold validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_terminal_zero_price_hours"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_stale_close_recovery_without_max_hold_hours() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 0;
        risk.shadow_stale_close_recovery_zero_price_enabled = true;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close recovery validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_recovery_zero_price_enabled"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_stale_close_recovery_without_dead_zone() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 6;
        risk.shadow_stale_close_terminal_zero_price_hours = 0;
        risk.shadow_stale_close_recovery_zero_price_enabled = true;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close recovery dead-zone validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_terminal_zero_price_hours"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn risk_guard_drawdown_1h_triggers_timed_pause() -> Result<()> {
        let (store, db_path) = make_test_store("drawdown-1h")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -0.3;
        cfg.shadow_drawdown_1h_pause_minutes = 30;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        store.insert_shadow_closed_trade(
            "sig-dd1",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.5,
            -0.5,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("drawdown_1h")),
            other => panic!("expected timed pause block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_timed_pause_auto_clears_and_records_event() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-autoclear")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected timed pause block from soft exposure cap, got {other:?}"),
        }
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert!(
            guard.soft_exposure_pause_latched,
            "soft exposure pause should latch after breach"
        );
        assert!(
            guard.soft_exposure_pause_until.is_some(),
            "soft exposure pause should capture its initial cooldown window"
        );

        store.delete_shadow_lot(lot_id)?;
        let decision_after_clear = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61)),
            true,
        );
        match decision_after_clear {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("expected timed pause auto-clear after exposure normalized, got {other:?}")
            }
        }
        assert!(
            !guard.soft_exposure_pause_latched,
            "soft exposure pause latch should clear after exposure normalizes below resume threshold"
        );
        assert!(
            guard.soft_exposure_pause_until.is_none(),
            "soft exposure pause window should clear after recovery below resume threshold"
        );
        assert!(
            guard.soft_exposure_pause_reason.is_none(),
            "soft exposure pause reason should clear after recovery below resume threshold"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_ignores_quarantined_legacy_shadow_open_notional_for_exposure_gates() -> Result<()>
    {
        let (store, db_path) = make_test_store("risk-guard-ignores-quarantined-open-notional")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);

        store.insert_shadow_lot("wallet-a", "token-risk", 10.0, 0.45, opened_ts)?;
        let quarantined_lot_id =
            store.insert_shadow_lot("wallet-a", "token-quarantine", 10.0, 0.30, opened_ts)?;
        store.update_shadow_lot_risk_context(
            quarantined_lot_id,
            copybot_storage::SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
        )?;

        assert!((store.shadow_open_notional_sol()? - 0.75).abs() < 1e-12);
        assert!((store.shadow_risk_open_notional_sol()? - 0.45).abs() < 1e-12);

        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected quarantined legacy exposure to stay out of live risk gating, got {other:?}"
            ),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_soft_exposure_pause_does_not_rearm_or_extend_while_still_above_resume_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-no-rearm")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected initial soft pause block, got {other:?}"),
        }
        let initial_until = guard
            .soft_exposure_pause_until
            .expect("soft pause should set initial until");
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let still_breached_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 10).max(10));
        match guard.can_open_buy(&store, still_breached_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected soft pause to stay active without rearm, got {other:?}"),
        }
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let after_initial_until =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        match guard.can_open_buy(&store, after_initial_until, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!(
                "expected soft exposure latch to remain blocked above resume threshold, got {other:?}"
            ),
        }
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        store.delete_shadow_lot(lot_id)?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_soft_exposure_pause_requires_recovery_below_resume_threshold_to_clear(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-hysteresis")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let _ = guard.can_open_buy(&store, now, true);
        store.update_shadow_lot(lot_id, 10.0, 0.45)?;
        let between_thresholds_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        match guard.can_open_buy(&store, between_thresholds_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!(
                "expected soft exposure latch to stay blocked between resume and soft thresholds, got {other:?}"
            ),
        }
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        store.update_shadow_lot(lot_id, 10.0, 0.35)?;
        let recovered_at = between_thresholds_at
            + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 1).max(6));
        match guard.can_open_buy(&store, recovered_at, true) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected recovery below resume threshold to clear soft pause, got {other:?}"
            ),
        }
        assert!(!guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_restores_active_soft_exposure_pause_after_restart_without_event_spam(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-restore-active")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        let _ = original_guard.can_open_buy(&store, now, true);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let restore_at = now + chrono::Duration::seconds(30);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at)?;

        match restarted_guard.can_open_buy(&store, restore_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected restored soft exposure pause after restart, got {other:?}"),
        }
        assert!(restarted_guard.soft_exposure_pause_latched);
        assert!(restarted_guard.soft_exposure_pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_restore_preserves_older_soft_exposure_pause_when_newer_timed_pause_was_cleared(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-restore-overlap")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -0.3;
        cfg.shadow_drawdown_1h_pause_minutes = 5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;
        store.insert_shadow_closed_trade(
            "sig-overlap-dd1",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.5,
            -0.5,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        match original_guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("drawdown_1h")),
            other => {
                panic!("expected drawdown timed pause with overlapping soft latch, got {other:?}")
            }
        }
        assert!(original_guard.soft_exposure_pause_latched);
        assert!(original_guard.pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 2);

        original_guard.clear_pause(&store, now + chrono::Duration::seconds(30))?;
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let restore_at = now + chrono::Duration::seconds(45);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at)?;

        assert!(
            restarted_guard.soft_exposure_pause_latched,
            "older soft exposure latch must be restored even when a newer timed pause was later cleared"
        );
        assert!(restarted_guard.soft_exposure_pause_until.is_some());
        assert!(
            restarted_guard.pause_until.is_none(),
            "cleared generic timed pause should not be restored"
        );
        match restarted_guard.can_open_buy(&store, restore_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                ..
            } => {}
            other => panic!(
                "expected restart path to stay blocked after restoring older soft exposure latch, got {other:?}"
            ),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_restores_soft_exposure_pause_after_until_expiry_when_exposure_stays_in_hysteresis_band(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-restore-expired-until")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        let _ = original_guard.can_open_buy(&store, now, true);
        assert!(original_guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        // Keep exposure in the hysteresis band after the initial pause window expires.
        store.update_shadow_lot(lot_id, 10.0, 0.45)?;

        let restore_at = now + chrono::Duration::minutes(2);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at)?;

        assert!(
            restarted_guard.soft_exposure_pause_latched,
            "soft exposure latch should survive restart even after initial until expires"
        );
        match restarted_guard.can_open_buy(&store, restore_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!(
                "expected hysteresis-band exposure to remain blocked after restart, got {other:?}"
            ),
        }
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_restore_preserves_soft_exposure_pause_buried_under_many_newer_timed_pause_rows(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-restore-many-newer-rows")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();

        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        original_guard.activate_soft_exposure_pause(
            &store,
            now,
            chrono::Duration::minutes(1),
            Lamports::new(600_000_000),
            Lamports::new(500_000_000),
            Lamports::new(400_000_000),
        )?;
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        for index in 0..80 {
            original_guard.activate_pause(
                &store,
                now + chrono::Duration::seconds(index as i64 + 1),
                chrono::Duration::minutes(5),
                "drawdown_1h",
                format!("synthetic timed pause spam #{index}"),
            )?;
        }
        assert!(
            store.risk_event_count_by_type("shadow_risk_pause")? > 64,
            "regression setup must exceed the old fixed restore scan limit"
        );

        let restore_at = now + chrono::Duration::seconds(30);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at)?;

        assert!(
            restarted_guard.soft_exposure_pause_latched,
            "soft exposure latch must restore even when buried under many newer generic pause rows"
        );
        assert!(
            restarted_guard.pause_until.is_some(),
            "latest active generic pause should still restore alongside the soft latch"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_restores_active_timed_pause_after_restart() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-restore-active")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        original_guard.activate_pause(
            &store,
            now,
            chrono::Duration::minutes(5),
            "restart_test",
            "active pause should survive restart".to_string(),
        )?;

        let restore_at = now + chrono::Duration::seconds(30);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at)?;

        match restarted_guard.can_open_buy(&store, restore_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("restart_test")),
            other => panic!("expected restored timed pause block after restart, got {other:?}"),
        }
        assert!(
            restarted_guard.pause_until.is_some(),
            "active durable pause should be restored into runtime state after restart"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_does_not_restore_cleared_timed_pause_after_restart() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-restore-cleared")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        original_guard.activate_pause(
            &store,
            now,
            chrono::Duration::minutes(5),
            "restart_test",
            "cleared pause should stay cleared".to_string(),
        )?;
        original_guard.clear_pause(&store, now + chrono::Duration::seconds(10))?;

        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, now + chrono::Duration::seconds(20))?;

        match restarted_guard.can_open_buy(&store, now + chrono::Duration::seconds(20), true) {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("expected cleared timed pause to stay cleared after restart, got {other:?}")
            }
        }
        assert!(
            restarted_guard.pause_until.is_none(),
            "cleared durable pause should not be restored after restart"
        );
        assert!(
            restarted_guard.pause_reason.is_none(),
            "cleared durable pause reason should stay empty after restart"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_rug_loss_triggers_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("rug-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_rug_loss_window_minutes = 120;
        cfg.shadow_rug_loss_count_threshold = 2;
        cfg.shadow_rug_loss_rate_sample_size = 10;
        cfg.shadow_rug_loss_rate_threshold = 0.99;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-rug-1",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;
        store.insert_shadow_closed_trade(
            "sig-rug-2",
            "wallet-b",
            "token-b",
            1.0,
            2.0,
            0.3,
            -1.7,
            now - chrono::Duration::minutes(25),
            now - chrono::Duration::minutes(10),
        )?;

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                detail,
            } => assert!(detail.contains("rug_loss")),
            other => panic!("expected hard stop block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades() -> Result<()> {
        let (store, db_path) = make_test_store("rug-rate-autoclear")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_rug_loss_window_minutes = 1;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_sample_size = 200;
        cfg.shadow_rug_loss_rate_threshold = 0.5;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-rug-rate-lock",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::seconds(55),
            now - chrono::Duration::seconds(50),
        )?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                ..
            } => {}
            other => panic!("expected hard-stop block from rug-rate breach, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_some(),
            "hard stop should be active after rug-rate breach"
        );

        let refresh_step_seconds = (RISK_DB_REFRESH_MIN_SECONDS + 1).max(6);
        let mut cleared = false;
        for cycle in 1..=30 {
            let cycle_ts = now + chrono::Duration::seconds(refresh_step_seconds * cycle as i64);
            if matches!(
                guard.can_open_buy(&store, cycle_ts, true),
                BuyRiskDecision::Allow
            ) {
                cleared = true;
                break;
            }
        }
        assert!(
            cleared,
            "hard stop should auto-clear after rug window expires even without new trades"
        );
        assert!(
            guard.hard_stop_reason.is_none(),
            "hard stop reason should clear after healthy refresh streak"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_stop_auto_clears_without_restart() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-autoclear")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-hard-stop-loss",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                ..
            } => {}
            other => panic!("expected hard-stop block after drawdown breach, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_some(),
            "hard stop should be set after drawdown breach"
        );

        store.insert_shadow_closed_trade(
            "sig-hard-stop-recover",
            "wallet-b",
            "token-b",
            1.0,
            1.0,
            3.0,
            2.0,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let refresh_step_seconds = (RISK_DB_REFRESH_MIN_SECONDS + 1).max(6);
        for cycle in 1..HARD_STOP_CLEAR_HEALTHY_REFRESHES {
            let cycle_ts = now + chrono::Duration::seconds(refresh_step_seconds * cycle as i64);
            match guard.can_open_buy(&store, cycle_ts, true) {
                BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::HardStop,
                    ..
                } => {}
                other => panic!(
                    "expected hard-stop to remain active during healthy cooldown (cycle {cycle}), got {other:?}"
                ),
            }
            assert!(
                guard.hard_stop_reason.is_some(),
                "hard stop should stay active until healthy cooldown is satisfied"
            );
        }

        let decision_after_recovery = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds(
                refresh_step_seconds * HARD_STOP_CLEAR_HEALTHY_REFRESHES as i64,
            ),
            true,
        );
        match decision_after_recovery {
            BuyRiskDecision::Allow => {}
            other => panic!("expected hard-stop auto-clear after recovery, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_none(),
            "hard stop should clear after metrics normalize"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_hard_stop_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-hard-stop-fatal",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_hard_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_hard_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal hard-stop risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk hard stop event with fatal sqlite I/O"),
            "expected hard-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.hard_stop_reason.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_hard_stop")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_exposure_hard_cap_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("exposure-hard-cap-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_exposure_hard_cap_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_exposure_hard_cap'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal exposure hard-cap risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk exposure hard cap event with fatal sqlite I/O"
            ),
            "expected hard-cap fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.exposure_hard_blocked);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_exposure_hard_cap")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_hard_stop_clear_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.hard_stop_reason = Some("drawdown_24h: synthetic prior breach".to_string());
        guard.hard_stop_clear_healthy_streak = HARD_STOP_CLEAR_HEALTHY_REFRESHES - 1;
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_hard_stop_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_hard_stop_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal hard-stop clear risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk hard stop clear event with fatal sqlite I/O"
            ),
            "expected hard-stop clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.hard_stop_reason.is_some(),
            "fatal hard-stop clear write must preserve runtime hard-stop state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_hard_stop_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_exposure_hard_cap_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("exposure-hard-cap-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.exposure_hard_blocked = true;
        guard.exposure_hard_detail =
            Some("risk_open_notional_sol=1.200000 hard_cap=1.000000".to_string());
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_exposure_hard_cap_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_exposure_hard_cap_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal exposure hard-cap clear risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk exposure hard cap clear event with fatal sqlite I/O"
            ),
            "expected hard-cap clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.exposure_hard_blocked,
            "fatal hard-cap clear write must preserve runtime hard-cap block"
        );
        assert!(guard.exposure_hard_detail.is_some());
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_exposure_hard_cap_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_soft_exposure_pause_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("soft-exposure-pause-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_soft_pause_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal soft-exposure pause risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk soft exposure pause event with fatal sqlite I/O"
            ),
            "expected soft-pause fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_drawdown_timed_pause_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("drawdown-timed-pause-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -0.3;
        cfg.shadow_drawdown_1h_pause_minutes = 30;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        store.insert_shadow_closed_trade(
            "sig-dd1-fatal",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.5,
            -0.5,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal timed-pause risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk timed pause event with fatal sqlite I/O"),
            "expected timed-pause fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_soft_exposure_pause_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let _ = guard.can_open_buy(&store, now, true);
        let initial_until = guard
            .soft_exposure_pause_until
            .expect("soft exposure pause should set until");
        let initial_reason = guard
            .soft_exposure_pause_reason
            .clone()
            .expect("soft exposure pause should set reason");
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        store.update_shadow_lot(lot_id, 10.0, 0.35)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_soft_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let recovered_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        let error = guard
            .maybe_refresh_db_state(&store, recovered_at)
            .expect_err("fatal soft-exposure pause clear write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk soft exposure pause clear event with fatal sqlite I/O"
            ),
            "expected soft-pause clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.soft_exposure_pause_latched,
            "fatal soft-exposure pause clear write must preserve runtime soft pause latch"
        );
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(
            guard.soft_exposure_pause_reason.as_deref(),
            Some(initial_reason.as_str())
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_expired_timed_pause_blocks_buy_on_fatal_clear_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let pause_started_at = Utc::now() - chrono::Duration::minutes(10);
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.activate_pause(
            &store,
            pause_started_at,
            chrono::Duration::minutes(5),
            "drawdown_1h",
            "expired pause should fail closed on fatal clear write".to_string(),
        )?;
        assert!(guard.pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, Utc::now(), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => assert!(detail.contains("timed_pause_clear_error")),
            other => panic!(
                "expected fail-closed block when fatal timed-pause clear write fails, got {other:?}"
            ),
        }
        assert!(
            guard.pause_until.is_some(),
            "fatal timed-pause clear write must preserve runtime pause state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_cached_refresh_error_blocks_buy_on_fatal_fail_closed_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("cached-refresh-fail-closed-risk-event-fatal")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        let now = Utc::now();
        guard.last_db_refresh_at = Some(now);
        guard.last_db_refresh_error = Some("simulated refresh failure".to_string());

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_fail_closed_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_fail_closed'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => {
                assert!(detail.contains("cached error"));
                assert!(detail.contains("fail_closed_event_error"));
                assert!(detail.contains(
                    "failed to persist shadow risk fail-closed event with fatal sqlite I/O"
                ));
                assert!(detail.contains("xShmMap"));
            }
            other => panic!(
                "expected fail-closed block when fatal fail-closed event write fails, got {other:?}"
            ),
        }
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_fail_closed")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_timed_pause_clear_blocks_buy_on_fatal_fail_closed_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("timed-pause-clear-fail-closed-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let pause_started_at = Utc::now() - chrono::Duration::minutes(10);
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.activate_pause(
            &store,
            pause_started_at,
            chrono::Duration::minutes(5),
            "drawdown_1h",
            "expired pause should fail closed on fatal fail-closed write".to_string(),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;
             CREATE TRIGGER fail_shadow_risk_fail_closed_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_fail_closed'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, Utc::now(), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => {
                assert!(detail.contains("timed_pause_clear_error"));
                assert!(detail.contains("fail_closed_event_error"));
                assert!(
                    detail.contains("failed to persist shadow risk fail-closed event with fatal sqlite I/O")
                );
                assert!(detail.contains("xShmMap"));
            }
            other => panic!(
                "expected fail-closed block when fatal fail-closed event write fails after timed-pause clear error, got {other:?}"
            ),
        }
        assert!(guard.pause_until.is_some());
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_fail_closed")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_error_resets_hard_stop_clear_streak() {
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        guard.hard_stop_clear_healthy_streak = 4;
        let now = Utc::now();

        assert!(guard.on_risk_refresh_error(now));
        assert_eq!(
            guard.hard_stop_clear_healthy_streak, 0,
            "refresh error must reset healthy streak to enforce consecutive healthy refreshes"
        );

        // Subsequent errors inside throttle window should be suppressed.
        assert!(!guard.on_risk_refresh_error(now + chrono::Duration::seconds(1)));
    }

    #[test]
    fn risk_guard_cached_refresh_error_blocks_buy_within_throttle_window() -> Result<()> {
        let (store, db_path) = make_test_store("cached-refresh-error")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        let now = Utc::now();
        guard.last_db_refresh_at = Some(now);
        guard.last_db_refresh_error = Some("simulated refresh failure".to_string());

        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => assert!(
                detail.contains("cached error"),
                "expected cached refresh error in fail-closed detail, got: {detail}"
            ),
            other => panic!("expected fail-closed block from cached refresh error, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_refresh_is_noop_when_killswitch_disabled() -> Result<()> {
        let unique = format!(
            "risk-noop-disabled-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let store = SqliteStore::open(Path::new(&db_path))?;

        let mut cfg = RiskConfig::default();
        cfg.shadow_killswitch_enabled = false;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.last_db_refresh_error = Some("stale error".to_string());

        guard.maybe_refresh_db_state(&store, Utc::now())?;
        assert!(
            guard.last_db_refresh_error.is_none(),
            "killswitch-disabled refresh should clear cached refresh errors"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn scheduler_keeps_single_inflight_per_wallet_token_key() {
        fn make_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();

        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_task("A1", "wallet-a", "token-x"),
            )
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_task("A2", "wallet-a", "token-x"),
            )
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_task("B1", "wallet-b", "token-y"),
            )
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 3);

        let first = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("first task");
        let second = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("second task");

        assert_eq!(first.swap.signature, "A1");
        assert_eq!(second.swap.signature, "B1");

        shadow_scheduler.mark_task_complete(&first.key);

        let third = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("third task");

        assert_eq!(third.swap.signature, "A2");
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
    }

    #[test]
    fn enqueue_shadow_task_enforces_capacity() {
        fn make_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let cap = 2usize;

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_task("A1", "wallet-a", "token-x"),)
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_task("A2", "wallet-a", "token-x"),)
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_task("B1", "wallet-b", "token-y"),)
            .is_err());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, cap);
    }

    #[test]
    fn queue_full_prefers_sell_over_buy_under_mixed_flow() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B0", "wallet-buy", "token-buy"),)
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);

        let overflow_buy = shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B1", "wallet-buy-2", "token-buy-2"))
            .expect_err("buy should overflow at cap");
        shadow_scheduler.handle_shadow_enqueue_overflow(
            ShadowSwapSide::Buy,
            overflow_buy,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );

        let sell_task = make_sell_task("S1", "wallet-sell", "token-sell");
        let sell_key = sell_task.key.clone();
        shadow_scheduler
            .inflight_shadow_keys
            .insert(sell_key.clone());
        let overflow_sell = shadow_scheduler
            .enqueue_shadow_task(cap, sell_task)
            .expect_err("sell should overflow at cap before policy handling");

        shadow_scheduler.handle_shadow_enqueue_overflow(
            ShadowSwapSide::Sell,
            overflow_sell,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
        assert!(
            !shadow_queue_full_outcome_counts.contains_key("queue_full_sell_dropped"),
            "sell should be kept when a pending buy can be evicted"
        );

        let queued_sell = shadow_scheduler
            .pending_shadow_tasks
            .get(&sell_key)
            .expect("sell task should remain queued");
        assert_eq!(queued_sell.len(), 1);
        assert!(
            shadow_scheduler
                .ready_shadow_key_set
                .get(&sell_key)
                .is_none(),
            "inflight key must not be marked ready"
        );
    }

    #[test]
    fn causal_holdback_counts_held_sells_against_global_capacity() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;
        let now = Utc::now();

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B0", "wallet-buy", "token-buy"))
            .is_ok());
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        let held_sell = make_sell_task("S1", "wallet-sell", "token-sell");
        let overflow_sell = shadow_scheduler
            .hold_sell_for_causality(cap, held_sell, 2_500, now)
            .expect_err("held sell should respect the global buffered cap");
        shadow_scheduler.handle_held_sell_overflow(
            overflow_sell,
            cap,
            2_500,
            now,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
        assert_eq!(
            shadow_scheduler
                .shadow_holdback_counts
                .get("queued_after_buy_evict"),
            Some(&1)
        );
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
    }

    #[test]
    fn release_held_shadow_sells_preserves_buffered_count_accounting() {
        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let now = Utc::now();

        assert!(shadow_scheduler
            .hold_sell_for_causality(
                2,
                make_sell_task("S1", "wallet-sell", "token-sell"),
                100,
                now
            )
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        shadow_scheduler.release_held_shadow_sells(
            &HashSet::new(),
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
            2,
            now + chrono::Duration::milliseconds(101),
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 0);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
        assert_eq!(
            shadow_scheduler
                .shadow_holdback_counts
                .get("released_expired"),
            Some(&1)
        );
    }

    #[test]
    fn enqueue_shadow_task_respects_global_buffered_capacity_with_held_sells() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let now = Utc::now();

        assert!(shadow_scheduler
            .hold_sell_for_causality(
                1,
                make_sell_task("S1", "wallet-sell", "token-sell"),
                100,
                now
            )
            .is_ok());
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        assert!(shadow_scheduler
            .enqueue_shadow_task(1, make_buy_task("B1", "wallet-buy", "token-buy"))
            .is_err());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
    }

    #[test]
    fn inline_processing_respects_per_key_serialization() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let mut shadow_scheduler = ShadowScheduler::new();

        assert!(shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));

        shadow_scheduler.inflight_shadow_keys.insert(key.clone());
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));

        shadow_scheduler.inflight_shadow_keys.clear();
        shadow_scheduler.pending_shadow_tasks.insert(
            key.clone(),
            VecDeque::from([ShadowTaskInput {
                swap: SwapEvent {
                    wallet: "wallet-a".to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-x".to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: "sig-queued".to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: key.clone(),
            }]),
        );
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            true,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            false,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_MAX_CONCURRENT_WORKERS,
            &key,
        ));
    }

    #[test]
    fn follow_snapshot_uses_temporal_promotions_and_demotions() {
        let mut snapshot = FollowSnapshot::default();
        let promoted = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        let demoted = DateTime::parse_from_rfc3339("2026-02-15T11:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        snapshot.promoted_at.insert("w".to_string(), promoted);
        snapshot.demoted_at.insert("w".to_string(), demoted);

        assert!(snapshot.is_followed_at("w", promoted + chrono::Duration::minutes(10)));
        assert!(!snapshot.is_followed_at("w", demoted + chrono::Duration::seconds(1)));
    }

    #[test]
    fn startup_follow_snapshot_defers_recovered_active_wallets_until_discovery_publish() {
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                HashSet::from(["wallet-a".to_string(), "wallet-b".to_string()]),
                None,
            );

        assert_eq!(recovered_active_wallets, 2);
        assert!(shadow_strategy_fail_closed);
        assert!(
            snapshot.active.is_empty(),
            "recovered historical followlist must not become runtime truth before discovery publishes fresh or degraded runtime truth"
        );
    }

    #[test]
    fn startup_follow_snapshot_keeps_runtime_open_without_recent_publication_or_residue() {
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(HashSet::new(), None);

        assert_eq!(recovered_active_wallets, 0);
        assert!(!shadow_strategy_fail_closed);
        assert!(
            snapshot.active.is_empty(),
            "startup should stay open when there is no recent published universe and no recovered followlist residue"
        );
    }

    #[test]
    fn startup_follow_snapshot_uses_recent_published_universe() {
        let recent_active_wallets = HashSet::from(["wallet-a".to_string(), "wallet-b".to_string()]);
        let recent_truth = RuntimePublicationTruthResolution::Recent(
            copybot_discovery::RuntimePublishedUniverseTruth {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Utc::now(),
                last_published_window_start: Utc::now(),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: recent_active_wallets.iter().cloned().collect(),
            },
        );
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                HashSet::from(["wallet-stale".to_string()]),
                Some(&recent_truth),
            );

        assert_eq!(recovered_active_wallets, 0);
        assert!(!shadow_strategy_fail_closed);
        assert_eq!(snapshot.active, recent_active_wallets);
    }

    #[test]
    fn nonpublished_fail_closed_discovery_cycle_clears_startup_recent_follow_snapshot() {
        let recent_active_wallets = HashSet::from([
            "wallet-a".to_string(),
            "wallet-b".to_string(),
            "wallet-c".to_string(),
        ]);
        let recent_truth = RuntimePublicationTruthResolution::Recent(
            copybot_discovery::RuntimePublishedUniverseTruth {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Utc::now(),
                last_published_window_start: Utc::now(),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: recent_active_wallets.iter().cloned().collect(),
            },
        );
        let (initial_snapshot, _recovered_active_wallets, mut shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(HashSet::new(), Some(&recent_truth));
        let mut follow_snapshot = Arc::new(initial_snapshot);
        let mut open_shadow_lots = HashSet::from([("wallet-a".to_string(), "token-a".to_string())]);
        let discovery_output =
            live_like_fail_closed_no_recent_published_universe_output(Utc::now());

        assert_eq!(follow_snapshot.active, recent_active_wallets);
        assert!(!shadow_strategy_fail_closed);
        assert!(!open_shadow_lots.is_empty());
        assert!(
            apply_fail_closed_runtime_follow_surface_if_needed(
                &mut follow_snapshot,
                &mut open_shadow_lots,
                &mut shadow_strategy_fail_closed,
                &discovery_output,
            ),
            "a non-published fail-closed discovery cycle must clear stale startup publication truth out of runtime memory"
        );
        assert!(shadow_strategy_fail_closed);
        assert!(follow_snapshot.active.is_empty());
        assert!(open_shadow_lots.is_empty());
    }

    #[test]
    fn nonpublished_degraded_discovery_cycle_preserves_startup_recent_follow_snapshot() {
        let recent_active_wallets = HashSet::from(["wallet-a".to_string(), "wallet-b".to_string()]);
        let recent_truth = RuntimePublicationTruthResolution::Recent(
            copybot_discovery::RuntimePublishedUniverseTruth {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Utc::now(),
                last_published_window_start: Utc::now(),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: recent_active_wallets.iter().cloned().collect(),
            },
        );
        let (initial_snapshot, _recovered_active_wallets, mut shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(HashSet::new(), Some(&recent_truth));
        let mut follow_snapshot = Arc::new(initial_snapshot);
        let mut open_shadow_lots = HashSet::from([("wallet-a".to_string(), "token-a".to_string())]);
        let discovery_output = live_like_degraded_published_universe_output(Utc::now(), 2, 2);

        assert!(
            !apply_fail_closed_runtime_follow_surface_if_needed(
                &mut follow_snapshot,
                &mut open_shadow_lots,
                &mut shadow_strategy_fail_closed,
                &discovery_output,
            ),
            "degraded unpublished runtime should preserve the still-recent published universe in memory"
        );
        assert!(!shadow_strategy_fail_closed);
        assert_eq!(follow_snapshot.active, recent_active_wallets);
        assert_eq!(
            open_shadow_lots,
            HashSet::from([("wallet-a".to_string(), "token-a".to_string())])
        );
    }

    #[test]
    fn startup_recent_published_universe_ignores_stale_followlist_residue() -> Result<()> {
        let (store, db_path) =
            make_test_store("startup-published-universe-ignores-stale-followlist")?;
        let now = DateTime::parse_from_rfc3339("2026-03-17T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.follow_top_n = 1;
        config.min_score = 0.1;

        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);

        store.upsert_wallet(
            "wallet_ranked_now",
            now - chrono::Duration::days(2),
            now - chrono::Duration::minutes(1),
            "candidate",
        )?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet_ranked_now".to_string(),
            window_start: metrics_window_start,
            pnl: 2.4,
            win_rate: 0.8,
            trades: 6,
            closed_trades: 6,
            hold_median_seconds: 120,
            score: 0.9,
            buy_total: 6,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet(
            "wallet_stale_residue",
            now - chrono::Duration::minutes(20),
            "legacy_bootstrap_residue",
        )?;
        let expected_published_wallets = vec![
            "wallet_published_exact".to_string(),
            "wallet_published_second".to_string(),
        ];
        let publication_policy_fingerprint = test_publication_policy_fingerprint(&config);
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "startup_recent_published_universe".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(10)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(expected_published_wallets.clone()),
            },
            false,
            Some(publication_policy_fingerprint.as_str()),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let startup_published_truth = startup_runtime_publication_truth(&discovery, &store, now)?
            .expect("startup published universe should be available from publication truth");
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                initial_active_wallets,
                Some(&startup_published_truth),
            );

        assert_eq!(recovered_active_wallets, 0);
        assert!(!shadow_strategy_fail_closed);
        assert_eq!(
            snapshot.active,
            HashSet::from([
                "wallet_published_exact".to_string(),
                "wallet_published_second".to_string(),
            ]),
            "startup runtime snapshot must come from the exact published universe control plane, not from stale followlist residue or current ranking output"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_bootstrap_degraded_publication_truth_keeps_runtime_fail_closed() -> Result<()> {
        let (store, db_path) =
            make_test_store("startup-bootstrap-degraded-ignores-stale-followlist")?;
        let now = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut config = copybot_config::DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.refresh_seconds = 600;
        config.metric_snapshot_interval_seconds = 30 * 60;
        config.follow_top_n = 1;
        config.min_score = 0.1;

        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        let metrics_window_start =
            bucketed_now - chrono::Duration::days(config.scoring_window_days.max(1) as i64);

        store.activate_follow_wallet(
            "wallet_stale_residue",
            now - chrono::Duration::minutes(20),
            "legacy_bootstrap_residue",
        )?;
        let publication_policy_fingerprint = test_publication_policy_fingerprint(&config);
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "stale_imported_runtime_artifact".to_string(),
                last_published_at: Some(now - chrono::Duration::minutes(61)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec![
                    "wallet_bootstrap_exact".to_string(),
                    "wallet_bootstrap_second".to_string(),
                ]),
            },
            false,
            Some(publication_policy_fingerprint.as_str()),
        )?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(now - chrono::Duration::minutes(5)),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let startup_published_truth = startup_runtime_publication_truth(&discovery, &store, now)?
            .expect("bootstrap-degraded publication truth should survive startup");
        assert!(matches!(
            startup_published_truth,
            RuntimePublicationTruthResolution::BootstrapDegraded(_)
        ));
        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                initial_active_wallets,
                Some(&startup_published_truth),
            );

        assert_eq!(recovered_active_wallets, 1);
        assert!(shadow_strategy_fail_closed);
        assert_eq!(
            snapshot.active,
            HashSet::new(),
            "explicit bootstrap-degraded startup must stay fail-closed instead of promoting stale imported publication truth into the runtime follow snapshot"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn validate_bootstrap_degraded_execution_contract_rejects_enabled_execution() -> Result<()> {
        let (store, db_path) = make_test_store("startup-bootstrap-degraded-execution-guard")?;
        let armed_at = DateTime::parse_from_rfc3339("2026-03-23T12:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(armed_at),
        )?;

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        let runtime = ExecutionRuntime::from_config(execution, RiskConfig::default());
        let error = validate_bootstrap_degraded_execution_contract(&store, &runtime)
            .expect_err("bootstrap-degraded startup must reject execution-enabled runtime");

        assert!(
            error
                .to_string()
                .contains("execution.enabled=true is not allowed while discovery bootstrap-degraded runtime state is active")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_trusted_selection_gate_status_falls_back_to_legacy_bool_when_typed_row_absent(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("trusted-selection-startup-legacy-bool-fallback")?;
        store.set_discovery_trusted_selection_bootstrap_required(true, "legacy_bool_only")?;

        let status = store.startup_trusted_selection_gate_status()?;
        assert!(status.bootstrap_required);
        assert!(status.startup_fail_closed);
        assert_eq!(status.selection_state, None);
        assert_eq!(status.reason.as_deref(), Some("legacy_bool_only"));
        assert!(status.legacy_bool_fallback_used);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_without_recent_published_universe_keeps_historical_open_shadow_lots_loaded(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("trusted-selection-startup-shadow-fail-close")?;
        let now = Utc::now();
        store.insert_shadow_lot("wallet-sell", "token-a", 5.0, 1.2, now)?;
        store.set_discovery_trusted_selection_bootstrap_required(false, "legacy_false")?;
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required: false,
            reason: "typed_invalid_selection".to_string(),
            selection_state: TrustedSelectionState::Invalid,
            active_snapshot_id: Some("snapshot-invalid".to_string()),
            active_snapshot_window_start: Some(now),
            last_bootstrap_source_kind: Some(TrustedSnapshotSourceKind::CloneLatestBridge),
            last_bootstrap_at: Some(now),
        })?;

        let initial_active_wallets = store.list_active_follow_wallets()?;
        let (follow_snapshot, _recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(initial_active_wallets, None);
        let open_shadow_lots = if shadow_strategy_fail_closed {
            HashSet::new()
        } else {
            store.list_shadow_open_pairs()?
        };

        let mut swap = test_swap("sig-sell-bootstrap-invalid");
        swap.wallet = "wallet-sell".to_string();
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();

        assert!(!shadow_strategy_fail_closed);
        assert!(
            !open_shadow_lots.is_empty(),
            "legacy trusted-selection recovery bookkeeping must not clear historical open shadow lots before discovery decides the runtime contract"
        );
        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn select_role_helius_http_url_prefers_role_specific() {
        let selected = select_role_helius_http_url(
            "https://role.endpoint/?api-key=abc",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected.as_deref(),
            Some("https://role.endpoint/?api-key=abc")
        );
    }

    #[test]
    fn select_role_helius_http_url_falls_back_and_rejects_placeholders() {
        let selected = select_role_helius_http_url(
            "https://role.endpoint/?api-key=REPLACE_ME",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected.as_deref(),
            Some("https://fallback.endpoint/?api-key=def")
        );

        let selected_none = select_role_helius_http_url("", "https://x/?api-key=REPLACE_ME");
        assert!(selected_none.is_none());

        let selected_lowercase = select_role_helius_http_url(
            "https://role.endpoint/?api-key=replace_me",
            "https://fallback.endpoint/?api-key=def",
        );
        assert_eq!(
            selected_lowercase.as_deref(),
            Some("https://fallback.endpoint/?api-key=def")
        );
    }

    #[test]
    fn follow_event_retention_spans_discovery_publish_cadence() {
        let retention = follow_event_retention_duration(30, 600);
        assert_eq!(retention, Duration::from_secs(1200));

        let retention = follow_event_retention_duration(2500, 600);
        assert_eq!(retention, Duration::from_secs(2500));
    }

    #[test]
    fn enforce_quality_gate_http_url_requires_endpoint_for_paper_prod() {
        let result = enforce_quality_gate_http_url("shadow", "paper", true, None);
        assert!(result.is_err());

        let result = enforce_quality_gate_http_url("shadow", "paper-canary", true, None);
        assert!(result.is_err());

        let result = enforce_quality_gate_http_url(
            "shadow",
            "prod",
            true,
            Some("https://endpoint".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn enforce_quality_gate_http_url_is_lenient_for_dev_or_disabled_gates() {
        let result = enforce_quality_gate_http_url("shadow", "dev", true, None);
        assert!(result.is_ok());

        let result = enforce_quality_gate_http_url("shadow", "paper", false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_shadow_quality_gate_contract_rejects_all_zero_thresholds_when_enabled() {
        let mut cfg = ShadowConfig::default();
        cfg.quality_gates_enabled = true;
        cfg.min_token_age_seconds = 0;
        cfg.min_holders = 0;
        cfg.min_liquidity_sol = 0.0;
        cfg.min_volume_5m_sol = 0.0;
        cfg.min_unique_traders_5m = 0;
        let err = validate_shadow_quality_gate_contract(&cfg, "prod")
            .expect_err("all-zero quality thresholds must fail when gates are enabled")
            .to_string();
        assert!(err.contains("all quality thresholds are zero"));
    }

    #[test]
    fn validate_shadow_quality_gate_contract_allows_non_zero_thresholds_when_enabled() {
        let mut cfg = ShadowConfig::default();
        cfg.quality_gates_enabled = true;
        cfg.min_token_age_seconds = 30;
        cfg.min_holders = 5;
        cfg.min_liquidity_sol = 1.0;
        cfg.min_volume_5m_sol = 0.5;
        cfg.min_unique_traders_5m = 2;
        validate_shadow_quality_gate_contract(&cfg, "prod")
            .expect("non-zero thresholds should pass quality gate validation");
    }

    #[test]
    fn parse_ingestion_source_override_reads_supported_source_key() {
        let content = r#"
# comment
SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone_grpc
"#;
        assert_eq!(
            parse_ingestion_source_override(content)
                .expect("valid override should parse")
                .as_deref(),
            Some("yellowstone_grpc")
        );
    }

    #[test]
    fn parse_ingestion_source_override_ignores_invalid_lines() {
        let content = r#"
FOO=bar
SOLANA_COPY_BOT_INGESTION_SOURCE=
this-is-not-a-valid-line
"#;
        assert!(parse_ingestion_source_override(content)
            .expect("invalid lines without source value should be ignored")
            .is_none());
    }

    #[test]
    fn parse_ingestion_source_override_rejects_invalid_source_value() {
        let content = r#"
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws
"#;
        let error = parse_ingestion_source_override(content)
            .expect_err("unsupported override source must warn and reject");
        assert!(
            error.to_string().contains("helius_ws"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_ingestion_source_override_normalizes_alias() {
        let content = r#"
SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone
"#;
        assert_eq!(
            parse_ingestion_source_override(content)
                .expect("yellowstone alias should normalize")
                .as_deref(),
            Some("yellowstone_grpc")
        );
    }

    #[test]
    fn observed_swap_writer_closed_channel_error_requires_restart() {
        let error = anyhow!("observed swap writer channel closed");
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_writer_closed_reply_error_requires_restart() {
        let error = anyhow!("observed swap writer reply channel closed");
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_writer_terminal_failure_error_requires_restart() {
        let error = anyhow!("forced async observed swap failure")
            .context(OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT);
        assert!(observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn startup_sqlite_wal_checkpoint_error_requires_abort_on_xshmmap_io_failure() {
        let primary = anyhow!("database is locked");
        let fallback =
            anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(!startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            None
        ));
        assert!(startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            Some(&fallback)
        ));
    }

    #[test]
    fn startup_sqlite_wal_checkpoint_error_does_not_abort_on_busy_only_failures() {
        let primary = anyhow!("database is locked");
        let fallback = anyhow!("database is busy");
        assert!(!startup_sqlite_wal_checkpoint_error_requires_abort(
            Some(&primary),
            Some(&fallback)
        ));
    }

    #[test]
    fn inline_startup_step_reports_started_and_completed() -> Result<()> {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let value = run_inline_startup_step(
            &reporter,
            "startup_inline_complete",
            Some(StdDuration::from_millis(50)),
            || Ok::<u64, anyhow::Error>(7),
        )?;
        assert_eq!(value, 7);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_complete"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "inline startup step must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_complete"
                    && event.outcome == StartupStepOutcome::Completed
            }),
            "inline startup step must emit a completed outcome"
        );
        Ok(())
    }

    #[test]
    fn inline_startup_step_reports_failed_outcome() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let error = run_inline_startup_step(
            &reporter,
            "startup_inline_failure",
            Some(StdDuration::from_millis(50)),
            || -> Result<()> { Err(anyhow!("inline startup failure")) },
        )
        .expect_err("failed inline startup step must surface an explicit error");
        assert!(
            error
                .to_string()
                .contains("startup step startup_inline_failure failed"),
            "failed inline startup step must be wrapped with the stage name"
        );

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_failure"
                    && event.outcome == StartupStepOutcome::Failed
                    && event
                        .detail
                        .as_deref()
                        .is_some_and(|detail| detail.contains("inline startup failure"))
            }),
            "failed inline startup step must emit an explicit failed outcome with detail"
        );
    }

    #[test]
    fn skipped_inline_startup_step_reports_started_and_skipped() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        skip_inline_startup_step(&reporter, "startup_inline_skipped", "not_required_for_test");

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_skipped"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "skipped inline startup step must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_inline_skipped"
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref() == Some("not_required_for_test")
            }),
            "skipped inline startup step must emit an explicit skipped outcome with detail"
        );
    }

    #[test]
    fn startup_step_policy_uses_fatal_timeout_behavior() {
        let policy = startup_step_policy(StdDuration::from_secs(30));
        assert_eq!(
            policy.timeout_behavior,
            StartupStepTimeoutBehavior::AbortProcess
        );
    }

    #[test]
    fn startup_wal_checkpoint_is_deferred_with_explicit_outcome() -> Result<()> {
        let (store, db_path) = make_test_store("startup-wal-checkpoint-deferred")?;
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let outcome = perform_startup_wal_checkpoint(&reporter);
        assert_eq!(outcome, StartupWalCheckpointOutcome::Deferred);
        store
            .record_heartbeat("copybot-app", "startup-deferred-checkpoint")
            .context("store should remain writable after deferred checkpoint")?;

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_sqlite_wal_checkpoint"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "deferred checkpoint must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_sqlite_wal_checkpoint"
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref() == Some(STARTUP_WAL_CHECKPOINT_DEFER_REASON)
            }),
            "deferred checkpoint must emit an explicit skipped outcome with reason"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn runtime_sqlite_write_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to commit heartbeat transaction: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(runtime_sqlite_write_error_requires_restart(&error));
    }

    #[test]
    fn runtime_sqlite_write_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!runtime_sqlite_write_error_requires_restart(&error));
    }

    #[test]
    fn history_retention_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed deleting retained risk events: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(history_retention_error_requires_restart(&error));
    }

    #[test]
    fn history_retention_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!history_retention_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_retention_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to delete observed swap retention slice: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(observed_swap_retention_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_retention_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!observed_swap_retention_error_requires_restart(&error));
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_during_startup_grace() {
        let now = StdInstant::now();
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            now,
            now,
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("startup grace should block sqlite maintenance");
        assert!(reason.contains("startup_grace_remaining_ms="));
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_writer_pending_requests() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = 3;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("writer backlog should block sqlite maintenance");
        assert_eq!(reason, "writer_pending_requests=3");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_aggregate_backlog() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.aggregate_queue_depth_batches = 2;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("aggregate backlog should block sqlite maintenance");
        assert_eq!(reason, "aggregate_queue_depth_batches=2");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_recent_raw_journal_backlog() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_queue_depth_batches = 2;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("recent raw journal backlog should block sqlite maintenance");
        assert_eq!(reason, "journal_queue_depth_batches=2");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_sqlite_contention_delta() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot {
                write_retry_total: 10,
                busy_error_total: 20,
            },
            SqliteContentionSnapshot {
                write_retry_total: 12,
                busy_error_total: 21,
            },
            None,
        )
        .expect("sqlite contention delta should block sqlite maintenance");
        assert_eq!(
            reason,
            "sqlite_contention_delta write_retry_delta=2 busy_error_delta=1"
        );
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_yellowstone_queue_pressure() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            Some(maintenance_test_ingestion_snapshot(
                SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO,
            )),
        )
        .expect("yellowstone queue pressure should block sqlite maintenance");
        assert!(reason.contains("yellowstone_output_queue_fill_ratio="));
        assert!(reason.contains("depth=25"));
        assert!(reason.contains("capacity=100"));
    }

    #[test]
    fn sqlite_maintenance_block_reason_allows_healthy_runtime() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            Some(maintenance_test_ingestion_snapshot(0.10)),
        );
        assert!(reason.is_none());
    }

    #[test]
    fn sqlite_maintenance_block_reason_key_dedupes_dynamic_startup_grace_reason() {
        let key_a = sqlite_maintenance_block_reason_key("startup_grace_remaining_ms=1799000");
        let key_b = sqlite_maintenance_block_reason_key("startup_grace_remaining_ms=1200000");
        assert_eq!(key_a, "startup_grace_remaining_ms");
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn sqlite_maintenance_block_reason_key_dedupes_dynamic_yellowstone_pressure_reason() {
        let key_a = sqlite_maintenance_block_reason_key(
            "yellowstone_output_queue_fill_ratio=0.2500 depth=25 capacity=100 oldest_age_ms=500",
        );
        let key_b = sqlite_maintenance_block_reason_key(
            "yellowstone_output_queue_fill_ratio=0.7400 depth=74 capacity=100 oldest_age_ms=2500",
        );
        assert_eq!(key_a, "yellowstone_output_queue_fill_ratio");
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn stale_lot_cleanup_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed stale close for wallet=wallet-a token=token-a lot_id=1: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(stale_lot_cleanup_error_requires_restart(&error));
    }

    #[test]
    fn stale_lot_cleanup_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!stale_lot_cleanup_error_requires_restart(&error));
    }

    #[test]
    fn alert_delivery_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to advance alert delivery cursor: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(alert_delivery_error_requires_restart(&error));
    }

    #[test]
    fn alert_delivery_error_does_not_require_restart_on_webhook_failure() {
        let error = anyhow!("alert webhook request failed");
        assert!(!alert_delivery_error_requires_restart(&error));
    }

    #[test]
    fn discovery_task_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed persisting discovery runtime cursor with fatal sqlite I/O: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(discovery_task_error_requires_restart(&error));
    }

    #[test]
    fn discovery_task_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("failed persisting discovery runtime cursor: database is locked");
        assert!(!discovery_task_error_requires_restart(&error));
    }

    #[test]
    fn risk_event_write_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to insert risk event: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(risk_event_write_error_requires_restart(&error));
    }

    #[test]
    fn risk_event_write_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!risk_event_write_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_pause_restore_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to list pause events: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_pause_restore_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_pause_restore_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_pause_restore_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_background_refresh_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed refreshing shadow risk state: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_background_refresh_error_requires_restart(
            &error
        ));
    }

    #[test]
    fn shadow_risk_background_refresh_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_background_refresh_error_requires_restart(
            &error
        ));
    }

    #[test]
    fn shadow_open_lot_refresh_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed listing shadow open pairs: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_open_lot_refresh_error_requires_restart(&error));
    }

    #[test]
    fn shadow_open_lot_refresh_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!shadow_open_lot_refresh_error_requires_restart(&error));
    }

    #[test]
    fn shadow_snapshot_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed building shadow snapshot: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_snapshot_error_requires_restart(&error));
    }

    #[test]
    fn shadow_snapshot_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_snapshot_error_requires_restart(&error));
    }

    #[test]
    fn execution_task_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed executing shadow batch: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(execution_task_error_requires_restart(&error));
    }

    #[test]
    fn execution_task_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!execution_task_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_state_event_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!(
            "failed to insert risk event: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_state_event_error_requires_abort(&error));
    }

    #[test]
    fn shadow_risk_state_event_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_state_event_error_requires_abort(&error));
    }

    #[test]
    fn transient_observed_swap_write_errors_do_not_require_restart() {
        let error = anyhow!("database is locked");
        assert!(!observed_swap_writer_error_requires_restart(&error));
    }

    #[test]
    fn recent_swap_signature_dedupe_rejects_duplicate_until_evicted() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-1",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "duplicate signature should be rejected while still inside bounded dedupe window"
        );

        for idx in 0..RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &format!("sig-evict-{idx}"),
            ));
        }

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "signature should become admissible again once it is evicted from bounded dedupe state"
        );
    }

    #[test]
    fn recent_swap_signature_dedupe_allows_retry_after_forget() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "signature should stay deduped until the failed enqueue rollback forgets it"
        );

        forget_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        );

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "rollback after failed enqueue should make the signature admissible again"
        );
    }

    #[test]
    fn persist_relevant_observed_swap_rolls_back_recent_signature_on_write_error() -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-write-failure")?;
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_relevant_observed_swap_insert
             BEFORE INSERT ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                false,
                DiscoveryAggregateWriteConfig::default(),
            )?;
            let swap = test_swap("sig-relevant-write-failure");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();

            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "signature should stay deduped until the failed relevant write forgets it"
            );

            let error = persist_relevant_observed_swap(
                &writer,
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap,
            )
            .await
            .expect_err("fatal relevant observed-swap write must bubble");
            let error_text = format!("{error:#}");
            assert!(
                error_text.contains("failed to insert observed swap batch with activity days"),
                "expected fatal writer batch error to survive relevant-path helper, got: {error_text}"
            );
            assert!(
                error_text.contains("xShmMap"),
                "expected fatal sqlite I/O marker to survive relevant-path helper, got: {error_text}"
            );

            assert!(
                note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "failed relevant write must forget the signature so the swap can be retried"
            );

            let shutdown_error = writer
                .shutdown()
                .expect_err("fatal writer failure should surface again on shutdown");
            assert!(
                error_chain_contains(&shutdown_error, "failed to insert observed swap batch"),
                "expected shutdown to surface the writer failure, got: {shutdown_error:#}"
            );

            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn persist_relevant_observed_swap_reports_db_duplicate_without_forgetting_signature(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-duplicate")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                false,
                DiscoveryAggregateWriteConfig::default(),
            )?;
            let swap = test_swap("sig-relevant-duplicate");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();

            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "first relevant write should insert the swap"
            );

            forget_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));

            assert!(
                !persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "db duplicate should be surfaced as Ok(false)"
            );
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "db duplicate should keep the signature inside recent dedupe state"
            );

            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unclassified_swaps_irrelevant() {
        let mut swap = test_swap("sig-unclassified");
        swap.token_in = "token-a".to_string();
        swap.token_out = "token-b".to_string();

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantUnclassified
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unfollowed_buy_irrelevant() {
        let swap = test_swap("sig-unfollowed-buy");

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Buy)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_followed_buy_relevant() {
        let swap = test_swap("sig-followed-buy");
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.active.insert(swap.wallet.clone());

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Buy)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_cold_sell_irrelevant() {
        let mut swap = test_swap("sig-cold-sell");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_sell_with_open_lot_relevant() {
        let mut swap = test_swap("sig-sell-open-lot");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let sell_key = shadow_task_key_for_swap(&swap, ShadowSwapSide::Sell);
        let open_shadow_lots = HashSet::from([(sell_key.wallet.clone(), sell_key.token.clone())]);

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &open_shadow_lots,
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_sell_with_recent_follow_history_relevant() {
        let mut swap = test_swap("sig-sell-recent-follow");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.demoted_at.insert(
            swap.wallet.clone(),
            swap.ts_utc - chrono::Duration::seconds(1),
        );

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn irrelevant_observed_swap_requires_discovery_critical_persistence_only_for_zero_universe_fail_closed_sol_legs_stage1(
    ) {
        let buy_swap = test_swap("sig-discovery-critical-buy");
        let mut sell_swap = test_swap("sig-discovery-critical-sell");
        sell_swap.token_in = "token-sell-critical".to_string();
        sell_swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let mut non_sol_swap = test_swap("sig-discovery-non-sol");
        non_sol_swap.token_in = "token-a".to_string();
        non_sol_swap.token_out = "token-b".to_string();

        let empty_follow_snapshot = FollowSnapshot::default();
        let populated_follow_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-test".to_string());
            snapshot
        };
        let buy_target_mints = HashSet::from([buy_swap.token_out.clone()]);
        let sell_target_mints = HashSet::from([sell_swap.token_in.clone()]);
        let unrelated_target_mints = HashSet::from(["token-other".to_string()]);
        let open_shadow_lots =
            HashSet::from([("wallet-test".to_string(), "token-test".to_string())]);

        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                false,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &populated_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &open_shadow_lots,
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &non_sol_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &buy_target_mints,
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &unrelated_target_mints,
            )
        );
        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &sell_target_mints,
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &buy_target_mints,
            )
        );
    }

    #[test]
    fn discovery_critical_irrelevant_backpressure_refresh_narrows_stale_empty_target_set_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("discovery-critical-backpressure-refresh-target-mints")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-generic-a", "token-generic-b", "token-target"],
        )?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut stale_target_buy_mints = HashSet::new();
        let mut generic_buy = test_swap("sig-generic-buy");
        generic_buy.token_out = "token-generic".to_string();
        let mut target_buy = test_swap("sig-target-buy");
        target_buy.token_out = "token-target".to_string();

        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &generic_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &stale_target_buy_mints,
            ),
            "with a stale empty in-memory target set, the old zero-universe fail-closed ownership would still treat every SOL buy as discovery-critical"
        );
        assert!(
            !refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                &store,
                &generic_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &mut stale_target_buy_mints,
            )?,
            "once the exact rebuild target mints are refreshed from persisted state, the same generic SOL buy must stop being treated as discovery-critical"
        );
        assert_eq!(
            stale_target_buy_mints,
            HashSet::from(["token-target".to_string()]),
            "refresh should narrow the in-memory target mint ownership onto the persisted rebuild exact buy-mint set"
        );
        assert!(
            refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                &store,
                &target_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &mut stale_target_buy_mints,
            )?,
            "a SOL buy into the persisted rebuild target mint must remain discovery-critical after refresh"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn load_discovery_critical_target_buy_mints_prefers_exact_candidate_targets_over_broad_unique_buy_universe_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("load-discovery-critical-target-buy-mints-prefers-exact-surface")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target-a", "token-target-b"],
            &[
                "token-generic-a",
                "token-generic-b",
                "token-target-a",
                "token-target-b",
            ],
        )?;

        let loaded = load_discovery_critical_target_buy_mints(&store)?;
        assert_eq!(
            loaded,
            HashSet::from([
                "token-target-a".to_string(),
                "token-target-b".to_string(),
            ]),
            "once discovery persists an exact candidate target-mint surface, app must prefer it over the much broader unique-buy-mint universe when deciding which irrelevant SOL legs are discovery-critical under pressure"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_empty_target_set_pending_queue_prunes_generic_backpressure_before_later_target_context_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("pending-irrelevant-prune-after-target-refresh")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-target"],
        )?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut telemetry = AppConsumerLoopTelemetry::default();
        let processing_started_at = StdInstant::now();
        let backpressure_started_at = StdInstant::now();
        let mut pending_irrelevant_swaps = VecDeque::new();

        for idx in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1) {
            let mut generic_swap = test_swap(&format!("sig-generic-pending-{idx:04}"));
            generic_swap.token_out = format!("token-generic-{idx:04}");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &generic_swap.signature,
            ));
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap: generic_swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at,
                last_backpressure_log_at: None,
            });
        }
        let mut target_swap = test_swap("sig-target-pending");
        target_swap.token_out = "token-target".to_string();
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &target_swap.signature,
        ));
        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
            swap: target_swap.clone(),
            discovery_critical: true,
            processing_started_at,
            backpressure_started_at,
            last_backpressure_log_at: None,
        });

        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "under the stale empty-target ownership, generic SOL buys can fully contaminate the bounded pending queue and pause ingestion before the later target-mint context is retried"
        );

        let mut refreshed_target_buy_mints = HashSet::new();
        refresh_discovery_critical_target_buy_mints_or_warn(
            &store,
            &mut refreshed_target_buy_mints,
        )?;
        let dropped = prune_noncritical_zero_universe_pending_irrelevant_swaps(
            &mut pending_irrelevant_swaps,
            &mut recent_signatures,
            &mut recent_signature_order,
            &mut telemetry,
            &empty_follow_snapshot,
            &HashSet::new(),
            true,
            &refreshed_target_buy_mints,
        );
        assert_eq!(
            dropped,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1),
            "once the exact rebuild target mints materialize, the app must drop the stale generic backlog instead of letting it keep the bounded queue full"
        );
        assert_eq!(pending_irrelevant_swaps.len(), 1);
        assert_eq!(
            pending_irrelevant_swaps
                .front()
                .expect("target swap should remain queued")
                .swap
                .signature,
            target_swap.signature,
            "pruning must preserve the still-relevant target-mint market-context swap rather than wiping the queue indiscriminately"
        );
        assert!(
            !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "after stale generic backlog is pruned, ingestion should no longer be paused before the later target-mint context can be retried"
        );
        assert!(
            recent_signatures.contains(&target_swap.signature),
            "the preserved target-mint swap must stay inside recent-signature dedupe state until it is retried"
        );
        assert!(
            !recent_signatures.contains("sig-generic-pending-0000"),
            "dropped stale generic backlog must release recent-signature ownership so those swaps do not stay artificially pinned forever"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn broad_unique_buy_universe_target_set_keeps_generic_backpressure_and_blocks_later_exact_context_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("pending-irrelevant-broad-unique-buy-universe-targets")?;
        let broad_unique_buy_mints: Vec<String> = (0
            ..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY)
            .map(|idx| format!("token-generic-{idx:04}"))
            .chain(std::iter::once("token-target".to_string()))
            .collect();
        let broad_unique_buy_mint_refs: Vec<&str> =
            broad_unique_buy_mints.iter().map(String::as_str).collect();
        seed_test_discovery_critical_target_buy_mints(&store, &[], &broad_unique_buy_mint_refs)?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut telemetry = AppConsumerLoopTelemetry::default();
        let processing_started_at = StdInstant::now();
        let backpressure_started_at = StdInstant::now();
        let mut pending_irrelevant_swaps = VecDeque::new();

        for idx in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1) {
            let mut generic_swap = test_swap(&format!("sig-broad-generic-pending-{idx:04}"));
            generic_swap.token_out = format!("token-generic-{idx:04}");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &generic_swap.signature,
            ));
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap: generic_swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at,
                last_backpressure_log_at: None,
            });
        }
        let mut target_swap = test_swap("sig-broad-target-pending");
        target_swap.token_out = "token-target".to_string();
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &target_swap.signature,
        ));
        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
            swap: target_swap,
            discovery_critical: true,
            processing_started_at,
            backpressure_started_at,
            last_backpressure_log_at: None,
        });

        let mut refreshed_target_buy_mints = HashSet::new();
        refresh_discovery_critical_target_buy_mints_or_warn(
            &store,
            &mut refreshed_target_buy_mints,
        )?;
        assert_eq!(
            refreshed_target_buy_mints.len(),
            broad_unique_buy_mints.len(),
            "without an exact candidate target surface, refresh still reloads the full unique-buy-mint universe"
        );
        let dropped = prune_noncritical_zero_universe_pending_irrelevant_swaps(
            &mut pending_irrelevant_swaps,
            &mut recent_signatures,
            &mut recent_signature_order,
            &mut telemetry,
            &empty_follow_snapshot,
            &HashSet::new(),
            true,
            &refreshed_target_buy_mints,
        );
        assert_eq!(
            dropped, 0,
            "when the persisted target set is still the whole unique-buy-mint universe, generic backlog remains discovery-critical and cannot be pruned away"
        );
        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "the same broad target-mint ownership leaves the bounded pending queue saturated, so later exact target context still cannot make forward progress"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_discovery_critical_uses_reserved_writer_capacity_without_waiting_for_commit_stage1(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-priority-enqueue")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let runtime_handle = std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(
                    sqlite_path,
                    2,
                    1,
                    false,
                    DiscoveryAggregateWriteConfig::default(),
                )?;
                let filler_swap = test_swap("sig-discovery-critical-filler");
                let critical_swap = test_swap("sig-discovery-critical-target");
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();
                for swap in [&filler_swap, &critical_swap] {
                    assert!(note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    ));
                }

                assert_eq!(
                    enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &filler_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );
                result_tx
                    .send("ready")
                    .expect("main thread should receive ready signal");
                let outcome = enqueue_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &critical_swap,
                    true,
                )
                .await?;
                assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
                result_tx
                    .send("critical_enqueued")
                    .expect("main thread should receive critical enqueue signal");

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context(
                    "writer should drain discovery-critical irrelevant swap after lock release",
                )??;
                writer.shutdown()?;
                Ok::<(), anyhow::Error>(())
            })
        });

        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_secs(1))
                .context("runtime thread never reached discovery-critical enqueue")?,
            "ready"
        );
        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_millis(100))
                .context("runtime thread never completed discovery-critical enqueue before lock release")?,
            "critical_enqueued",
            "discovery-critical irrelevant swap should return immediately once it claims reserved writer capacity instead of waiting for sqlite durability"
        );

        let verify_before_commit = SqliteStore::open(&db_path)?;
        assert_eq!(
            verify_before_commit
                .load_observed_swaps_since(
                    DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                        .expect("timestamp")
                        .with_timezone(&Utc),
                )?
                .len(),
            0,
            "sqlite lock should still keep discovery-critical irrelevant swaps invisible until the writer thread can commit them"
        );

        blocker_conn.execute_batch("COMMIT")?;
        runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context(
                "discovery-critical irrelevant swap should still persist after lock release",
            )?;

        let verify_store = SqliteStore::open(&db_path)?;
        let persisted = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(persisted.len(), 2);
        assert!(
            persisted
                .iter()
                .any(|swap| swap.signature == "sig-discovery-critical-target"),
            "the discovery-critical irrelevant swap should persist once the durable write unblocks"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn pending_irrelevant_swap_backpressure_blocks_ingestion_only_at_bounded_capacity_stage1() {
        let now = StdInstant::now();
        let mut pending = VecDeque::new();
        assert!(!pending_irrelevant_swap_backpressure_blocks_ingestion(
            &pending
        ));

        pending.push_back(PendingIrrelevantObservedSwap {
            swap: test_swap("sig-pending-bounded-one"),
            discovery_critical: true,
            processing_started_at: now,
            backpressure_started_at: now,
            last_backpressure_log_at: None,
        });
        assert!(
            !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending),
            "one pending discovery-critical swap should no longer block ingestion polling"
        );

        while pending.len() < DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
            pending.push_back(PendingIrrelevantObservedSwap {
                swap: test_swap(&format!("sig-pending-bounded-{}", pending.len())),
                discovery_critical: true,
                processing_started_at: now,
                backpressure_started_at: now,
                last_backpressure_log_at: None,
            });
        }
        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending),
            "ingestion should only pause once the bounded discovery-critical backlog is actually exhausted"
        );
    }

    #[test]
    fn sustained_discovery_critical_irrelevant_backpressure_buffers_multiple_swaps_before_ingestion_pause_stage1(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-sustained-buffer")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let db_path_for_runtime = db_path.clone();
        let runtime_handle = std::thread::spawn(move || -> Result<(usize, usize)> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(
                    db_path_for_runtime
                        .to_str()
                        .context("sqlite path must be valid utf-8")?
                        .to_string(),
                    2,
                    1,
                    false,
                    DiscoveryAggregateWriteConfig::default(),
                )?;
                let filler_swap = test_swap("sig-discovery-critical-sustained-filler");
                let critical_swaps = (0..4)
                    .map(|idx| test_swap(&format!("sig-discovery-critical-sustained-{idx}")))
                    .collect::<Vec<_>>();
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();
                assert!(note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &filler_swap.signature,
                ));
                for swap in &critical_swaps {
                    assert!(note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    ));
                }

                assert_eq!(
                    enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &filler_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );

                let processing_started_at = StdInstant::now();
                let mut pending = VecDeque::new();
                let mut directly_enqueued = 0usize;
                for swap in &critical_swaps {
                    match enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        swap,
                        true,
                    )
                    .await? {
                        IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                            directly_enqueued = directly_enqueued.saturating_add(1);
                        }
                        IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                            pending.push_back(PendingIrrelevantObservedSwap {
                                swap: swap.clone(),
                                discovery_critical: true,
                                processing_started_at,
                                backpressure_started_at: StdInstant::now(),
                                last_backpressure_log_at: None,
                            });
                            assert!(
                                !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending),
                                "the bounded pending queue should allow the consumer to keep polling after the first discovery-critical backpressure event"
                            );
                        }
                    }
                }

                assert!(
                    directly_enqueued >= 1,
                    "the reserved writer slot should accept at least one discovery-critical swap before the writer is fully saturated"
                );
                assert!(
                    pending.len() >= 2,
                    "sustained discovery-critical pressure should now buffer multiple swaps instead of freezing after the first pending one"
                );

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if pending.is_empty() {
                            break Ok::<(), anyhow::Error>(());
                        }
                        let mut blocked_front = None;
                        while let Some(pending_swap) = pending.pop_front() {
                            match enqueue_irrelevant_observed_swap(
                                &writer,
                                &mut recent_signatures,
                                &mut recent_signature_order,
                                &pending_swap.swap,
                                pending_swap.discovery_critical,
                            )
                            .await? {
                                IrrelevantObservedSwapEnqueueOutcome::Enqueued => {}
                                IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                                    blocked_front = Some(pending_swap);
                                    break;
                                }
                            }
                        }
                        if let Some(blocked_front) = blocked_front {
                            pending.push_front(blocked_front);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }
                })
                .await
                .context("pending discovery-critical irrelevant swaps should eventually drain")??;

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("writer should drain sustained discovery-critical irrelevant swaps")??;

                writer.shutdown()?;
                Ok::<(usize, usize), anyhow::Error>((directly_enqueued, critical_swaps.len()))
            })
        });

        std::thread::sleep(StdDuration::from_millis(150));
        blocker_conn.execute_batch("COMMIT")?;

        let (directly_enqueued, critical_total) = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("sustained discovery-critical backpressure path should stay healthy")?;

        let verify_store = SqliteStore::open(&db_path)?;
        let persisted = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(persisted.len(), 1 + critical_total);
        assert!(
            directly_enqueued < critical_total,
            "the repro must actually exercise buffered discovery-critical backpressure rather than only direct enqueue success"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_reports_pending_backpressure_without_forgetting_signature(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-observed-swap-backpressure")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let db_path_for_runtime = db_path.clone();
        let runtime_handle = std::thread::spawn(move || -> Result<usize> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(
                    db_path_for_runtime
                        .to_str()
                        .context("sqlite path must be valid utf-8")?
                        .to_string(),
                    1,
                    1,
                    false,
                    DiscoveryAggregateWriteConfig::default(),
                )?;
                let first_swap = test_swap("sig-irrelevant-backpressure-a");
                let second_swap = test_swap("sig-irrelevant-backpressure-b");
                let third_swap = test_swap("sig-irrelevant-backpressure-c");
                let fourth_swap = test_swap("sig-irrelevant-backpressure-d");
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();

                for swap in [&first_swap, &second_swap, &third_swap, &fourth_swap] {
                    assert!(note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    ));
                }

                assert_eq!(
                    enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &first_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );
                let mut pending_swap = None;
                let mut expected_persisted = 1usize;
                for swap in [&second_swap, &third_swap, &fourth_swap] {
                    let outcome = tokio::time::timeout(
                        Duration::from_millis(50),
                        enqueue_irrelevant_observed_swap(
                            &writer,
                            &mut recent_signatures,
                            &mut recent_signature_order,
                            swap,
                            false,
                        ),
                    )
                    .await
                    .context("irrelevant observed swap backpressure check stalled runtime")??;
                    match outcome {
                        IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                            expected_persisted = expected_persisted.saturating_add(1);
                        }
                        IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                            pending_swap = Some(swap.clone());
                            expected_persisted = expected_persisted.saturating_add(1);
                            break;
                        }
                    }
                }
                let pending_swap = pending_swap
                    .context("expected bounded irrelevant observed swap queue to report backpressure")?;
                assert!(
                    !note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &pending_swap.signature,
                    ),
                    "backpressured irrelevant observed swap must remain deduped until retried"
                );

                tokio::time::timeout(
                    Duration::from_millis(50),
                    tokio::time::sleep(Duration::from_millis(10)),
                )
                    .await
                    .context("current-thread runtime stalled while irrelevant observed swap was backpressured")?;

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        match enqueue_irrelevant_observed_swap(
                            &writer,
                            &mut recent_signatures,
                            &mut recent_signature_order,
                            &pending_swap,
                            false,
                        )
                        .await? {
                            IrrelevantObservedSwapEnqueueOutcome::Enqueued => {
                                break Ok::<(), anyhow::Error>(())
                            }
                            IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure => {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                    }
                })
                .await
                .context("backpressured irrelevant observed swap should eventually enqueue")??;

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("observed swap writer should drain pending irrelevant swaps")??;

                writer.shutdown()?;
                Ok::<usize, anyhow::Error>(expected_persisted)
            })
        });

        std::thread::sleep(StdDuration::from_millis(150));
        blocker_conn.execute_batch("COMMIT")?;

        let expected_persisted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("irrelevant observed swap backpressure path should stay healthy")?;

        let verify_store = SqliteStore::open(&db_path)?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), expected_persisted);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn app_consumer_loop_telemetry_reports_follow_rejected_ratio_and_resets() {
        let mut telemetry = AppConsumerLoopTelemetry::default();

        telemetry.note_swap_seen();
        telemetry.note_processing_duration(5);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(15);
        telemetry.note_swap_seen();
        telemetry.note_follow_rejected();
        telemetry.note_processing_duration(25);

        let snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 3,
                follow_rejected: 2,
                follow_rejected_ratio: 2.0 / 3.0,
                processing_ms_p95: 25,
            }
        );

        let empty_snapshot = telemetry.snapshot_and_reset();
        assert_eq!(
            empty_snapshot,
            AppConsumerLoopTelemetrySnapshot {
                swaps_seen: 0,
                follow_rejected: 0,
                follow_rejected_ratio: 0.0,
                processing_ms_p95: 0,
            }
        );
    }

    #[test]
    fn parse_app_log_env_filter_uses_default_when_missing() {
        with_app_log_filter_env(None, None, || {
            parse_app_log_env_filter("info").expect("missing env must use default app log filter");
        });
    }

    #[test]
    fn parse_app_log_env_filter_rejects_invalid_syntax() {
        with_app_log_filter_env(Some(OsString::from("[")), None, || {
            assert_eq!(
                env::var(APP_LOG_FILTER_ENV).as_deref(),
                Ok("["),
                "test helper failed to set app log filter env"
            );
            let error = parse_app_log_env_filter("info").expect_err("invalid filter must reject");
            assert!(
                error.to_string().contains(APP_LOG_FILTER_ENV),
                "unexpected error: {}",
                error
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn parse_app_log_env_filter_rejects_non_utf8() {
        use std::os::unix::ffi::OsStringExt;

        with_app_log_filter_env(Some(OsString::from_vec(vec![0xff])), None, || {
            let error = parse_app_log_env_filter("info").expect_err("non-UTF8 filter must reject");
            assert!(
                error.to_string().contains(APP_LOG_FILTER_ENV),
                "unexpected error: {}",
                error
            );
            assert!(
                error.to_string().contains("UTF-8"),
                "unexpected error: {}",
                error
            );
        });
    }

    #[test]
    fn parse_app_log_env_filter_ignores_rust_log() {
        with_app_log_filter_env(None, Some(OsString::from("error")), || {
            parse_app_log_env_filter("info")
                .expect("ambient RUST_LOG should not affect app log filter");
        });
    }

    #[test]
    fn ingestion_error_backoff_ms_uses_progressive_schedule() {
        assert_eq!(ingestion_error_backoff_ms(0), 100);
        assert_eq!(ingestion_error_backoff_ms(1), 100);
        assert_eq!(ingestion_error_backoff_ms(2), 250);
        assert_eq!(ingestion_error_backoff_ms(3), 500);
        assert_eq!(ingestion_error_backoff_ms(4), 1_000);
        assert_eq!(ingestion_error_backoff_ms(5), 2_000);
        assert_eq!(ingestion_error_backoff_ms(6), 5_000);
    }

    #[test]
    fn ingestion_error_backoff_ms_saturates_at_max_step() {
        assert_eq!(ingestion_error_backoff_ms(7), 5_000);
        assert_eq!(ingestion_error_backoff_ms(100), 5_000);
        assert_eq!(ingestion_error_backoff_ms(u32::MAX), 5_000);
    }

    #[test]
    fn apply_ingestion_source_override_has_priority_over_existing_source() {
        let mut source = "mock".to_string();
        let applied =
            apply_ingestion_source_override(&mut source, Some("yellowstone_grpc".to_string()));
        assert_eq!(applied.as_deref(), Some("yellowstone_grpc"));
        assert_eq!(source, "yellowstone_grpc");
    }

    #[test]
    fn parse_operator_emergency_stop_reason_ignores_comments() {
        let content = r#"
# stop trading immediately

manual override by operator
"#;
        assert_eq!(
            parse_operator_emergency_stop_reason(content).as_deref(),
            Some("manual override by operator")
        );
    }

    #[test]
    fn operator_emergency_stop_refresh_returns_error_on_fatal_activation_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("operator-stop-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stop_path = std::env::temp_dir().join(format!(
            "copybot-operator-stop-{}-{}.flag",
            std::process::id(),
            now.timestamp_nanos_opt()
                .unwrap_or(now.timestamp_micros() * 1000)
        ));
        std::fs::write(&stop_path, "manual override by operator\n")?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_operator_emergency_stop_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'operator_emergency_stop_activated'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut stop = OperatorEmergencyStop {
            path: stop_path.clone(),
            poll_interval: Duration::from_millis(100),
            next_refresh_at: StdInstant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(StdInstant::now),
            active: false,
            detail: String::new(),
        };
        let error = stop
            .refresh(&store, now)
            .expect_err("fatal operator stop risk-event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist operator emergency stop activation event with fatal sqlite I/O"
            ),
            "expected operator-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            stop.is_active(),
            "activation state should already be fail-closed"
        );
        assert_eq!(
            store.risk_event_count_by_type("operator_emergency_stop_activated")?,
            0
        );

        let _ = std::fs::remove_file(stop_path);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn operator_emergency_stop_refresh_returns_error_on_fatal_clear_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("operator-stop-clear-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stop_path = std::env::temp_dir().join(format!(
            "copybot-operator-stop-clear-{}-{}.flag",
            std::process::id(),
            now.timestamp_nanos_opt()
                .unwrap_or(now.timestamp_micros() * 1000)
        ));
        let _ = std::fs::remove_file(&stop_path);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_operator_emergency_stop_clear_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'operator_emergency_stop_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut stop = OperatorEmergencyStop {
            path: stop_path.clone(),
            poll_interval: Duration::from_millis(100),
            next_refresh_at: StdInstant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(StdInstant::now),
            active: true,
            detail: "manual override by operator".to_string(),
        };
        let error = stop
            .refresh(&store, now)
            .expect_err("fatal operator stop clear risk-event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist operator emergency stop clear event with fatal sqlite I/O"
            ),
            "expected operator-stop clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            stop.is_active(),
            "fatal operator stop clear write must preserve active fail-closed state"
        );
        assert_eq!(stop.detail(), "manual override by operator");
        assert_eq!(
            store.risk_event_count_by_type("operator_emergency_stop_cleared")?,
            0
        );

        let _ = std::fs::remove_file(stop_path);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn record_shadow_queue_backpressure_risk_event_returns_error_on_fatal_write() -> Result<()> {
        let (store, db_path) = make_test_store("shadow-queue-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_queue_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_queue_saturated'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = record_shadow_queue_backpressure_risk_event(&store, 7, 2, 256, 256, now)
            .expect_err("fatal queue-backpressure risk-event write must abort");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow queue backpressure risk event with fatal sqlite I/O"
            ),
            "expected queue-backpressure fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_queue_saturated")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_block_respects_pause_new_trades_on_outage_flag() -> Result<()> {
        let (store, db_path) = make_test_store("infra-outage-flag")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        guard.infra_block_reason = Some("ingestion_degraded".to_string());
        let now = Utc::now();

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Infra,
                detail,
            } => assert!(detail.contains("ingestion_degraded")),
            other => panic!("expected infra block when outage pausing is enabled, got {other:?}"),
        }
        match guard.can_open_buy(&store, now, false) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected outage infra block bypass when pause_new_trades_on_outage=false, got {other:?}"
            ),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn causal_holdback_holds_sell_without_open_lot_or_pending_key() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let shadow_scheduler = ShadowScheduler::new();
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots,
        ));
    }

    #[test]
    fn causal_holdback_skips_when_open_lot_or_pending_exists() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let pending_task = ShadowTaskInput {
            swap: SwapEvent {
                wallet: "wallet-a".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-x".to_string(),
                amount_in: 1.0,
                amount_out: 1000.0,
                signature: "sig-pending".to_string(),
                slot: 1,
                ts_utc: Utc::now(),
                exact_amounts: None,
            },
            follow_snapshot: Arc::new(FollowSnapshot::default()),
            key: key.clone(),
        };
        let mut shadow_scheduler = ShadowScheduler::new();
        shadow_scheduler
            .pending_shadow_tasks
            .insert(key.clone(), VecDeque::from([pending_task]));
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(!shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots,
        ));

        shadow_scheduler.pending_shadow_tasks.clear();
        let open_lots_yes = HashSet::from([(key.wallet.clone(), key.token.clone())]);
        assert!(!shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots_yes,
        ));
    }

    fn with_app_log_filter_env<T>(
        app_value: Option<OsString>,
        rust_log_value: Option<OsString>,
        run: impl FnOnce() -> T,
    ) -> T {
        let _guard = APP_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved_app = env::var_os(APP_LOG_FILTER_ENV);
        let saved_rust_log = env::var_os(LEGACY_RUST_LOG_ENV);
        env::remove_var(APP_LOG_FILTER_ENV);
        env::remove_var(LEGACY_RUST_LOG_ENV);
        if let Some(value) = app_value {
            env::set_var(APP_LOG_FILTER_ENV, value);
        }
        if let Some(value) = rust_log_value {
            env::set_var(LEGACY_RUST_LOG_ENV, value);
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        env::remove_var(APP_LOG_FILTER_ENV);
        env::remove_var(LEGACY_RUST_LOG_ENV);
        if let Some(value) = saved_app {
            env::set_var(APP_LOG_FILTER_ENV, value);
        }
        if let Some(value) = saved_rust_log {
            env::set_var(LEGACY_RUST_LOG_ENV, value);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}
