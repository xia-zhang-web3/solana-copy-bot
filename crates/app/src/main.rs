use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
#[cfg(test)]
use copybot_config::ExecutionConfig;
use copybot_config::{
    load_from_env_or_default, normalize_ingestion_source, RiskConfig, ShadowConfig,
};
#[cfg(test)]
use copybot_core_types::{SwapEvent, TokenQuantity};
use copybot_discovery::DiscoveryService;
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{FollowSnapshot, ShadowService};
use copybot_storage::{
    sqlite_contention_snapshot, DiscoveryAggregateWriteConfig, Lamports, SignedLamports,
    SqliteStore,
};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::env;
#[cfg(test)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::Instant as StdInstant;
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
use crate::observed_swap_writer::ObservedSwapWriter;
use crate::observed_swap_writer::{
    OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT, OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT,
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
const STALE_LOT_CLEANUP_BATCH_LIMIT: u32 = 300;
const HARD_STOP_CLEAR_HEALTHY_REFRESHES: u64 = 6;
const DEFAULT_INGESTION_OVERRIDE_PATH: &str = "state/ingestion_source_override.env";
const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
const DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS: u64 = 500;
const APP_LOG_FILTER_ENV: &str = "COPYBOT_APP_LOG_FILTER";
const LEGACY_RUST_LOG_ENV: &str = "RUST_LOG";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
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
    validate_execution_runtime_contract(&config.execution, &config.system.env)?;
    validate_execution_risk_contract(&config.risk)?;

    let mut store = SqliteStore::open(Path::new(&config.sqlite.path))
        .context("failed to initialize sqlite store")?;
    let migrations_dir = resolve_migrations_dir(&loaded_config_path, &config.system.migrations_dir);
    let applied = store
        .run_migrations(&migrations_dir)
        .with_context(|| format!("failed to apply migrations in {}", migrations_dir.display()))?;
    info!(applied, "sqlite migrations applied");
    match store.checkpoint_wal_truncate() {
        Ok((busy, log_frames, checkpointed_frames)) if busy == 0 => {
            info!(
                wal_checkpoint_mode = "truncate",
                wal_checkpoint_busy = busy,
                wal_log_frames = log_frames,
                wal_checkpointed_frames = checkpointed_frames,
                "startup sqlite wal checkpoint completed"
            );
        }
        Ok((busy, log_frames, checkpointed_frames)) => match store.checkpoint_wal_passive() {
            Ok((passive_busy, passive_log_frames, passive_checkpointed_frames)) => {
                warn!(
                    wal_checkpoint_mode = "truncate_then_passive",
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    wal_passive_checkpoint_busy = passive_busy,
                    wal_passive_log_frames = passive_log_frames,
                    wal_passive_checkpointed_frames = passive_checkpointed_frames,
                    "startup sqlite wal truncate checkpoint was blocked; passive checkpoint attempted"
                );
            }
            Err(error) => {
                warn!(
                    error = %error,
                    wal_checkpoint_mode = "truncate_then_passive",
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "startup sqlite wal truncate checkpoint was blocked and passive fallback failed"
                );
            }
        },
        Err(error) => match store.checkpoint_wal_passive() {
            Ok((passive_busy, passive_log_frames, passive_checkpointed_frames)) => {
                warn!(
                    error = %error,
                    wal_checkpoint_mode = "passive_fallback",
                    wal_passive_checkpoint_busy = passive_busy,
                    wal_passive_log_frames = passive_log_frames,
                    wal_passive_checkpointed_frames = passive_checkpointed_frames,
                    "startup sqlite wal truncate checkpoint failed; passive checkpoint attempted"
                );
            }
            Err(passive_error) => {
                warn!(
                    error = %error,
                    fallback_error = %passive_error,
                    "startup sqlite wal checkpoints failed (non-fatal, continuing)"
                );
            }
        },
    }

    store
        .record_heartbeat("copybot-app", "startup")
        .context("failed to write startup heartbeat")?;
    let alert_dispatcher =
        AlertDispatcher::from_env().context("failed to initialize alert delivery")?;
    if let Some(dispatcher) = &alert_dispatcher {
        store
            .ensure_alert_delivery_cursor("webhook")
            .context("failed to initialize alert delivery cursor")?;
        if dispatcher.test_on_startup() {
            dispatcher
                .send_startup_test(&config.system.env)
                .await
                .context("failed sending startup alert delivery test")?;
            info!("startup alert delivery test sent");
        }
    }

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

fn observed_swap_writer_error_requires_restart(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message == OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT
            || message == OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT
    })
}

fn ingestion_error_backoff_ms(consecutive_errors: u32) -> u64 {
    let index =
        (consecutive_errors.saturating_sub(1) as usize).min(INGESTION_ERROR_BACKOFF_MS.len() - 1);
    INGESTION_ERROR_BACKOFF_MS[index]
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

    fn refresh(&mut self, store: &SqliteStore, now: DateTime<Utc>) {
        let instant_now = StdInstant::now();
        if instant_now < self.next_refresh_at {
            return;
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
            return;
        }

        self.active = active;
        self.detail = detail;
        let path_display = self.path.display().to_string();

        if self.active {
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
            if let Err(error) = store.insert_risk_event(
                "operator_emergency_stop_activated",
                "warn",
                now,
                Some(&details_json),
            ) {
                warn!(
                    error = %error,
                    "failed to persist operator emergency stop activation event"
                );
            }
        } else {
            info!(path = %path_display, "operator emergency stop cleared");
            let details_json = format!(
                "{{\"path\":\"{}\",\"state\":\"cleared\"}}",
                sanitize_json_value(&path_display)
            );
            if let Err(error) = store.insert_risk_event(
                "operator_emergency_stop_cleared",
                "info",
                now,
                Some(&details_json),
            ) {
                warn!(
                    error = %error,
                    "failed to persist operator emergency stop clear event"
                );
            }
        }
    }
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

    fn restore_pause_from_store(&mut self, store: &SqliteStore, now: DateTime<Utc>) {
        if !self.config.shadow_killswitch_enabled {
            return;
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
                warn!(error = %error, "failed to restore shadow risk timed pause");
            }
        }
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

    fn observe_discovery_cycle(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        eligible_wallets: usize,
        active_follow_wallets: usize,
    ) {
        if !self.config.shadow_killswitch_enabled {
            return;
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
            self.universe_blocked = should_block;
            if should_block {
                let details_json = format!(
                    "{{\"active_follow_wallets\":{},\"eligible_wallets\":{},\"streak\":{},\"min_active_follow_wallets\":{},\"min_eligible_wallets\":{}}}",
                    active_follow_wallets,
                    eligible_wallets,
                    self.universe_breach_streak,
                    self.config.shadow_universe_min_active_follow_wallets,
                    self.config.shadow_universe_min_eligible_wallets
                );
                warn!(
                    active_follow_wallets,
                    eligible_wallets,
                    streak = self.universe_breach_streak,
                    min_active_follow_wallets =
                        self.config.shadow_universe_min_active_follow_wallets,
                    min_eligible_wallets = self.config.shadow_universe_min_eligible_wallets,
                    "shadow risk universe stop activated"
                );
                self.record_risk_event(
                    store,
                    "shadow_risk_universe_stop",
                    "warn",
                    now,
                    &details_json,
                );
            } else {
                info!(
                    active_follow_wallets,
                    eligible_wallets, "shadow risk universe stop cleared"
                );
                self.record_risk_event(
                    store,
                    "shadow_risk_universe_cleared",
                    "info",
                    now,
                    "{\"state\":\"cleared\"}",
                );
            }
        }
    }

    fn observe_ingestion_snapshot(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        snapshot: Option<IngestionRuntimeSnapshot>,
    ) {
        if !self.config.shadow_killswitch_enabled {
            return;
        }
        let Some(snapshot) = snapshot else {
            return;
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
                    return;
                }

                if self.infra_block_key.is_some() {
                    if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                        self.infra_block_key = Some(signal.key);
                        self.infra_block_reason = Some(signal.reason);
                    }
                    return;
                }

                if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                    let reason = signal.reason;
                    self.infra_block_key = Some(signal.key);
                    self.infra_block_reason = Some(reason.clone());
                    warn!(reason = %reason, "shadow risk infra stop activated");
                    let details_json = format!("{{\"reason\":\"{}\"}}", reason);
                    if self.should_emit_infra_event(now) {
                        self.record_risk_event(
                            store,
                            "shadow_risk_infra_stop",
                            "warn",
                            now,
                            &details_json,
                        );
                    }
                }
            }
            None => {
                self.infra_candidate_key = None;
                self.infra_candidate_streak = 0;
                if self.infra_block_key.is_none() {
                    return;
                }
                self.infra_healthy_streak = self.infra_healthy_streak.saturating_add(1);
                if self.infra_healthy_streak < RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
                    return;
                }
                self.infra_healthy_streak = 0;
                self.infra_block_key = None;
                self.infra_block_reason = None;
                info!("shadow risk infra stop cleared");
                if self.should_emit_infra_event(now) {
                    self.record_risk_event(
                        store,
                        "shadow_risk_infra_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                    );
                }
            }
        }
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
            let should_log = self.on_risk_refresh_error(now);
            if should_log {
                warn!(error = %error, "shadow risk fail-closed activated");
                let details_json = format!(
                    "{{\"error\":\"{}\"}}",
                    sanitize_json_value(&error.to_string())
                );
                self.record_risk_event(
                    store,
                    "shadow_risk_fail_closed",
                    "error",
                    now,
                    &details_json,
                );
            }
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail: format!("risk_check_error: {error}"),
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
            self.clear_pause(store, now);
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
                self.activate_hard_stop(store, now, stop_type, detail);
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
                self.clear_hard_stop(store, now);
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
                self.exposure_hard_blocked = exposure_blocked_now;
                if exposure_blocked_now {
                    self.exposure_hard_detail = Some(exposure_detail.clone());
                    warn!(
                        detail = %exposure_detail,
                        "shadow risk exposure hard cap active"
                    );
                    let details_json = format!(
                        "{{\"state\":\"active\",\"detail\":\"{}\"}}",
                        sanitize_json_value(&exposure_detail)
                    );
                    self.record_risk_event(
                        store,
                        "shadow_risk_exposure_hard_cap",
                        "warn",
                        now,
                        &details_json,
                    );
                } else {
                    self.exposure_hard_detail = None;
                    info!("shadow risk exposure hard cap cleared");
                    self.record_risk_event(
                        store,
                        "shadow_risk_exposure_hard_cap_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                    );
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
                self.clear_soft_exposure_pause(store, now);
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
                );
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
                );
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
                );
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

    fn compute_infra_block_signal(&self, now: DateTime<Utc>) -> Option<InfraBlockSignal> {
        if self.infra_samples.is_empty() {
            return None;
        }

        if let Some(started_at) = self.lag_breach_since {
            if now - started_at
                >= chrono::Duration::minutes(
                    self.config.shadow_infra_lag_breach_minutes.max(1) as i64
                )
            {
                return Some(InfraBlockSignal {
                    key: InfraBlockKey::LagP95,
                    reason: format!(
                        "lag_p95_over_threshold_for={}m threshold_ms={}",
                        self.config.shadow_infra_lag_breach_minutes,
                        self.config.shadow_infra_lag_p95_threshold_ms
                    ),
                });
            }
        }

        let window_start =
            now - chrono::Duration::minutes(self.config.shadow_infra_window_minutes.max(1) as i64);
        let latest = self.infra_samples.back().copied()?;
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
                reason: format!(
                    "no_ingestion_progress_for={}m",
                    self.config.shadow_infra_window_minutes.max(1)
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
                        reason: format!(
                            "parser_stall_for={}m tx_updates_delta={} parser_errors_delta={} error_ratio={:.4}",
                            self.config.shadow_infra_window_minutes.max(1),
                            delta_grpc_transaction_updates_total,
                            parser_errors_delta,
                            parser_error_ratio
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
                        reason: format!(
                            "replaced_ratio={:.4} delta_replaced={} delta_enqueued={}",
                            replaced_ratio, delta_replaced, delta_enqueued
                        ),
                    });
                }
            }
        }
        if delta_rpc_429 >= self.config.shadow_infra_rpc429_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc429,
                reason: format!(
                    "rpc_429_delta={} threshold={}",
                    delta_rpc_429, self.config.shadow_infra_rpc429_delta_threshold
                ),
            });
        }
        if delta_rpc_5xx >= self.config.shadow_infra_rpc5xx_delta_threshold {
            return Some(InfraBlockSignal {
                key: InfraBlockKey::Rpc5xx,
                reason: format!(
                    "rpc_5xx_delta={} threshold={}",
                    delta_rpc_5xx, self.config.shadow_infra_rpc5xx_delta_threshold
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
    ) {
        self.hard_stop_clear_healthy_streak = 0;
        if self.hard_stop_reason.is_some() {
            return;
        }
        let reason = format!("{stop_type}: {detail}");
        self.hard_stop_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk hard stop activated");
        let details_json = format!(
            "{{\"stop_type\":\"{}\",\"detail\":\"{}\"}}",
            sanitize_json_value(stop_type),
            sanitize_json_value(&detail)
        );
        self.record_risk_event(store, "shadow_risk_hard_stop", "error", now, &details_json);
    }

    fn clear_hard_stop(&mut self, store: &SqliteStore, now: DateTime<Utc>) {
        self.hard_stop_clear_healthy_streak = 0;
        let Some(previous_reason) = self.hard_stop_reason.take() else {
            return;
        };
        info!(
            previous_reason = %previous_reason,
            "shadow risk hard stop cleared"
        );
        let details_json = format!(
            "{{\"state\":\"cleared\",\"previous_reason\":\"{}\"}}",
            sanitize_json_value(&previous_reason)
        );
        self.record_risk_event(
            store,
            "shadow_risk_hard_stop_cleared",
            "info",
            now,
            &details_json,
        );
    }

    fn activate_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        pause_type: &str,
        detail: String,
    ) {
        let duration = if duration <= chrono::Duration::zero() {
            chrono::Duration::minutes(1)
        } else {
            duration
        };
        let until = now + duration;
        let should_update = self.pause_until.map(|value| until > value).unwrap_or(true);
        if !should_update {
            return;
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
        self.record_risk_event(store, "shadow_risk_pause", "warn", now, &details_json);
    }

    fn activate_soft_exposure_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        exposure_lamports: Lamports,
        soft_cap_lamports: Lamports,
        resume_below_lamports: Lamports,
    ) {
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
        self.record_risk_event(store, "shadow_risk_pause", "warn", now, &details_json);
    }

    fn clear_pause(&mut self, store: &SqliteStore, now: DateTime<Utc>) {
        let Some(previous_until) = self.pause_until.take() else {
            self.pause_reason = None;
            return;
        };
        let previous_reason = self
            .pause_reason
            .take()
            .unwrap_or_else(|| format!("paused_until={}", previous_until.to_rfc3339()));
        let cleared_pause_type = previous_reason
            .split_once(':')
            .map(|(pause_type, _)| pause_type.trim())
            .filter(|pause_type| !pause_type.is_empty())
            .unwrap_or("timed_pause");
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk timed pause cleared"
        );
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"{}\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(cleared_pause_type),
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        self.record_risk_event(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
        );
    }

    fn clear_soft_exposure_pause(&mut self, store: &SqliteStore, now: DateTime<Utc>) {
        let Some(previous_until) = self.soft_exposure_pause_until.take() else {
            self.soft_exposure_pause_latched = false;
            self.soft_exposure_pause_reason = None;
            return;
        };
        let previous_reason = self.soft_exposure_pause_reason.take().unwrap_or_else(|| {
            format!(
                "exposure_soft_cap: paused_until={}",
                previous_until.to_rfc3339()
            )
        });
        self.soft_exposure_pause_latched = false;
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk soft exposure pause cleared"
        );
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"exposure_soft_cap\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        self.record_risk_event(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
        );
    }

    fn record_risk_event(
        &self,
        store: &SqliteStore,
        event_type: &str,
        severity: &str,
        ts: DateTime<Utc>,
        details_json: &str,
    ) {
        if let Err(error) = store.insert_risk_event(event_type, severity, ts, Some(details_json)) {
            warn!(
                error = %error,
                event_type,
                severity,
                "failed to persist shadow risk event"
            );
        }
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
    let mut follow_snapshot = Arc::new(FollowSnapshot::from_active_wallets(initial_active_wallets));
    let follow_event_retention =
        follow_event_retention_duration(shadow_max_signal_lag_seconds, discovery_refresh_seconds);
    let mut open_shadow_lots = store
        .list_shadow_open_pairs()
        .context("failed to load open shadow lot index")?;
    let stale_lot_max_hold_hours = risk_config.max_hold_hours;
    let stale_lot_terminal_zero_price_hours =
        risk_config.shadow_stale_close_terminal_zero_price_hours;
    let stale_lot_recovery_zero_price_enabled =
        risk_config.shadow_stale_close_recovery_zero_price_enabled;
    let mut shadow_risk_guard =
        ShadowRiskGuard::new_with_ingestion_source(risk_config, ingestion_source);
    shadow_risk_guard.restore_pause_from_store(&store, Utc::now());
    let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut discovery_handle: Option<JoinHandle<Result<DiscoveryTaskOutput>>> = None;
    let mut shadow_scheduler = ShadowScheduler::new();
    let mut execution_handle: Option<JoinHandle<Result<ExecutionBatchReport>>> = None;
    let observed_swap_writer = ObservedSwapWriter::start(
        sqlite_path.clone(),
        observed_swaps_retention_days,
        discovery_scoring_retention_days,
        discovery_scoring_writes_enabled,
        discovery_aggregate_write_config,
    )
    .context("failed to start observed swap writer")?;
    let history_retention = HistoryRetentionRunner::new(history_retention_config);
    let history_retention_sweep_interval = Duration::from_secs(history_retention.sweep_seconds());
    let mut last_history_retention_sweep = StdInstant::now()
        .checked_sub(history_retention_sweep_interval)
        .unwrap_or_else(StdInstant::now);
    let mut operator_emergency_stop = OperatorEmergencyStop::from_env();
    let mut execution_emergency_stop_active_logged = false;
    let mut execution_hard_stop_pause_logged = false;
    let mut execution_shadow_risk_pause_logged_reason: Option<String> = None;
    let mut execution_outage_pause_logged = false;
    let mut ingestion_error_streak: u32 = 0;
    let mut ingestion_backoff_until: Option<time::Instant> = None;
    operator_emergency_stop.refresh(&store, Utc::now());
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

    if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }

    loop {
        operator_emergency_stop.refresh(&store, Utc::now());

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
            let details_json = format!(
                "{{\"reason\":\"queue_backpressure\",\"pending\":{},\"held\":{},\"buffered\":{},\"capacity\":{}}}",
                shadow_scheduler.pending_shadow_task_count,
                shadow_scheduler.held_shadow_sell_count(),
                shadow_buffered_task_count,
                SHADOW_PENDING_TASK_CAPACITY
            );
            if let Err(error) = store.insert_risk_event(
                "shadow_queue_saturated",
                "warn",
                Utc::now(),
                Some(&details_json),
            ) {
                warn!(
                    error = %error,
                    "failed to persist shadow queue backpressure risk event"
                );
            }
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
                        if discovery_output.published {
                            let mut snapshot = (*follow_snapshot).clone();
                            apply_follow_snapshot_update(
                                &mut snapshot,
                                discovery_output.active_wallets,
                                discovery_output.cycle_ts,
                                follow_event_retention,
                            );
                            follow_snapshot = Arc::new(snapshot);
                            shadow_risk_guard.observe_discovery_cycle(
                                &store,
                                discovery_output.cycle_ts,
                                discovery_output.eligible_wallets,
                                discovery_output.active_follow_wallets,
                            );
                        }
                    }
                    Ok(Err(error)) => {
                        warn!(error = %error, "discovery cycle failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "discovery task join failed");
                    }
                }
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
                        );
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
                        match store.list_shadow_open_pairs() {
                            Ok(pairs) => {
                                open_shadow_lots = pairs;
                            }
                            Err(error) => {
                                warn!(error = %error, "failed to refresh open shadow lot index");
                            }
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
                        warn!(error = %error, "shadow snapshot failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "shadow snapshot task join failed");
                    }
                }
            }
            _ = interval.tick() => {
                if let Err(error) = store.record_heartbeat("copybot-app", "alive") {
                    warn!(error = %error, "heartbeat write failed");
                }
                if let Some(dispatcher) = &alert_dispatcher {
                    match dispatcher.deliver_pending(&store).await {
                        Ok(delivered) if delivered > 0 => {
                            info!(delivered, "delivered pending alert webhooks");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, "alert delivery poll failed");
                        }
                    }
                }
                if history_retention.enabled()
                    && last_history_retention_sweep.elapsed() >= history_retention_sweep_interval
                {
                    match history_retention.apply(&store, Utc::now(), alert_dispatcher.is_some()) {
                        Ok(summary) if !summary.is_empty() => {
                            info!(
                                risk_events_deleted = summary.risk_events_deleted,
                                copy_signals_deleted = summary.copy_signals_deleted,
                                orders_deleted = summary.orders_deleted,
                                fills_deleted = summary.fills_deleted,
                                shadow_closed_trades_deleted = summary.shadow_closed_trades_deleted,
                                "history retention sweep applied"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, "history retention sweep failed");
                        }
                    }
                    last_history_retention_sweep = StdInstant::now();
                }
                let sqlite_contention = sqlite_contention_snapshot();
                let wal_size_bytes = std::fs::metadata(format!("{sqlite_path}-wal"))
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                info!(
                    sqlite_write_retry_total = sqlite_contention.write_retry_total,
                    sqlite_busy_error_total = sqlite_contention.busy_error_total,
                    sqlite_wal_size_bytes = wal_size_bytes,
                    "sqlite contention counters"
                );
            }
            _ = risk_refresh_interval.tick() => {
                let now = Utc::now();
                shadow_risk_guard.observe_ingestion_snapshot(
                    &store,
                    now,
                    ingestion.runtime_snapshot(),
                );
                if !shadow_risk_guard.config.shadow_killswitch_enabled {
                    continue;
                }
                if let Err(error) = shadow_risk_guard.maybe_refresh_db_state(&store, now) {
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
                        warn!(error = %error, "execution batch failed");
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "execution task join failed");
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
            maybe_swap = ingestion.next_swap(), if ingestion_backoff_until.is_none() => {
                let now = Utc::now();
                shadow_risk_guard.observe_ingestion_snapshot(
                    &store,
                    now,
                    ingestion.runtime_snapshot(),
                );
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

                match observed_swap_writer.write(&swap).await {
                    Ok(true) => {
                        debug!(
                            signature = %swap.signature,
                            wallet = %swap.wallet,
                            dex = %swap.dex,
                            token_in = %swap.token_in,
                            token_out = %swap.token_out,
                            amount_in = swap.amount_in,
                            amount_out = swap.amount_out,
                            ingestion_lag_ms = (Utc::now() - swap.ts_utc).num_milliseconds().max(0),
                            "observed swap stored"
                        );

                        let Some(side) = classify_swap_side(&swap) else {
                            continue;
                        };
                        if matches!(side, ShadowSwapSide::Buy)
                            && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
                        {
                            let reason = "not_followed";
                            *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
                            *shadow_drop_stage_counts.entry("follow").or_insert(0) += 1;
                            debug!(
                                stage = "follow",
                                reason,
                                side = "buy",
                                wallet = %swap.wallet,
                                signature = %swap.signature,
                                "shadow gate dropped"
                            );
                            continue;
                        }

                        if matches!(side, ShadowSwapSide::Sell)
                            && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
                        {
                            let wallet_has_recent_follow_history = follow_snapshot.is_active(&swap.wallet)
                                || follow_snapshot.promoted_at.contains_key(&swap.wallet)
                                || follow_snapshot.demoted_at.contains_key(&swap.wallet);
                            let sell_key = shadow_task_key_for_swap(&swap, side);
                            let key_tuple = (sell_key.wallet.clone(), sell_key.token.clone());
                            let has_pending_or_inflight =
                                shadow_scheduler.key_has_pending_or_inflight(&sell_key);
                            if !wallet_has_recent_follow_history
                                && !has_pending_or_inflight
                                && !open_shadow_lots.contains(&key_tuple)
                            {
                                let reason = "not_followed";
                                *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
                                *shadow_drop_stage_counts.entry("follow").or_insert(0) += 1;
                                debug!(
                                    stage = "follow",
                                    reason,
                                    side = "sell",
                                    wallet = %swap.wallet,
                                    signature = %swap.signature,
                                    "shadow gate dropped"
                                );
                                continue;
                            }
                        }

                        if matches!(side, ShadowSwapSide::Buy) {
                            if operator_emergency_stop.is_active() {
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

                            match shadow_risk_guard.can_open_buy(
                                &store,
                                now,
                                pause_new_trades_on_outage,
                            ) {
                                BuyRiskDecision::Allow => {}
                                BuyRiskDecision::Blocked { reason, detail } => {
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
                            continue;
                        }
                        if shadow_scheduler.should_process_shadow_inline(
                            shadow_queue_full,
                            shadow_scheduler.shadow_scheduler_needs_reset,
                            shadow_scheduler.shadow_workers.len(),
                            &task_input.key,
                        )
                        {
                            if shadow_scheduler.inflight_shadow_keys.insert(task_input.key.clone()) {
                                spawn_shadow_worker_task(
                                    &mut shadow_scheduler.shadow_workers,
                                    &shadow,
                                    &sqlite_path,
                                    task_input,
                                );
                            } else {
                                if let Err(dropped_task) = shadow_scheduler.enqueue_shadow_task(
                                    SHADOW_PENDING_TASK_CAPACITY,
                                    task_input,
                                )
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
                            }
                        } else {
                            if let Err(dropped_task) = shadow_scheduler.enqueue_shadow_task(
                                SHADOW_PENDING_TASK_CAPACITY,
                                task_input,
                            ) {
                                shadow_scheduler.handle_shadow_enqueue_overflow(
                                    side,
                                    dropped_task,
                                    SHADOW_PENDING_TASK_CAPACITY,
                                    &mut shadow_drop_reason_counts,
                                    &mut shadow_drop_stage_counts,
                                    &mut shadow_queue_full_outcome_counts,
                                );
                            }
                        }
                    }
                    Ok(false) => {
                        debug!(signature = %swap.signature, "duplicate swap ignored");
                    }
                    Err(error) => {
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
                            "failed writing observed swap"
                        );
                    }
                }
            }
            _ = discovery_interval.tick() => {
                if discovery_handle.is_some() {
                    warn!("discovery cycle still running, skipping scheduled trigger");
                    continue;
                }
                discovery_handle = Some(tokio::task::spawn_blocking(spawn_discovery_task(
                    sqlite_path.clone(),
                    discovery.clone(),
                    Utc::now(),
                )));
            }
            _ = shadow_interval.tick() => {
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

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    eligible_wallets: usize,
    active_follow_wallets: usize,
    published: bool,
}

#[cfg(test)]
mod app_tests {
    use super::*;
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

    fn error_chain_contains(error: &anyhow::Error, needle: &str) -> bool {
        error
            .chain()
            .any(|cause| cause.to_string().contains(needle))
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
        }
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
            );
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
        );
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
            );
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
        );
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
            );
        }
        assert!(
            guard.infra_block_reason.is_none(),
            "yellowstone replaced_ratio should not activate infra gate even after sustained breaches"
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
            );
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
        );
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

        guard.observe_discovery_cycle(&store, now, 70, 14);
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(3), 70, 14);
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(6), 70, 14);
        assert!(guard.universe_blocked);

        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(9), 150, 30);
        assert!(!guard.universe_blocked);

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
        restarted_guard.restore_pause_from_store(&store, restore_at);

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

        original_guard.clear_pause(&store, now + chrono::Duration::seconds(30));
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let restore_at = now + chrono::Duration::seconds(45);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at);

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
        restarted_guard.restore_pause_from_store(&store, restore_at);

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
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        for index in 0..80 {
            original_guard.activate_pause(
                &store,
                now + chrono::Duration::seconds(index as i64 + 1),
                chrono::Duration::minutes(5),
                "drawdown_1h",
                format!("synthetic timed pause spam #{index}"),
            );
        }
        assert!(
            store.risk_event_count_by_type("shadow_risk_pause")? > 64,
            "regression setup must exceed the old fixed restore scan limit"
        );

        let restore_at = now + chrono::Duration::seconds(30);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at);

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
        );

        let restore_at = now + chrono::Duration::seconds(30);
        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, restore_at);

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
        );
        original_guard.clear_pause(&store, now + chrono::Duration::seconds(10));

        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, now + chrono::Duration::seconds(20));

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
    fn transient_observed_swap_write_errors_do_not_require_restart() {
        let error = anyhow!("database is locked");
        assert!(!observed_swap_writer_error_requires_restart(&error));
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
