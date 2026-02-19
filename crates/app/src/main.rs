use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_env_or_default, ExecutionConfig, RiskConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery::DiscoveryService;
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSnapshot,
};
use copybot_storage::{
    is_retryable_sqlite_anyhow_error, note_sqlite_busy_error, note_sqlite_write_retry,
    sqlite_contention_snapshot, SqliteStore,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant as StdInstant;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";
const SHADOW_WORKER_POOL_SIZE: usize = 4;
const SHADOW_INLINE_WORKER_OVERFLOW: usize = 1;
const SHADOW_MAX_CONCURRENT_WORKERS: usize =
    SHADOW_WORKER_POOL_SIZE + SHADOW_INLINE_WORKER_OVERFLOW;
const SHADOW_PENDING_TASK_CAPACITY: usize = 256;
const OBSERVED_SWAP_WRITE_MAX_RETRIES: usize = 3;
const OBSERVED_SWAP_RETRY_BACKOFF_MS: [u64; OBSERVED_SWAP_WRITE_MAX_RETRIES] = [50, 125, 250];
const RISK_DB_REFRESH_MIN_SECONDS: i64 = 5;
const RISK_INFRA_SAMPLE_MIN_SECONDS: i64 = 10;
const RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS: i64 = 60;
const RISK_INFRA_EVENT_THROTTLE_SECONDS: i64 = 300;
const STALE_LOT_CLEANUP_BATCH_LIMIT: u32 = 300;
const HARD_STOP_CLEAR_HEALTHY_REFRESHES: u64 = 6;
const DEFAULT_INGESTION_OVERRIDE_PATH: &str = "state/ingestion_source_override.env";
const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
const DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS: u64 = 500;

#[tokio::main]
async fn main() -> Result<()> {
    let cli_config = parse_config_arg();
    let default_path = cli_config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (mut config, loaded_config_path) = load_from_env_or_default(&default_path)?;
    let mut applied_source_override: Option<String> = None;
    if env::var("SOLANA_COPY_BOT_INGESTION_SOURCE").is_err() {
        if let Some(source_override) = load_ingestion_source_override() {
            config.ingestion.source = source_override.clone();
            applied_source_override = Some(source_override);
        }
    }
    resolve_execution_adapter_secrets(&mut config.execution)?;

    init_tracing(&config.system.log_level, config.system.log_json);
    info!(
        config_path = %loaded_config_path.display(),
        env = %config.system.env,
        "configuration loaded"
    );
    if let Some(source_override) = applied_source_override.as_deref() {
        info!(
            source = %source_override,
            "applying ingestion source override from failover file"
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

    store
        .record_heartbeat("copybot-app", "startup")
        .context("failed to write startup heartbeat")?;

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
    let discovery = DiscoveryService::new_with_helius(
        config.discovery.clone(),
        config.shadow.clone(),
        discovery_http_url,
    );
    let shadow = ShadowService::new_with_helius(config.shadow.clone(), shadow_http_url);
    let execution_runtime =
        ExecutionRuntime::from_config(config.execution.clone(), config.risk.clone());

    run_app_loop(
        store,
        ingestion,
        discovery,
        shadow,
        execution_runtime,
        config.risk.clone(),
        config.sqlite.path.clone(),
        config.system.heartbeat_seconds,
        config.discovery.refresh_seconds,
        config.shadow.refresh_seconds,
        config.shadow.max_signal_lag_seconds,
        config.shadow.causal_holdback_enabled,
        config.shadow.causal_holdback_ms,
        config.system.pause_new_trades_on_outage,
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

fn load_ingestion_source_override() -> Option<String> {
    let override_path = env::var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE")
        .unwrap_or_else(|_| DEFAULT_INGESTION_OVERRIDE_PATH.to_string());
    let content = fs::read_to_string(&override_path).ok()?;
    parse_ingestion_source_override(&content)
}

fn resolve_execution_adapter_secrets(config: &mut ExecutionConfig) -> Result<()> {
    if !config.enabled || config.mode.trim().to_ascii_lowercase() != "adapter_submit_confirm" {
        return Ok(());
    }

    let auth_token_file = config.submit_adapter_auth_token_file.trim();
    if !auth_token_file.is_empty() {
        if !config.submit_adapter_auth_token.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_adapter_auth_token and execution.submit_adapter_auth_token_file cannot be set at the same time"
            ));
        }
        config.submit_adapter_auth_token =
            read_trimmed_secret_file(auth_token_file).with_context(|| {
                format!(
                    "failed loading execution.submit_adapter_auth_token_file from {}",
                    auth_token_file
                )
            })?;
    }

    let hmac_secret_file = config.submit_adapter_hmac_secret_file.trim();
    if !hmac_secret_file.is_empty() {
        if !config.submit_adapter_hmac_secret.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_adapter_hmac_secret and execution.submit_adapter_hmac_secret_file cannot be set at the same time"
            ));
        }
        config.submit_adapter_hmac_secret = read_trimmed_secret_file(hmac_secret_file)
            .with_context(|| {
                format!(
                    "failed loading execution.submit_adapter_hmac_secret_file from {}",
                    hmac_secret_file
                )
            })?;
    }

    Ok(())
}

fn read_trimmed_secret_file(path: &str) -> Result<String> {
    let value =
        fs::read_to_string(path).with_context(|| format!("failed reading secret file {}", path))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("secret file {} is empty", path));
    }
    Ok(trimmed.to_string())
}

fn parse_ingestion_source_override(content: &str) -> Option<String> {
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
            return Some(source);
        }
    }
    None
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
    if !role_specific.is_empty() && !role_specific.contains("REPLACE_ME") {
        return Some(role_specific.to_string());
    }

    let fallback = fallback.trim();
    if !fallback.is_empty() && !fallback.contains("REPLACE_ME") {
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

fn validate_execution_runtime_contract(config: &ExecutionConfig, env: &str) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    let mode = config.mode.trim().to_ascii_lowercase();
    if !matches!(
        mode.as_str(),
        "paper" | "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) {
        return Err(anyhow!(
            "execution.enabled=true but execution.mode={} is unsupported in current runtime; supported modes: paper, paper_rpc_confirm, paper_rpc_pretrade_confirm, adapter_submit_confirm",
            if mode.is_empty() { "<empty>" } else { mode.as_str() }
        ));
    }
    if matches!(
        mode.as_str(),
        "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) {
        let primary = config.rpc_http_url.trim();
        let fallback = config.rpc_fallback_http_url.trim();
        if primary.is_empty() && fallback.is_empty() {
            return Err(anyhow!(
                "execution.mode={} requires execution.rpc_http_url or execution.rpc_fallback_http_url",
                mode
            ));
        }
    }
    if matches!(
        mode.as_str(),
        "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) && config.execution_signer_pubkey.trim().is_empty()
    {
        return Err(anyhow!(
            "execution.mode={} requires non-empty execution.execution_signer_pubkey",
            mode
        ));
    }
    if mode == "adapter_submit_confirm" {
        let submit_primary = config.submit_adapter_http_url.trim();
        let submit_fallback = config.submit_adapter_fallback_http_url.trim();
        if submit_primary.is_empty() && submit_fallback.is_empty() {
            return Err(anyhow!(
                "execution.mode=adapter_submit_confirm requires execution.submit_adapter_http_url or execution.submit_adapter_fallback_http_url"
            ));
        }
        let hmac_key_id = config.submit_adapter_hmac_key_id.trim();
        let hmac_secret = config.submit_adapter_hmac_secret.trim();
        if hmac_key_id.is_empty() ^ hmac_secret.is_empty() {
            return Err(anyhow!(
                "execution.mode=adapter_submit_confirm requires execution.submit_adapter_hmac_key_id and execution.submit_adapter_hmac_secret to be both set or both empty"
            ));
        }
        if !hmac_key_id.is_empty() && !(5..=300).contains(&config.submit_adapter_hmac_ttl_sec) {
            return Err(anyhow!(
                "execution.submit_adapter_hmac_ttl_sec must be in 5..=300 when adapter HMAC auth is enabled"
            ));
        }
    }

    if config.batch_size == 0 {
        return Err(anyhow!(
            "execution.batch_size must be >= 1 when execution is enabled"
        ));
    }
    if config.poll_interval_ms < 100 {
        return Err(anyhow!(
            "execution.poll_interval_ms must be >= 100ms when execution is enabled"
        ));
    }
    if config.max_confirm_seconds == 0 {
        return Err(anyhow!(
            "execution.max_confirm_seconds must be >= 1 when execution is enabled"
        ));
    }
    if config.max_submit_attempts == 0 {
        return Err(anyhow!(
            "execution.max_submit_attempts must be >= 1 when execution is enabled"
        ));
    }
    if config.submit_timeout_ms < 100 {
        return Err(anyhow!(
            "execution.submit_timeout_ms must be >= 100ms when execution is enabled"
        ));
    }
    if !config.pretrade_min_sol_reserve.is_finite() || config.pretrade_min_sol_reserve < 0.0 {
        return Err(anyhow!(
            "execution.pretrade_min_sol_reserve must be finite and >= 0 when execution is enabled"
        ));
    }
    if config.slippage_bps < 0.0 {
        return Err(anyhow!(
            "execution.slippage_bps must be >= 0 when execution is enabled"
        ));
    }
    if config.submit_allowed_routes.is_empty() {
        return Err(anyhow!(
            "execution.submit_allowed_routes must not be empty when execution is enabled"
        ));
    }
    let default_route = config.default_route.trim().to_ascii_lowercase();
    let default_route = if default_route.is_empty() {
        "paper".to_string()
    } else {
        default_route
    };
    let route_allowed = config
        .submit_allowed_routes
        .iter()
        .any(|route| route.trim().to_ascii_lowercase().eq(default_route.as_str()));
    if !route_allowed {
        return Err(anyhow!(
            "execution.default_route={} must be present in execution.submit_allowed_routes",
            default_route
        ));
    }
    if !config.submit_route_order.is_empty() {
        for route in &config.submit_route_order {
            let normalized = route.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                return Err(anyhow!(
                    "execution.submit_route_order contains an empty route value"
                ));
            }
            let allowed = config.submit_allowed_routes.iter().any(|candidate| {
                candidate
                    .trim()
                    .to_ascii_lowercase()
                    .eq(normalized.as_str())
            });
            if !allowed {
                return Err(anyhow!(
                    "execution.submit_route_order route={} must be present in execution.submit_allowed_routes",
                    normalized
                ));
            }
        }
        let has_default = config
            .submit_route_order
            .iter()
            .any(|route| route.trim().to_ascii_lowercase().eq(default_route.as_str()));
        if !has_default {
            return Err(anyhow!(
                "execution.submit_route_order must include execution.default_route={}",
                default_route
            ));
        }
    }
    if config.submit_route_max_slippage_bps.is_empty() {
        return Err(anyhow!(
            "execution.submit_route_max_slippage_bps must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS=route:cap,route2:cap2)"
        ));
    }
    for (route, cap) in &config.submit_route_max_slippage_bps {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_max_slippage_bps contains empty route key"
            ));
        }
        if !cap.is_finite() || *cap <= 0.0 {
            return Err(anyhow!(
                "execution.submit_route_max_slippage_bps route={} must be finite and > 0, got {}",
                route,
                cap
            ));
        }
    }
    if config.submit_route_compute_unit_limit.is_empty() {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_limit must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT=route:limit,route2:limit2)"
        ));
    }
    for (route, limit) in &config.submit_route_compute_unit_limit {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit contains empty route key"
            ));
        }
        if *limit == 0 || *limit > 1_400_000 {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit route={} must be in 1..=1400000, got {}",
                route,
                limit
            ));
        }
    }
    if config
        .submit_route_compute_unit_price_micro_lamports
        .is_empty()
    {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_price_micro_lamports must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS=route:price,route2:price2)"
        ));
    }
    for (route, price) in &config.submit_route_compute_unit_price_micro_lamports {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports contains empty route key"
            ));
        }
        if *price == 0 || *price > 10_000_000 {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports route={} must be in 1..=10000000, got {}",
                route,
                price
            ));
        }
    }
    if mode == "adapter_submit_confirm" {
        let find_route_cap = |route: &str| -> Option<f64> {
            config
                .submit_route_max_slippage_bps
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, cap)| *cap)
        };
        let find_route_cu_limit = |route: &str| -> Option<u32> {
            config
                .submit_route_compute_unit_limit
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, limit)| *limit)
        };
        let find_route_cu_price = |route: &str| -> Option<u64> {
            config
                .submit_route_compute_unit_price_micro_lamports
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, price)| *price)
        };
        for allowed_route in &config.submit_allowed_routes {
            let route = allowed_route.trim();
            if route.is_empty() {
                continue;
            }
            if find_route_cap(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_max_slippage_bps is missing cap for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS format route:cap)",
                    route
                ));
            }
            if find_route_cu_limit(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_compute_unit_limit is missing limit for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT format route:limit)",
                    route
                ));
            }
            if find_route_cu_price(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_compute_unit_price_micro_lamports is missing price for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS format route:price)",
                    route
                ));
            }
        }
        let default_route_cap = find_route_cap(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_max_slippage_bps is missing cap for default route={}",
                default_route
            )
        })?;
        let default_route_limit = find_route_cu_limit(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_compute_unit_limit is missing limit for default route={}",
                default_route
            )
        })?;
        let default_route_price = find_route_cu_price(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports is missing price for default route={}",
                default_route
            )
        })?;
        if config.slippage_bps > default_route_cap {
            return Err(anyhow!(
                "execution.slippage_bps ({}) cannot exceed cap ({}) for default route {}",
                config.slippage_bps,
                default_route_cap,
                default_route
            ));
        }
        if config.pretrade_max_priority_fee_lamports > 0
            && default_route_price > config.pretrade_max_priority_fee_lamports
        {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports default route {} price ({}) cannot exceed execution.pretrade_max_priority_fee_lamports ({}) (unit: micro-lamports per CU for both fields)",
                default_route,
                default_route_price,
                config.pretrade_max_priority_fee_lamports
            ));
        }
        if default_route_limit < 100_000 {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit default route {} limit ({}) is too low for reliable swaps; expected >= 100000",
                default_route,
                default_route_limit
            ));
        }
    }

    if matches!(
        env.trim().to_ascii_lowercase().as_str(),
        "paper" | "prod" | "paper-canary" | "paper-canary-yellowstone"
    ) {
        info!(
            mode,
            batch_size = config.batch_size,
            poll_interval_ms = config.poll_interval_ms,
            submit_timeout_ms = config.submit_timeout_ms,
            submit_allowed_routes = ?config.submit_allowed_routes,
            submit_route_max_slippage_bps = ?config.submit_route_max_slippage_bps,
            submit_route_compute_unit_limit = ?config.submit_route_compute_unit_limit,
            submit_route_compute_unit_price_micro_lamports = ?config.submit_route_compute_unit_price_micro_lamports,
            pretrade_require_token_account = config.pretrade_require_token_account,
            pretrade_max_priority_fee_micro_lamports_per_cu = config.pretrade_max_priority_fee_lamports,
            "execution runtime contract validated"
        );
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

fn init_tracing(log_level: &str, json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
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

#[derive(Debug, Default)]
struct ShadowRiskGuard {
    config: RiskConfig,
    hard_stop_reason: Option<String>,
    hard_stop_clear_healthy_streak: u64,
    last_db_refresh_error: Option<String>,
    exposure_hard_blocked: bool,
    exposure_hard_detail: Option<String>,
    pause_until: Option<DateTime<Utc>>,
    pause_reason: Option<String>,
    universe_breach_streak: u64,
    universe_blocked: bool,
    infra_samples: VecDeque<IngestionRuntimeSnapshot>,
    lag_breach_since: Option<DateTime<Utc>>,
    infra_block_reason: Option<String>,
    infra_last_event_at: Option<DateTime<Utc>>,
    last_db_refresh_at: Option<DateTime<Utc>>,
    last_fail_closed_log_at: Option<DateTime<Utc>>,
}

impl ShadowRiskGuard {
    fn new(config: RiskConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
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

        let new_reason = self.compute_infra_block_reason(sample_ts);
        if new_reason != self.infra_block_reason {
            self.infra_block_reason = new_reason.clone();
            if let Some(reason) = new_reason {
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
            } else {
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
            self.pause_until = None;
            self.pause_reason = None;
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
            let (_, pnl_1h) = store.shadow_realized_pnl_since(now - chrono::Duration::hours(1))?;
            let (_, pnl_6h) = store.shadow_realized_pnl_since(now - chrono::Duration::hours(6))?;
            let (_, pnl_24h) =
                store.shadow_realized_pnl_since(now - chrono::Duration::hours(24))?;

            let rug_window_start = now
                - chrono::Duration::minutes(
                    self.config.shadow_rug_loss_window_minutes.max(1) as i64
                );
            let rug_count_since = store.shadow_rug_loss_count_since(
                rug_window_start,
                self.config.shadow_rug_loss_return_threshold,
            )?;
            let (_, rug_sample_total, rug_rate_recent) = store.shadow_rug_loss_rate_recent(
                self.config.shadow_rug_loss_rate_sample_size.max(1),
                self.config.shadow_rug_loss_return_threshold,
            )?;

            let hard_stop_breach = if pnl_24h <= self.config.shadow_drawdown_24h_stop_sol {
                Some((
                    "drawdown_24h",
                    format!(
                        "pnl_24h={:.6} <= stop={:.6}",
                        pnl_24h, self.config.shadow_drawdown_24h_stop_sol
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

            let exposure_sol = store.shadow_open_notional_sol()?;
            let exposure_blocked_now = exposure_sol >= self.config.shadow_hard_exposure_cap_sol;
            let exposure_detail = format!(
                "open_notional_sol={:.6} hard_cap={:.6}",
                exposure_sol, self.config.shadow_hard_exposure_cap_sol
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

            if exposure_sol >= self.config.shadow_hard_exposure_cap_sol {
                return Ok(());
            }
            if exposure_sol >= self.config.shadow_soft_exposure_cap_sol {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(self.config.shadow_soft_pause_minutes.max(1) as i64),
                    "exposure_soft_cap",
                    format!(
                        "open_notional_sol={:.6} >= soft_cap={:.6}",
                        exposure_sol, self.config.shadow_soft_exposure_cap_sol
                    ),
                );
            }
            if pnl_6h <= self.config.shadow_drawdown_6h_stop_sol {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_6h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_6h",
                    format!(
                        "pnl_6h={:.6} <= stop={:.6}",
                        pnl_6h, self.config.shadow_drawdown_6h_stop_sol
                    ),
                );
            }
            if pnl_1h <= self.config.shadow_drawdown_1h_stop_sol {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_1h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_1h",
                    format!(
                        "pnl_1h={:.6} <= stop={:.6}",
                        pnl_1h, self.config.shadow_drawdown_1h_stop_sol
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
        if self.infra_samples.is_empty() {
            return None;
        }

        if let Some(started_at) = self.lag_breach_since {
            if now - started_at
                >= chrono::Duration::minutes(
                    self.config.shadow_infra_lag_breach_minutes.max(1) as i64
                )
            {
                return Some(format!(
                    "lag_p95_over_threshold_for={}m threshold_ms={}",
                    self.config.shadow_infra_lag_breach_minutes,
                    self.config.shadow_infra_lag_p95_threshold_ms
                ));
            }
        }

        let window_start =
            now - chrono::Duration::minutes(self.config.shadow_infra_window_minutes.max(1) as i64);
        let latest = self.infra_samples.back().copied()?;
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
        let delta_rpc_429 = latest.rpc_429.saturating_sub(baseline.rpc_429);
        let delta_rpc_5xx = latest.rpc_5xx.saturating_sub(baseline.rpc_5xx);

        if delta_enqueued > 0 {
            let replaced_ratio = delta_replaced as f64 / delta_enqueued as f64;
            if replaced_ratio > self.config.shadow_infra_replaced_ratio_threshold {
                return Some(format!(
                    "replaced_ratio={:.4} delta_replaced={} delta_enqueued={}",
                    replaced_ratio, delta_replaced, delta_enqueued
                ));
            }
        }
        if delta_rpc_429 >= self.config.shadow_infra_rpc429_delta_threshold {
            return Some(format!(
                "rpc_429_delta={} threshold={}",
                delta_rpc_429, self.config.shadow_infra_rpc429_delta_threshold
            ));
        }
        if delta_rpc_5xx >= self.config.shadow_infra_rpc5xx_delta_threshold {
            return Some(format!(
                "rpc_5xx_delta={} threshold={}",
                delta_rpc_5xx, self.config.shadow_infra_rpc5xx_delta_threshold
            ));
        }
        None
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
    value.replace('\\', "\\\\").replace('"', "\\\"")
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
    discovery_refresh_seconds: u64,
    shadow_refresh_seconds: u64,
    shadow_max_signal_lag_seconds: u64,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
    pause_new_trades_on_outage: bool,
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
        time::interval(Duration::from_secs(discovery_refresh_seconds.max(10)));
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
    let follow_event_retention = Duration::from_secs(shadow_max_signal_lag_seconds.max(1));
    let mut open_shadow_lots = store
        .list_shadow_open_pairs()
        .context("failed to load open shadow lot index")?;
    let stale_lot_max_hold_hours = risk_config.max_hold_hours;
    let mut shadow_risk_guard = ShadowRiskGuard::new(risk_config);
    let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut discovery_handle: Option<JoinHandle<Result<DiscoveryTaskOutput>>> = None;
    let mut shadow_workers: JoinSet<ShadowTaskOutput> = JoinSet::new();
    let mut shadow_snapshot_handle: Option<JoinHandle<Result<ShadowSnapshot>>> = None;
    let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
        HashMap::new();
    let mut pending_shadow_task_count: usize = 0;
    let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
    let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
    let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
    let mut shadow_queue_backpressure_active = false;
    let mut shadow_scheduler_needs_reset = false;
    let mut held_shadow_sells: HashMap<ShadowTaskKey, VecDeque<HeldShadowSell>> = HashMap::new();
    let mut shadow_holdback_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut execution_handle: Option<JoinHandle<Result<ExecutionBatchReport>>> = None;
    let mut operator_emergency_stop = OperatorEmergencyStop::from_env();
    let mut execution_emergency_stop_active_logged = false;
    let mut execution_hard_stop_pause_logged = false;
    let mut execution_outage_pause_logged = false;
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

        if shadow_scheduler_needs_reset {
            if shadow_workers.is_empty() {
                inflight_shadow_keys.clear();
                rebuild_shadow_ready_queue(
                    &pending_shadow_tasks,
                    &mut ready_shadow_keys,
                    &mut ready_shadow_key_set,
                    &inflight_shadow_keys,
                );
                shadow_scheduler_needs_reset = false;
                warn!("shadow scheduler recovered after worker join error");
            }
        } else {
            spawn_shadow_tasks_up_to_limit(
                &mut shadow_workers,
                &mut pending_shadow_tasks,
                &mut pending_shadow_task_count,
                &mut ready_shadow_keys,
                &mut ready_shadow_key_set,
                &mut inflight_shadow_keys,
                &sqlite_path,
                &shadow,
                SHADOW_WORKER_POOL_SIZE,
            );
        }
        release_held_shadow_sells(
            &mut held_shadow_sells,
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            &open_shadow_lots,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
            &mut shadow_holdback_counts,
            SHADOW_PENDING_TASK_CAPACITY,
            Utc::now(),
        );

        let shadow_queue_full = pending_shadow_task_count >= SHADOW_PENDING_TASK_CAPACITY;
        if shadow_queue_full && !shadow_queue_backpressure_active {
            shadow_queue_backpressure_active = true;
            warn!(
                pending_shadow_task_count,
                shadow_pending_capacity = SHADOW_PENDING_TASK_CAPACITY,
                "shadow queue backpressure active; switching to inline shadow processing"
            );
            let details_json = format!(
                "{{\"reason\":\"queue_backpressure\",\"pending\":{},\"capacity\":{}}}",
                pending_shadow_task_count, SHADOW_PENDING_TASK_CAPACITY
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
        } else if !shadow_queue_full && shadow_queue_backpressure_active {
            shadow_queue_backpressure_active = false;
            info!(
                pending_shadow_task_count,
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
                    Ok(Err(error)) => {
                        warn!(error = %error, "discovery cycle failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "discovery task join failed");
                    }
                }
            }
            shadow_result = shadow_workers.join_next(), if !shadow_workers.is_empty() => {
                match shadow_result {
                    Some(Ok(task_output)) => {
                        mark_shadow_task_complete(
                            &task_output.key,
                            &pending_shadow_tasks,
                            &mut ready_shadow_keys,
                            &mut ready_shadow_key_set,
                            &mut inflight_shadow_keys,
                        );
                        handle_shadow_task_output(
                            task_output,
                            &mut open_shadow_lots,
                            &mut shadow_drop_reason_counts,
                            &mut shadow_drop_stage_counts,
                        );
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "shadow task join failed");
                        shadow_scheduler_needs_reset = true;
                    }
                    None => {}
                }
            }
            snapshot_result = async {
                if let Some(handle) = &mut shadow_snapshot_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if shadow_snapshot_handle.is_some() => {
                shadow_snapshot_handle = None;
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
                        if !shadow_holdback_counts.is_empty() {
                            info!(
                                holdback_outcomes = ?shadow_holdback_counts,
                                "shadow causal holdback outcomes"
                            );
                            shadow_holdback_counts.clear();
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
                let sqlite_contention = sqlite_contention_snapshot();
                info!(
                    sqlite_write_retry_total = sqlite_contention.write_retry_total,
                    sqlite_busy_error_total = sqlite_contention.busy_error_total,
                    "sqlite contention counters"
                );
            }
            _ = risk_refresh_interval.tick() => {
                if !shadow_risk_guard.config.shadow_killswitch_enabled {
                    continue;
                }
                let now = Utc::now();
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
                        if report.attempted > 0 || report.failed > 0 {
                            info!(
                                attempted = report.attempted,
                                confirmed = report.confirmed,
                                dropped = report.dropped,
                                failed = report.failed,
                                skipped = report.skipped,
                                "execution batch processed"
                            );
                        }
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
                let mut buy_submit_pause_reason: Option<String> = None;
                if operator_emergency_stop.is_active() {
                    if !execution_emergency_stop_active_logged {
                        warn!(
                            detail = %operator_emergency_stop.detail(),
                            "execution BUY submission paused by operator emergency stop"
                        );
                        execution_emergency_stop_active_logged = true;
                    }
                    buy_submit_pause_reason = Some(format!(
                        "operator_emergency_stop: {}",
                        operator_emergency_stop.detail()
                    ));
                } else if execution_emergency_stop_active_logged {
                    info!("execution BUY submission resumed after operator emergency stop clear");
                    execution_emergency_stop_active_logged = false;
                }

                if let Some(reason) = shadow_risk_guard.hard_stop_reason.as_deref() {
                    if !execution_hard_stop_pause_logged {
                        warn!(
                            reason = %reason,
                            "execution BUY submission paused by risk hard stop"
                        );
                        execution_hard_stop_pause_logged = true;
                    }
                    if buy_submit_pause_reason.is_none() {
                        buy_submit_pause_reason = Some(format!("risk_hard_stop: {}", reason));
                    }
                } else if execution_hard_stop_pause_logged {
                    info!("execution BUY submission resumed after risk hard stop clear");
                    execution_hard_stop_pause_logged = false;
                }

                if pause_new_trades_on_outage {
                    if let Some(reason) = shadow_risk_guard.infra_block_reason.as_deref() {
                        if !execution_outage_pause_logged {
                            warn!(
                                reason = %reason,
                                "execution BUY submission paused due to infra outage gate"
                            );
                            execution_outage_pause_logged = true;
                        }
                        if buy_submit_pause_reason.is_none() {
                            buy_submit_pause_reason = Some(format!("risk_infra_stop: {}", reason));
                        }
                    } else if execution_outage_pause_logged {
                        info!("execution BUY submission resumed after infra outage gate cleared");
                        execution_outage_pause_logged = false;
                    }
                } else if execution_outage_pause_logged {
                    info!("execution BUY submission resumed after infra outage gate disabled");
                    execution_outage_pause_logged = false;
                }

                if execution_handle.is_some() {
                    warn!("execution batch still running, skipping scheduled trigger");
                    continue;
                }

                let now = Utc::now();
                execution_handle = Some(tokio::task::spawn_blocking(spawn_execution_task(
                    sqlite_path.clone(),
                    Arc::clone(&execution_runtime),
                    now,
                    buy_submit_pause_reason.clone(),
                )));
            }
            maybe_swap = ingestion.next_swap() => {
                let swap = match maybe_swap {
                    Ok(Some(swap)) => swap,
                    Ok(None) => {
                        debug!("ingestion emitted no swap");
                        continue;
                    }
                    Err(error) => {
                        warn!(error = %error, "ingestion error");
                        continue;
                    }
                };
                let now = Utc::now();
                shadow_risk_guard.observe_ingestion_snapshot(
                    &store,
                    now,
                    ingestion.runtime_snapshot(),
                );

                match insert_observed_swap_with_retry(&store, &swap).await {
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
                            let has_pending_or_inflight = key_has_pending_or_inflight(
                                &sell_key,
                                &pending_shadow_tasks,
                                &inflight_shadow_keys,
                            );
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
                        if should_hold_sell_for_causality(
                            shadow_causal_holdback_enabled,
                            shadow_causal_holdback_ms,
                            side,
                            &task_input.key,
                            &pending_shadow_tasks,
                            &inflight_shadow_keys,
                            &open_shadow_lots,
                        ) {
                            hold_sell_for_causality(
                                &mut held_shadow_sells,
                                task_input,
                                shadow_causal_holdback_ms,
                                Utc::now(),
                                &mut shadow_holdback_counts,
                            );
                            continue;
                        }
                        if should_process_shadow_inline(
                            shadow_queue_full,
                            shadow_scheduler_needs_reset,
                            shadow_workers.len(),
                            &task_input.key,
                            &pending_shadow_tasks,
                            &inflight_shadow_keys,
                        ) {
                            if inflight_shadow_keys.insert(task_input.key.clone()) {
                                spawn_shadow_worker_task(
                                    &mut shadow_workers,
                                    &shadow,
                                    &sqlite_path,
                                    task_input,
                                );
                            } else {
                                if let Err(dropped_task) = enqueue_shadow_task(
                                    &mut pending_shadow_tasks,
                                    &mut pending_shadow_task_count,
                                    &mut ready_shadow_keys,
                                    &mut ready_shadow_key_set,
                                    &inflight_shadow_keys,
                                    SHADOW_PENDING_TASK_CAPACITY,
                                    task_input,
                                ) {
                                    handle_shadow_enqueue_overflow(
                                        side,
                                        dropped_task,
                                        &mut pending_shadow_tasks,
                                        &mut pending_shadow_task_count,
                                        &mut ready_shadow_keys,
                                        &mut ready_shadow_key_set,
                                        &inflight_shadow_keys,
                                        SHADOW_PENDING_TASK_CAPACITY,
                                        &mut shadow_drop_reason_counts,
                                        &mut shadow_drop_stage_counts,
                                        &mut shadow_queue_full_outcome_counts,
                                    );
                                }
                            }
                        } else {
                            if let Err(dropped_task) = enqueue_shadow_task(
                                &mut pending_shadow_tasks,
                                &mut pending_shadow_task_count,
                                &mut ready_shadow_keys,
                                &mut ready_shadow_key_set,
                                &inflight_shadow_keys,
                                SHADOW_PENDING_TASK_CAPACITY,
                                task_input,
                            ) {
                                handle_shadow_enqueue_overflow(
                                    side,
                                    dropped_task,
                                    &mut pending_shadow_tasks,
                                    &mut pending_shadow_task_count,
                                    &mut ready_shadow_keys,
                                    &mut ready_shadow_key_set,
                                    &inflight_shadow_keys,
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
                    cleanup_now,
                ) {
                    Ok((closed, skipped)) if closed > 0 || skipped > 0 => {
                        info!(
                            closed,
                            skipped,
                            max_hold_hours = stale_lot_max_hold_hours,
                            "stale lot cleanup tick"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(error = %error, "stale lot cleanup failed");
                    }
                }
                if shadow_snapshot_handle.is_some() {
                    warn!("shadow snapshot still running, skipping scheduled trigger");
                    continue;
                }
                shadow_snapshot_handle = Some(tokio::task::spawn_blocking(spawn_shadow_snapshot_task(
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

    store
        .record_heartbeat("copybot-app", "shutdown")
        .context("failed to write shutdown heartbeat")?;
    Ok(())
}

fn close_stale_shadow_lots(
    store: &SqliteStore,
    open_shadow_lots: &mut HashSet<(String, String)>,
    max_hold_hours: u32,
    now: DateTime<Utc>,
) -> Result<(u64, u64)> {
    const EPS: f64 = 1e-12;

    if max_hold_hours == 0 {
        return Ok((0, 0));
    }

    let cutoff = now - chrono::Duration::hours(max_hold_hours as i64);
    let stale_lots = store
        .list_open_shadow_lots_older_than(cutoff, STALE_LOT_CLEANUP_BATCH_LIMIT)
        .context("failed to list stale shadow lots")?;
    if stale_lots.is_empty() {
        return Ok((0, 0));
    }

    let mut closed = 0u64;
    let mut skipped_unpriced = 0u64;

    for lot in stale_lots {
        if lot.qty <= EPS {
            continue;
        }

        let Some(exit_price_sol) = store.latest_token_sol_price(&lot.token, now)? else {
            skipped_unpriced = skipped_unpriced.saturating_add(1);
            continue;
        };
        if exit_price_sol <= EPS {
            skipped_unpriced = skipped_unpriced.saturating_add(1);
            continue;
        }

        let signal_id = format!("stale-close-{}-{}", lot.id, now.timestamp_millis());
        let close = store
            .close_shadow_lots_fifo_atomic(
                &signal_id,
                &lot.wallet_id,
                &lot.token,
                lot.qty,
                exit_price_sol,
                now,
            )
            .with_context(|| {
                format!(
                    "failed stale close for wallet={} token={} lot_id={}",
                    lot.wallet_id, lot.token, lot.id
                )
            })?;

        if close.closed_qty > EPS {
            closed = closed.saturating_add(1);
        }

        let key = (lot.wallet_id.clone(), lot.token.clone());
        if close.has_open_lots_after {
            open_shadow_lots.insert(key);
        } else {
            open_shadow_lots.remove(&key);
        }
    }

    Ok((closed, skipped_unpriced))
}

fn reason_to_key(reason: ShadowDropReason) -> &'static str {
    reason.as_str()
}

fn reason_to_stage(reason: ShadowDropReason) -> &'static str {
    match reason {
        ShadowDropReason::Disabled => "disabled",
        ShadowDropReason::NotFollowed => "follow",
        ShadowDropReason::NotSolLeg => "pair",
        ShadowDropReason::BelowNotional => "notional",
        ShadowDropReason::LagExceeded => "lag",
        ShadowDropReason::TooNew
        | ShadowDropReason::LowHolders
        | ShadowDropReason::LowLiquidity
        | ShadowDropReason::LowVolume
        | ShadowDropReason::ThinMarket => "quality",
        ShadowDropReason::InvalidSizing => "sizing",
        ShadowDropReason::DuplicateSignal => "dedupe",
        ShadowDropReason::UnsupportedSide => "side",
    }
}

async fn insert_observed_swap_with_retry(store: &SqliteStore, swap: &SwapEvent) -> Result<bool> {
    for attempt in 0..=OBSERVED_SWAP_WRITE_MAX_RETRIES {
        match store.insert_observed_swap(swap) {
            Ok(written) => return Ok(written),
            Err(error) => {
                let retryable = is_retryable_sqlite_anyhow_error(&error);
                if retryable {
                    note_sqlite_busy_error();
                }
                if attempt < OBSERVED_SWAP_WRITE_MAX_RETRIES && retryable {
                    note_sqlite_write_retry();
                    let backoff_ms = OBSERVED_SWAP_RETRY_BACKOFF_MS[attempt];
                    debug!(
                        signature = %swap.signature,
                        attempt = attempt + 1,
                        max_attempts = OBSERVED_SWAP_WRITE_MAX_RETRIES + 1,
                        backoff_ms,
                        error = %error,
                        "retrying observed swap write after sqlite contention"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
    unreachable!("retry loop must return on success or terminal error");
}

fn format_error_chain(error: &anyhow::Error) -> String {
    let mut chain = String::new();
    for (idx, cause) in error.chain().enumerate() {
        if idx > 0 {
            chain.push_str(" | ");
        }
        chain.push_str(&cause.to_string());
    }
    chain
}

fn spawn_discovery_task(
    sqlite_path: String,
    discovery: DiscoveryService,
    now: chrono::DateTime<Utc>,
) -> impl FnOnce() -> Result<DiscoveryTaskOutput> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for discovery task: {sqlite_path}")
        })?;
        let summary = discovery.run_cycle(&store, now)?;
        let active_wallets = store.list_active_follow_wallets()?;
        Ok(DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
            eligible_wallets: summary.eligible_wallets,
            active_follow_wallets: summary.active_follow_wallets,
        })
    }
}

fn spawn_shadow_snapshot_task(
    sqlite_path: String,
    shadow: ShadowService,
    now: chrono::DateTime<Utc>,
) -> impl FnOnce() -> Result<ShadowSnapshot> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow snapshot task: {sqlite_path}")
        })?;
        shadow.snapshot_24h(&store, now)
    }
}

fn spawn_execution_task(
    sqlite_path: String,
    execution_runtime: Arc<ExecutionRuntime>,
    now: chrono::DateTime<Utc>,
    buy_submit_pause_reason: Option<String>,
) -> impl FnOnce() -> Result<ExecutionBatchReport> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for execution task: {sqlite_path}")
        })?;
        execution_runtime.process_batch(&store, now, buy_submit_pause_reason.as_deref())
    }
}

struct ShadowTaskOutput {
    signature: String,
    key: ShadowTaskKey,
    outcome: Result<ShadowProcessOutcome>,
}

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    eligible_wallets: usize,
    active_follow_wallets: usize,
}

struct ShadowTaskInput {
    swap: SwapEvent,
    follow_snapshot: Arc<FollowSnapshot>,
    key: ShadowTaskKey,
}

struct HeldShadowSell {
    task_input: ShadowTaskInput,
    hold_until: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShadowTaskKey {
    wallet: String,
    token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShadowSwapSide {
    Buy,
    Sell,
}

fn classify_swap_side(swap: &SwapEvent) -> Option<ShadowSwapSide> {
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    if swap.token_in == SOL_MINT && swap.token_out != SOL_MINT {
        return Some(ShadowSwapSide::Buy);
    }
    if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
        return Some(ShadowSwapSide::Sell);
    }
    None
}

fn shadow_task_key_for_swap(swap: &SwapEvent, side: ShadowSwapSide) -> ShadowTaskKey {
    match side {
        ShadowSwapSide::Buy => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_out.clone(),
        },
        ShadowSwapSide::Sell => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_in.clone(),
        },
    }
}

fn key_has_pending_or_inflight(
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) -> bool {
    inflight_shadow_keys.contains(key)
        || pending_shadow_tasks
            .get(key)
            .is_some_and(|pending| !pending.is_empty())
}

fn should_hold_sell_for_causality(
    holdback_enabled: bool,
    holdback_ms: u64,
    side: ShadowSwapSide,
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    open_shadow_lots: &HashSet<(String, String)>,
) -> bool {
    if !holdback_enabled || holdback_ms == 0 || !matches!(side, ShadowSwapSide::Sell) {
        return false;
    }
    if key_has_pending_or_inflight(key, pending_shadow_tasks, inflight_shadow_keys) {
        return false;
    }
    let key_tuple = (key.wallet.clone(), key.token.clone());
    !open_shadow_lots.contains(&key_tuple)
}

fn hold_sell_for_causality(
    held_shadow_sells: &mut HashMap<ShadowTaskKey, VecDeque<HeldShadowSell>>,
    task_input: ShadowTaskInput,
    holdback_ms: u64,
    now: DateTime<Utc>,
    shadow_holdback_counts: &mut BTreeMap<&'static str, u64>,
) {
    let hold_until = now + chrono::Duration::milliseconds(holdback_ms.max(1) as i64);
    held_shadow_sells
        .entry(task_input.key.clone())
        .or_default()
        .push_back(HeldShadowSell {
            task_input,
            hold_until,
        });
    *shadow_holdback_counts.entry("queued").or_insert(0) += 1;
}

fn release_held_shadow_sells(
    held_shadow_sells: &mut HashMap<ShadowTaskKey, VecDeque<HeldShadowSell>>,
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
    shadow_holdback_counts: &mut BTreeMap<&'static str, u64>,
    capacity: usize,
    now: DateTime<Utc>,
) {
    let keys: Vec<ShadowTaskKey> = held_shadow_sells.keys().cloned().collect();
    for key in keys {
        loop {
            let release_reason = match held_shadow_sells.get(&key).and_then(|queue| queue.front()) {
                Some(front) => {
                    let key_tuple = (key.wallet.clone(), key.token.clone());
                    if open_shadow_lots.contains(&key_tuple) {
                        Some("released_open_lot")
                    } else if key_has_pending_or_inflight(
                        &key,
                        pending_shadow_tasks,
                        inflight_shadow_keys,
                    ) {
                        Some("released_key_busy")
                    } else if now >= front.hold_until {
                        Some("released_expired")
                    } else {
                        Some("hold")
                    }
                }
                None => None,
            };

            let Some(release_reason) = release_reason else {
                break;
            };
            if release_reason == "hold" {
                break;
            }

            let held_task = {
                let Some(queue) = held_shadow_sells.get_mut(&key) else {
                    break;
                };
                queue.pop_front()
            };
            let Some(held_task) = held_task else {
                break;
            };
            *shadow_holdback_counts.entry(release_reason).or_insert(0) += 1;

            if let Err(dropped_task) = enqueue_shadow_task(
                pending_shadow_tasks,
                pending_shadow_task_count,
                ready_shadow_keys,
                ready_shadow_key_set,
                inflight_shadow_keys,
                capacity,
                held_task.task_input,
            ) {
                *shadow_holdback_counts
                    .entry("release_enqueue_overflow")
                    .or_insert(0) += 1;
                handle_shadow_enqueue_overflow(
                    ShadowSwapSide::Sell,
                    dropped_task,
                    pending_shadow_tasks,
                    pending_shadow_task_count,
                    ready_shadow_keys,
                    ready_shadow_key_set,
                    inflight_shadow_keys,
                    capacity,
                    shadow_drop_reason_counts,
                    shadow_drop_stage_counts,
                    shadow_queue_full_outcome_counts,
                );
            }
        }

        let remove_key = held_shadow_sells
            .get(&key)
            .is_some_and(|queue| queue.is_empty());
        if remove_key {
            held_shadow_sells.remove(&key);
        }
    }
}

fn should_process_shadow_inline(
    shadow_queue_full: bool,
    shadow_scheduler_needs_reset: bool,
    shadow_worker_count: usize,
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) -> bool {
    shadow_queue_full
        && !shadow_scheduler_needs_reset
        && shadow_worker_count < SHADOW_MAX_CONCURRENT_WORKERS
        && !key_has_pending_or_inflight(key, pending_shadow_tasks, inflight_shadow_keys)
}

fn apply_follow_snapshot_update(
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

fn spawn_shadow_worker_task(
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

fn spawn_shadow_tasks_up_to_limit(
    shadow_workers: &mut JoinSet<ShadowTaskOutput>,
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
    sqlite_path: &str,
    shadow: &ShadowService,
    max_workers: usize,
) {
    while shadow_workers.len() < max_workers {
        let Some(next) = dequeue_next_shadow_task(
            pending_shadow_tasks,
            pending_shadow_task_count,
            ready_shadow_keys,
            ready_shadow_key_set,
            inflight_shadow_keys,
        ) else {
            return;
        };
        spawn_shadow_worker_task(shadow_workers, shadow, sqlite_path, next);
    }
}

fn enqueue_shadow_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    capacity: usize,
    task_input: ShadowTaskInput,
) -> std::result::Result<(), ShadowTaskInput> {
    if *pending_shadow_task_count >= capacity {
        return Err(task_input);
    }
    let key = task_input.key.clone();
    pending_shadow_tasks
        .entry(key.clone())
        .or_default()
        .push_back(task_input);
    *pending_shadow_task_count = pending_shadow_task_count.saturating_add(1);
    if !inflight_shadow_keys.contains(&key) && ready_shadow_key_set.insert(key.clone()) {
        ready_shadow_keys.push_back(key);
    }
    Ok(())
}

fn handle_shadow_enqueue_overflow(
    overflow_side: ShadowSwapSide,
    overflow_task: ShadowTaskInput,
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    capacity: usize,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    match overflow_side {
        ShadowSwapSide::Buy => {
            record_shadow_queue_full_buy_drop(
                &overflow_task.swap,
                shadow_drop_reason_counts,
                shadow_drop_stage_counts,
                shadow_queue_full_outcome_counts,
            );
        }
        ShadowSwapSide::Sell => {
            if let Some(evicted_buy_task) = evict_one_pending_buy_task(
                pending_shadow_tasks,
                pending_shadow_task_count,
                ready_shadow_keys,
                ready_shadow_key_set,
            ) {
                let sell_swap_for_log = overflow_task.swap.clone();
                match enqueue_shadow_task(
                    pending_shadow_tasks,
                    pending_shadow_task_count,
                    ready_shadow_keys,
                    ready_shadow_key_set,
                    inflight_shadow_keys,
                    capacity,
                    overflow_task,
                ) {
                    Ok(()) => {
                        record_shadow_queue_full_buy_drop(
                            &evicted_buy_task.swap,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                        record_shadow_queue_full_sell_outcome(
                            &sell_swap_for_log,
                            true,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                    }
                    Err(dropped_sell_task) => {
                        if let Err(still_evicted_buy_task) = enqueue_shadow_task(
                            pending_shadow_tasks,
                            pending_shadow_task_count,
                            ready_shadow_keys,
                            ready_shadow_key_set,
                            inflight_shadow_keys,
                            capacity,
                            evicted_buy_task,
                        ) {
                            record_shadow_queue_full_buy_drop(
                                &still_evicted_buy_task.swap,
                                shadow_drop_reason_counts,
                                shadow_drop_stage_counts,
                                shadow_queue_full_outcome_counts,
                            );
                        }
                        record_shadow_queue_full_sell_outcome(
                            &dropped_sell_task.swap,
                            false,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                    }
                }
            } else {
                record_shadow_queue_full_sell_outcome(
                    &overflow_task.swap,
                    false,
                    shadow_drop_reason_counts,
                    shadow_drop_stage_counts,
                    shadow_queue_full_outcome_counts,
                );
            }
        }
    }
}

fn evict_one_pending_buy_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
) -> Option<ShadowTaskInput> {
    let ready_candidate = ready_shadow_keys.iter().find_map(|key| {
        pending_shadow_tasks
            .get(key)
            .and_then(find_last_pending_buy_index)
            .map(|index| (key.clone(), index))
    });
    let candidate = ready_candidate.or_else(|| {
        let keys: Vec<ShadowTaskKey> = pending_shadow_tasks.keys().cloned().collect();
        keys.into_iter().find_map(|key| {
            pending_shadow_tasks
                .get(&key)
                .and_then(find_last_pending_buy_index)
                .map(|index| (key, index))
        })
    })?;

    let (key, index) = candidate;
    let mut remove_key = false;
    let removed = if let Some(queue) = pending_shadow_tasks.get_mut(&key) {
        let task = queue.remove(index);
        remove_key = queue.is_empty();
        task
    } else {
        None
    };

    if remove_key {
        pending_shadow_tasks.remove(&key);
        ready_shadow_key_set.remove(&key);
        ready_shadow_keys.retain(|ready_key| ready_key != &key);
    }

    if removed.is_some() {
        *pending_shadow_task_count = pending_shadow_task_count.saturating_sub(1);
    }
    removed
}

fn find_last_pending_buy_index(queue: &VecDeque<ShadowTaskInput>) -> Option<usize> {
    (0..queue.len()).rev().find(|index| {
        queue
            .get(*index)
            .and_then(|task| classify_swap_side(&task.swap))
            .is_some_and(|side| matches!(side, ShadowSwapSide::Buy))
    })
}

fn record_shadow_queue_full_buy_drop(
    swap: &SwapEvent,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_buy_drop";
    *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
    *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
    *shadow_queue_full_outcome_counts.entry(reason).or_insert(0) += 1;
    warn!(
        stage = "scheduler",
        reason,
        side = "buy",
        wallet = %swap.wallet,
        token = %swap.token_out,
        signature = %swap.signature,
        "shadow gate dropped"
    );
}

fn record_shadow_queue_full_sell_outcome(
    swap: &SwapEvent,
    kept: bool,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_sell_kept_or_dropped";
    let outcome_key = if kept {
        "queue_full_sell_kept"
    } else {
        "queue_full_sell_dropped"
    };
    *shadow_queue_full_outcome_counts
        .entry(outcome_key)
        .or_insert(0) += 1;
    if kept {
        info!(
            stage = "scheduler",
            reason,
            outcome = "kept",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow queue_full sell outcome"
        );
    } else {
        *shadow_drop_reason_counts
            .entry("queue_full_sell_dropped")
            .or_insert(0) += 1;
        *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
        warn!(
            stage = "scheduler",
            reason,
            outcome = "dropped",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow gate dropped"
        );
    }
}

fn dequeue_next_shadow_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
) -> Option<ShadowTaskInput> {
    while let Some(key) = ready_shadow_keys.pop_front() {
        ready_shadow_key_set.remove(&key);
        if inflight_shadow_keys.contains(&key) {
            continue;
        }

        let mut remove_key = false;
        let next_task = if let Some(queue) = pending_shadow_tasks.get_mut(&key) {
            let task = queue.pop_front();
            remove_key = queue.is_empty();
            task
        } else {
            None
        };

        if remove_key {
            pending_shadow_tasks.remove(&key);
        }

        if let Some(task) = next_task {
            inflight_shadow_keys.insert(key);
            *pending_shadow_task_count = pending_shadow_task_count.saturating_sub(1);
            return Some(task);
        }
    }
    None
}

fn rebuild_shadow_ready_queue(
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) {
    ready_shadow_keys.clear();
    ready_shadow_key_set.clear();
    for (key, pending) in pending_shadow_tasks {
        if pending.is_empty() || inflight_shadow_keys.contains(key) {
            continue;
        }
        if ready_shadow_key_set.insert(key.clone()) {
            ready_shadow_keys.push_back(key.clone());
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

fn mark_shadow_task_complete(
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
) {
    inflight_shadow_keys.remove(key);
    if pending_shadow_tasks
        .get(key)
        .is_some_and(|pending| !pending.is_empty())
        && ready_shadow_key_set.insert(key.clone())
    {
        ready_shadow_keys.push_back(key.clone());
    }
}

fn handle_shadow_task_output(
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
mod app_tests {
    use super::*;
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
        Ok(path)
    }

    #[test]
    fn resolve_execution_adapter_secrets_reads_file_sources() -> Result<()> {
        let auth_path = write_temp_secret_file("auth-token", "token-from-file\n")?;
        let hmac_path = write_temp_secret_file("hmac-secret", "hmac-from-file \n")?;

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token_file = auth_path.to_string_lossy().to_string();
        execution.submit_adapter_hmac_secret_file = hmac_path.to_string_lossy().to_string();

        resolve_execution_adapter_secrets(&mut execution)?;
        assert_eq!(execution.submit_adapter_auth_token, "token-from-file");
        assert_eq!(execution.submit_adapter_hmac_secret, "hmac-from-file");

        let _ = std::fs::remove_file(auth_path);
        let _ = std::fs::remove_file(hmac_path);
        Ok(())
    }

    #[test]
    fn resolve_execution_adapter_secrets_rejects_inline_and_file_conflict() -> Result<()> {
        let auth_path = write_temp_secret_file("auth-conflict", "token-from-file\n")?;

        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.mode = "adapter_submit_confirm".to_string();
        execution.submit_adapter_auth_token = "inline-token".to_string();
        execution.submit_adapter_auth_token_file = auth_path.to_string_lossy().to_string();

        let error = resolve_execution_adapter_secrets(&mut execution)
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
    fn risk_guard_infra_ratio_uses_window_delta_not_cumulative() -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(30),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 9_000,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 16_500_000,
                ws_notifications_replaced_oldest: 14_400_000,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 16_500_176,
                ws_notifications_replaced_oldest: 14_400_134,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
            },
        ]);
        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "rolling delta ratio (134/176 ~= 0.76) should not trigger threshold 0.80"
        );

        guard.observe_ingestion_snapshot(
            &store,
            now,
            Some(IngestionRuntimeSnapshot {
                ts_utc: now + chrono::Duration::seconds(20),
                ws_notifications_enqueued: 16_500_300,
                ws_notifications_replaced_oldest: 14_400_270,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
            }),
        );
        assert!(
            guard.infra_block_reason.is_some(),
            "rolling delta ratio should trigger once it exceeds threshold"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
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
    fn stale_lot_cleanup_closes_old_lots_using_latest_price() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-cleanup")?;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-price".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let (closed, skipped) = close_stale_shadow_lots(&store, &mut open_pairs, 8, now)?;

        assert_eq!(closed, 1);
        assert_eq!(skipped, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let (trades, pnl) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 1);
        assert!(pnl > 0.0, "expected positive pnl after stale cleanup close");

        let _ = std::fs::remove_file(db_path);
        Ok(())
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
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("A1", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("A2", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("B1", "wallet-b", "token-y"),
        )
        .is_ok());
        assert_eq!(pending_shadow_task_count, 3);

        let first = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("first task");
        let second = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("second task");

        assert_eq!(first.swap.signature, "A1");
        assert_eq!(second.swap.signature, "B1");

        mark_shadow_task_complete(
            &first.key,
            &pending_shadow_tasks,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        );

        let third = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("third task");

        assert_eq!(third.swap.signature, "A2");
        assert_eq!(pending_shadow_task_count, 0);
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
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
        let cap = 2usize;

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("A1", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("A2", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("B1", "wallet-b", "token-y"),
        )
        .is_err());
        assert_eq!(pending_shadow_task_count, cap);
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
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_buy_task("B0", "wallet-buy", "token-buy"),
        )
        .is_ok());
        assert_eq!(pending_shadow_task_count, 1);

        let overflow_buy = enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_buy_task("B1", "wallet-buy-2", "token-buy-2"),
        )
        .expect_err("buy should overflow at cap");
        handle_shadow_enqueue_overflow(
            ShadowSwapSide::Buy,
            overflow_buy,
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );

        let sell_task = make_sell_task("S1", "wallet-sell", "token-sell");
        let sell_key = sell_task.key.clone();
        inflight_shadow_keys.insert(sell_key.clone());
        let overflow_sell = enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            sell_task,
        )
        .expect_err("sell should overflow at cap before policy handling");

        handle_shadow_enqueue_overflow(
            ShadowSwapSide::Sell,
            overflow_sell,
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
        assert!(
            !shadow_queue_full_outcome_counts.contains_key("queue_full_sell_dropped"),
            "sell should be kept when a pending buy can be evicted"
        );

        let queued_sell = pending_shadow_tasks
            .get(&sell_key)
            .expect("sell task should remain queued");
        assert_eq!(queued_sell.len(), 1);
        assert!(
            ready_shadow_key_set.get(&sell_key).is_none(),
            "inflight key must not be marked ready"
        );
    }

    #[test]
    fn inline_processing_respects_per_key_serialization() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();

        assert!(should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));

        inflight_shadow_keys.insert(key.clone());
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));

        inflight_shadow_keys.clear();
        pending_shadow_tasks.insert(
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
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: key.clone(),
            }]),
        );
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            true,
            true,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            false,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_MAX_CONCURRENT_WORKERS,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
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
    fn parse_ingestion_source_override_reads_valid_source_key() {
        let content = r#"
# comment
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws
"#;
        assert_eq!(
            parse_ingestion_source_override(content).as_deref(),
            Some("helius_ws")
        );
    }

    #[test]
    fn parse_ingestion_source_override_ignores_invalid_lines() {
        let content = r#"
FOO=bar
SOLANA_COPY_BOT_INGESTION_SOURCE=
this-is-not-a-valid-line
"#;
        assert!(parse_ingestion_source_override(content).is_none());
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
        let pending: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> = HashMap::new();
        let inflight: HashSet<ShadowTaskKey> = HashSet::new();
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &pending,
            &inflight,
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
            },
            follow_snapshot: Arc::new(FollowSnapshot::default()),
            key: key.clone(),
        };
        let mut pending: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> = HashMap::new();
        pending.insert(key.clone(), VecDeque::from([pending_task]));
        let inflight: HashSet<ShadowTaskKey> = HashSet::new();
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(!should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &pending,
            &inflight,
            &open_lots,
        ));

        let pending_empty: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> = HashMap::new();
        let open_lots_yes = HashSet::from([(key.wallet.clone(), key.token.clone())]);
        assert!(!should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &pending_empty,
            &inflight,
            &open_lots_yes,
        ));
    }
}
