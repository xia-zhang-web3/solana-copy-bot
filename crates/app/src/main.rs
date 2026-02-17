use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_env_or_default, RiskConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery::DiscoveryService;
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSnapshot,
};
use copybot_storage::{is_retryable_sqlite_anyhow_error, SqliteStore};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli_config = parse_config_arg();
    let default_path = cli_config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (config, loaded_config_path) = load_from_env_or_default(&default_path)?;

    init_tracing(&config.system.log_level, config.system.log_json);
    info!(
        config_path = %loaded_config_path.display(),
        env = %config.system.env,
        "configuration loaded"
    );

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
    let discovery = DiscoveryService::new_with_helius(
        config.discovery.clone(),
        config.shadow.clone(),
        discovery_http_url,
    );
    let shadow = ShadowService::new_with_helius(config.shadow.clone(), shadow_http_url);

    run_app_loop(
        store,
        ingestion,
        discovery,
        shadow,
        config.risk.clone(),
        config.sqlite.path.clone(),
        config.system.heartbeat_seconds,
        config.discovery.refresh_seconds,
        config.shadow.refresh_seconds,
        config.shadow.max_signal_lag_seconds,
        config.shadow.causal_holdback_enabled,
        config.shadow.causal_holdback_ms,
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
    TimedPause,
    Infra,
    Universe,
    FailClosed,
}

impl BuyRiskBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::HardStop => "risk_hard_stop",
            Self::TimedPause => "risk_timed_pause",
            Self::Infra => "risk_infra_stop",
            Self::Universe => "risk_universe_stop",
            Self::FailClosed => "risk_fail_closed",
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

    fn can_open_buy(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> BuyRiskDecision {
        if !self.config.shadow_killswitch_enabled {
            return BuyRiskDecision::Allow;
        }

        if let Err(error) = self.maybe_refresh_db_state(store, now) {
            let should_log = self
                .last_fail_closed_log_at
                .map(|logged_at| {
                    now - logged_at
                        >= chrono::Duration::seconds(RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS.max(1))
                })
                .unwrap_or(true);
            if should_log {
                self.last_fail_closed_log_at = Some(now);
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

        if let Some(reason) = self.infra_block_reason.as_deref() {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Infra,
                detail: reason.to_string(),
            };
        }

        BuyRiskDecision::Allow
    }

    fn maybe_refresh_db_state(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        if self.hard_stop_reason.is_some() {
            return Ok(());
        }
        if let Some(last_refresh) = self.last_db_refresh_at {
            if now - last_refresh < chrono::Duration::seconds(RISK_DB_REFRESH_MIN_SECONDS.max(1)) {
                return Ok(());
            }
        }
        self.last_db_refresh_at = Some(now);

        let exposure_sol = store.shadow_open_notional_sol()?;
        if exposure_sol >= self.config.shadow_hard_exposure_cap_sol {
            self.activate_hard_stop(
                store,
                now,
                "exposure_hard_cap",
                format!(
                    "open_notional_sol={:.6} >= hard_cap={:.6}",
                    exposure_sol, self.config.shadow_hard_exposure_cap_sol
                ),
            );
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

        let (_, pnl_1h) = store.shadow_realized_pnl_since(now - chrono::Duration::hours(1))?;
        let (_, pnl_6h) = store.shadow_realized_pnl_since(now - chrono::Duration::hours(6))?;
        let (_, pnl_24h) = store.shadow_realized_pnl_since(now - chrono::Duration::hours(24))?;

        if pnl_24h <= self.config.shadow_drawdown_24h_stop_sol {
            self.activate_hard_stop(
                store,
                now,
                "drawdown_24h",
                format!(
                    "pnl_24h={:.6} <= stop={:.6}",
                    pnl_24h, self.config.shadow_drawdown_24h_stop_sol
                ),
            );
            return Ok(());
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

        let rug_window_start = now
            - chrono::Duration::minutes(self.config.shadow_rug_loss_window_minutes.max(1) as i64);
        let rug_count_since = store.shadow_rug_loss_count_since(
            rug_window_start,
            self.config.shadow_rug_loss_return_threshold,
        )?;
        let (_, rug_sample_total, rug_rate_recent) = store.shadow_rug_loss_rate_recent(
            self.config.shadow_rug_loss_rate_sample_size.max(1),
            self.config.shadow_rug_loss_return_threshold,
        )?;
        if rug_count_since >= self.config.shadow_rug_loss_count_threshold
            || rug_rate_recent > self.config.shadow_rug_loss_rate_threshold
        {
            self.activate_hard_stop(
                store,
                now,
                "rug_loss",
                format!(
                    "rug_count_since={} sample_total={} rug_rate_recent={:.4}",
                    rug_count_since, rug_sample_total, rug_rate_recent
                ),
            );
        }

        Ok(())
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
    risk_config: RiskConfig,
    sqlite_path: String,
    heartbeat_seconds: u64,
    discovery_refresh_seconds: u64,
    shadow_refresh_seconds: u64,
    shadow_max_signal_lag_seconds: u64,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut discovery_interval =
        time::interval(Duration::from_secs(discovery_refresh_seconds.max(10)));
    let mut shadow_interval = time::interval(Duration::from_secs(shadow_refresh_seconds.max(10)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
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

    if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }

    loop {
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
                            match shadow_risk_guard.can_open_buy(&store, now) {
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

    store
        .record_heartbeat("copybot-app", "shutdown")
        .context("failed to write shutdown heartbeat")?;
    Ok(())
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
                if attempt < OBSERVED_SWAP_WRITE_MAX_RETRIES
                    && is_retryable_sqlite_anyhow_error(&error)
                {
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
    fn risk_guard_hard_exposure_stop_blocks_new_buys() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let _ = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;

        let decision = guard.can_open_buy(&store, Utc::now());
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                ..
            } => {}
            other => panic!("expected hard stop block, got {other:?}"),
        }

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

        let decision = guard.can_open_buy(&store, now);
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

        let decision = guard.can_open_buy(&store, now);
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
