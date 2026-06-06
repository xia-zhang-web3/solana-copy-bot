use serde::Deserialize;
use std::fmt;

#[path = "schema_discovery.rs"]
mod discovery;
#[path = "schema_ingestion.rs"]
mod ingestion;
#[path = "schema_shadow.rs"]
mod shadow;

pub use self::discovery::DiscoveryConfig;
pub use self::ingestion::IngestionConfig;
pub use self::shadow::{RiskConfig, ShadowConfig};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub system: SystemConfig,
    pub sqlite: SqliteConfig,
    pub recent_raw_journal: RecentRawJournalConfig,
    pub runtime_restore_ops: RuntimeRestoreOpsConfig,
    pub history_retention: HistoryRetentionConfig,
    pub ingestion: IngestionConfig,
    pub discovery: DiscoveryConfig,
    pub shadow: ShadowConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SystemConfig {
    pub env: String,
    pub log_level: String,
    pub log_json: bool,
    pub heartbeat_seconds: u64,
    pub migrations_dir: String,
    pub pause_new_trades_on_outage: bool,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            env: "dev".to_string(),
            log_level: "info".to_string(),
            log_json: false,
            heartbeat_seconds: 30,
            migrations_dir: "migrations".to_string(),
            pause_new_trades_on_outage: true,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub enabled: bool,
    pub canary_enabled: bool,
    pub canary_dry_run: bool,
    pub canary_route: String,
    pub canary_interval_seconds: u64,
    pub canary_batch_limit: u32,
    pub canary_max_signal_age_seconds: u64,
    pub canary_buy_size_sol: f64,
    pub canary_max_open_positions: u32,
    pub canary_max_daily_loss_sol: f64,
    pub canary_kill_switch_path: String,
    pub canary_wallet_pubkey: String,
    pub quote_canary_enabled: bool,
    pub quote_canary_base_url: String,
    pub quote_canary_api_key: String,
    pub quote_canary_timeout_ms: u64,
    pub quote_canary_public_parallel_enabled: bool,
    pub quote_canary_public_base_url: String,
    pub quote_canary_pump_fun_parallel_enabled: bool,
    pub swap_instructions_dry_run_enabled: bool,
    pub swap_transaction_dry_run_enabled: bool,
    pub quote_canary_buy_size_sol: f64,
    pub quote_canary_slippage_bps: u64,
    pub quote_canary_buy_slippage_bps: u64,
    pub quote_canary_sell_slippage_bps: u64,
    pub priority_fee_canary_enabled: bool,
    pub priority_fee_canary_rpc_url: String,
    pub priority_fee_canary_timeout_ms: u64,
    pub priority_fee_canary_min_request_interval_ms: u64,
    pub priority_fee_canary_cache_ttl_ms: u64,
    pub priority_fee_canary_account: String,
    pub priority_fee_canary_last_n_blocks: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            canary_enabled: false,
            canary_dry_run: true,
            canary_route: "dry_run".to_string(),
            canary_interval_seconds: 15,
            canary_batch_limit: 1,
            canary_max_signal_age_seconds: 120,
            canary_buy_size_sol: 0.01,
            canary_max_open_positions: 1,
            canary_max_daily_loss_sol: 0.02,
            canary_kill_switch_path: "state/execution_canary.stop".to_string(),
            canary_wallet_pubkey: String::new(),
            quote_canary_enabled: false,
            quote_canary_base_url: "https://api.jup.ag/swap/v1".to_string(),
            quote_canary_api_key: String::new(),
            quote_canary_timeout_ms: 1_500,
            quote_canary_public_parallel_enabled: false,
            quote_canary_public_base_url: "https://public.jupiterapi.com".to_string(),
            quote_canary_pump_fun_parallel_enabled: false,
            swap_instructions_dry_run_enabled: false,
            swap_transaction_dry_run_enabled: false,
            quote_canary_buy_size_sol: 0.2,
            quote_canary_slippage_bps: 100,
            quote_canary_buy_slippage_bps: 0,
            quote_canary_sell_slippage_bps: 0,
            priority_fee_canary_enabled: false,
            priority_fee_canary_rpc_url: String::new(),
            priority_fee_canary_timeout_ms: 1_500,
            priority_fee_canary_min_request_interval_ms: 1_100,
            priority_fee_canary_cache_ttl_ms: 5_000,
            priority_fee_canary_account: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            priority_fee_canary_last_n_blocks: 100,
        }
    }
}

impl fmt::Debug for ExecutionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionConfig")
            .field("enabled", &self.enabled)
            .field("canary_enabled", &self.canary_enabled)
            .field("canary_dry_run", &self.canary_dry_run)
            .field("canary_route", &self.canary_route)
            .field("canary_interval_seconds", &self.canary_interval_seconds)
            .field("canary_batch_limit", &self.canary_batch_limit)
            .field(
                "canary_max_signal_age_seconds",
                &self.canary_max_signal_age_seconds,
            )
            .field("canary_buy_size_sol", &self.canary_buy_size_sol)
            .field("canary_max_open_positions", &self.canary_max_open_positions)
            .field("canary_max_daily_loss_sol", &self.canary_max_daily_loss_sol)
            .field("canary_kill_switch_path", &self.canary_kill_switch_path)
            .field("canary_wallet_pubkey", &self.canary_wallet_pubkey)
            .field("quote_canary_enabled", &self.quote_canary_enabled)
            .field(
                "quote_canary_base_url",
                &redacted_url_debug_value(&self.quote_canary_base_url),
            )
            .field(
                "quote_canary_api_key",
                &redacted_secret_debug_value(&self.quote_canary_api_key),
            )
            .field("quote_canary_timeout_ms", &self.quote_canary_timeout_ms)
            .field(
                "quote_canary_public_parallel_enabled",
                &self.quote_canary_public_parallel_enabled,
            )
            .field(
                "quote_canary_public_base_url",
                &redacted_url_debug_value(&self.quote_canary_public_base_url),
            )
            .field(
                "quote_canary_pump_fun_parallel_enabled",
                &self.quote_canary_pump_fun_parallel_enabled,
            )
            .field(
                "swap_instructions_dry_run_enabled",
                &self.swap_instructions_dry_run_enabled,
            )
            .field(
                "swap_transaction_dry_run_enabled",
                &self.swap_transaction_dry_run_enabled,
            )
            .field("quote_canary_buy_size_sol", &self.quote_canary_buy_size_sol)
            .field("quote_canary_slippage_bps", &self.quote_canary_slippage_bps)
            .field(
                "quote_canary_buy_slippage_bps",
                &self.quote_canary_buy_slippage_bps,
            )
            .field(
                "quote_canary_sell_slippage_bps",
                &self.quote_canary_sell_slippage_bps,
            )
            .field(
                "priority_fee_canary_enabled",
                &self.priority_fee_canary_enabled,
            )
            .field(
                "priority_fee_canary_rpc_url",
                &redacted_url_debug_value(&self.priority_fee_canary_rpc_url),
            )
            .field(
                "priority_fee_canary_timeout_ms",
                &self.priority_fee_canary_timeout_ms,
            )
            .field(
                "priority_fee_canary_min_request_interval_ms",
                &self.priority_fee_canary_min_request_interval_ms,
            )
            .field(
                "priority_fee_canary_cache_ttl_ms",
                &self.priority_fee_canary_cache_ttl_ms,
            )
            .field(
                "priority_fee_canary_account",
                &self.priority_fee_canary_account,
            )
            .field(
                "priority_fee_canary_last_n_blocks",
                &self.priority_fee_canary_last_n_blocks,
            )
            .finish()
    }
}

fn redacted_secret_debug_value(value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        "[REDACTED]".to_string()
    }
}

fn redacted_url_debug_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.split_once('?') {
        Some((prefix, _)) => format!("{prefix}?<redacted>"),
        None => trimmed.to_string(),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    pub path: String,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "state/copybot.db".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RecentRawJournalConfig {
    pub path: String,
    pub retention_safety_buffer_days: u32,
    pub writer_queue_capacity_batches: usize,
    pub replay_batch_size: usize,
}

impl Default for RecentRawJournalConfig {
    fn default() -> Self {
        Self {
            path: "state/discovery_recent_raw.db".to_string(),
            retention_safety_buffer_days: 2,
            writer_queue_capacity_batches: 64,
            replay_batch_size: 1_024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeRestoreOpsConfig {
    pub artifact_dir: String,
    pub artifact_retention: usize,
    pub artifact_cadence_minutes: u64,
    pub journal_snapshot_dir: String,
    pub journal_snapshot_retention: usize,
    pub journal_snapshot_cadence_minutes: u64,
    pub drill_workspace_dir: String,
}

impl Default for RuntimeRestoreOpsConfig {
    fn default() -> Self {
        Self {
            artifact_dir: "state/discovery_restore/artifacts".to_string(),
            artifact_retention: 288,
            artifact_cadence_minutes: 10,
            journal_snapshot_dir: "state/discovery_restore/recent_raw".to_string(),
            journal_snapshot_retention: 144,
            journal_snapshot_cadence_minutes: 10,
            drill_workspace_dir: "state/discovery_restore/drills".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HistoryRetentionConfig {
    pub enabled: bool,
    pub sweep_seconds: u64,
    pub protected_history_days: u32,
    pub risk_events_days: u32,
    pub copy_signals_days: u32,
    pub orders_days: u32,
    pub fills_days: u32,
    pub shadow_closed_trades_days: u32,
}

impl Default for HistoryRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sweep_seconds: 60 * 60,
            protected_history_days: 30,
            risk_events_days: 30,
            copy_signals_days: 30,
            orders_days: 30,
            fills_days: 30,
            shadow_closed_trades_days: 90,
        }
    }
}
