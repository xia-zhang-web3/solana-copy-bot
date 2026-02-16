use anyhow::{Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub system: SystemConfig,
    pub sqlite: SqliteConfig,
    pub ingestion: IngestionConfig,
    pub discovery: DiscoveryConfig,
    pub shadow: ShadowConfig,
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
pub struct IngestionConfig {
    pub source: String,
    pub helius_ws_url: String,
    pub helius_http_url: String,
    pub helius_http_urls: Vec<String>,
    pub fetch_concurrency: usize,
    pub ws_queue_capacity: usize,
    pub output_queue_capacity: usize,
    pub prefetch_stale_drop_ms: u64,
    pub seen_signatures_ttl_ms: u64,
    pub queue_overflow_policy: String,
    pub reorder_hold_ms: u64,
    pub reorder_max_buffer: usize,
    pub telemetry_report_seconds: u64,
    pub subscribe_program_ids: Vec<String>,
    pub raydium_program_ids: Vec<String>,
    pub pumpswap_program_ids: Vec<String>,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub tx_fetch_retries: u32,
    pub tx_fetch_retry_delay_ms: u64,
    pub tx_fetch_retry_max_ms: u64,
    pub tx_fetch_retry_jitter_ms: u64,
    pub tx_request_timeout_ms: u64,
    pub global_rpc_rps_limit: u64,
    pub per_endpoint_rpc_rps_limit: u64,
    pub seen_signatures_limit: usize,
    pub mock_interval_ms: u64,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            source: "mock".to_string(),
            helius_ws_url: "wss://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
            helius_http_url: "https://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
            helius_http_urls: Vec::new(),
            fetch_concurrency: 8,
            ws_queue_capacity: 2048,
            output_queue_capacity: 1024,
            prefetch_stale_drop_ms: 45_000,
            seen_signatures_ttl_ms: 10 * 60 * 1_000,
            queue_overflow_policy: "block".to_string(),
            reorder_hold_ms: 2000,
            reorder_max_buffer: 512,
            telemetry_report_seconds: 30,
            subscribe_program_ids: Vec::new(),
            raydium_program_ids: vec![
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
                "CPMMoo8L3F4NbTegBCKVN6DKuQh8fYfY4yR4j3uP9s5".to_string(),
            ],
            pumpswap_program_ids: vec!["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()],
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 3,
            tx_fetch_retry_delay_ms: 150,
            tx_fetch_retry_max_ms: 2_000,
            tx_fetch_retry_jitter_ms: 150,
            tx_request_timeout_ms: 5_000,
            global_rpc_rps_limit: 0,
            per_endpoint_rpc_rps_limit: 0,
            seen_signatures_limit: 5_000,
            mock_interval_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    pub scoring_window_days: u32,
    pub decay_window_days: u32,
    pub follow_top_n: u32,
    pub min_leader_notional_sol: f64,
    pub helius_http_url: String,
    pub refresh_seconds: u64,
    pub min_trades: u32,
    pub min_active_days: u32,
    pub min_score: f64,
    pub max_tx_per_minute: u32,
    pub min_buy_count: u32,
    pub min_tradable_ratio: f64,
    pub max_rug_ratio: f64,
    pub rug_lookahead_seconds: u64,
    pub thin_market_min_volume_sol: f64,
    pub thin_market_min_unique_traders: u32,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            scoring_window_days: 30,
            decay_window_days: 7,
            follow_top_n: 20,
            min_leader_notional_sol: 0.5,
            helius_http_url: String::new(),
            refresh_seconds: 300,
            min_trades: 8,
            min_active_days: 3,
            min_score: 0.55,
            max_tx_per_minute: 50,
            min_buy_count: 10,
            min_tradable_ratio: 0.25,
            max_rug_ratio: 0.60,
            rug_lookahead_seconds: 30 * 60,
            thin_market_min_volume_sol: 3.0,
            thin_market_min_unique_traders: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    pub max_position_sol: f64,
    pub max_total_exposure_sol: f64,
    pub max_concurrent_positions: u32,
    pub daily_loss_limit_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_hold_hours: u32,
    pub max_copy_delay_sec: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShadowConfig {
    pub enabled: bool,
    pub helius_http_url: String,
    pub causal_holdback_enabled: bool,
    pub causal_holdback_ms: u64,
    pub refresh_seconds: u64,
    pub copy_notional_sol: f64,
    pub min_leader_notional_sol: f64,
    pub max_signal_lag_seconds: u64,
    pub quality_gates_enabled: bool,
    pub min_token_age_seconds: u64,
    pub min_holders: u64,
    pub min_liquidity_sol: f64,
    pub min_volume_5m_sol: f64,
    pub min_unique_traders_5m: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            helius_http_url: String::new(),
            causal_holdback_enabled: true,
            causal_holdback_ms: 2_500,
            refresh_seconds: 60,
            copy_notional_sol: 0.25,
            min_leader_notional_sol: 0.5,
            max_signal_lag_seconds: 45,
            quality_gates_enabled: true,
            min_token_age_seconds: 0,
            min_holders: 0,
            min_liquidity_sol: 0.0,
            min_volume_5m_sol: 0.0,
            min_unique_traders_5m: 0,
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_sol: 0.75,
            max_total_exposure_sol: 4.0,
            max_concurrent_positions: 5,
            daily_loss_limit_pct: 2.0,
            max_drawdown_pct: 8.0,
            max_hold_hours: 8,
            max_copy_delay_sec: 5,
        }
    }
}

pub fn load_from_path(path: impl AsRef<Path>) -> Result<AppConfig> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    let cfg: AppConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse TOML: {}", path.display()))?;
    Ok(cfg)
}

pub fn load_from_env_or_default(default_path: &Path) -> Result<(AppConfig, PathBuf)> {
    let configured = env::var("SOLANA_COPY_BOT_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_path.to_path_buf());
    let mut config = load_from_path(&configured)?;

    if let Ok(source) = env::var("SOLANA_COPY_BOT_INGESTION_SOURCE") {
        config.ingestion.source = source;
    }
    if let Ok(ws_url) = env::var("SOLANA_COPY_BOT_HELIUS_WS_URL") {
        config.ingestion.helius_ws_url = ws_url;
    }
    if let Ok(http_url) = env::var("SOLANA_COPY_BOT_HELIUS_HTTP_URL") {
        config.ingestion.helius_http_url = http_url;
    }
    if let Ok(http_urls_csv) = env::var("SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS") {
        let values: Vec<String> = http_urls_csv
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .split(',')
            .map(str::trim)
            .map(|value| value.trim_matches('"').trim_matches('\''))
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect();
        if !values.is_empty() {
            config.ingestion.helius_http_urls = values;
        }
    }
    if let Some(fetch_concurrency) = env::var("SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.ingestion.fetch_concurrency = fetch_concurrency;
    }
    if let Some(ws_queue_capacity) = env::var("SOLANA_COPY_BOT_INGESTION_WS_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.ingestion.ws_queue_capacity = ws_queue_capacity;
    }
    if let Some(output_queue_capacity) = env::var("SOLANA_COPY_BOT_INGESTION_OUTPUT_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.ingestion.output_queue_capacity = output_queue_capacity;
    }
    if let Some(prefetch_stale_drop_ms) =
        env::var("SOLANA_COPY_BOT_INGESTION_PREFETCH_STALE_DROP_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.prefetch_stale_drop_ms = prefetch_stale_drop_ms;
    }
    if let Some(seen_signatures_ttl_ms) =
        env::var("SOLANA_COPY_BOT_INGESTION_SEEN_SIGNATURES_TTL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.seen_signatures_ttl_ms = seen_signatures_ttl_ms;
    }
    if let Ok(policy) = env::var("SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY") {
        let trimmed = policy.trim();
        if !trimmed.is_empty() {
            config.ingestion.queue_overflow_policy = trimmed.to_string();
        }
    }
    if let Some(reorder_hold_ms) = env::var("SOLANA_COPY_BOT_INGESTION_REORDER_HOLD_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.reorder_hold_ms = reorder_hold_ms;
    }
    if let Some(reorder_max_buffer) = env::var("SOLANA_COPY_BOT_INGESTION_REORDER_MAX_BUFFER")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        config.ingestion.reorder_max_buffer = reorder_max_buffer;
    }
    if let Some(telemetry_report_seconds) =
        env::var("SOLANA_COPY_BOT_INGESTION_TELEMETRY_REPORT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.telemetry_report_seconds = telemetry_report_seconds;
    }
    if let Some(tx_fetch_retries) = env::var("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRIES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    {
        config.ingestion.tx_fetch_retries = tx_fetch_retries;
    }
    if let Some(tx_fetch_retry_delay_ms) =
        env::var("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.tx_fetch_retry_delay_ms = tx_fetch_retry_delay_ms;
    }
    if let Some(tx_fetch_retry_max_ms) = env::var("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_MAX_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.tx_fetch_retry_max_ms = tx_fetch_retry_max_ms;
    }
    if let Some(tx_fetch_retry_jitter_ms) =
        env::var("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_JITTER_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.tx_fetch_retry_jitter_ms = tx_fetch_retry_jitter_ms;
    }
    if let Some(tx_request_timeout_ms) = env::var("SOLANA_COPY_BOT_INGESTION_TX_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.tx_request_timeout_ms = tx_request_timeout_ms;
    }
    if let Some(global_rpc_rps_limit) = env::var("SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.global_rpc_rps_limit = global_rpc_rps_limit;
    }
    if let Some(per_endpoint_rpc_rps_limit) =
        env::var("SOLANA_COPY_BOT_INGESTION_PER_ENDPOINT_RPC_RPS_LIMIT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.per_endpoint_rpc_rps_limit = per_endpoint_rpc_rps_limit;
    }
    if let Ok(discovery_http_url) = env::var("SOLANA_COPY_BOT_DISCOVERY_HELIUS_HTTP_URL") {
        config.discovery.helius_http_url = discovery_http_url;
    }
    if let Ok(shadow_http_url) = env::var("SOLANA_COPY_BOT_SHADOW_HELIUS_HTTP_URL") {
        config.shadow.helius_http_url = shadow_http_url;
    }
    if let Some(holdback_enabled) = env::var("SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_ENABLED")
        .ok()
        .and_then(parse_env_bool)
    {
        config.shadow.causal_holdback_enabled = holdback_enabled;
    }
    if let Some(holdback_ms) = env::var("SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.shadow.causal_holdback_ms = holdback_ms;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_PROGRAM_IDS") {
        let values: Vec<String> = program_ids_csv
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect();
        if !values.is_empty() {
            config.ingestion.subscribe_program_ids = values;
        }
    }

    Ok((config, configured))
}

fn parse_env_bool(value: String) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
