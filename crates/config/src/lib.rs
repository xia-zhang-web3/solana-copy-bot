use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub const EXECUTION_ROUTE_TIP_LAMPORTS_MAX: u64 = 100_000_000;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub system: SystemConfig,
    pub sqlite: SqliteConfig,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub enabled: bool,
    pub mode: String,
    pub poll_interval_ms: u64,
    pub batch_size: u32,
    pub default_route: String,
    pub rpc_http_url: String,
    pub rpc_fallback_http_url: String,
    pub rpc_devnet_http_url: String,
    pub submit_adapter_http_url: String,
    pub submit_adapter_fallback_http_url: String,
    pub submit_adapter_auth_token: String,
    pub submit_adapter_auth_token_file: String,
    pub submit_adapter_hmac_key_id: String,
    pub submit_adapter_hmac_secret: String,
    pub submit_adapter_hmac_secret_file: String,
    pub submit_adapter_hmac_ttl_sec: u64,
    pub submit_adapter_contract_version: String,
    pub submit_adapter_require_policy_echo: bool,
    pub submit_allowed_routes: Vec<String>,
    pub submit_route_order: Vec<String>,
    pub submit_route_max_slippage_bps: BTreeMap<String, f64>,
    pub submit_route_tip_lamports: BTreeMap<String, u64>,
    pub submit_route_compute_unit_limit: BTreeMap<String, u32>,
    pub submit_route_compute_unit_price_micro_lamports: BTreeMap<String, u64>,
    pub submit_timeout_ms: u64,
    pub execution_signer_pubkey: String,
    pub pretrade_min_sol_reserve: f64,
    pub pretrade_require_token_account: bool,
    pub pretrade_max_priority_fee_lamports: u64,
    pub slippage_bps: f64,
    pub max_confirm_seconds: u64,
    pub max_submit_attempts: u32,
    pub simulate_before_submit: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "paper".to_string(),
            poll_interval_ms: 750,
            batch_size: 32,
            default_route: "paper".to_string(),
            rpc_http_url: String::new(),
            rpc_fallback_http_url: String::new(),
            rpc_devnet_http_url: String::new(),
            submit_adapter_http_url: String::new(),
            submit_adapter_fallback_http_url: String::new(),
            submit_adapter_auth_token: String::new(),
            submit_adapter_auth_token_file: String::new(),
            submit_adapter_hmac_key_id: String::new(),
            submit_adapter_hmac_secret: String::new(),
            submit_adapter_hmac_secret_file: String::new(),
            submit_adapter_hmac_ttl_sec: 30,
            submit_adapter_contract_version: "v1".to_string(),
            submit_adapter_require_policy_echo: false,
            submit_allowed_routes: vec!["paper".to_string()],
            submit_route_order: Vec::new(),
            submit_route_max_slippage_bps: BTreeMap::from([(String::from("paper"), 50.0)]),
            submit_route_tip_lamports: BTreeMap::from([(String::from("paper"), 0)]),
            submit_route_compute_unit_limit: BTreeMap::from([(String::from("paper"), 300_000)]),
            submit_route_compute_unit_price_micro_lamports: BTreeMap::from([(
                String::from("paper"),
                1_000,
            )]),
            submit_timeout_ms: 3_000,
            execution_signer_pubkey: String::new(),
            pretrade_min_sol_reserve: 0.05,
            pretrade_require_token_account: false,
            pretrade_max_priority_fee_lamports: 0,
            slippage_bps: 50.0,
            max_confirm_seconds: 15,
            max_submit_attempts: 3,
            simulate_before_submit: true,
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
    pub yellowstone_grpc_url: String,
    pub yellowstone_x_token: String,
    pub yellowstone_connect_timeout_ms: u64,
    pub yellowstone_subscribe_timeout_ms: u64,
    pub yellowstone_stream_buffer_capacity: usize,
    pub yellowstone_reconnect_initial_ms: u64,
    pub yellowstone_reconnect_max_ms: u64,
    pub yellowstone_program_ids: Vec<String>,
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
            yellowstone_grpc_url: "REPLACE_ME".to_string(),
            yellowstone_x_token: "REPLACE_ME".to_string(),
            yellowstone_connect_timeout_ms: 5_000,
            yellowstone_subscribe_timeout_ms: 15_000,
            yellowstone_stream_buffer_capacity: 2_048,
            yellowstone_reconnect_initial_ms: 500,
            yellowstone_reconnect_max_ms: 8_000,
            yellowstone_program_ids: Vec::new(),
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
    pub max_exposure_per_token_sol: f64,
    pub max_concurrent_positions: u32,
    pub daily_loss_limit_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_hold_hours: u32,
    pub max_copy_delay_sec: u64,
    pub shadow_killswitch_enabled: bool,
    pub shadow_soft_exposure_cap_sol: f64,
    pub shadow_soft_pause_minutes: u64,
    pub shadow_hard_exposure_cap_sol: f64,
    pub shadow_drawdown_1h_stop_sol: f64,
    pub shadow_drawdown_1h_pause_minutes: u64,
    pub shadow_drawdown_6h_stop_sol: f64,
    pub shadow_drawdown_6h_pause_minutes: u64,
    pub shadow_drawdown_24h_stop_sol: f64,
    pub shadow_rug_loss_return_threshold: f64,
    pub shadow_rug_loss_window_minutes: u64,
    pub shadow_rug_loss_count_threshold: u64,
    pub shadow_rug_loss_rate_sample_size: u64,
    pub shadow_rug_loss_rate_threshold: f64,
    pub shadow_infra_window_minutes: u64,
    pub shadow_infra_lag_p95_threshold_ms: u64,
    pub shadow_infra_lag_breach_minutes: u64,
    pub shadow_infra_replaced_ratio_threshold: f64,
    pub shadow_infra_rpc429_delta_threshold: u64,
    pub shadow_infra_rpc5xx_delta_threshold: u64,
    pub shadow_universe_min_active_follow_wallets: u64,
    pub shadow_universe_min_eligible_wallets: u64,
    pub shadow_universe_breach_cycles: u64,
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
            max_exposure_per_token_sol: 1.0,
            max_concurrent_positions: 5,
            daily_loss_limit_pct: 2.0,
            max_drawdown_pct: 8.0,
            max_hold_hours: 8,
            max_copy_delay_sec: 5,
            shadow_killswitch_enabled: true,
            shadow_soft_exposure_cap_sol: 10.0,
            shadow_soft_pause_minutes: 15,
            shadow_hard_exposure_cap_sol: 12.0,
            shadow_drawdown_1h_stop_sol: -0.7,
            shadow_drawdown_1h_pause_minutes: 30,
            shadow_drawdown_6h_stop_sol: -2.5,
            shadow_drawdown_6h_pause_minutes: 240,
            shadow_drawdown_24h_stop_sol: -5.0,
            shadow_rug_loss_return_threshold: -0.70,
            shadow_rug_loss_window_minutes: 120,
            shadow_rug_loss_count_threshold: 3,
            shadow_rug_loss_rate_sample_size: 200,
            shadow_rug_loss_rate_threshold: 0.04,
            shadow_infra_window_minutes: 20,
            shadow_infra_lag_p95_threshold_ms: 10_000,
            shadow_infra_lag_breach_minutes: 10,
            shadow_infra_replaced_ratio_threshold: 0.85,
            shadow_infra_rpc429_delta_threshold: 5,
            shadow_infra_rpc5xx_delta_threshold: 20,
            shadow_universe_min_active_follow_wallets: 15,
            shadow_universe_min_eligible_wallets: 80,
            shadow_universe_breach_cycles: 3,
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
    if let Ok(grpc_url) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL") {
        config.ingestion.yellowstone_grpc_url = grpc_url;
    }
    if let Ok(x_token) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN") {
        config.ingestion.yellowstone_x_token = x_token;
    }
    if let Some(connect_timeout_ms) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.yellowstone_connect_timeout_ms = connect_timeout_ms;
    }
    if let Some(subscribe_timeout_ms) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_SUBSCRIBE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.yellowstone_subscribe_timeout_ms = subscribe_timeout_ms;
    }
    if let Some(stream_buffer_capacity) =
        env::var("SOLANA_COPY_BOT_YELLOWSTONE_STREAM_BUFFER_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
    {
        config.ingestion.yellowstone_stream_buffer_capacity = stream_buffer_capacity;
    }
    if let Some(reconnect_initial_ms) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_INITIAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.yellowstone_reconnect_initial_ms = reconnect_initial_ms;
    }
    if let Some(reconnect_max_ms) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_MAX_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.ingestion.yellowstone_reconnect_max_ms = reconnect_max_ms;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS") {
        let values: Vec<String> = program_ids_csv
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
            config.ingestion.yellowstone_program_ids = values;
        }
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
    if let Some(enabled) = env::var("SOLANA_COPY_BOT_EXECUTION_ENABLED")
        .ok()
        .and_then(parse_env_bool)
    {
        config.execution.enabled = enabled;
    }
    if let Ok(mode) = env::var("SOLANA_COPY_BOT_EXECUTION_MODE") {
        let trimmed = mode.trim();
        if !trimmed.is_empty() {
            config.execution.mode = trimmed.to_string();
        }
    }
    if let Some(poll_interval_ms) = env::var("SOLANA_COPY_BOT_EXECUTION_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.poll_interval_ms = poll_interval_ms;
    }
    if let Some(batch_size) = env::var("SOLANA_COPY_BOT_EXECUTION_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    {
        config.execution.batch_size = batch_size;
    }
    if let Ok(route) = env::var("SOLANA_COPY_BOT_EXECUTION_DEFAULT_ROUTE") {
        let trimmed = route.trim();
        if !trimmed.is_empty() {
            config.execution.default_route = trimmed.to_string();
        }
    }
    if let Ok(rpc_http_url) = env::var("SOLANA_COPY_BOT_EXECUTION_RPC_HTTP_URL") {
        config.execution.rpc_http_url = rpc_http_url;
    }
    if let Ok(rpc_fallback_http_url) = env::var("SOLANA_COPY_BOT_EXECUTION_RPC_FALLBACK_HTTP_URL") {
        config.execution.rpc_fallback_http_url = rpc_fallback_http_url;
    }
    if let Ok(rpc_devnet_http_url) = env::var("SOLANA_COPY_BOT_EXECUTION_RPC_DEVNET_HTTP_URL") {
        config.execution.rpc_devnet_http_url = rpc_devnet_http_url;
    }
    if let Ok(submit_adapter_http_url) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HTTP_URL")
    {
        config.execution.submit_adapter_http_url = submit_adapter_http_url;
    }
    if let Ok(submit_adapter_fallback_http_url) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_FALLBACK_HTTP_URL")
    {
        config.execution.submit_adapter_fallback_http_url = submit_adapter_fallback_http_url;
    }
    if let Ok(submit_adapter_auth_token) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_AUTH_TOKEN")
    {
        config.execution.submit_adapter_auth_token = submit_adapter_auth_token;
    }
    if let Ok(submit_adapter_auth_token_file) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_AUTH_TOKEN_FILE")
    {
        config.execution.submit_adapter_auth_token_file = submit_adapter_auth_token_file;
    }
    if let Ok(submit_adapter_hmac_key_id) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_KEY_ID")
    {
        config.execution.submit_adapter_hmac_key_id = submit_adapter_hmac_key_id;
    }
    if let Ok(submit_adapter_hmac_secret) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_SECRET")
    {
        config.execution.submit_adapter_hmac_secret = submit_adapter_hmac_secret;
    }
    if let Ok(submit_adapter_hmac_secret_file) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_SECRET_FILE")
    {
        config.execution.submit_adapter_hmac_secret_file = submit_adapter_hmac_secret_file;
    }
    if let Some(submit_adapter_hmac_ttl_sec) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_TTL_SEC")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.submit_adapter_hmac_ttl_sec = submit_adapter_hmac_ttl_sec;
    }
    if let Ok(submit_adapter_contract_version) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_CONTRACT_VERSION")
    {
        let trimmed = submit_adapter_contract_version.trim();
        if !trimmed.is_empty() {
            config.execution.submit_adapter_contract_version = trimmed.to_string();
        }
    }
    if let Some(submit_adapter_require_policy_echo) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO")
            .ok()
            .and_then(parse_env_bool)
    {
        config.execution.submit_adapter_require_policy_echo = submit_adapter_require_policy_echo;
    }
    if let Ok(submit_allowed_routes_csv) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES")
    {
        let routes: Vec<String> = submit_allowed_routes_csv
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect();
        if !routes.is_empty() {
            config.execution.submit_allowed_routes = routes;
        }
    }
    if let Ok(submit_route_order_csv) = env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER") {
        let routes: Vec<String> = submit_route_order_csv
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect();
        if !routes.is_empty() {
            config.execution.submit_route_order = routes;
        }
    }
    if let Ok(submit_route_max_slippage_bps_csv) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS")
    {
        let route_caps = parse_execution_route_map_env(
            &submit_route_max_slippage_bps_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
            |value| value.trim().parse::<f64>().ok(),
        )?;
        if !route_caps.is_empty() {
            config.execution.submit_route_max_slippage_bps = route_caps;
        }
    }
    if let Ok(submit_route_tip_lamports_csv) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS")
    {
        let route_tips = parse_execution_route_map_env(
            &submit_route_tip_lamports_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
            |value| value.trim().parse::<u64>().ok(),
        )?;
        if !route_tips.is_empty() {
            config.execution.submit_route_tip_lamports = route_tips;
        }
    }
    if let Ok(submit_route_compute_unit_limit_csv) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT")
    {
        let route_limits = parse_execution_route_map_env(
            &submit_route_compute_unit_limit_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT",
            |value| value.trim().parse::<u32>().ok(),
        )?;
        if !route_limits.is_empty() {
            config.execution.submit_route_compute_unit_limit = route_limits;
        }
    }
    if let Ok(submit_route_compute_unit_price_micro_lamports_csv) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS")
    {
        let route_prices = parse_execution_route_map_env(
            &submit_route_compute_unit_price_micro_lamports_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS",
            |value| value.trim().parse::<u64>().ok(),
        )?;
        if !route_prices.is_empty() {
            config
                .execution
                .submit_route_compute_unit_price_micro_lamports = route_prices;
        }
    }
    if let Some(submit_timeout_ms) = env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.submit_timeout_ms = submit_timeout_ms;
    }
    if let Ok(execution_signer_pubkey) = env::var("SOLANA_COPY_BOT_EXECUTION_SIGNER_PUBKEY") {
        config.execution.execution_signer_pubkey = execution_signer_pubkey;
    }
    if let Some(pretrade_min_sol_reserve) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.execution.pretrade_min_sol_reserve = pretrade_min_sol_reserve;
    }
    if let Some(pretrade_require_token_account) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRETRADE_REQUIRE_TOKEN_ACCOUNT")
            .ok()
            .and_then(parse_env_bool)
    {
        config.execution.pretrade_require_token_account = pretrade_require_token_account;
    }
    if let Some(pretrade_max_priority_fee_lamports) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_LAMPORTS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.pretrade_max_priority_fee_lamports = pretrade_max_priority_fee_lamports;
    }
    if let Some(slippage_bps) = env::var("SOLANA_COPY_BOT_EXECUTION_SLIPPAGE_BPS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.execution.slippage_bps = slippage_bps;
    }
    if let Some(max_confirm_seconds) = env::var("SOLANA_COPY_BOT_EXECUTION_MAX_CONFIRM_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.max_confirm_seconds = max_confirm_seconds;
    }
    if let Some(max_submit_attempts) = env::var("SOLANA_COPY_BOT_EXECUTION_MAX_SUBMIT_ATTEMPTS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    {
        config.execution.max_submit_attempts = max_submit_attempts;
    }
    if let Some(simulate_before_submit) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SIMULATE_BEFORE_SUBMIT")
            .ok()
            .and_then(parse_env_bool)
    {
        config.execution.simulate_before_submit = simulate_before_submit;
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

fn parse_execution_route_map_env<T, F>(
    csv: &str,
    env_name: &str,
    parse_value: F,
) -> Result<BTreeMap<String, T>>
where
    F: Fn(&str) -> Option<T>,
{
    let mut values = BTreeMap::new();
    let mut seen_normalized = HashSet::new();
    for token in csv.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let Some((route, raw_value)) = token.split_once(':') else {
            continue;
        };
        let route = route.trim().to_ascii_lowercase();
        if route.is_empty() {
            continue;
        }
        let Some(parsed_value) = parse_value(raw_value) else {
            continue;
        };
        if !seen_normalized.insert(route.clone()) {
            return Err(anyhow!(
                "{env_name} contains duplicate route after normalization: {}",
                route
            ));
        }
        values.insert(route, parsed_value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    static TEMP_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn ingestion_yellowstone_defaults_are_applied() {
        let ingestion = IngestionConfig::default();
        assert_eq!(ingestion.yellowstone_grpc_url, "REPLACE_ME");
        assert_eq!(ingestion.yellowstone_x_token, "REPLACE_ME");
        assert_eq!(ingestion.yellowstone_connect_timeout_ms, 5_000);
        assert_eq!(ingestion.yellowstone_subscribe_timeout_ms, 15_000);
        assert_eq!(ingestion.yellowstone_stream_buffer_capacity, 2_048);
        assert_eq!(ingestion.yellowstone_reconnect_initial_ms, 500);
        assert_eq!(ingestion.yellowstone_reconnect_max_ms, 8_000);
        assert!(ingestion.yellowstone_program_ids.is_empty());
    }

    #[test]
    fn load_from_env_rejects_duplicate_normalized_route_max_slippage_keys() {
        assert_duplicate_normalized_route_env_rejected(
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS",
            "rpc:50,RPC:75",
        );
    }

    #[test]
    fn load_from_env_rejects_duplicate_normalized_route_tip_keys() {
        assert_duplicate_normalized_route_env_rejected(
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS",
            "rpc:100,RPC:200",
        );
    }

    #[test]
    fn load_from_env_rejects_duplicate_normalized_route_cu_limit_keys() {
        assert_duplicate_normalized_route_env_rejected(
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT",
            "rpc:300000,RPC:350000",
        );
    }

    #[test]
    fn load_from_env_rejects_duplicate_normalized_route_cu_price_keys() {
        assert_duplicate_normalized_route_env_rejected(
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS",
            "rpc:1000,RPC:2000",
        );
    }

    fn assert_duplicate_normalized_route_env_rejected(env_name: &'static str, env_value: &str) {
        with_temp_config_file("", |config_path| {
            with_clean_copybot_env(|| {
                with_env_var(env_name, env_value, || {
                    let err = load_from_env_or_default(config_path)
                        .expect_err("duplicate normalized route keys should fail")
                        .to_string();
                    assert!(
                        err.contains(env_name),
                        "error should mention env var, got: {err}"
                    );
                    assert!(
                        err.contains("duplicate route after normalization"),
                        "error should describe duplicate normalization, got: {err}"
                    );
                });
            });
        });
    }

    fn with_env_var<T>(key: &'static str, value: &str, run: impl FnOnce() -> T) -> T {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        restore_env_var(key, previous);
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn restore_env_var(key: &'static str, previous: Option<OsString>) {
        match previous {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }

    fn with_clean_copybot_env<T>(run: impl FnOnce() -> T) -> T {
        // Serialize all SOLANA_COPY_BOT_* env mutations in this test module.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved: Vec<(OsString, OsString)> = std::env::vars_os()
            .filter(|(key, _)| key.to_string_lossy().starts_with("SOLANA_COPY_BOT_"))
            .collect();
        for (key, _) in &saved {
            std::env::remove_var(key);
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        for (key, value) in saved {
            std::env::set_var(key, value);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn with_temp_config_file<T>(contents: &str, run: impl FnOnce(&Path) -> T) -> T {
        let path = unique_temp_path();
        fs::write(&path, contents).expect("write temp config");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&path)));
        let _ = fs::remove_file(&path);
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn unique_temp_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let seq = TEMP_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("copybot-config-test-{pid}-{nanos}-{seq}.toml"))
    }
}
