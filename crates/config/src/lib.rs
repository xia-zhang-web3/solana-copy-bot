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
    pub subscribe_program_ids: Vec<String>,
    pub raydium_program_ids: Vec<String>,
    pub pumpswap_program_ids: Vec<String>,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub tx_fetch_retries: u32,
    pub tx_fetch_retry_delay_ms: u64,
    pub tx_request_timeout_ms: u64,
    pub seen_signatures_limit: usize,
    pub mock_interval_ms: u64,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            source: "mock".to_string(),
            helius_ws_url: "wss://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
            helius_http_url: "https://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
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
            tx_request_timeout_ms: 5_000,
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
