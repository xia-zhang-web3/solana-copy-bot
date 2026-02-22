use serde::Deserialize;
use std::collections::BTreeMap;

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
    pub submit_dynamic_cu_price_enabled: bool,
    pub submit_dynamic_cu_price_percentile: u8,
    pub submit_dynamic_cu_price_api_primary_url: String,
    pub submit_dynamic_cu_price_api_fallback_url: String,
    pub submit_dynamic_cu_price_api_auth_token: String,
    pub submit_dynamic_cu_price_api_auth_token_file: String,
    pub submit_dynamic_tip_lamports_enabled: bool,
    pub submit_dynamic_tip_lamports_multiplier_bps: u32,
    pub submit_timeout_ms: u64,
    pub execution_signer_pubkey: String,
    pub pretrade_min_sol_reserve: f64,
    pub pretrade_require_token_account: bool,
    /// Legacy field name kept for backward compatibility.
    /// Unit is micro-lamports per CU (not plain lamports).
    #[serde(alias = "pretrade_max_priority_fee_micro_lamports_per_cu")]
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
            submit_dynamic_cu_price_enabled: false,
            submit_dynamic_cu_price_percentile: 75,
            submit_dynamic_cu_price_api_primary_url: String::new(),
            submit_dynamic_cu_price_api_fallback_url: String::new(),
            submit_dynamic_cu_price_api_auth_token: String::new(),
            submit_dynamic_cu_price_api_auth_token_file: String::new(),
            submit_dynamic_tip_lamports_enabled: false,
            submit_dynamic_tip_lamports_multiplier_bps: 10_000,
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
            min_token_age_seconds: 30,
            min_holders: 1,
            min_liquidity_sol: 1.0,
            min_volume_5m_sol: 0.5,
            min_unique_traders_5m: 1,
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
