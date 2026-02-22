use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use super::env_parsing::{
    parse_env_bool, parse_execution_route_list_env, parse_execution_route_map_env,
    validate_adapter_route_policy_completeness,
};
use super::AppConfig;

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
        let routes = parse_execution_route_list_env(
            &submit_allowed_routes_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES",
        )?;
        if !routes.is_empty() {
            config.execution.submit_allowed_routes = routes;
        }
    }
    if let Ok(submit_route_order_csv) = env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER") {
        let routes = parse_execution_route_list_env(
            &submit_route_order_csv,
            "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER",
        )?;
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
    if let Some(submit_dynamic_cu_price_enabled) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED")
            .ok()
            .and_then(parse_env_bool)
    {
        config.execution.submit_dynamic_cu_price_enabled = submit_dynamic_cu_price_enabled;
    }
    if let Some(submit_dynamic_cu_price_percentile) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_PERCENTILE")
            .ok()
            .and_then(|value| value.parse::<u8>().ok())
    {
        config.execution.submit_dynamic_cu_price_percentile = submit_dynamic_cu_price_percentile;
    }
    if let Ok(value) = env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_PRIMARY_URL")
    {
        config.execution.submit_dynamic_cu_price_api_primary_url = value;
    }
    if let Ok(value) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_FALLBACK_URL")
    {
        config.execution.submit_dynamic_cu_price_api_fallback_url = value;
    }
    if let Ok(value) = env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_AUTH_TOKEN")
    {
        config.execution.submit_dynamic_cu_price_api_auth_token = value;
    }
    if let Ok(value) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_API_AUTH_TOKEN_FILE")
    {
        config.execution.submit_dynamic_cu_price_api_auth_token_file = value;
    }
    if let Some(submit_dynamic_tip_lamports_enabled) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_ENABLED")
            .ok()
            .and_then(parse_env_bool)
    {
        config.execution.submit_dynamic_tip_lamports_enabled = submit_dynamic_tip_lamports_enabled;
    }
    if let Some(submit_dynamic_tip_lamports_multiplier_bps) =
        env::var("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_MULTIPLIER_BPS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
    {
        config.execution.submit_dynamic_tip_lamports_multiplier_bps =
            submit_dynamic_tip_lamports_multiplier_bps;
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
    if let Some(pretrade_max_priority_fee_micro_lamports) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_MICRO_LAMPORTS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.execution.pretrade_max_priority_fee_lamports =
            pretrade_max_priority_fee_micro_lamports;
    } else if let Some(pretrade_max_priority_fee_lamports) =
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
    if let Some(quality_gates_enabled) = env::var("SOLANA_COPY_BOT_SHADOW_QUALITY_GATES_ENABLED")
        .ok()
        .and_then(parse_env_bool)
    {
        config.shadow.quality_gates_enabled = quality_gates_enabled;
    }
    if let Some(min_token_age_seconds) = env::var("SOLANA_COPY_BOT_SHADOW_MIN_TOKEN_AGE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.shadow.min_token_age_seconds = min_token_age_seconds;
    }
    if let Some(min_holders) = env::var("SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.shadow.min_holders = min_holders;
    }
    if let Some(min_liquidity_sol) = env::var("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.shadow.min_liquidity_sol = min_liquidity_sol;
    }
    if let Some(min_volume_5m_sol) = env::var("SOLANA_COPY_BOT_SHADOW_MIN_VOLUME_5M_SOL")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.shadow.min_volume_5m_sol = min_volume_5m_sol;
    }
    if let Some(min_unique_traders_5m) = env::var("SOLANA_COPY_BOT_SHADOW_MIN_UNIQUE_TRADERS_5M")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.shadow.min_unique_traders_5m = min_unique_traders_5m;
    }
    if let Some(max_position_sol) = env::var("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.max_position_sol = max_position_sol;
    }
    if let Some(max_total_exposure_sol) = env::var("SOLANA_COPY_BOT_RISK_MAX_TOTAL_EXPOSURE_SOL")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.max_total_exposure_sol = max_total_exposure_sol;
    }
    if let Some(max_exposure_per_token_sol) =
        env::var("SOLANA_COPY_BOT_RISK_MAX_EXPOSURE_PER_TOKEN_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.max_exposure_per_token_sol = max_exposure_per_token_sol;
    }
    if let Some(max_concurrent_positions) =
        env::var("SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
    {
        config.risk.max_concurrent_positions = max_concurrent_positions;
    }
    if let Some(daily_loss_limit_pct) = env::var("SOLANA_COPY_BOT_RISK_DAILY_LOSS_LIMIT_PCT")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.daily_loss_limit_pct = daily_loss_limit_pct;
    }
    if let Some(max_drawdown_pct) = env::var("SOLANA_COPY_BOT_RISK_MAX_DRAWDOWN_PCT")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.max_drawdown_pct = max_drawdown_pct;
    }
    if let Some(max_hold_hours) = env::var("SOLANA_COPY_BOT_RISK_MAX_HOLD_HOURS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    {
        config.risk.max_hold_hours = max_hold_hours;
    }
    if let Some(max_copy_delay_sec) = env::var("SOLANA_COPY_BOT_RISK_MAX_COPY_DELAY_SEC")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.max_copy_delay_sec = max_copy_delay_sec;
    }
    if let Some(shadow_killswitch_enabled) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED")
            .ok()
            .and_then(parse_env_bool)
    {
        config.risk.shadow_killswitch_enabled = shadow_killswitch_enabled;
    }
    if let Some(shadow_soft_exposure_cap_sol) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_soft_exposure_cap_sol = shadow_soft_exposure_cap_sol;
    }
    if let Some(shadow_soft_pause_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_PAUSE_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_soft_pause_minutes = shadow_soft_pause_minutes;
    }
    if let Some(shadow_hard_exposure_cap_sol) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_HARD_EXPOSURE_CAP_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_hard_exposure_cap_sol = shadow_hard_exposure_cap_sol;
    }
    if let Some(shadow_drawdown_1h_stop_sol) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_STOP_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_drawdown_1h_stop_sol = shadow_drawdown_1h_stop_sol;
    }
    if let Some(shadow_drawdown_1h_pause_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_PAUSE_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_drawdown_1h_pause_minutes = shadow_drawdown_1h_pause_minutes;
    }
    if let Some(shadow_drawdown_6h_stop_sol) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_STOP_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_drawdown_6h_stop_sol = shadow_drawdown_6h_stop_sol;
    }
    if let Some(shadow_drawdown_6h_pause_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_PAUSE_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_drawdown_6h_pause_minutes = shadow_drawdown_6h_pause_minutes;
    }
    if let Some(shadow_drawdown_24h_stop_sol) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_24H_STOP_SOL")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_drawdown_24h_stop_sol = shadow_drawdown_24h_stop_sol;
    }
    if let Some(shadow_rug_loss_return_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RETURN_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_rug_loss_return_threshold = shadow_rug_loss_return_threshold;
    }
    if let Some(shadow_rug_loss_window_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_WINDOW_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_rug_loss_window_minutes = shadow_rug_loss_window_minutes;
    }
    if let Some(shadow_rug_loss_count_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_COUNT_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_rug_loss_count_threshold = shadow_rug_loss_count_threshold;
    }
    if let Some(shadow_rug_loss_rate_sample_size) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_SAMPLE_SIZE")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_rug_loss_rate_sample_size = shadow_rug_loss_rate_sample_size;
    }
    if let Some(shadow_rug_loss_rate_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_rug_loss_rate_threshold = shadow_rug_loss_rate_threshold;
    }
    if let Some(shadow_infra_window_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_WINDOW_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_infra_window_minutes = shadow_infra_window_minutes;
    }
    if let Some(shadow_infra_lag_p95_threshold_ms) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_P95_THRESHOLD_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_infra_lag_p95_threshold_ms = shadow_infra_lag_p95_threshold_ms;
    }
    if let Some(shadow_infra_lag_breach_minutes) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_BREACH_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_infra_lag_breach_minutes = shadow_infra_lag_breach_minutes;
    }
    if let Some(shadow_infra_replaced_ratio_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_REPLACED_RATIO_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    {
        config.risk.shadow_infra_replaced_ratio_threshold = shadow_infra_replaced_ratio_threshold;
    }
    if let Some(shadow_infra_rpc429_delta_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC429_DELTA_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_infra_rpc429_delta_threshold = shadow_infra_rpc429_delta_threshold;
    }
    if let Some(shadow_infra_rpc5xx_delta_threshold) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC5XX_DELTA_THRESHOLD")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_infra_rpc5xx_delta_threshold = shadow_infra_rpc5xx_delta_threshold;
    }
    if let Some(shadow_universe_min_active_follow_wallets) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ACTIVE_FOLLOW_WALLETS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_universe_min_active_follow_wallets =
            shadow_universe_min_active_follow_wallets;
    }
    if let Some(shadow_universe_min_eligible_wallets) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ELIGIBLE_WALLETS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_universe_min_eligible_wallets = shadow_universe_min_eligible_wallets;
    }
    if let Some(shadow_universe_breach_cycles) =
        env::var("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    {
        config.risk.shadow_universe_breach_cycles = shadow_universe_breach_cycles;
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

    validate_adapter_route_policy_completeness(&config.execution)?;

    Ok((config, configured))
}
