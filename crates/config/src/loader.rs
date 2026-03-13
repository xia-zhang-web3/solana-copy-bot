use anyhow::{anyhow, Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use super::env_parsing::{
    normalize_ingestion_queue_overflow_policy, normalize_ingestion_source, parse_env_bool,
    parse_env_number, parse_env_string_list, parse_execution_route_list_env,
    parse_execution_route_map_env, validate_adapter_route_policy_completeness,
};
use super::AppConfig;

pub fn load_from_path(path: impl AsRef<Path>) -> Result<AppConfig> {
    let path = path.as_ref();
    let mut cfg = parse_from_path(path)?;
    normalize_loaded_config(&mut cfg)?;
    validate_loaded_config(&cfg)?;
    Ok(cfg)
}

fn parse_from_path(path: &Path) -> Result<AppConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("failed to parse TOML: {}", path.display()))
}

pub fn load_from_env_or_default(default_path: &Path) -> Result<(AppConfig, PathBuf)> {
    let configured = env::var("SOLANA_COPY_BOT_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_path.to_path_buf());
    let mut config = parse_from_path(&configured)?;

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
    if let Some(connect_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS", "u64")?
    {
        config.ingestion.yellowstone_connect_timeout_ms = connect_timeout_ms;
    }
    if let Some(subscribe_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_SUBSCRIBE_TIMEOUT_MS", "u64")?
    {
        config.ingestion.yellowstone_subscribe_timeout_ms = subscribe_timeout_ms;
    }
    if let Some(stream_buffer_capacity) = parse_env_number::<usize>(
        "SOLANA_COPY_BOT_YELLOWSTONE_STREAM_BUFFER_CAPACITY",
        "usize",
    )? {
        config.ingestion.yellowstone_stream_buffer_capacity = stream_buffer_capacity;
    }
    if let Some(reconnect_initial_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_INITIAL_MS", "u64")?
    {
        config.ingestion.yellowstone_reconnect_initial_ms = reconnect_initial_ms;
    }
    if let Some(reconnect_max_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_MAX_MS", "u64")?
    {
        config.ingestion.yellowstone_reconnect_max_ms = reconnect_max_ms;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS") {
        config.ingestion.yellowstone_program_ids =
            parse_env_string_list(&program_ids_csv, "SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS")?;
    }
    if let Ok(http_urls_csv) = env::var("SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS") {
        config.ingestion.helius_http_urls =
            parse_env_string_list(&http_urls_csv, "SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS")?;
    }
    if let Some(enabled) = parse_env_bool("SOLANA_COPY_BOT_HISTORY_RETENTION_ENABLED")? {
        config.history_retention.enabled = enabled;
    }
    if let Some(sweep_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_HISTORY_RETENTION_SWEEP_SECONDS", "u64")?
    {
        config.history_retention.sweep_seconds = sweep_seconds;
    }
    if let Some(protected_history_days) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_HISTORY_RETENTION_PROTECTED_HISTORY_DAYS",
        "u32",
    )? {
        config.history_retention.protected_history_days = protected_history_days;
    }
    if let Some(risk_events_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_RISK_EVENTS_DAYS", "u32")?
    {
        config.history_retention.risk_events_days = risk_events_days;
    }
    if let Some(copy_signals_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_COPY_SIGNALS_DAYS", "u32")?
    {
        config.history_retention.copy_signals_days = copy_signals_days;
    }
    if let Some(orders_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_ORDERS_DAYS", "u32")?
    {
        config.history_retention.orders_days = orders_days;
    }
    if let Some(fills_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_FILLS_DAYS", "u32")?
    {
        config.history_retention.fills_days = fills_days;
    }
    if let Some(shadow_closed_trades_days) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_HISTORY_RETENTION_SHADOW_CLOSED_TRADES_DAYS",
        "u32",
    )? {
        config.history_retention.shadow_closed_trades_days = shadow_closed_trades_days;
    }
    if let Some(fetch_concurrency) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY", "usize")?
    {
        config.ingestion.fetch_concurrency = fetch_concurrency;
    }
    if let Some(ws_queue_capacity) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_WS_QUEUE_CAPACITY", "usize")?
    {
        config.ingestion.ws_queue_capacity = ws_queue_capacity;
    }
    if let Some(output_queue_capacity) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_OUTPUT_QUEUE_CAPACITY", "usize")?
    {
        config.ingestion.output_queue_capacity = output_queue_capacity;
    }
    if let Some(prefetch_stale_drop_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_PREFETCH_STALE_DROP_MS", "u64")?
    {
        config.ingestion.prefetch_stale_drop_ms = prefetch_stale_drop_ms;
    }
    if let Some(seen_signatures_ttl_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_SEEN_SIGNATURES_TTL_MS", "u64")?
    {
        config.ingestion.seen_signatures_ttl_ms = seen_signatures_ttl_ms;
    }
    if let Ok(policy) = env::var("SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY") {
        config.ingestion.queue_overflow_policy =
            normalize_ingestion_queue_overflow_policy(&policy)?;
    }
    if let Some(reorder_hold_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_REORDER_HOLD_MS", "u64")?
    {
        config.ingestion.reorder_hold_ms = reorder_hold_ms;
    }
    if let Some(reorder_max_buffer) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_REORDER_MAX_BUFFER", "usize")?
    {
        config.ingestion.reorder_max_buffer = reorder_max_buffer;
    }
    if let Some(telemetry_report_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TELEMETRY_REPORT_SECONDS", "u64")?
    {
        config.ingestion.telemetry_report_seconds = telemetry_report_seconds;
    }
    if let Some(tx_fetch_retries) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRIES", "u32")?
    {
        config.ingestion.tx_fetch_retries = tx_fetch_retries;
    }
    if let Some(tx_fetch_retry_delay_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_DELAY_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_delay_ms = tx_fetch_retry_delay_ms;
    }
    if let Some(tx_fetch_retry_max_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_MAX_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_max_ms = tx_fetch_retry_max_ms;
    }
    if let Some(tx_fetch_retry_jitter_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_JITTER_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_jitter_ms = tx_fetch_retry_jitter_ms;
    }
    if let Some(tx_request_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_REQUEST_TIMEOUT_MS", "u64")?
    {
        config.ingestion.tx_request_timeout_ms = tx_request_timeout_ms;
    }
    if let Some(global_rpc_rps_limit) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT", "u64")?
    {
        config.ingestion.global_rpc_rps_limit = global_rpc_rps_limit;
    }
    if let Some(per_endpoint_rpc_rps_limit) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_INGESTION_PER_ENDPOINT_RPC_RPS_LIMIT",
        "u64",
    )? {
        config.ingestion.per_endpoint_rpc_rps_limit = per_endpoint_rpc_rps_limit;
    }
    if let Ok(discovery_http_url) = env::var("SOLANA_COPY_BOT_DISCOVERY_HELIUS_HTTP_URL") {
        config.discovery.helius_http_url = discovery_http_url;
    }
    if let Some(fetch_refresh_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_DISCOVERY_FETCH_REFRESH_SECONDS", "u64")?
    {
        config.discovery.fetch_refresh_seconds = fetch_refresh_seconds;
    }
    if let Some(refresh_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_DISCOVERY_REFRESH_SECONDS", "u64")?
    {
        config.discovery.refresh_seconds = refresh_seconds;
    }
    if let Some(rug_lookahead_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_DISCOVERY_RUG_LOOKAHEAD_SECONDS", "u64")?
    {
        config.discovery.rug_lookahead_seconds = rug_lookahead_seconds;
    }
    if let Some(metric_snapshot_interval_seconds) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_DISCOVERY_METRIC_SNAPSHOT_INTERVAL_SECONDS",
        "u64",
    )? {
        config.discovery.metric_snapshot_interval_seconds = metric_snapshot_interval_seconds;
    }
    if let Some(max_window_swaps_in_memory) = parse_env_number::<usize>(
        "SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY",
        "usize",
    )? {
        config.discovery.max_window_swaps_in_memory = max_window_swaps_in_memory;
    }
    if let Some(max_fetch_swaps_per_cycle) = parse_env_number::<usize>(
        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE",
        "usize",
    )? {
        config.discovery.max_fetch_swaps_per_cycle = max_fetch_swaps_per_cycle;
    }
    if let Some(max_fetch_pages_per_cycle) = parse_env_number::<usize>(
        "SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_PAGES_PER_CYCLE",
        "usize",
    )? {
        config.discovery.max_fetch_pages_per_cycle = max_fetch_pages_per_cycle;
    }
    if let Some(fetch_time_budget_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_DISCOVERY_FETCH_TIME_BUDGET_MS", "u64")?
    {
        config.discovery.fetch_time_budget_ms = fetch_time_budget_ms;
    }
    if let Some(observed_swaps_retention_days) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_DISCOVERY_OBSERVED_SWAPS_RETENTION_DAYS",
        "u32",
    )? {
        config.discovery.observed_swaps_retention_days = observed_swaps_retention_days;
    }
    if let Some(scoring_aggregates_write_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_DISCOVERY_SCORING_AGGREGATES_WRITE_ENABLED")?
    {
        config.discovery.scoring_aggregates_write_enabled = scoring_aggregates_write_enabled;
    }
    if let Some(scoring_aggregates_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_DISCOVERY_SCORING_AGGREGATES_ENABLED")?
    {
        config.discovery.scoring_aggregates_enabled = scoring_aggregates_enabled;
    }
    if let Ok(shadow_http_url) = env::var("SOLANA_COPY_BOT_SHADOW_HELIUS_HTTP_URL") {
        config.shadow.helius_http_url = shadow_http_url;
    }
    if let Some(enabled) = parse_env_bool("SOLANA_COPY_BOT_EXECUTION_ENABLED")? {
        config.execution.enabled = enabled;
    }
    if let Ok(mode) = env::var("SOLANA_COPY_BOT_EXECUTION_MODE") {
        let trimmed = mode.trim();
        if !trimmed.is_empty() {
            config.execution.mode = trimmed.to_string();
        }
    }
    if let Some(poll_interval_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_POLL_INTERVAL_MS", "u64")?
    {
        config.execution.poll_interval_ms = poll_interval_ms;
    }
    if let Some(batch_size) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_EXECUTION_BATCH_SIZE", "u32")?
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
    if let Some(submit_adapter_hmac_ttl_sec) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_TTL_SEC",
        "u64",
    )? {
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
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO")?
    {
        config.execution.submit_adapter_require_policy_echo = submit_adapter_require_policy_echo;
    }
    if let Some(submit_fastlane_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED")?
    {
        config.execution.submit_fastlane_enabled = submit_fastlane_enabled;
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
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_ENABLED")?
    {
        config.execution.submit_dynamic_cu_price_enabled = submit_dynamic_cu_price_enabled;
    }
    if let Some(submit_dynamic_cu_price_percentile) = parse_env_number::<u8>(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_CU_PRICE_PERCENTILE",
        "u8",
    )? {
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
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_ENABLED")?
    {
        config.execution.submit_dynamic_tip_lamports_enabled = submit_dynamic_tip_lamports_enabled;
    }
    if let Some(submit_dynamic_tip_lamports_multiplier_bps) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_EXECUTION_SUBMIT_DYNAMIC_TIP_LAMPORTS_MULTIPLIER_BPS",
        "u32",
    )? {
        config.execution.submit_dynamic_tip_lamports_multiplier_bps =
            submit_dynamic_tip_lamports_multiplier_bps;
    }
    if let Some(submit_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_SUBMIT_TIMEOUT_MS", "u64")?
    {
        config.execution.submit_timeout_ms = submit_timeout_ms;
    }
    if let Ok(execution_signer_pubkey) = env::var("SOLANA_COPY_BOT_EXECUTION_SIGNER_PUBKEY") {
        config.execution.execution_signer_pubkey = execution_signer_pubkey;
    }
    if let Some(pretrade_min_sol_reserve) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_EXECUTION_PRETRADE_MIN_SOL_RESERVE", "f64")?
    {
        config.execution.pretrade_min_sol_reserve = pretrade_min_sol_reserve;
    }
    if let Some(pretrade_max_fee_overhead_bps) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_FEE_OVERHEAD_BPS",
        "u32",
    )? {
        config.execution.pretrade_max_fee_overhead_bps = pretrade_max_fee_overhead_bps;
    }
    if let Some(pretrade_require_token_account) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_PRETRADE_REQUIRE_TOKEN_ACCOUNT")?
    {
        config.execution.pretrade_require_token_account = pretrade_require_token_account;
    }
    if let Some(pretrade_max_priority_fee_micro_lamports) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_MICRO_LAMPORTS",
        "u64",
    )? {
        config.execution.pretrade_max_priority_fee_lamports =
            pretrade_max_priority_fee_micro_lamports;
    } else if let Some(pretrade_max_priority_fee_lamports) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_LAMPORTS",
        "u64",
    )? {
        config.execution.pretrade_max_priority_fee_lamports = pretrade_max_priority_fee_lamports;
    }
    if let Some(slippage_bps) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_EXECUTION_SLIPPAGE_BPS", "f64")?
    {
        config.execution.slippage_bps = slippage_bps;
    }
    if let Some(max_confirm_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_MAX_CONFIRM_SECONDS", "u64")?
    {
        config.execution.max_confirm_seconds = max_confirm_seconds;
    }
    if let Some(max_submit_attempts) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_EXECUTION_MAX_SUBMIT_ATTEMPTS", "u32")?
    {
        config.execution.max_submit_attempts = max_submit_attempts;
    }
    if let Some(simulate_before_submit) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SIMULATE_BEFORE_SUBMIT")?
    {
        config.execution.simulate_before_submit = simulate_before_submit;
    }
    if let Some(holdback_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_ENABLED")?
    {
        config.shadow.causal_holdback_enabled = holdback_enabled;
    }
    if let Some(holdback_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_MS", "u64")?
    {
        config.shadow.causal_holdback_ms = holdback_ms;
    }
    if let Some(quality_gates_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_SHADOW_QUALITY_GATES_ENABLED")?
    {
        config.shadow.quality_gates_enabled = quality_gates_enabled;
    }
    if let Some(min_token_age_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_SHADOW_MIN_TOKEN_AGE_SECONDS", "u64")?
    {
        config.shadow.min_token_age_seconds = min_token_age_seconds;
    }
    if let Some(min_holders) = parse_env_number::<u64>("SOLANA_COPY_BOT_SHADOW_MIN_HOLDERS", "u64")?
    {
        config.shadow.min_holders = min_holders;
    }
    if let Some(min_liquidity_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_SHADOW_MIN_LIQUIDITY_SOL", "f64")?
    {
        config.shadow.min_liquidity_sol = min_liquidity_sol;
    }
    if let Some(min_volume_5m_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_SHADOW_MIN_VOLUME_5M_SOL", "f64")?
    {
        config.shadow.min_volume_5m_sol = min_volume_5m_sol;
    }
    if let Some(min_unique_traders_5m) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_SHADOW_MIN_UNIQUE_TRADERS_5M", "u64")?
    {
        config.shadow.min_unique_traders_5m = min_unique_traders_5m;
    }
    if let Some(max_position_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL", "f64")?
    {
        config.risk.max_position_sol = max_position_sol;
    }
    if let Some(max_total_exposure_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_TOTAL_EXPOSURE_SOL", "f64")?
    {
        config.risk.max_total_exposure_sol = max_total_exposure_sol;
    }
    if let Some(max_exposure_per_token_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_EXPOSURE_PER_TOKEN_SOL", "f64")?
    {
        config.risk.max_exposure_per_token_sol = max_exposure_per_token_sol;
    }
    if let Some(max_concurrent_positions) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS", "u32")?
    {
        config.risk.max_concurrent_positions = max_concurrent_positions;
    }
    if let Some(execution_buy_cooldown_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS", "u64")?
    {
        config.risk.execution_buy_cooldown_seconds = execution_buy_cooldown_seconds;
    }
    if let Some(daily_loss_limit_pct) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_DAILY_LOSS_LIMIT_PCT", "f64")?
    {
        config.risk.daily_loss_limit_pct = daily_loss_limit_pct;
    }
    if let Some(max_drawdown_pct) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_DRAWDOWN_PCT", "f64")?
    {
        config.risk.max_drawdown_pct = max_drawdown_pct;
    }
    if let Some(max_hold_hours) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_RISK_MAX_HOLD_HOURS", "u32")?
    {
        config.risk.max_hold_hours = max_hold_hours;
    }
    if let Some(shadow_stale_close_terminal_zero_price_hours) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_TERMINAL_ZERO_PRICE_HOURS",
        "u32",
    )? {
        config.risk.shadow_stale_close_terminal_zero_price_hours =
            shadow_stale_close_terminal_zero_price_hours;
    }
    if let Some(max_copy_delay_sec) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_MAX_COPY_DELAY_SEC", "u64")?
    {
        config.risk.max_copy_delay_sec = max_copy_delay_sec;
    }
    if let Some(shadow_killswitch_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED")?
    {
        config.risk.shadow_killswitch_enabled = shadow_killswitch_enabled;
    }
    if let Some(shadow_soft_exposure_cap_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL", "f64")?
    {
        config.risk.shadow_soft_exposure_cap_sol = shadow_soft_exposure_cap_sol;
    }
    if let Some(shadow_soft_exposure_resume_below_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
        "f64",
    )? {
        config.risk.shadow_soft_exposure_resume_below_sol = shadow_soft_exposure_resume_below_sol;
    }
    if let Some(shadow_soft_pause_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_PAUSE_MINUTES", "u64")?
    {
        config.risk.shadow_soft_pause_minutes = shadow_soft_pause_minutes;
    }
    if let Some(shadow_hard_exposure_cap_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_HARD_EXPOSURE_CAP_SOL", "f64")?
    {
        config.risk.shadow_hard_exposure_cap_sol = shadow_hard_exposure_cap_sol;
    }
    if let Some(shadow_drawdown_1h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_1h_stop_sol = shadow_drawdown_1h_stop_sol;
    }
    if let Some(shadow_drawdown_1h_pause_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_PAUSE_MINUTES",
        "u64",
    )? {
        config.risk.shadow_drawdown_1h_pause_minutes = shadow_drawdown_1h_pause_minutes;
    }
    if let Some(shadow_drawdown_6h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_6h_stop_sol = shadow_drawdown_6h_stop_sol;
    }
    if let Some(shadow_drawdown_6h_pause_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_PAUSE_MINUTES",
        "u64",
    )? {
        config.risk.shadow_drawdown_6h_pause_minutes = shadow_drawdown_6h_pause_minutes;
    }
    if let Some(shadow_drawdown_24h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_24H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_24h_stop_sol = shadow_drawdown_24h_stop_sol;
    }
    if let Some(shadow_rug_loss_return_threshold) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RETURN_THRESHOLD",
        "f64",
    )? {
        config.risk.shadow_rug_loss_return_threshold = shadow_rug_loss_return_threshold;
    }
    if let Some(shadow_rug_loss_window_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_WINDOW_MINUTES", "u64")?
    {
        config.risk.shadow_rug_loss_window_minutes = shadow_rug_loss_window_minutes;
    }
    if let Some(shadow_rug_loss_count_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_COUNT_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_rug_loss_count_threshold = shadow_rug_loss_count_threshold;
    }
    if let Some(shadow_rug_loss_rate_sample_size) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_SAMPLE_SIZE",
        "u64",
    )? {
        config.risk.shadow_rug_loss_rate_sample_size = shadow_rug_loss_rate_sample_size;
    }
    if let Some(shadow_rug_loss_rate_threshold) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_THRESHOLD", "f64")?
    {
        config.risk.shadow_rug_loss_rate_threshold = shadow_rug_loss_rate_threshold;
    }
    if let Some(shadow_infra_window_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_WINDOW_MINUTES", "u64")?
    {
        config.risk.shadow_infra_window_minutes = shadow_infra_window_minutes;
    }
    if let Some(shadow_infra_lag_p95_threshold_ms) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_P95_THRESHOLD_MS",
        "u64",
    )? {
        config.risk.shadow_infra_lag_p95_threshold_ms = shadow_infra_lag_p95_threshold_ms;
    }
    if let Some(shadow_infra_lag_breach_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_BREACH_MINUTES",
        "u64",
    )? {
        config.risk.shadow_infra_lag_breach_minutes = shadow_infra_lag_breach_minutes;
    }
    if let Some(shadow_infra_replaced_ratio_threshold) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_REPLACED_RATIO_THRESHOLD",
        "f64",
    )? {
        config.risk.shadow_infra_replaced_ratio_threshold = shadow_infra_replaced_ratio_threshold;
    }
    if let Some(shadow_infra_rpc429_delta_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC429_DELTA_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_infra_rpc429_delta_threshold = shadow_infra_rpc429_delta_threshold;
    }
    if let Some(shadow_infra_rpc5xx_delta_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC5XX_DELTA_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_infra_rpc5xx_delta_threshold = shadow_infra_rpc5xx_delta_threshold;
    }
    if let Some(shadow_universe_min_active_follow_wallets) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ACTIVE_FOLLOW_WALLETS",
        "u64",
    )? {
        config.risk.shadow_universe_min_active_follow_wallets =
            shadow_universe_min_active_follow_wallets;
    }
    if let Some(shadow_universe_min_eligible_wallets) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ELIGIBLE_WALLETS",
        "u64",
    )? {
        config.risk.shadow_universe_min_eligible_wallets = shadow_universe_min_eligible_wallets;
    }
    if let Some(shadow_universe_breach_cycles) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES", "u64")?
    {
        config.risk.shadow_universe_breach_cycles = shadow_universe_breach_cycles;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_PROGRAM_IDS") {
        config.ingestion.subscribe_program_ids =
            parse_env_string_list(&program_ids_csv, "SOLANA_COPY_BOT_PROGRAM_IDS")?;
    }

    normalize_loaded_config(&mut config)?;
    validate_loaded_config(&config)?;

    Ok((config, configured))
}

fn normalize_loaded_config(config: &mut AppConfig) -> Result<()> {
    config.ingestion.source = normalize_ingestion_source(&config.ingestion.source)?;
    config.ingestion.queue_overflow_policy =
        normalize_ingestion_queue_overflow_policy(&config.ingestion.queue_overflow_policy)?;
    Ok(())
}

fn validate_loaded_config(config: &AppConfig) -> Result<()> {
    validate_adapter_route_policy_completeness(&config.execution)?;
    validate_shadow_universe_config(config)?;
    validate_shadow_quality_thresholds(config)?;
    validate_discovery_storage_mitigation_config(config)?;
    validate_discovery_aggregate_activation_config(config)?;
    validate_execution_exact_sizing_config(config)?;
    validate_history_retention_config(config)?;
    Ok(())
}

fn validate_shadow_universe_config(config: &AppConfig) -> Result<()> {
    let follow_top_n = u64::from(config.discovery.follow_top_n);
    let min_active = config.risk.shadow_universe_min_active_follow_wallets;
    if follow_top_n < min_active {
        return Err(anyhow!(
            "discovery.follow_top_n ({follow_top_n}) must be >= risk.shadow_universe_min_active_follow_wallets ({min_active})"
        ));
    }
    Ok(())
}

fn validate_discovery_aggregate_activation_config(config: &AppConfig) -> Result<()> {
    if config.discovery.scoring_aggregates_enabled
        && !config.discovery.scoring_aggregates_write_enabled
    {
        return Err(anyhow!(
            "discovery.scoring_aggregates_enabled requires discovery.scoring_aggregates_write_enabled = true"
        ));
    }
    Ok(())
}

fn validate_shadow_quality_thresholds(config: &AppConfig) -> Result<()> {
    let min_holders = config.shadow.min_holders;
    if (1..5).contains(&min_holders) {
        return Err(anyhow!(
            "shadow.min_holders ({min_holders}) must be either 0 (disable holder gate) or >= 5"
        ));
    }
    Ok(())
}

fn validate_discovery_storage_mitigation_config(config: &AppConfig) -> Result<()> {
    if config.discovery.max_fetch_swaps_per_cycle > config.discovery.max_window_swaps_in_memory {
        return Err(anyhow!(
            "discovery.max_fetch_swaps_per_cycle ({}) must be <= discovery.max_window_swaps_in_memory ({})",
            config.discovery.max_fetch_swaps_per_cycle,
            config.discovery.max_window_swaps_in_memory
        ));
    }
    if config.discovery.max_fetch_pages_per_cycle == 0 {
        return Err(anyhow!(
            "discovery.max_fetch_pages_per_cycle ({}) must be >= 1",
            config.discovery.max_fetch_pages_per_cycle
        ));
    }
    if config.discovery.fetch_refresh_seconds == 0 {
        return Err(anyhow!(
            "discovery.fetch_refresh_seconds ({}) must be >= 1",
            config.discovery.fetch_refresh_seconds
        ));
    }
    if config.discovery.refresh_seconds < config.discovery.fetch_refresh_seconds {
        return Err(anyhow!(
            "discovery.refresh_seconds ({}) must be >= discovery.fetch_refresh_seconds ({})",
            config.discovery.refresh_seconds,
            config.discovery.fetch_refresh_seconds
        ));
    }
    if config.discovery.fetch_time_budget_ms == 0 {
        return Err(anyhow!(
            "discovery.fetch_time_budget_ms ({}) must be >= 1",
            config.discovery.fetch_time_budget_ms
        ));
    }
    Ok(())
}

fn validate_execution_exact_sizing_config(config: &AppConfig) -> Result<()> {
    validate_sol_boundary(
        "execution.pretrade_min_sol_reserve",
        config.execution.pretrade_min_sol_reserve,
        true,
    )?;
    validate_sol_boundary("risk.max_position_sol", config.risk.max_position_sol, false)?;
    validate_sol_boundary(
        "risk.max_total_exposure_sol",
        config.risk.max_total_exposure_sol,
        false,
    )?;
    validate_sol_boundary(
        "risk.max_exposure_per_token_sol",
        config.risk.max_exposure_per_token_sol,
        false,
    )?;
    Ok(())
}

fn validate_sol_boundary(label: &str, value: f64, allow_zero: bool) -> Result<()> {
    if !value.is_finite() {
        return Err(anyhow!("{label} must be finite, got {value}"));
    }
    if value < 0.0 || (!allow_zero && value <= 0.0) {
        return Err(anyhow!(
            "{label} must be {} 0, got {value}",
            if allow_zero { ">=" } else { ">" }
        ));
    }

    let lamports = value * 1_000_000_000.0;
    if !lamports.is_finite() || lamports > u64::MAX as f64 {
        return Err(anyhow!("{label} overflows lamports, got {value}"));
    }
    if !allow_zero && value > 0.0 && lamports < 1.0 {
        return Err(anyhow!(
            "{label} must be representable as at least 1 lamport, got {value}"
        ));
    }
    Ok(())
}

fn validate_history_retention_config(config: &AppConfig) -> Result<()> {
    let retention = &config.history_retention;
    if !retention.enabled {
        return Ok(());
    }

    let protected_history_days = retention.protected_history_days;
    if protected_history_days == 0 {
        return Err(anyhow!(
            "history_retention.protected_history_days ({protected_history_days}) must be >= 1"
        ));
    }

    let sweep_seconds = retention.sweep_seconds;
    if sweep_seconds == 0 {
        return Err(anyhow!(
            "history_retention.sweep_seconds ({sweep_seconds}) must be >= 1"
        ));
    }

    for (name, value) in [
        ("risk_events_days", retention.risk_events_days),
        ("copy_signals_days", retention.copy_signals_days),
        ("orders_days", retention.orders_days),
        ("fills_days", retention.fills_days),
        (
            "shadow_closed_trades_days",
            retention.shadow_closed_trades_days,
        ),
    ] {
        if value == 0 {
            return Err(anyhow!(
                "history_retention.{name} ({value}) must be >= 1 when history_retention.enabled=true"
            ));
        }
    }

    let orders_days = retention.orders_days;
    let fills_days = retention.fills_days;
    if fills_days != orders_days {
        return Err(anyhow!(
            "history_retention.fills_days ({fills_days}) must equal history_retention.orders_days ({orders_days}) to preserve order/fill query parity"
        ));
    }

    let copy_signals_days = retention.copy_signals_days;
    if copy_signals_days < orders_days {
        return Err(anyhow!(
            "history_retention.copy_signals_days ({copy_signals_days}) must be >= history_retention.orders_days ({orders_days}) for FK-safe execution history retention"
        ));
    }

    let effective_shadow_closed_trades_days = retention
        .shadow_closed_trades_days
        .max(protected_history_days);
    let required_shadow_closed_trade_days =
        ((config.risk.shadow_rug_loss_window_minutes.max(24 * 60) + 24 * 60 - 1) / (24 * 60))
            as u32;
    if effective_shadow_closed_trades_days < required_shadow_closed_trade_days {
        return Err(anyhow!(
            "effective shadow closed trade retention ({effective_shadow_closed_trades_days} days) must be >= {} days to preserve runtime risk windows",
            required_shadow_closed_trade_days
        ));
    }

    Ok(())
}
