use anyhow::Result;
use std::env;

use crate::env_parsing::{parse_env_bool, parse_env_number};
use crate::AppConfig;

pub(super) fn apply_discovery_shadow_execution_env_overrides(config: &mut AppConfig) -> Result<()> {
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
    if let Some(status_scan_window_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_DISCOVERY_STATUS_SCAN_WINDOW_MINUTES",
        "u64",
    )? {
        config.discovery.status_scan_window_minutes = status_scan_window_minutes;
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
    if let Some(max_bootstrap_snapshot_age_seconds) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_DISCOVERY_MAX_BOOTSTRAP_SNAPSHOT_AGE_SECONDS",
        "u64",
    )? {
        config.discovery.max_bootstrap_snapshot_age_seconds = max_bootstrap_snapshot_age_seconds;
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
    if let Some(canary_enabled) = parse_env_bool("SOLANA_COPY_BOT_EXECUTION_CANARY_ENABLED")? {
        config.execution.canary_enabled = canary_enabled;
    }
    if let Some(canary_dry_run) = parse_env_bool("SOLANA_COPY_BOT_EXECUTION_CANARY_DRY_RUN")? {
        config.execution.canary_dry_run = canary_dry_run;
    }
    if let Some(tiny_submit_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED")?
    {
        config.execution.canary_tiny_submit_enabled = tiny_submit_enabled;
    }
    if let Ok(canary_route) = env::var("SOLANA_COPY_BOT_EXECUTION_CANARY_ROUTE") {
        config.execution.canary_route = canary_route;
    }
    if let Some(canary_interval_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_CANARY_INTERVAL_SECONDS", "u64")?
    {
        config.execution.canary_interval_seconds = canary_interval_seconds;
    }
    if let Some(canary_batch_limit) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_EXECUTION_CANARY_BATCH_LIMIT", "u32")?
    {
        config.execution.canary_batch_limit = canary_batch_limit;
    }
    if let Some(canary_max_signal_age_seconds) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_SIGNAL_AGE_SECONDS",
        "u64",
    )? {
        config.execution.canary_max_signal_age_seconds = canary_max_signal_age_seconds;
    }
    if let Some(canary_buy_size_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_EXECUTION_CANARY_BUY_SIZE_SOL", "f64")?
    {
        config.execution.canary_buy_size_sol = canary_buy_size_sol;
    }
    if let Some(canary_max_open_positions) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_OPEN_POSITIONS", "u32")?
    {
        config.execution.canary_max_open_positions = canary_max_open_positions;
    }
    if let Some(canary_max_daily_loss_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_EXECUTION_CANARY_MAX_DAILY_LOSS_SOL", "f64")?
    {
        config.execution.canary_max_daily_loss_sol = canary_max_daily_loss_sol;
    }
    if let Ok(canary_kill_switch_path) =
        env::var("SOLANA_COPY_BOT_EXECUTION_CANARY_KILL_SWITCH_PATH")
    {
        config.execution.canary_kill_switch_path = canary_kill_switch_path;
    }
    if let Ok(canary_wallet_pubkey) = env::var("SOLANA_COPY_BOT_EXECUTION_CANARY_WALLET_PUBKEY") {
        config.execution.canary_wallet_pubkey = canary_wallet_pubkey;
    }
    if let Some(quote_canary_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_ENABLED")?
    {
        config.execution.quote_canary_enabled = quote_canary_enabled;
    }
    if let Ok(quote_canary_base_url) = env::var("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BASE_URL") {
        config.execution.quote_canary_base_url = quote_canary_base_url;
    }
    if let Ok(quote_canary_api_key) = env::var("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_API_KEY") {
        config.execution.quote_canary_api_key = quote_canary_api_key;
    }
    if let Some(quote_canary_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_TIMEOUT_MS", "u64")?
    {
        config.execution.quote_canary_timeout_ms = quote_canary_timeout_ms;
    }
    if let Some(public_parallel_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUBLIC_PARALLEL_ENABLED")?
    {
        config.execution.quote_canary_public_parallel_enabled = public_parallel_enabled;
    }
    if let Ok(public_base_url) = env::var("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUBLIC_BASE_URL")
    {
        config.execution.quote_canary_public_base_url = public_base_url;
    }
    if let Some(pump_fun_parallel_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_PUMP_FUN_PARALLEL_ENABLED")?
    {
        config.execution.quote_canary_pump_fun_parallel_enabled = pump_fun_parallel_enabled;
    }
    if let Some(swap_instructions_dry_run_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SWAP_INSTRUCTIONS_DRY_RUN_ENABLED")?
    {
        config.execution.swap_instructions_dry_run_enabled = swap_instructions_dry_run_enabled;
    }
    if let Some(swap_transaction_dry_run_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_SWAP_TRANSACTION_DRY_RUN_ENABLED")?
    {
        config.execution.swap_transaction_dry_run_enabled = swap_transaction_dry_run_enabled;
    }
    if let Some(quote_canary_buy_size_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BUY_SIZE_SOL", "f64")?
    {
        config.execution.quote_canary_buy_size_sol = quote_canary_buy_size_sol;
    }
    if let Some(quote_canary_slippage_bps) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_SLIPPAGE_BPS", "u64")?
    {
        config.execution.quote_canary_slippage_bps = quote_canary_slippage_bps;
    }
    if let Some(quote_canary_buy_slippage_bps) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_BUY_SLIPPAGE_BPS",
        "u64",
    )? {
        config.execution.quote_canary_buy_slippage_bps = quote_canary_buy_slippage_bps;
    }
    if let Some(quote_canary_sell_slippage_bps) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_QUOTE_CANARY_SELL_SLIPPAGE_BPS",
        "u64",
    )? {
        config.execution.quote_canary_sell_slippage_bps = quote_canary_sell_slippage_bps;
    }
    if let Some(priority_fee_canary_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_ENABLED")?
    {
        config.execution.priority_fee_canary_enabled = priority_fee_canary_enabled;
    }
    if let Ok(priority_fee_canary_rpc_url) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_RPC_URL")
    {
        config.execution.priority_fee_canary_rpc_url = priority_fee_canary_rpc_url;
    }
    if let Some(priority_fee_canary_timeout_ms) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_TIMEOUT_MS",
        "u64",
    )? {
        config.execution.priority_fee_canary_timeout_ms = priority_fee_canary_timeout_ms;
    }
    if let Some(priority_fee_canary_min_request_interval_ms) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_MIN_REQUEST_INTERVAL_MS",
        "u64",
    )? {
        config.execution.priority_fee_canary_min_request_interval_ms =
            priority_fee_canary_min_request_interval_ms;
    }
    if let Some(priority_fee_canary_cache_ttl_ms) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_CACHE_TTL_MS",
        "u64",
    )? {
        config.execution.priority_fee_canary_cache_ttl_ms = priority_fee_canary_cache_ttl_ms;
    }
    if let Ok(priority_fee_canary_account) =
        env::var("SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_ACCOUNT")
    {
        config.execution.priority_fee_canary_account = priority_fee_canary_account;
    }
    if let Some(priority_fee_canary_last_n_blocks) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_EXECUTION_PRIORITY_FEE_CANARY_LAST_N_BLOCKS",
        "u64",
    )? {
        config.execution.priority_fee_canary_last_n_blocks = priority_fee_canary_last_n_blocks;
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
    Ok(())
}
