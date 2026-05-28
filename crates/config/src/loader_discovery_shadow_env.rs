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
