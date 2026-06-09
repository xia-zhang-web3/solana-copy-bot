use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;

const EXECUTION_CANARY_MAX_DAILY_LOSS_SOL_CAP: f64 = 2.00;

pub(crate) fn contains_placeholder_value(value: &str) -> bool {
    value.to_ascii_uppercase().contains("REPLACE_ME")
}

pub(crate) fn validate_execution_runtime_contract(
    config: &ExecutionConfig,
    _env: &str,
) -> Result<()> {
    if config.enabled {
        return Err(anyhow!(
            "execution.enabled=true is not supported by copybot-app after execution runtime quarantine; keep execution.enabled=false"
        ));
    }
    validate_execution_canary_contract(config)?;
    Ok(())
}

pub(crate) fn validate_execution_canary_contract(config: &ExecutionConfig) -> Result<()> {
    if !config.canary_enabled {
        return Ok(());
    }
    if !config.canary_dry_run {
        return Err(anyhow!(
            "execution.canary_dry_run=false is not supported yet; canary execution is dry-run only"
        ));
    }
    if config.canary_tiny_submit_enabled {
        validate_execution_canary_tiny_submit_contract(config)?;
    }
    let route = config.canary_route.trim();
    if route.is_empty() || contains_placeholder_value(route) {
        return Err(anyhow!(
            "execution.canary_route must be a non-placeholder route when execution canary is enabled"
        ));
    }
    if config.canary_interval_seconds == 0 {
        return Err(anyhow!(
            "execution.canary_interval_seconds must be >= 1 when execution canary is enabled"
        ));
    }
    if config.canary_batch_limit == 0 {
        return Err(anyhow!(
            "execution.canary_batch_limit must be >= 1 when execution canary is enabled"
        ));
    }
    if config.canary_max_signal_age_seconds == 0 || config.canary_max_signal_age_seconds > 300 {
        return Err(anyhow!(
            "execution.canary_max_signal_age_seconds must be within 1..=300 for canary rollout"
        ));
    }
    if !config.canary_buy_size_sol.is_finite()
        || config.canary_buy_size_sol < 0.01
        || config.canary_buy_size_sol > 0.02
    {
        return Err(anyhow!(
            "execution.canary_buy_size_sol must be within 0.01..=0.02 SOL for canary rollout"
        ));
    }
    if config.canary_max_open_positions == 0 || config.canary_max_open_positions > 20 {
        return Err(anyhow!(
            "execution.canary_max_open_positions must be within 1..=20 for canary rollout"
        ));
    }
    if !config.canary_max_daily_loss_sol.is_finite()
        || config.canary_max_daily_loss_sol < 0.0
        || config.canary_max_daily_loss_sol > EXECUTION_CANARY_MAX_DAILY_LOSS_SOL_CAP
    {
        return Err(anyhow!(
            "execution.canary_max_daily_loss_sol must be finite and <= {:.2} SOL for canary rollout",
            EXECUTION_CANARY_MAX_DAILY_LOSS_SOL_CAP
        ));
    }
    if config.canary_kill_switch_path.trim().is_empty() {
        return Err(anyhow!(
            "execution.canary_kill_switch_path must be configured when execution canary is enabled"
        ));
    }
    if config.quote_canary_enabled {
        if config.quote_canary_base_url.trim().is_empty()
            || contains_placeholder_value(&config.quote_canary_base_url)
        {
            return Err(anyhow!(
                "execution.quote_canary_base_url must be configured when quote canary is enabled"
            ));
        }
        if config.quote_canary_timeout_ms < 100 || config.quote_canary_timeout_ms > 10_000 {
            return Err(anyhow!(
                "execution.quote_canary_timeout_ms must be within 100..=10000"
            ));
        }
        if !config.quote_canary_buy_size_sol.is_finite()
            || config.quote_canary_buy_size_sol <= 0.0
            || config.quote_canary_buy_size_sol > 1.0
        {
            return Err(anyhow!(
                "execution.quote_canary_buy_size_sol must be finite and within (0, 1] SOL"
            ));
        }
        if config.quote_canary_slippage_bps > 5_000 {
            return Err(anyhow!(
                "execution.quote_canary_slippage_bps must be <= 5000"
            ));
        }
        if config.quote_canary_buy_slippage_bps > 5_000 {
            return Err(anyhow!(
                "execution.quote_canary_buy_slippage_bps must be <= 5000"
            ));
        }
        if config.quote_canary_sell_slippage_bps > 5_000 {
            return Err(anyhow!(
                "execution.quote_canary_sell_slippage_bps must be <= 5000"
            ));
        }
    }
    if config.swap_instructions_dry_run_enabled && !config.quote_canary_enabled {
        return Err(anyhow!(
            "execution.swap_instructions_dry_run_enabled requires quote canary metadata"
        ));
    }
    if config.swap_transaction_dry_run_enabled && !config.quote_canary_enabled {
        return Err(anyhow!(
            "execution.swap_transaction_dry_run_enabled requires quote canary metadata"
        ));
    }
    if config.priority_fee_canary_enabled {
        if config.priority_fee_canary_rpc_url.trim().is_empty()
            || contains_placeholder_value(&config.priority_fee_canary_rpc_url)
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_rpc_url must be configured when priority fee canary is enabled"
            ));
        }
        if config.priority_fee_canary_timeout_ms < 100
            || config.priority_fee_canary_timeout_ms > 10_000
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_timeout_ms must be within 100..=10000"
            ));
        }
        if config.priority_fee_canary_min_request_interval_ms < 100
            || config.priority_fee_canary_min_request_interval_ms > 10_000
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_min_request_interval_ms must be within 100..=10000"
            ));
        }
        if config.priority_fee_canary_cache_ttl_ms < 250
            || config.priority_fee_canary_cache_ttl_ms > 60_000
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_cache_ttl_ms must be within 250..=60000"
            ));
        }
        if config.priority_fee_canary_account.trim().is_empty()
            || contains_placeholder_value(&config.priority_fee_canary_account)
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_account must be configured when priority fee canary is enabled"
            ));
        }
        if config.priority_fee_canary_last_n_blocks == 0
            || config.priority_fee_canary_last_n_blocks > 1_000
        {
            return Err(anyhow!(
                "execution.priority_fee_canary_last_n_blocks must be within 1..=1000"
            ));
        }
    }
    Ok(())
}

fn validate_execution_canary_tiny_submit_contract(config: &ExecutionConfig) -> Result<()> {
    if config.submit_adapter_http_url.trim().is_empty()
        || contains_placeholder_value(&config.submit_adapter_http_url)
    {
        return Err(anyhow!(
            "execution.canary_tiny_submit_enabled requires submit_adapter_http_url"
        ));
    }
    if config.execution_signer_pubkey.trim().is_empty()
        || contains_placeholder_value(&config.execution_signer_pubkey)
    {
        return Err(anyhow!(
            "execution.canary_tiny_submit_enabled requires execution_signer_pubkey"
        ));
    }
    if config.execution_signer_keypair_path.trim().is_empty()
        || contains_placeholder_value(&config.execution_signer_keypair_path)
    {
        return Err(anyhow!(
            "execution.canary_tiny_submit_enabled requires execution_signer_keypair_path"
        ));
    }
    if !config.swap_transaction_dry_run_enabled {
        return Err(anyhow!(
            "execution.canary_tiny_submit_enabled requires swap_transaction_dry_run_enabled"
        ));
    }
    Ok(())
}

pub(crate) fn validate_live_ingestion_source_contract(source: &str, env: &str) -> Result<()> {
    let env_norm = env.trim().to_ascii_lowercase();
    let prod_like = matches!(
        env_norm.as_str(),
        "prod" | "production" | "live" | "prod-live"
    ) || env_norm.starts_with("prod-");
    if prod_like && source == "mock" {
        return Err(anyhow!(
            "ingestion.source=mock is not supported in production env {}; use yellowstone_grpc",
            env
        ));
    }
    Ok(())
}
