use super::tiny_submit_wallet_balance::fetch_wallet_token_balance;
use crate::execution_pump_fun_quote_http::fetch_pump_fun_quote_sample;
use crate::execution_quote_canary_helpers::{
    price_sol_per_token, quote_canary_slippage_limit_bps, ui_amount_to_raw_string,
    DECISION_WOULD_EXECUTE, DECISION_WOULD_FORCE_EXIT, QUOTE_STATUS_OK, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use crate::execution_quote_provider_selection::{
    QUOTE_SOURCE_GENERIC_METIS, QUOTE_SOURCE_PUMP_FUN_PAID,
};
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ExecutionCanaryOwnedPosition, SqliteStore};
use serde_json::Value;

pub(crate) async fn owned_position_sell_metadata(
    config: &ExecutionConfig,
    store: &SqliteStore,
    token: &str,
    metadata: ExecutionBuildPlanMetadata,
) -> Result<ExecutionBuildPlanMetadata> {
    let position = store
        .load_execution_canary_open_position(token)?
        .ok_or_else(|| anyhow!("missing owned execution canary position for sell {token}"))?;
    let http = reqwest::Client::new();
    let amount_raw = owned_position_amount_raw(config, &http, token, &position).await?;
    let prefer_pump_fun = metadata.quote_source.as_deref() == Some(QUOTE_SOURCE_PUMP_FUN_PAID);

    if prefer_pump_fun {
        if let Some(pump_fun_metadata) =
            pump_fun_owned_sell_metadata(config, &http, token, &position, &amount_raw, &metadata)
                .await?
        {
            return Ok(pump_fun_metadata);
        }
    }

    let fallback_metadata = metadata.clone();
    match generic_owned_sell_metadata(config, &http, token, &position, &amount_raw, metadata).await
    {
        Ok(metadata) => Ok(metadata),
        Err(generic_error) if !prefer_pump_fun => {
            match pump_fun_owned_sell_metadata(
                config,
                &http,
                token,
                &position,
                &amount_raw,
                &fallback_metadata,
            )
            .await
            {
                Ok(Some(metadata)) => Ok(metadata),
                Ok(None) => Err(generic_error),
                Err(pump_fun_error) => Err(anyhow!(
                    "owned sell generic quote failed: {generic_error}; pump.fun owned sell fallback failed: {pump_fun_error}"
                )),
            }
        }
        Err(error) => Err(error),
    }
}

pub(super) fn validate_tiny_sell_metadata(
    metadata: &ExecutionBuildPlanMetadata,
) -> Option<&'static str> {
    if metadata.quote_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some("sell_quote_not_ok");
    }
    match metadata.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE) | Some(DECISION_WOULD_FORCE_EXIT) => {}
        _ => return Some("sell_quote_not_executable"),
    }
    if metadata.quote_event_id.as_deref().is_none_or(str::is_empty) {
        return Some("missing_quote_event_id");
    }
    if metadata
        .quote_in_amount_raw
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_quote_in_amount_raw");
    }
    if metadata
        .quote_out_amount_raw
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_quote_out_amount_raw");
    }
    if metadata
        .quote_price_sol
        .is_none_or(|price| !price.is_finite() || price <= 0.0)
    {
        return Some("missing_quote_price_sol");
    }
    if metadata
        .route_plan_json
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Some("missing_route_plan_json");
    }
    if metadata.priority_fee_status.as_deref() != Some(QUOTE_STATUS_OK) {
        return Some("priority_fee_not_ok");
    }
    if metadata.priority_fee_lamports.is_none() {
        return Some("missing_priority_fee_lamports");
    }
    None
}

async fn generic_owned_sell_metadata(
    config: &ExecutionConfig,
    http: &reqwest::Client,
    token: &str,
    position: &ExecutionCanaryOwnedPosition,
    amount_raw: &str,
    metadata: ExecutionBuildPlanMetadata,
) -> Result<ExecutionBuildPlanMetadata> {
    let quote = fetch_quote_sample(
        http,
        config,
        token,
        SOL_MINT,
        amount_raw,
        quote_canary_slippage_limit_bps(config, SIDE_SELL),
    )
    .await?;
    apply_owned_sell_quote_metadata(metadata, quote, position, QUOTE_SOURCE_GENERIC_METIS)
}

async fn pump_fun_owned_sell_metadata(
    config: &ExecutionConfig,
    http: &reqwest::Client,
    token: &str,
    position: &ExecutionCanaryOwnedPosition,
    amount_raw: &str,
    metadata: &ExecutionBuildPlanMetadata,
) -> Result<Option<ExecutionBuildPlanMetadata>> {
    if !config.quote_canary_pump_fun_parallel_enabled {
        return Ok(None);
    }
    let quote = fetch_pump_fun_quote_sample(http, config, SIDE_SELL, token, amount_raw).await?;
    if pump_fun_quote_is_completed(&quote.response_json) != Some(false) {
        return Ok(None);
    }
    apply_owned_sell_quote_metadata(
        metadata.clone(),
        quote,
        position,
        QUOTE_SOURCE_PUMP_FUN_PAID,
    )
    .map(Some)
}

fn apply_owned_sell_quote_metadata(
    mut metadata: ExecutionBuildPlanMetadata,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
    position: &ExecutionCanaryOwnedPosition,
    quote_source: &str,
) -> Result<ExecutionBuildPlanMetadata> {
    let out_lamports = quote
        .out_amount
        .parse::<u64>()
        .with_context(|| format!("invalid owned sell quote outAmount {}", quote.out_amount))?;
    metadata.quote_source = Some(quote_source.to_string());
    metadata.quote_status = Some(QUOTE_STATUS_OK.to_string());
    metadata.quote_in_amount_raw = Some(quote.in_amount);
    metadata.quote_out_amount_raw = Some(quote.out_amount);
    metadata.quote_response_json = Some(quote.response_json);
    metadata.quote_price_sol =
        price_sol_per_token(out_lamports as f64 / 1_000_000_000.0, position.qty);
    metadata.price_impact_pct = quote.price_impact_pct;
    metadata.route_plan_json = quote.route_plan_json;
    Ok(metadata)
}

async fn owned_position_amount_raw(
    config: &ExecutionConfig,
    http: &reqwest::Client,
    token: &str,
    position: &ExecutionCanaryOwnedPosition,
) -> Result<String> {
    let wallet = fetch_wallet_token_balance(http, config, token)
        .await?
        .ok_or_else(|| anyhow!("owned sell wallet token account missing for {token}"))?;
    if wallet.raw == 0 {
        return Err(anyhow!(
            "owned sell wallet token balance is zero for {token}"
        ));
    }
    let position_raw = if let Some(qty) = position.qty_exact {
        if qty.decimals() != wallet.decimals {
            return Err(anyhow!(
                "owned sell wallet decimals mismatch for {token}: position={} wallet={}",
                qty.decimals(),
                wallet.decimals
            ));
        }
        qty.raw()
    } else {
        let decimals = resolve_spl_token_decimals(http, config, token, Some(wallet.decimals))
            .await
            .unwrap_or(wallet.decimals);
        let raw = ui_amount_to_raw_string(position.qty, decimals).ok_or_else(|| {
            anyhow!(
                "invalid owned sell qty {} decimals {decimals}",
                position.qty
            )
        })?;
        raw.parse::<u64>()
            .with_context(|| format!("invalid owned sell raw amount {raw} for {token}"))?
    };
    let amount_raw = position_raw.min(wallet.raw);
    if amount_raw == 0 {
        return Err(anyhow!("owned sell amount is zero for {token}"));
    }
    Ok(amount_raw.to_string())
}

fn pump_fun_quote_is_completed(raw: &str) -> Option<bool> {
    serde_json::from_str::<Value>(raw).ok().and_then(|value| {
        value
            .pointer("/quote/meta/isCompleted")
            .and_then(Value::as_bool)
    })
}
