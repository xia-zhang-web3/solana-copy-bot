use super::tiny_submit_wallet_balance::fetch_wallet_token_balance;
use crate::execution_pump_fun_quote_http::fetch_pump_fun_quote_sample;
use crate::execution_quote_canary_helpers::{
    price_sol_per_token, quote_canary_slippage_limit_bps, DECISION_WOULD_EXECUTE,
    DECISION_WOULD_FORCE_EXIT, QUOTE_STATUS_OK, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_http::fetch_quote_sample;
use crate::execution_quote_provider_selection::{
    QUOTE_SOURCE_GENERIC_METIS, QUOTE_SOURCE_PUMP_FUN_PAID,
};
use crate::execution_source_sell_guard::{self as source_guard, Snapshot};
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
    guarded_owned_position_sell_metadata(config, store, token, metadata, None).await
}

pub(crate) async fn guarded_owned_position_sell_metadata(
    config: &ExecutionConfig,
    store: &SqliteStore,
    token: &str,
    metadata: ExecutionBuildPlanMetadata,
    source: Option<&Snapshot>,
) -> Result<ExecutionBuildPlanMetadata> {
    source_guard::recheck(source, store)?;
    let position = store
        .load_execution_canary_open_position(token)?
        .ok_or_else(|| anyhow!("missing owned execution canary position for sell {token}"))?;
    let selection = source_guard::amount::Selection::new(&position)?;
    let http = reqwest::Client::new();
    let wallet = fetch_wallet_token_balance(&http, config, token).await;
    source_guard::recheck(source, store)?;
    selection.recheck(store)?;
    let wallet =
        wallet?.ok_or_else(|| anyhow!("owned sell wallet token account missing for {token}"))?;
    let selected_raw = selection.amount(wallet.raw, wallet.decimals)?;
    let amount_raw = selected_raw.to_string();
    let finish = |m| {
        selection.finish(
            m,
            source,
            &config.canary_wallet_pubkey,
            wallet.raw,
            selected_raw,
        )
    };
    let prefer_pump_fun = metadata.quote_source.as_deref() == Some(QUOTE_SOURCE_PUMP_FUN_PAID);
    let mut pump_fun_preferred_error = None;

    if prefer_pump_fun {
        let result =
            pump_fun_owned_sell_metadata(config, &http, token, &position, &amount_raw, &metadata)
                .await;
        source_guard::recheck(source, store)?;
        selection.recheck(store)?;
        match result {
            Ok(Some(pump_fun_metadata)) => return finish(pump_fun_metadata),
            Ok(None) => {}
            Err(error) => pump_fun_preferred_error = Some(error),
        }
    }

    let fallback_metadata = metadata.clone();
    let result =
        generic_owned_sell_metadata(config, &http, token, &position, &amount_raw, metadata).await;
    source_guard::recheck(source, store)?;
    selection.recheck(store)?;
    match result {
        Ok(metadata) => finish(metadata),
        Err(generic_error) if !prefer_pump_fun => {
            let result = pump_fun_owned_sell_metadata(
                config,
                &http,
                token,
                &position,
                &amount_raw,
                &fallback_metadata,
            )
            .await;
            source_guard::recheck(source, store)?;
            selection.recheck(store)?;
            match result {
                Ok(Some(metadata)) => finish(metadata),
                Ok(None) => Err(generic_error),
                Err(pump_fun_error) => Err(anyhow!(
                    "owned sell generic quote failed: {generic_error}; pump.fun owned sell fallback failed: {pump_fun_error}"
                )),
            }
        }
        Err(generic_error) => {
            if let Some(pump_fun_error) = pump_fun_preferred_error {
                Err(anyhow!(
                    "owned sell pump.fun preferred quote failed: {pump_fun_error}; generic owned sell fallback failed: {generic_error}"
                ))
            } else {
                Err(generic_error)
            }
        }
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
    if crate::execution_priority_fee::metadata_fee(metadata).is_err() {
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
    metadata.http_request_started_ts = quote.http_request_started_ts;
    metadata.quote_response_available_ts = quote.quote_response_available_ts;
    metadata.quote_response_json = Some(quote.response_json);
    metadata.quote_price_sol =
        price_sol_per_token(out_lamports as f64 / 1_000_000_000.0, position.qty);
    metadata.price_impact_pct = quote.price_impact_pct;
    metadata.route_plan_json = quote.route_plan_json;
    Ok(metadata)
}

fn pump_fun_quote_is_completed(raw: &str) -> Option<bool> {
    serde_json::from_str::<Value>(raw).ok().and_then(|value| {
        value
            .pointer("/quote/meta/isCompleted")
            .and_then(Value::as_bool)
    })
}
