use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_signing_envelope::validate_serialized_transaction_base64;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{
    disable_shared_accounts, is_missing_account_simulation_error,
    metis_fallback_swap_builder_endpoint, post_no_shared_skip_user_accounts_json_with_retry,
    primary_swap_builder_endpoint, public_fallback_swap_builder_endpoint, simulation_error_text,
    swap_request_body, SwapBuilderSource,
};
use crate::execution_swap_http_retry::{
    is_market_not_found_error, is_missing_token_program_error, post_swap_json_with_retry,
};
#[path = "execution_swap_transaction_builder_fallback.rs"]
mod builder_fallback;
use anyhow::{anyhow, Result};
use builder_fallback::{
    retry_metis_swap_transaction_builder, retry_public_swap_transaction_builder,
};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::Duration as StdDuration;

pub(crate) async fn fetch_swap_transaction_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<SwapTransactionDryRunResult>> {
    if !config.swap_transaction_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for swap transaction dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for swap transaction dry-run"))?;
    let body = swap_request_body(plan, user_pubkey, "swap transaction")?;
    let primary = primary_swap_builder_endpoint(config, plan, "swap", "swap transaction")?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let response = post_swap_json_with_retry(
        http,
        primary.url.clone(),
        &primary.api_key,
        &body,
        timeout,
        "swap transaction dry-run",
    )
    .await;
    let (endpoint, response) = match response {
        Ok(response) => (primary, response),
        Err(error) if should_use_public_builder_fallback(config, primary.source, &error) => {
            let fallback = public_fallback_swap_builder_endpoint(config, "swap", "public swap")?;
            let fallback_response = match post_swap_json_with_retry(
                http,
                fallback.url.clone(),
                &fallback.api_key,
                &body,
                timeout,
                "public swap transaction dry-run fallback",
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    return Err(error);
                }
            };
            (fallback, fallback_response)
        }
        Err(error) if should_use_metis_builder_fallback(config, primary.source, &error) => {
            let fallback = metis_fallback_swap_builder_endpoint(config, "swap", "metis swap")?;
            let fallback_response = match post_swap_json_with_retry(
                http,
                fallback.url.clone(),
                &fallback.api_key,
                &body,
                timeout,
                "metis swap transaction dry-run fallback",
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    return Err(error);
                }
            };
            (fallback, fallback_response)
        }
        Err(error) => {
            return Err(error);
        }
    };
    if is_missing_account_simulation_error(&response.value) {
        let mut no_shared_body = body.clone();
        disable_shared_accounts(&mut no_shared_body);
        let retry = post_swap_json_with_retry(
            http,
            endpoint.url.clone(),
            &endpoint.api_key,
            &no_shared_body,
            timeout,
            "swap transaction dry-run no-shared-accounts fallback",
        )
        .await?;
        if is_missing_account_simulation_error(&retry.value)
            && endpoint.source == SwapBuilderSource::Metis
        {
            let skip_retry = post_no_shared_skip_user_accounts_json_with_retry(
                http,
                &endpoint.url,
                &endpoint.api_key,
                &body,
                timeout,
                "swap transaction dry-run no-shared skip-user-accounts fallback",
            )
            .await?;
            if !is_missing_account_simulation_error(&skip_retry.value) {
                return Ok(Some(swap_transaction_response_summary(
                    skip_retry.value,
                    skip_retry.elapsed_ms,
                    skip_retry.attempts,
                    endpoint.source,
                    true,
                    true,
                )?));
            }
        }
        if is_missing_account_simulation_error(&retry.value)
            && can_use_public_builder_fallback(config, endpoint.source)
        {
            let result =
                retry_public_swap_transaction_builder(http, config, &body, timeout, "swap").await;
            return result;
        }
        if is_missing_account_simulation_error(&retry.value)
            && can_use_metis_builder_fallback(config, endpoint.source)
        {
            let result =
                retry_metis_swap_transaction_builder(http, config, &body, timeout, "swap").await;
            return result;
        }
        let result = swap_transaction_response_summary(
            retry.value,
            retry.elapsed_ms,
            retry.attempts,
            endpoint.source,
            true,
            false,
        )
        .map(Some);
        return result;
    }
    let result = swap_transaction_response_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
        endpoint.source,
        false,
        false,
    )
    .map(Some);
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SwapTransactionDryRunResult {
    pub(crate) summary: String,
    pub(crate) serialized_transaction_base64: String,
    pub(crate) source: String,
}

fn swap_transaction_response_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
    source: SwapBuilderSource,
    shared_accounts_disabled: bool,
    skip_user_accounts_rpc_calls: bool,
) -> Result<SwapTransactionDryRunResult> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap transaction dry-run error source={} shared_accounts_disabled={} skip_user_accounts_rpc_calls={}: {}",
            source.summary_tag(shared_accounts_disabled),
            shared_accounts_disabled,
            skip_user_accounts_rpc_calls,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let swap_transaction = value
        .get("swapTransaction")
        .and_then(Value::as_str)
        .filter(|item| !item.trim().is_empty())
        .ok_or_else(|| anyhow!("swap transaction dry-run missing swapTransaction"))?;
    validate_serialized_transaction_base64(swap_transaction)?;
    let simulation_error = simulation_error_text(&value).map(|item| truncate_for_log(&item, 180));
    let payload_source = payload_source(
        source,
        shared_accounts_disabled,
        skip_user_accounts_rpc_calls,
    );
    let summary = format!(
        "metis_swap_transaction_{} base64_len={} serialized_transaction_base64_ready=true latency_ms={} attempts={} skip_user_accounts_rpc_calls={} simulation_error={}",
        source.summary_tag(shared_accounts_disabled),
        swap_transaction.len(),
        elapsed_ms,
        attempts,
        skip_user_accounts_rpc_calls,
        simulation_error.unwrap_or_else(|| "none".to_string())
    );
    Ok(SwapTransactionDryRunResult {
        summary: truncate_for_log(&summary, 500),
        serialized_transaction_base64: swap_transaction.to_string(),
        source: payload_source.to_string(),
    })
}

fn payload_source(
    source: SwapBuilderSource,
    shared_accounts_disabled: bool,
    skip_user_accounts_rpc_calls: bool,
) -> String {
    let base = source.payload_source(shared_accounts_disabled);
    if skip_user_accounts_rpc_calls {
        format!("{base}_skip_user_accounts")
    } else {
        base.to_string()
    }
}

fn should_use_public_builder_fallback(
    config: &ExecutionConfig,
    source: SwapBuilderSource,
    error: &anyhow::Error,
) -> bool {
    can_use_public_builder_fallback(config, source)
        && (is_missing_token_program_error(error) || is_market_not_found_error(error))
}

fn should_use_metis_builder_fallback(
    config: &ExecutionConfig,
    source: SwapBuilderSource,
    error: &anyhow::Error,
) -> bool {
    can_use_metis_builder_fallback(config, source)
        && (is_missing_token_program_error(error) || is_market_not_found_error(error))
}

fn can_use_public_builder_fallback(config: &ExecutionConfig, source: SwapBuilderSource) -> bool {
    source == SwapBuilderSource::Metis
        && config.quote_canary_public_parallel_enabled
        && !config.quote_canary_public_base_url.trim().is_empty()
        && config.quote_canary_public_base_url.trim() != config.quote_canary_base_url.trim()
}

fn can_use_metis_builder_fallback(config: &ExecutionConfig, source: SwapBuilderSource) -> bool {
    source == SwapBuilderSource::PublicSelected
        && config.quote_canary_public_parallel_enabled
        && !config.quote_canary_base_url.trim().is_empty()
        && !config.quote_canary_public_base_url.trim().is_empty()
        && config.quote_canary_public_base_url.trim() != config.quote_canary_base_url.trim()
}
