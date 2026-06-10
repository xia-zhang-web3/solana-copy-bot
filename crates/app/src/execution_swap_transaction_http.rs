use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_signing_envelope::validate_serialized_transaction_base64;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{
    disable_shared_accounts, is_missing_account_simulation_error,
    post_no_shared_skip_user_accounts_json_with_retry,
    post_no_shared_skip_user_accounts_static_cu_json_with_retry, primary_swap_builder_endpoint,
    simulation_error_text, swap_request_body, SwapBuilderSource,
};
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use crate::execution_transaction_rpc_simulation::verify_serialized_transaction_rpc_simulation;
use anyhow::{anyhow, Result};
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
    let mut body = swap_request_body(plan, user_pubkey, "swap transaction")?;
    disable_shared_accounts(&mut body);
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
                return Ok(Some(
                    verified_swap_transaction_response_summary(
                        http,
                        config,
                        skip_retry.value,
                        skip_retry.elapsed_ms,
                        skip_retry.attempts,
                        endpoint.source,
                        true,
                        true,
                        true,
                        timeout,
                    )
                    .await?,
                ));
            }
            let static_cu_retry = post_no_shared_skip_user_accounts_static_cu_json_with_retry(
                http,
                &endpoint.url,
                &endpoint.api_key,
                &body,
                timeout,
                "swap transaction dry-run no-shared skip-user-accounts static-cu fallback",
            )
            .await?;
            if !is_missing_account_simulation_error(&static_cu_retry.value) {
                return Ok(Some(
                    verified_swap_transaction_response_summary(
                        http,
                        config,
                        static_cu_retry.value,
                        static_cu_retry.elapsed_ms,
                        static_cu_retry.attempts,
                        endpoint.source,
                        true,
                        true,
                        false,
                        timeout,
                    )
                    .await?,
                ));
            }
            return Ok(Some(
                verified_swap_transaction_response_summary(
                    http,
                    config,
                    static_cu_retry.value,
                    static_cu_retry.elapsed_ms,
                    static_cu_retry.attempts,
                    endpoint.source,
                    true,
                    true,
                    false,
                    timeout,
                )
                .await?,
            ));
        }
        return Ok(Some(
            verified_swap_transaction_response_summary(
                http,
                config,
                retry.value,
                retry.elapsed_ms,
                retry.attempts,
                endpoint.source,
                true,
                false,
                true,
                timeout,
            )
            .await?,
        ));
    }
    Ok(Some(
        verified_swap_transaction_response_summary(
            http,
            config,
            response.value,
            response.elapsed_ms,
            response.attempts,
            endpoint.source,
            true,
            false,
            true,
            timeout,
        )
        .await?,
    ))
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
    dynamic_compute_unit_limit: bool,
) -> Result<SwapTransactionDryRunResult> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap transaction dry-run error source={} shared_accounts_disabled={} skip_user_accounts_rpc_calls={} dynamic_compute_unit_limit={}: {}",
            source.summary_tag(shared_accounts_disabled),
            shared_accounts_disabled,
            skip_user_accounts_rpc_calls,
            dynamic_compute_unit_limit,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if let Some(error) = simulation_error_text(&value) {
        return Err(anyhow!(
            "swap transaction dry-run simulation error source={} shared_accounts_disabled={} skip_user_accounts_rpc_calls={} dynamic_compute_unit_limit={}: {}",
            source.summary_tag(shared_accounts_disabled),
            shared_accounts_disabled,
            skip_user_accounts_rpc_calls,
            dynamic_compute_unit_limit,
            truncate_for_log(&error, 240)
        ));
    }
    let swap_transaction = value
        .get("swapTransaction")
        .and_then(Value::as_str)
        .filter(|item| !item.trim().is_empty())
        .ok_or_else(|| anyhow!("swap transaction dry-run missing swapTransaction"))?;
    validate_serialized_transaction_base64(swap_transaction)?;
    let payload_source = payload_source(
        source,
        shared_accounts_disabled,
        skip_user_accounts_rpc_calls,
        dynamic_compute_unit_limit,
    );
    let summary = format!(
        "metis_swap_transaction_{} base64_len={} serialized_transaction_base64_ready=true latency_ms={} attempts={} skip_user_accounts_rpc_calls={} dynamic_compute_unit_limit={} simulation_error={}",
        source.summary_tag(shared_accounts_disabled),
        swap_transaction.len(),
        elapsed_ms,
        attempts,
        skip_user_accounts_rpc_calls,
        dynamic_compute_unit_limit,
        "none"
    );
    Ok(SwapTransactionDryRunResult {
        summary: truncate_for_log(&summary, 500),
        serialized_transaction_base64: swap_transaction.to_string(),
        source: payload_source.to_string(),
    })
}

async fn verified_swap_transaction_response_summary(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
    source: SwapBuilderSource,
    shared_accounts_disabled: bool,
    skip_user_accounts_rpc_calls: bool,
    dynamic_compute_unit_limit: bool,
    timeout: StdDuration,
) -> Result<SwapTransactionDryRunResult> {
    let result = swap_transaction_response_summary(
        value,
        elapsed_ms,
        attempts,
        source,
        shared_accounts_disabled,
        skip_user_accounts_rpc_calls,
        dynamic_compute_unit_limit,
    )?;
    verify_rpc_simulation(http, config, result, timeout).await
}

async fn verify_rpc_simulation(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    mut result: SwapTransactionDryRunResult,
    timeout: StdDuration,
) -> Result<SwapTransactionDryRunResult> {
    verify_serialized_transaction_rpc_simulation(
        http,
        config,
        &result.serialized_transaction_base64,
        &result.source,
        timeout,
    )
    .await?;
    result.summary = truncate_for_log(&format!("{} rpc_simulation=passed", result.summary), 500);
    Ok(result)
}

fn payload_source(
    source: SwapBuilderSource,
    shared_accounts_disabled: bool,
    skip_user_accounts_rpc_calls: bool,
    dynamic_compute_unit_limit: bool,
) -> String {
    let base = source.payload_source(shared_accounts_disabled);
    let mut value = if skip_user_accounts_rpc_calls {
        format!("{base}_skip_user_accounts")
    } else {
        base.to_string()
    };
    if !dynamic_compute_unit_limit {
        value.push_str("_static_cu");
    }
    value
}
