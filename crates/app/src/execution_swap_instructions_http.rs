use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{
    disable_shared_accounts, is_missing_account_simulation_error, primary_swap_builder_endpoint,
    simulation_error_text, swap_request_body, SwapBuilderSource,
};
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::Duration as StdDuration;

pub(crate) async fn fetch_swap_instructions_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<String>> {
    if !config.swap_instructions_dry_run_enabled {
        return Ok(None);
    }
    let Some(blueprint) = plan.swap_blueprint.as_ref() else {
        return Err(anyhow!(
            "missing swap blueprint for swap-instructions dry-run"
        ));
    };
    let user_pubkey = blueprint
        .wallet_pubkey
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing user public key for swap-instructions dry-run"))?;
    let body = swap_request_body(plan, user_pubkey, "swap-instructions")?;
    let primary =
        primary_swap_builder_endpoint(config, plan, "swap-instructions", "swap-instructions")?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let response = post_swap_json_with_retry(
        http,
        primary.url.clone(),
        &primary.api_key,
        &body,
        timeout,
        "swap-instructions dry-run",
    )
    .await;
    let (endpoint, response) = match response {
        Ok(response) => (primary, response),
        Err(error) => return Err(error),
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
            "swap-instructions dry-run no-shared-accounts fallback",
        )
        .await?;
        return Ok(Some(swap_instructions_response_summary(
            retry.value,
            retry.elapsed_ms,
            retry.attempts,
            endpoint.source,
            true,
        )?));
    }
    Ok(Some(swap_instructions_response_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
        endpoint.source,
        false,
    )?))
}

fn swap_instructions_response_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
    source: SwapBuilderSource,
    shared_accounts_disabled: bool,
) -> Result<String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap-instructions dry-run error source={} shared_accounts_disabled={}: {}",
            source.summary_tag(shared_accounts_disabled),
            shared_accounts_disabled,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if let Some(error) = value
        .get("simulationError")
        .filter(|error| !error.is_null())
    {
        return Err(anyhow!(
            "swap-instructions dry-run simulation error source={} shared_accounts_disabled={}: {}",
            source.summary_tag(shared_accounts_disabled),
            shared_accounts_disabled,
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if value
        .get("swapInstruction")
        .filter(|item| item.is_object())
        .is_none()
    {
        return Err(anyhow!("swap-instructions dry-run missing swapInstruction"));
    }
    let simulation_error = simulation_error_text(&value).map(|item| truncate_for_log(&item, 180));
    let summary = format!(
        "metis_swap_instructions_{} compute={} setup={} other={} alt={} cleanup={} latency_ms={} attempts={} simulation_error={}",
        source.summary_tag(shared_accounts_disabled),
        array_len(value.get("computeBudgetInstructions")),
        array_len(value.get("setupInstructions")),
        array_len(value.get("otherInstructions")),
        array_len(value.get("addressLookupTableAddresses")),
        value.get("cleanupInstruction")
            .filter(|item| !item.is_null())
            .is_some(),
        elapsed_ms,
        attempts,
        simulation_error.unwrap_or_else(|| "none".to_string())
    );
    Ok(truncate_for_log(&summary, 500))
}

fn array_len(value: Option<&Value>) -> usize {
    value.and_then(Value::as_array).map(Vec::len).unwrap_or(0)
}
