use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::{
    disable_shared_accounts, is_missing_account_simulation_error, simulation_error_text,
    swap_endpoint_url, swap_request_body,
};
use crate::execution_swap_http_retry::{is_missing_token_program_error, post_swap_json_with_retry};
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
    let url = swap_endpoint_url(
        &config.quote_canary_base_url,
        "swap-instructions",
        "swap-instructions",
    )?;
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let api_key = config.quote_canary_api_key.trim();
    let response = post_swap_json_with_retry(
        http,
        url.clone(),
        api_key,
        &body,
        timeout,
        "swap-instructions dry-run",
    )
    .await;
    let (url, api_key, response, fallback_used) = match response {
        Ok(response) => (url, api_key, response, false),
        Err(error) if should_use_public_builder_fallback(config, &error) => {
            let fallback_url = swap_endpoint_url(
                &config.quote_canary_public_base_url,
                "swap-instructions",
                "public swap-instructions fallback",
            )?;
            let fallback_response = post_swap_json_with_retry(
                http,
                fallback_url.clone(),
                "",
                &body,
                timeout,
                "public swap-instructions dry-run fallback",
            )
            .await?;
            (fallback_url, "", fallback_response, true)
        }
        Err(error) => return Err(error),
    };
    if is_missing_account_simulation_error(&response.value) {
        let mut no_shared_body = body.clone();
        disable_shared_accounts(&mut no_shared_body);
        let retry = post_swap_json_with_retry(
            http,
            url,
            api_key,
            &no_shared_body,
            timeout,
            "swap-instructions dry-run no-shared-accounts fallback",
        )
        .await?;
        return Ok(Some(swap_instructions_response_summary(
            retry.value,
            retry.elapsed_ms,
            retry.attempts,
            fallback_used,
            true,
        )?));
    }
    Ok(Some(swap_instructions_response_summary(
        response.value,
        response.elapsed_ms,
        response.attempts,
        fallback_used,
        false,
    )?))
}

fn swap_instructions_response_summary(
    value: Value,
    elapsed_ms: u64,
    attempts: usize,
    fallback_used: bool,
    shared_accounts_disabled: bool,
) -> Result<String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "swap-instructions dry-run error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    if let Some(error) = value
        .get("simulationError")
        .filter(|error| !error.is_null())
    {
        return Err(anyhow!(
            "swap-instructions dry-run simulation error: {}",
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
        if fallback_used && shared_accounts_disabled {
            "public_fallback_no_shared_accounts_ok"
        } else if fallback_used {
            "public_fallback_ok"
        } else if shared_accounts_disabled {
            "no_shared_accounts_ok"
        } else {
            "ok"
        },
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

fn should_use_public_builder_fallback(config: &ExecutionConfig, error: &anyhow::Error) -> bool {
    config.quote_canary_public_parallel_enabled
        && is_missing_token_program_error(error)
        && !config.quote_canary_public_base_url.trim().is_empty()
        && config.quote_canary_public_base_url.trim() != config.quote_canary_base_url.trim()
}

fn array_len(value: Option<&Value>) -> usize {
    value.and_then(Value::as_array).map(Vec::len).unwrap_or(0)
}
