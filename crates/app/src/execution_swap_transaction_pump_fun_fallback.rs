use crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_http_request::is_missing_account_error_text;
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;
use serde_json::Value;

pub(crate) async fn retry_on_pump_fun_amm_builder_error(
    generic_result: Result<Option<SwapTransactionDryRunResult>>,
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<SwapTransactionDryRunResult>> {
    let generic_error = match generic_result {
        Ok(result) => return Ok(result),
        Err(error) => error,
    };
    let generic_message = generic_error.to_string();
    if !should_retry_pump_fun_amm(config, plan, &generic_message) {
        return Err(generic_error);
    }
    match fetch_pump_fun_swap_transaction_dry_run(http, config, plan).await {
        Ok(Some(result)) => Ok(Some(result)),
        Ok(None) => Err(anyhow!(
            "generic Pump.fun AMM transaction builder failed; pump.fun paid fallback returned no transaction; generic error: {}",
            generic_message
        )),
        Err(pump_error) => Err(anyhow!(
            "generic Pump.fun AMM transaction builder failed; pump.fun paid fallback failed: {}; generic error: {}",
            pump_error,
            generic_message
        )),
    }
}

fn should_retry_pump_fun_amm(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    generic_error: &str,
) -> bool {
    config.quote_canary_pump_fun_parallel_enabled
        && retryable_pump_fun_amm_builder_error(generic_error)
        && route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref())
}

fn retryable_pump_fun_amm_builder_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    is_missing_account_error_text(error)
        || lower.contains("missing token program")
        || lower.contains("market_not_found")
        || (lower.contains("market") && lower.contains("not found"))
        || lower.contains("no_routes_found")
        || lower.contains("no routes found")
}

fn route_plan_has_pump_fun_amm(route_plan_json: Option<&str>) -> bool {
    let Some(raw) = route_plan_json.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return false;
    };
    value.as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item.pointer("/swapInfo/label")
                .and_then(Value::as_str)
                .is_some_and(|label| label.eq_ignore_ascii_case("Pump.fun Amm"))
        })
    })
}
