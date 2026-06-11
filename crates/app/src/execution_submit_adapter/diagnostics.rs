use super::ExecutionTransactionPlan;
use crate::execution_build_plan_age::quote_age_ms_summary_field;
use crate::execution_pumpswap_error::{
    is_pump_fun_bonding_curve_not_found, pumpswap_custom_errors_summary_field,
};
use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_route_plan::route_plan_has_pump_fun_amm;
use crate::telemetry::format_error_chain;

pub(super) fn should_try_pump_fun_paid_sell_migration_fallback(
    plan: &ExecutionTransactionPlan,
    error: &anyhow::Error,
) -> bool {
    plan.side.eq_ignore_ascii_case("sell")
        && is_pump_fun_bonding_curve_not_found(&format_error_chain(error))
}

pub(super) fn execution_error_for_plan(
    plan: &ExecutionTransactionPlan,
    error: &anyhow::Error,
    max_len: usize,
) -> String {
    execution_error_text_for_plan(plan, &format_error_chain(error), max_len)
}

pub(crate) fn pumpswap_direct_error_for_plan(
    plan: &ExecutionTransactionPlan,
    error: &anyhow::Error,
    max_len: usize,
) -> String {
    execution_error_text_with_pamm_context(
        plan,
        &format_error_chain(error),
        max_len,
        route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref())
            || route_plan_has_pump_fun_amm(plan.entry_route_plan_json.as_deref()),
    )
}

pub(crate) fn execution_error_text_for_plan(
    plan: &ExecutionTransactionPlan,
    message: &str,
    max_len: usize,
) -> String {
    execution_error_text_with_pamm_context(
        plan,
        message,
        max_len,
        route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref()),
    )
}

fn execution_error_text_with_pamm_context(
    plan: &ExecutionTransactionPlan,
    message: &str,
    max_len: usize,
    include_pamm_errors: bool,
) -> String {
    let pamm_errors = if include_pamm_errors {
        pumpswap_custom_errors_summary_field(message).unwrap_or_default()
    } else {
        String::new()
    };
    let quote_age = quote_age_ms_summary_field(&plan.metadata);
    let suffix_len = pamm_errors.len().saturating_add(quote_age.len());
    let text_len = max_len.saturating_sub(suffix_len).max(1);
    let text = truncate_for_log(message, text_len);
    let pamm_errors = if text.contains("pamm_custom_errors=") {
        ""
    } else {
        pamm_errors.as_str()
    };
    format!("{text}{pamm_errors}{quote_age}")
}
