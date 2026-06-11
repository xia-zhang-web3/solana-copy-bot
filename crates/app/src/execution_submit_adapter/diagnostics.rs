use super::ExecutionTransactionPlan;
use crate::execution_build_plan_age::quote_age_ms_summary_field;
use crate::execution_pumpswap_error::{
    annotate_pumpswap_custom_errors, is_pump_fun_bonding_curve_not_found,
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

pub(super) fn execution_error_text_for_plan(
    plan: &ExecutionTransactionPlan,
    message: &str,
    max_len: usize,
) -> String {
    let text = if route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref()) {
        annotate_pumpswap_custom_errors(message)
    } else {
        message.to_string()
    };
    let quote_age = quote_age_ms_summary_field(&plan.metadata);
    let text_len = max_len.saturating_sub(quote_age.len()).max(1);
    format!("{}{}", truncate_for_log(&text, text_len), quote_age)
}
