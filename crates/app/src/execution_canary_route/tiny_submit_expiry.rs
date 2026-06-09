use crate::execution_quote_canary_helpers::truncate_for_log;
use copybot_storage_core::ExecutionCanaryOrder;

pub(super) const TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED: &str = "submit_retry_budget_exhausted";

pub(super) fn retry_budget_exhausted_reason(order: &ExecutionCanaryOrder) -> String {
    let Some(last_reason) = order
        .simulation_error
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .filter(|reason| *reason != TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED)
    else {
        return TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED.to_string();
    };
    format!(
        "{}_after:{}",
        TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED,
        truncate_for_log(last_reason, 240)
    )
}
