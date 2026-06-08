use crate::execution_canary::ExecutionCanaryTickSummary;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_quote_canary::ExecutionQuoteCanaryTickSummary;

pub(crate) fn apply_quote_summary(
    summary: &mut ExecutionCanaryTickSummary,
    quote: ExecutionQuoteCanaryTickSummary,
) {
    summary.quote_entry_candidates = quote.entry_candidates;
    summary.quote_entry_inserted = quote.entry_inserted;
    summary.quote_entry_existing = quote.entry_existing;
    summary.quote_entry_errors = quote.entry_errors;
    summary.quote_close_candidates = quote.close_candidates;
    summary.quote_close_inserted = quote.close_inserted;
    summary.quote_close_existing = quote.close_existing;
    summary.quote_close_errors = quote.close_errors;
    summary.quote_would_execute = quote.would_execute;
    summary.quote_would_force_exit = quote.would_force_exit;
    summary.quote_would_skip = quote.would_skip;
    summary.quote_decision_unknown = quote.decision_unknown;
    summary.last_quote_event_id = quote.last_event_id;
}

pub(crate) fn apply_state_machine_summary(
    summary: &mut ExecutionCanaryTickSummary,
    state: ExecutionCanaryStateMachineSummary,
) {
    summary.state_machine_reserved += state.reserved;
    summary.state_machine_existing += state.existing;
    summary.state_machine_built += state.built;
    summary.state_machine_simulated += state.simulated;
    summary.state_machine_submit_disabled += state.submit_disabled;
    summary.state_machine_failed += state.failed;
    summary.state_machine_safety_blocked += state.safety_blocked;
    summary.state_machine_entry_gate_blocked += state.entry_gate_blocked;
    if state.skipped_reason.is_some() {
        summary.state_machine_skipped_reason = state.skipped_reason;
    }
    summary.state_machine_open_positions = state.open_positions;
    summary.state_machine_daily_loss_sol = state.daily_loss_sol;
    if state.last_order_id.is_some() {
        summary.last_state_machine_order_id = state.last_order_id;
    }
}
