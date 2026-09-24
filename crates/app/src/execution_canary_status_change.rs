//! Bounded tick telemetry status predicate.
use super::ExecutionCanaryTickSummary;

impl ExecutionCanaryTickSummary {
    pub(crate) fn has_status_change(&self) -> bool {
        self.source_sell_production.visits > 0
            || self.pre_submit_refusals.count() > 0
            || self.source_sell_write_off_refusals.count() > 0
            || self.source_sell_refusals.count() > 0
            || self.inserted > 0
            || self.existing > 0
            || self.skipped_reason.is_some()
            || self.quote_entry_inserted > 0
            || self.quote_entry_existing > 0
            || self.quote_entry_errors > 0
            || self.quote_close_inserted > 0
            || self.quote_close_existing > 0
            || self.quote_close_errors > 0
            || self.quote_would_execute > 0
            || self.quote_would_force_exit > 0
            || self.quote_would_skip > 0
            || self.quote_decision_unknown > 0
            || self.state_machine_reserved > 0
            || self.state_machine_existing > 0
            || self.state_machine_built > 0
            || self.state_machine_simulated > 0
            || self.state_machine_submit_disabled > 0
            || self.state_machine_failed > 0
            || self.state_machine_safety_blocked > 0
            || self.state_machine_entry_gate_blocked > 0
            || (self.orphan_recovery_checked > 0 && self.last_error.is_some())
            || self.orphan_recovery_recovered > 0
            || self.orphan_recovery_reconciled > 0
            || self.orphan_recovery_errors > 0
    }
}
