//! Continuity gates new financial actions; submitted obligations still reconcile.
use super::{ExecutionCanaryRunner, ExecutionCanaryTickSummary};
use anyhow::Result;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
impl ExecutionCanaryRunner {
    pub(crate) fn with_ingress_hold(mut self, hold: Option<Arc<AtomicBool>>) -> Self {
        // An HTTP-configured runner starts held. Missing wiring cannot open it.
        if let Some(hold) = hold {
            self.ingress_hold = Some(hold.clone());
            self.quote_canary = self.quote_canary.with_ingress_hold(hold);
        }
        self
    }
    pub(super) fn ingress_pending(&self) -> bool {
        self.ingress_hold.as_ref().is_some_and(|h| h.load(Ordering::SeqCst))
    }
    pub(super) fn reconcile_owned_sell(&self, summary: &mut ExecutionCanaryTickSummary) -> Result<()> {
        if let Some(recovery) = self.owned_sell_recovery.as_ref() {
            if let Some(done) = recovery.tick(&self.config)? {
                summary.orphan_recovery_checked = done.checked;
                summary.orphan_recovery_reconciled = done.reconciled;
                summary.last_error = done.pending_reason;
            }
        }
        Ok(())
    }
}
