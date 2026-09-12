use crate::execution_canary_quote_pnl_compute::compute_quote_pnl;
use crate::{ExecutableWalletFeedback, SqliteDiscoveryStore};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

impl SqliteDiscoveryStore {
    /// Compatibility entry point: all retained closes since `since`.
    pub fn executable_wallet_feedback_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, ExecutableWalletFeedback>> {
        self.executable_wallet_feedback_window(since, None)
    }

    pub fn executable_wallet_feedback_as_of(
        &self,
        since: DateTime<Utc>,
        as_of: DateTime<Utc>,
    ) -> Result<HashMap<String, ExecutableWalletFeedback>> {
        self.executable_wallet_feedback_window(since, Some(as_of))
    }

    fn executable_wallet_feedback_window(
        &self,
        since: DateTime<Utc>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<HashMap<String, ExecutableWalletFeedback>> {
        if !self.sqlite_table_exists("shadow_closed_trades")?
            || !self.sqlite_table_exists("execution_quote_canary_events")?
        {
            return Ok(HashMap::new());
        }
        let mut feedback = HashMap::<String, ExecutableWalletFeedback>::new();
        for row in self.quote_fee_cohort_rows(since, as_of, None)? {
            // A proven skipped entry is counterfactual only. Ambiguous/missing
            // bindings cannot silently vanish from the unknown denominator.
            if row.entry_attributed && row.buy.decision_status.as_deref() == Some("would_skip") {
                continue;
            }
            let supported = row.binding_error.is_none()
                && row.buy.quote_status.as_deref() == Some("ok")
                && row.sell.quote_status.as_deref() == Some("ok")
                && row.buy.decision_status.as_deref() == Some("would_execute")
                && matches!(
                    row.sell.decision_status.as_deref(),
                    Some("would_execute" | "would_force_exit")
                );
            let net = if supported {
                compute_quote_pnl(row.amounts())?
                    .and_then(|p| p.quote_adjusted_pnl_after_priority_fee_sol)
            } else {
                None
            };
            feedback
                .entry(row.wallet_id)
                .or_default()
                .record(row.shadow_pnl_sol, net);
        }
        Ok(feedback)
    }
}
