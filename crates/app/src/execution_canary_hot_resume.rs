//! One deferred canonical BUY, driven by the existing main-loop execution owner.
use super::*;
use crate::execution_quote_canary::job::HotQuoteOrigin;
use copybot_shadow::FollowSnapshot;

impl ExecutionCanaryRunner {
    pub(super) fn hot_buy_owner_gate(
        &self,
        store: &SqliteStore,
        origin: &HotQuoteOrigin,
        follow: &FollowSnapshot,
        strategy_closed: bool,
        risk: &mut crate::ShadowRiskGuard,
        stop: &crate::OperatorEmergencyStop,
        pause_new_trades_on_outage: bool,
        now: DateTime<Utc>,
    ) -> Option<&'static str> {
        let swap = &origin.swap;
        if strategy_closed {
            Some("hot_quote_publication_closed")
        } else if stop.is_active() {
            Some("hot_quote_operator_stop")
        } else if !follow.active.contains(&swap.wallet) {
            Some("hot_quote_source_changed")
        } else {
            match risk.can_complete_buy_quote(
                store,
                now,
                pause_new_trades_on_outage,
                swap,
                origin.buy_receipt.as_ref(),
            ) {
                crate::BuyRiskDecision::Allow => None,
                crate::BuyRiskDecision::Blocked { reason, .. } => Some(reason.as_str()),
            }
        }
    }

    pub(crate) async fn resume_hot_buy(
        &self,
        store: &SqliteStore,
        mut origin: HotQuoteOrigin,
        follow: &FollowSnapshot,
        strategy_closed: bool,
        risk: &mut crate::ShadowRiskGuard,
        stop: &crate::OperatorEmergencyStop,
        pause_new_trades_on_outage: bool,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let gate = self.hot_buy_owner_gate(
            store,
            &origin,
            follow,
            strategy_closed,
            risk,
            stop,
            pause_new_trades_on_outage,
            now,
        );
        if let Some(reason) = self
            .quote_canary
            .hot_origin_refusal(store, &origin, now, gate)?
        {
            self.quote_canary.refuse_hot_buy(store, &origin, reason)?;
            crate::telemetry::hot_quote::record(&origin.signal_id(), reason, 0, 0);
            return Ok(());
        }
        let expected = origin
            .saved_entry
            .as_ref()
            .context("hot BUY owner snapshot missing")?;
        store.complete_execution_quote_entry_owner(expected)?;
        let signal = origin.shadow_recorded.take();
        drop(origin); // Release only this canonical claim before the same runner resumes.
        if let Some(signal) = signal {
            let summary = self
                .process_recorded_shadow_signal(store, &signal, now)
                .await?;
            if summary.has_status_change() {
                crate::telemetry::record_execution_canary_shadow_signal(&summary, &signal);
            }
            #[cfg(test)]
            crate::app_tests::b70_hooks::mark("hot_buy_resumed", &signal.signal_id);
        }
        Ok(())
    }
}
