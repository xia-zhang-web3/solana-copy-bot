//! Main-loop lifecycle of quote-only work; never calls execution from completion.
use super::*;
use crate::shadow_scheduler::{hot_quotes::Completion, ShadowScheduler};
use copybot_shadow::FollowSnapshot;

impl ExecutionCanaryRunner {
    pub(crate) fn admit_hot_observed_buy_quote(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        now: DateTime<Utc>,
        scheduler: &mut ShadowScheduler,
    ) -> Result<()> {
        if self.strict_quotes
            || !self.config.canary_enabled
            || Path::new(&self.config.canary_kill_switch_path).exists()
        {
            return Ok(());
        }
        let Some(mut job) = self.quote_canary.prepare_hot_quote(store, swap, now)? else {
            return Ok(());
        };
        job.origin.wait_for_shadow = true;
        let id = job.origin.signal_id();
        let reason = if let Err(job) = scheduler.admit_hot_quote(job) {
            self.quote_canary
                .refuse_hot_quote(store, &job.origin, "hot_quote_capacity")?;
            "hot_quote_capacity"
        } else {
            "hot_quote_admitted"
        };
        crate::telemetry::hot_quote::record(
            &id,
            reason,
            scheduler.active_task_count(),
            scheduler.buffered_shadow_task_count(),
        );
        Ok(())
    }
    pub(crate) fn make_room_for_owned_or_shadow_sell(
        &self,
        store: &SqliteStore,
        scheduler: &mut ShadowScheduler,
    ) -> Result<()> {
        if scheduler.buffered_shadow_task_count() >= crate::SHADOW_PENDING_TASK_CAPACITY {
            if let Some(job) = scheduler.hot_quotes.evict_pending() {
                self.quote_canary.refuse_hot_quote(
                    store,
                    &job.origin,
                    "hot_quote_evicted_for_sell",
                )?;
                crate::telemetry::hot_quote::record(
                    &job.origin.signal_id(),
                    "hot_quote_evicted_for_sell",
                    scheduler.active_task_count(),
                    scheduler.buffered_shadow_task_count(),
                );
            }
        }
        Ok(())
    }
    pub(crate) fn complete_hot_observed_buy_quote(
        &self,
        store: &SqliteStore,
        mut completion: Completion,
        follow: &FollowSnapshot,
        strategy_closed: bool,
        risk: &mut crate::ShadowRiskGuard,
        stop: &crate::OperatorEmergencyStop,
        pause_new_trades_on_outage: bool,
        now: DateTime<Utc>,
        active: usize,
        pending: usize,
    ) -> Option<crate::execution_quote_canary::job::HotQuoteOrigin> {
        let gate = self.hot_buy_owner_gate(
            store,
            &completion.origin,
            follow,
            strategy_closed,
            risk,
            stop,
            pause_new_trades_on_outage,
            now,
        );
        let id = completion.origin.signal_id();
        let outcome = self.quote_canary.finish_hot_quote(
            store,
            &completion.origin,
            completion.output,
            now,
            gate,
        );
        let ready = matches!(
            outcome,
            Ok("hot_quote_recorded" | "hot_quote_priority_completed")
        );
        match outcome {
            Ok(reason) => crate::telemetry::hot_quote::record(&id, reason, active, pending),
            Err(error) => crate::telemetry::hot_quote::failure(
                &id,
                "hot_quote_completion_error",
                &error,
                active,
                pending,
            ),
        }
        #[cfg(test)]
        crate::app_tests::b70_hooks::mark("hot_quote_completed", &completion.origin.swap.signature);
        // Keep the claim until the separate owner step. Errors drop it immediately.
        if ready {
            match store.load_execution_quote_canary_event_by_id(&completion.origin.event_id()) {
                Ok(Some(event)) => {
                    completion.origin.saved_entry = Some(event);
                    return Some(completion.origin);
                }
                Err(error) => crate::telemetry::hot_quote::failure(
                    &id,
                    "hot_quote_completion_error",
                    &error,
                    active,
                    pending,
                ),
                Ok(None) => {}
            }
        }
        None
    }
}
