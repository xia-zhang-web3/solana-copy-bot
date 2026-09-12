//! A single collected completion remains charged to the existing active ceiling.
use super::*;
use crate::execution_quote_canary::job::HotQuoteOrigin;

impl hot_quotes::HotQuotes {
    pub(crate) fn can_collect(&self) -> bool {
        self.completed.is_none() && self.active() > 0
    }
    pub(crate) async fn collect_next(&mut self) {
        self.completed = self.finish_next().await;
        #[cfg(test)]
        if let Some(completion) = &self.completed {
            crate::app_tests::b70_hooks::mark(
                "hot_quote_network_ready",
                &completion.origin.swap.signature,
            );
        }
    }
    pub(crate) fn take_completion(&mut self) -> Option<hot_quotes::Completion> {
        self.completed.take()
    }
    pub(crate) fn note_shadow_output(&mut self, output: &ShadowTaskOutput) {
        fn update(origin: &mut HotQuoteOrigin, output: &ShadowTaskOutput) {
            if output.signal_id.as_deref() != Some(origin.signal_id().as_str()) {
                return;
            }
            origin.shadow_finished = true;
            if let Ok(copybot_shadow::ShadowProcessOutcome::Recorded(signal)) = &output.outcome {
                origin.shadow_recorded = Some(signal.clone());
                origin.buy_receipt = output.buy_receipt.clone();
            }
        }
        for origin in self.origins.values_mut() {
            update(origin, output);
        }
        for job in &mut self.pending {
            update(&mut job.origin, output);
        }
        if let Some(completion) = &mut self.completed {
            update(&mut completion.origin, output);
        }
    }
}
impl ShadowScheduler {
    pub(crate) fn hot_completion_ready(&self) -> bool {
        let Some(completion) = &self.hot_quotes.completed else {
            return false;
        };
        let origin = &completion.origin;
        let key = ShadowTaskKey {
            wallet: origin.swap.wallet.clone(),
            token: origin.swap.token_out.clone(),
        };
        // An already-running Shadow can commit at any point before its join.
        // Await that worker's proof, without racing a separate DB existence read.
        // Unstarted Shadow cannot hold every active HTTP slot in a wait cycle.
        !origin.wait_for_shadow
            || origin.shadow_finished
            || !self.inflight_shadow_keys.contains(&key)
    }
}
