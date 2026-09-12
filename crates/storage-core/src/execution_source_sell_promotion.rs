use crate::{
    source_sell_intent_rows, source_sell_promotion_rows as rows, source_sell_validation,
    ExecutionSourceSellPromotion, ExecutionSourceSellPromotionOutcome as Outcome,
    ExecutionSourceSellPromotionReject as Reject, SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;

impl SqliteDiscoveryStore {
    /// Atomic, explicit promotion only. No runtime producer is connected yet.
    pub fn promote_execution_source_sell_intent(&self, intent_id: &str) -> Result<Outcome> {
        self.with_immediate_transaction_retry("promote source SELL intent", |conn| {
            let reject = |reason| Ok(Outcome::Rejected(reason));
            let Some(staged) = source_sell_intent_rows::load(conn, intent_id)? else {
                return reject(Reject::StagedMissing);
            };
            let signal = rows::signal(&staged)?;
            let binding = rows::load(conn, Some(&signal.signal_id), Some(intent_id))?;
            if binding
                .as_ref()
                .is_some_and(|b| b.signal_id != signal.signal_id || b.intent_id != intent_id)
            {
                return reject(Reject::BindingConflict);
            }
            // These methods read/write self.conn, the same IMMEDIATE transaction.
            let saved = self.load_copy_signal_by_signal_id(&signal.signal_id)?;
            match (&binding, &saved) {
                (None, Some(_)) => return reject(Reject::SignalAlreadyExists),
                (Some(_), None) => return reject(Reject::SignalMissing),
                (Some(_), Some(old)) if !rows::same_identity(old, &signal) => {
                    return reject(Reject::SignalConflict)
                }
                _ => {}
            }
            if let Some(reason) = source_sell_validation::revalidate(self, conn, &staged)? {
                return reject(Reject::Validation(reason));
            }
            if let Some(binding) = binding {
                // Existing is history only. Never reset status or quote/order/retry state.
                return Ok(Outcome::Existing(binding));
            }
            ensure!(
                self.insert_copy_signal(&signal)?,
                "source SELL signal insertion refused"
            );
            let binding = ExecutionSourceSellPromotion {
                signal_id: signal.signal_id.clone(),
                intent_id: intent_id.to_owned(),
                promoted_at: Utc::now(),
            };
            rows::insert(conn, &binding)?;
            ensure!(
                rows::load(conn, Some(&signal.signal_id), Some(intent_id))?
                    == Some(binding.clone()),
                "source SELL binding changed during insertion"
            );
            let saved = self
                .load_copy_signal_by_signal_id(&signal.signal_id)?
                .context("source SELL signal missing after insertion")?;
            ensure!(
                rows::same_identity(&saved, &signal) && saved.status == signal.status,
                "source SELL signal changed during insertion"
            );
            Ok(Outcome::Inserted(binding))
        })
        .context("promote staged source SELL")
    }
}
