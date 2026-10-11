//! Native BUY uses an availability fence. The quote event has no source UTC.
use super::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary, QuoteEventBundle};
use crate::execution_canary_route::NativeBuyGuard;
use crate::execution_quote_canary_helpers::{entry_error_event, entry_quote_event_id, PriorityFeeSample};
#[cfg(test)]
use crate::execution_quote_canary_helpers::QuoteSample;
use anyhow::{ensure, Result};
#[cfg(test)]
use anyhow::Context;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{native_buy, SqliteStore};

/// An explicit total budget for the bounded native cohort, not a provider estimate.
/// Other routes and an enabled sampler retain their existing fee policy.
pub(super) fn configured_cohort_priority(
    config: &copybot_config::ExecutionConfig,
    signal: &CopySignalRow,
) -> Result<Option<PriorityFeeSample>> {
    if config.priority_fee_canary_enabled
        || !copybot_config::native_first_buy_activation(config)
        || signal.status != native_buy::STATUS
        || !signal.side.eq_ignore_ascii_case("buy")
        || !signal.signal_id.starts_with("native-buy-v1:")
    {
        return Ok(None);
    }
    let Some(cohort) = config.technical_cohort.as_ref().filter(|p|
        p.activate && p.policy == copybot_config::TECHNICAL_COHORT_V1
            && config.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && p.wallet_ids.contains(&signal.wallet_id)
    ) else { return Ok(None); };
    let cap = config.pretrade_max_priority_fee_lamports;
    // This is the existing technical-cohort financial cap, including its units.
    ensure!((1..=50_000).contains(&cap), "native_cohort_priority_fee_cap");
    Ok(Some(PriorityFeeSample {
        status: "ok".into(),
        lamports: Some(cap),
        json: Some(serde_json::json!({"version":1,
            "source":"native_cohort_configured_cap",
            "unit":"total_priority_fee_lamports","value":cap,
            "run_id":cohort.run_id}).to_string()),
        error: None,
    }))
}

impl ExecutionQuoteCanaryRunner {
    pub(crate) async fn process_native_buy_signal(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        guard: &NativeBuyGuard,
        now: DateTime<Utc>,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        ensure!(
            self.is_enabled() && signal.status == native_buy::STATUS,
            "native_buy_quote_disabled"
        );
        if !guard.check(store)? {
            return Ok(summary);
        }
        summary.entry_candidates = 1;
        let event_id = entry_quote_event_id(&signal.signal_id);
        if store
            .load_execution_quote_canary_event_by_id(&event_id)?
            .is_some()
        {
            summary.entry_existing = 1;
            summary.last_event_id = Some(event_id);
            return Ok(summary);
        }
        let mut priority_sample = None;
        #[cfg(test)]
        let priority = if let Some(mock) = guard.mock_io() {
            mock.count(|c| c.priority += 1);
            priority_sample = Some(mock.runner.as_ref()
                .context("native_runner_mock_missing")?.priority.clone());
            priority_sample.as_ref()
        } else {
            self.priority_fee_sample_if_needed(&mut priority_sample).await
        };
        #[cfg(not(test))]
        let priority = self
            .priority_fee_sample_if_needed(&mut priority_sample)
            .await;
        if !guard.check(store)? {
            return Ok(summary);
        }
        #[cfg(test)]
        let built = if let Some(mock) = guard.mock_io() {
            mock.count(|c| c.initial_quote += 1);
            self.build_entry_quote_event_with_external_quote(
                store, signal, now, priority,
                mock.runner.as_ref().context("native_runner_mock_missing")?.initial_quote.clone(),
            ).await
        } else {
            self.build_entry_quote_event(store, signal, now, priority).await
        };
        #[cfg(not(test))]
        let built = self.build_entry_quote_event(store, signal, now, priority).await;
        let bundle = match built {
            Ok(bundle) => bundle,
            Err(error) => QuoteEventBundle::event_only(entry_error_event(signal, now, &error)),
        };
        self.record_native_buy_bundle(store, guard, bundle, &mut summary)?;
        Ok(summary)
    }

    pub(crate) fn record_native_buy_bundle(
        &self,
        store: &SqliteStore,
        guard: &NativeBuyGuard,
        mut bundle: QuoteEventBundle,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        // The app availability instant is not the source transaction timestamp.
        bundle.event.signal_ts = None;
        bundle.event.decision_delay_ms = None;
        if !guard.check(store)? {
            return Ok(());
        }
        // The native guard is the owner at this boundary. The hot-shadow owner
        // pending protocol requires a source UTC and cannot represent this clock.
        let mut native_owner = self.clone();
        native_owner.owner_decisions = false;
        native_owner.record_entry_event(store, bundle, summary)
    }


}
