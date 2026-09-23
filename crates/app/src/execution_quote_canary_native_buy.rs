//! Native BUY uses an availability fence. The quote event has no source UTC.
use super::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary, QuoteEventBundle};
use crate::execution_canary_route::NativeBuyGuard;
use crate::execution_quote_canary_helpers::{entry_error_event, entry_quote_event_id};
#[cfg(test)]
use crate::execution_quote_canary_helpers::{PriorityFeeSample, QuoteSample};
use anyhow::{ensure, Result};
#[cfg(test)]
use anyhow::Context;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{native_buy, SqliteStore};

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

    fn record_native_buy_bundle(
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

    #[cfg(test)]
    pub(crate) fn process_native_buy_signal_with_mock_external_quote(
        &self, store: &SqliteStore, signal: &CopySignalRow,
        guard: &NativeBuyGuard, now: DateTime<Utc>,
        quote: QuoteSample, priority: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        ensure!(self.is_enabled() && signal.status == native_buy::STATUS,
            "native_buy_quote_disabled");
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        if !guard.check(store)? { return Ok(summary); }
        summary.entry_candidates = 1;
        let bundle = self.build_entry_quote_event_with_mock_external_quote(
            store, signal, now, quote, priority,
        )?;
        self.record_native_buy_bundle(store, guard, bundle, &mut summary)?;
        Ok(summary)
    }

}
