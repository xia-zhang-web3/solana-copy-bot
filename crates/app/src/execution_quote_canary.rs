use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_priority_fee::PriorityFeeSampler;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_shadow::ShadowSignalResult;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteStore, PROVIDER_GENERIC_METIS,
};

#[path = "execution_strict_quote_jobs.rs"]
pub(crate) mod strict;

#[path = "execution_hot_quote_claims.rs"]
pub(crate) mod claims;
#[path = "execution_hot_quote_lifecycle.rs"]
mod hot_lifecycle;
#[path = "execution_hot_quote_network.rs"]
mod hot_network;
#[path = "execution_hot_quote_job.rs"]
pub(crate) mod job;

#[path = "execution_quote_canary_builders.rs"]
mod builders;
#[path = "execution_quote_canary_hot_observed.rs"]
mod hot_observed;
#[path = "execution_quote_canary_owned_sell.rs"]
mod owned_sell;
#[path = "execution_quote_canary_native_buy.rs"]
mod native_buy;
#[path = "execution_quote_canary_parallel_samples.rs"]
mod parallel_samples;
#[path = "execution_quote_canary_priority_retry.rs"]
mod priority_retry;
#[path = "execution_quote_canary_provider_compare.rs"]
mod provider_compare;
#[path = "execution_quote_canary_pump_fun_parallel.rs"]
mod pump_fun_parallel;
#[path = "execution_pump_fun_quote_http.rs"]
mod pump_fun_quote_http;
#[path = "execution_quote_canary_fetch.rs"]
mod quote_fetch;
#[path = "execution_quote_canary_summary.rs"]
mod summary;

pub(crate) use parallel_samples::append_parallel_provider_samples;
pub(crate) use provider_compare::{buy_quote_price_and_slippage, QuoteEventBundle};
use provider_compare::{generic_provider_sample, sell_quote_price_and_slippage};
pub(crate) use summary::ExecutionQuoteCanaryTickSummary;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionQuoteCanaryRunner {
    config: ExecutionConfig,
    http: reqwest::Client,
    priority_fee: PriorityFeeSampler,
    entry_claims: claims::EntryClaims,
    owner_decisions: bool,
    strict: Option<strict::StrictQuotes>,
}

impl ExecutionQuoteCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        let http = reqwest::Client::new();
        Self {
            config: config.clone(),
            entry_claims: Default::default(),
            owner_decisions: false,
            strict: None,
            http: http.clone(),
            priority_fee: PriorityFeeSampler::new(config, http),
        }
    }

    pub(crate) fn requiring_owner(mut self) -> Self {
        self.owner_decisions = true;
        self
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.quote_canary_enabled
    }

    pub(crate) async fn process_tick(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        if self.strict.is_some() {
            return self.strict_tick();
        }
        let mut priority_fee_sample = None;
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        self.process_entry_candidates(
            store,
            copy_signal_status,
            now,
            since,
            batch_limit,
            &mut priority_fee_sample,
            &mut summary,
        )
        .await?;
        self.process_entry_priority_fee_retry_candidates(
            store,
            copy_signal_status,
            since,
            batch_limit,
            &mut priority_fee_sample,
            &mut summary,
        )
        .await?;
        self.process_close_candidates(
            store,
            now,
            since,
            batch_limit,
            &mut priority_fee_sample,
            &mut summary,
        )
        .await?;
        self.process_owned_sell_signal_candidates(
            store,
            copy_signal_status,
            now,
            batch_limit,
            &mut priority_fee_sample,
            &mut summary,
        )
        .await?;
        Ok(summary)
    }

    pub(crate) async fn process_recorded_shadow_signal(
        &self,
        store: &SqliteStore,
        signal: &ShadowSignalResult,
        now: DateTime<Utc>,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        if !self.is_enabled() {
            return Ok(summary);
        }
        let mut priority_fee_sample = None;
        match signal.side.as_str() {
            SIDE_BUY => {
                if self.entry_pending(&signal.signal_id) {
                    return Ok(summary);
                }
                let Some(copy_signal) = store
                    .load_copy_signal_by_signal_id(&signal.signal_id)
                    .with_context(|| {
                        format!(
                            "failed loading copy signal {} for quote canary",
                            signal.signal_id
                        )
                    })?
                else {
                    return Ok(summary);
                };
                summary.entry_candidates = 1;
                if self
                    .record_existing_entry_event_if_present(
                        store,
                        &copy_signal.signal_id,
                        &mut priority_fee_sample,
                        &mut summary,
                    )
                    .await?
                {
                    return Ok(summary);
                }
                let priority = self
                    .priority_fee_sample_if_needed(&mut priority_fee_sample)
                    .await;
                let bundle = match self
                    .build_entry_quote_event(store, &copy_signal, now, priority)
                    .await
                {
                    Ok(bundle) => bundle,
                    Err(error) => {
                        QuoteEventBundle::event_only(entry_error_event(&copy_signal, now, &error))
                    }
                };
                self.record_entry_event(store, bundle, &mut summary)?;
            }
            SIDE_SELL => {
                let closes = store
                    .list_execution_quote_canary_close_candidates_for_signal(
                        &signal.signal_id,
                        self.config.canary_batch_limit.max(1),
                    )
                    .with_context(|| {
                        format!(
                            "failed loading quote canary close candidates for signal {}",
                            signal.signal_id
                        )
                    })?;
                summary.close_candidates = closes.len();
                if closes.is_empty() {
                    if let Some(copy_signal) =
                        store.load_copy_signal_by_signal_id(&signal.signal_id)?
                    {
                        self.process_owned_sell_signal(
                            store,
                            &copy_signal,
                            now,
                            &mut priority_fee_sample,
                            &mut summary,
                        )
                        .await?;
                    }
                    return Ok(summary);
                }
                let priority = self
                    .priority_fee_sample_if_needed(&mut priority_fee_sample)
                    .await;
                for close in closes {
                    let bundle = match self
                        .build_close_quote_event(store, &close, now, priority)
                        .await
                    {
                        Ok(bundle) => bundle,
                        Err(error) => {
                            QuoteEventBundle::event_only(close_error_event(&close, now, &error))
                        }
                    };
                    self.record_close_event(store, bundle, &mut summary)?;
                }
            }
            _ => {}
        }
        Ok(summary)
    }

    async fn process_entry_candidates(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let signals = store
            .list_execution_quote_canary_entry_candidates(copy_signal_status, since, batch_limit)
            .context("failed loading execution quote canary entry candidates")?
            .into_iter()
            .filter(|s| !self.entry_pending(&s.signal_id))
            .collect::<Vec<_>>();
        summary.entry_candidates = signals.len();
        if signals.is_empty() {
            return Ok(());
        }
        let priority = self
            .priority_fee_sample_if_needed(priority_fee_sample)
            .await;
        for signal in signals {
            let bundle = match self
                .build_entry_quote_event(store, &signal, now, priority)
                .await
            {
                Ok(bundle) => bundle,
                Err(error) => QuoteEventBundle::event_only(entry_error_event(&signal, now, &error)),
            };
            self.record_entry_event(store, bundle, summary)?;
        }
        Ok(())
    }

    async fn process_close_candidates(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let closes = store
            .list_execution_quote_canary_close_candidates(since, batch_limit)
            .context("failed loading execution quote canary close candidates")?;
        summary.close_candidates = closes.len();
        if !closes.is_empty() {
            let priority = self
                .priority_fee_sample_if_needed(priority_fee_sample)
                .await;
            for close in closes {
                let bundle = match self
                    .build_close_quote_event(store, &close, now, priority)
                    .await
                {
                    Ok(bundle) => bundle,
                    Err(error) => {
                        QuoteEventBundle::event_only(close_error_event(&close, now, &error))
                    }
                };
                self.record_close_event(store, bundle, summary)?;
            }
        }
        self.process_close_priority_fee_retry_candidates(
            store,
            now,
            batch_limit,
            priority_fee_sample,
            summary,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn priority_fee_sample_if_needed<'a>(
        &self,
        sample: &'a mut Option<PriorityFeeSample>,
    ) -> Option<&'a PriorityFeeSample> {
        if sample.is_none() {
            *sample = self.priority_fee.sample_if_enabled().await;
        }
        sample.as_ref()
    }

    fn record_entry_event(
        &self,
        store: &SqliteStore,
        mut bundle: QuoteEventBundle,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
        let event = &mut bundle.event;
        finalize_quote_decision(event, limit_bps);
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.entry_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.last_event_id = Some(event.event_id.clone());
        let provider_event = event.clone();
        if self.owner_decisions {
            copybot_storage_core::pend_execution_quote_entry(event)?;
        }
        match store
            .record_execution_quote_canary_event(event)
            .with_context(|| {
                format!(
                    "failed recording execution entry quote canary event {}",
                    event.event_id
                )
            })? {
            ExecutionQuoteCanaryRecordOutcome::Inserted => summary.entry_inserted += 1,
            ExecutionQuoteCanaryRecordOutcome::Existing => {
                summary.entry_existing += 1;
                return Ok(()); // Never append provider samples from a losing completion.
            }
        }
        self.record_provider_samples(store, &provider_event, bundle.provider_samples, limit_bps)?;
        Ok(())
    }

    fn record_close_event(
        &self,
        store: &SqliteStore,
        mut bundle: QuoteEventBundle,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        let event = &mut bundle.event;
        finalize_quote_decision(event, limit_bps);
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.close_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.last_event_id = Some(event.event_id.clone());
        match store
            .record_execution_quote_canary_event(event)
            .with_context(|| {
                format!(
                    "failed recording execution close quote canary event {}",
                    event.event_id
                )
            })? {
            ExecutionQuoteCanaryRecordOutcome::Inserted => summary.close_inserted += 1,
            ExecutionQuoteCanaryRecordOutcome::Existing => summary.close_existing += 1,
        }
        self.record_provider_samples(store, &event, bundle.provider_samples, limit_bps)?;
        Ok(())
    }

    fn record_provider_samples(
        &self,
        store: &SqliteStore,
        event: &ExecutionQuoteCanaryEventInsert,
        provider_samples: Vec<ExecutionQuoteCanaryProviderSampleInsert>,
        limit_bps: u64,
    ) -> Result<()> {
        if !provider_samples
            .iter()
            .any(|sample| sample.provider == PROVIDER_GENERIC_METIS)
        {
            let generic = generic_provider_sample(event, limit_bps);
            store.record_execution_quote_canary_provider_sample(&generic)?;
        }
        for sample in provider_samples {
            store.record_execution_quote_canary_provider_sample(&sample)?;
        }
        Ok(())
    }
}

pub(super) fn apply_decision_summary(
    event: &ExecutionQuoteCanaryEventInsert,
    summary: &mut ExecutionQuoteCanaryTickSummary,
) {
    match event.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE) => summary.would_execute += 1,
        Some(DECISION_WOULD_FORCE_EXIT) => summary.would_force_exit += 1,
        Some(DECISION_WOULD_SKIP) => summary.would_skip += 1,
        Some(DECISION_UNKNOWN) | None => summary.decision_unknown += 1,
        Some(_) => summary.decision_unknown += 1,
    }
}
