use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_priority_fee::PriorityFeeSampler;
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_shadow::ShadowSignalResult;
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionQuoteCanaryEventInsert,
    ExecutionQuoteCanaryProviderSampleInsert, ExecutionQuoteCanaryRecordOutcome, SqliteStore,
    PROVIDER_GENERIC_METIS,
};

#[path = "execution_quote_canary_hot_observed.rs"]
mod hot_observed;
#[path = "execution_quote_canary_owned_sell.rs"]
mod owned_sell;
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

pub(crate) use parallel_samples::append_parallel_provider_samples;
use provider_compare::{
    buy_quote_price_and_slippage, generic_provider_sample, sell_quote_price_and_slippage,
    QuoteEventBundle,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionQuoteCanaryTickSummary {
    pub entry_candidates: usize,
    pub entry_inserted: usize,
    pub entry_existing: usize,
    pub entry_errors: usize,
    pub close_candidates: usize,
    pub close_inserted: usize,
    pub close_existing: usize,
    pub close_errors: usize,
    pub would_execute: usize,
    pub would_force_exit: usize,
    pub would_skip: usize,
    pub decision_unknown: usize,
    pub last_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionQuoteCanaryRunner {
    config: ExecutionConfig,
    http: reqwest::Client,
    priority_fee: PriorityFeeSampler,
}

impl ExecutionQuoteCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        let http = reqwest::Client::new();
        Self {
            config: config.clone(),
            http: http.clone(),
            priority_fee: PriorityFeeSampler::new(config, http),
        }
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
            .context("failed loading execution quote canary entry candidates")?;
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
        match store
            .record_execution_quote_canary_event(event)
            .with_context(|| {
                format!(
                    "failed recording execution entry quote canary event {}",
                    event.event_id
                )
            })? {
            ExecutionQuoteCanaryRecordOutcome::Inserted => summary.entry_inserted += 1,
            ExecutionQuoteCanaryRecordOutcome::Existing => summary.entry_existing += 1,
        }
        self.record_provider_samples(store, &event, bundle.provider_samples, limit_bps)?;
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

    async fn build_entry_quote_event(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        let observed = load_matching_observed_entry_leg(store, signal)?;
        let mut event = ExecutionQuoteCanaryEventInsert {
            event_id: entry_quote_event_id(&signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: SIDE_BUY.to_string(),
            quote_status: QUOTE_STATUS_SKIPPED.to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: duration_ms_between(signal.ts, now),
            quote_latency_ms: None,
            leader_notional_sol: observed
                .as_ref()
                .map(|value| value.sol_notional)
                .or(Some(signal.notional_sol)),
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_response_json: None,
            quote_price_sol: None,
            shadow_price_sol: observed
                .as_ref()
                .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty)),
            slippage_bps: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            decision_status: None,
            decision_reason: None,
            error: None,
        };
        attach_priority_fee(&mut event, priority_fee_sample);

        let amount = match sol_to_lamports_raw(self.config.quote_canary_buy_size_sol) {
            Ok(value) => value,
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
                return Ok(QuoteEventBundle::event_only(event));
            }
        };
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
        let mut token_decimals = observed.as_ref().and_then(observed_token_decimals);
        match fetch_quote_sample(
            &self.http,
            &self.config,
            SOL_MINT,
            &signal.token,
            &amount,
            limit_bps,
        )
        .await
        {
            Ok(quote) => {
                apply_quote_sample_to_event(&mut event, quote);
                token_decimals = resolve_spl_token_decimals(
                    &self.http,
                    &self.config,
                    &signal.token,
                    token_decimals,
                )
                .await;
                if let Some(decimals) = token_decimals {
                    let (price, slippage) = buy_quote_price_and_slippage(&event, decimals);
                    event.quote_price_sol = price;
                    event.slippage_bps = slippage;
                }
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        let mut bundle = QuoteEventBundle::event_only(event);
        append_parallel_provider_samples(
            &mut bundle,
            &self.http,
            &self.config,
            SOL_MINT,
            &signal.token,
            &amount,
            token_decimals,
            limit_bps,
        )
        .await;
        Ok(bundle)
    }

    async fn build_close_quote_event(
        &self,
        store: &SqliteStore,
        close: &ExecutionCanaryCloseCandidate,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        let mut event = close_quote_event(close, now);
        attach_priority_fee(&mut event, priority_fee_sample);
        let observed =
            load_matching_observed_leg_for_signal(store, &close.signal_id, &close.token)?;
        let decimals = close
            .qty_decimals
            .or_else(|| observed.as_ref().and_then(observed_token_decimals));
        let decimals =
            resolve_spl_token_decimals(&self.http, &self.config, &close.token, decimals).await;
        let amount = close
            .qty_raw
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
            .or_else(|| decimals.and_then(|value| ui_amount_to_raw_string(close.qty, value)));
        let Some(amount) = amount else {
            event.error = Some("missing exact close qty_raw and inferred decimals".to_string());
            return Ok(QuoteEventBundle::event_only(event));
        };
        event.quote_in_amount_raw = Some(amount.clone());
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        match fetch_quote_sample(
            &self.http,
            &self.config,
            &close.token,
            SOL_MINT,
            &amount,
            limit_bps,
        )
        .await
        {
            Ok(quote) => {
                apply_quote_sample_to_event(&mut event, quote);
                if let Some(decimals) = decimals {
                    let (price, slippage) = sell_quote_price_and_slippage(&event, decimals);
                    event.quote_price_sol = price;
                    event.slippage_bps = slippage;
                }
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        let mut bundle = QuoteEventBundle::event_only(event);
        append_parallel_provider_samples(
            &mut bundle,
            &self.http,
            &self.config,
            &close.token,
            SOL_MINT,
            &amount,
            decimals,
            limit_bps,
        )
        .await;
        Ok(bundle)
    }
}

fn close_quote_event(
    close: &ExecutionCanaryCloseCandidate,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: close_quote_event_id(close.id),
        signal_id: Some(close.signal_id.clone()),
        shadow_closed_trade_id: Some(close.id),
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts: Some(close.closed_ts),
        decision_delay_ms: duration_ms_between(close.closed_ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(close.exit_value_sol),
        quote_in_amount_raw: close.qty_raw.clone(),
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(close.exit_value_sol, close.qty),
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
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
