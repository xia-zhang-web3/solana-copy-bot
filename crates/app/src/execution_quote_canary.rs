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
    ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};

#[path = "execution_quote_canary_hot_observed.rs"]
mod hot_observed;

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
        self.process_close_candidates(
            store,
            now,
            since,
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
                if self.record_existing_entry_event_if_present(
                    store,
                    &copy_signal.signal_id,
                    &mut summary,
                )? {
                    return Ok(summary);
                }
                let priority = self
                    .priority_fee_sample_if_needed(&mut priority_fee_sample)
                    .await;
                let event = match self
                    .build_entry_quote_event(store, &copy_signal, now, priority)
                    .await
                {
                    Ok(event) => event,
                    Err(error) => entry_error_event(&copy_signal, now, &error),
                };
                self.record_entry_event(store, event, &mut summary)?;
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
                    return Ok(summary);
                }
                let priority = self
                    .priority_fee_sample_if_needed(&mut priority_fee_sample)
                    .await;
                for close in closes {
                    let event = match self
                        .build_close_quote_event(store, &close, now, priority)
                        .await
                    {
                        Ok(event) => event,
                        Err(error) => close_error_event(&close, now, &error),
                    };
                    self.record_close_event(store, event, &mut summary)?;
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
            let event = match self
                .build_entry_quote_event(store, &signal, now, priority)
                .await
            {
                Ok(event) => event,
                Err(error) => entry_error_event(&signal, now, &error),
            };
            self.record_entry_event(store, event, summary)?;
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
        if closes.is_empty() {
            return Ok(());
        }
        let priority = self
            .priority_fee_sample_if_needed(priority_fee_sample)
            .await;
        for close in closes {
            let event = match self
                .build_close_quote_event(store, &close, now, priority)
                .await
            {
                Ok(event) => event,
                Err(error) => close_error_event(&close, now, &error),
            };
            self.record_close_event(store, event, summary)?;
        }
        Ok(())
    }

    async fn priority_fee_sample_if_needed<'a>(
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
        mut event: ExecutionQuoteCanaryEventInsert,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
        finalize_quote_decision(&mut event, limit_bps);
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.entry_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.last_event_id = Some(event.event_id.clone());
        match store
            .record_execution_quote_canary_event(&event)
            .with_context(|| {
                format!(
                    "failed recording execution entry quote canary event {}",
                    event.event_id
                )
            })? {
            ExecutionQuoteCanaryRecordOutcome::Inserted => summary.entry_inserted += 1,
            ExecutionQuoteCanaryRecordOutcome::Existing => summary.entry_existing += 1,
        }
        Ok(())
    }

    pub(super) fn record_existing_entry_event_if_present(
        &self,
        store: &SqliteStore,
        signal_id: &str,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<bool> {
        let Some(event) = store
            .load_latest_execution_quote_canary_entry_event(signal_id)
            .with_context(|| {
                format!("failed loading existing execution entry quote event for {signal_id}")
            })?
        else {
            return Ok(false);
        };
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.entry_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.entry_existing += 1;
        summary.last_event_id = Some(event.event_id);
        Ok(true)
    }

    fn record_close_event(
        &self,
        store: &SqliteStore,
        mut event: ExecutionQuoteCanaryEventInsert,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        finalize_quote_decision(&mut event, limit_bps);
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.close_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.last_event_id = Some(event.event_id.clone());
        match store
            .record_execution_quote_canary_event(&event)
            .with_context(|| {
                format!(
                    "failed recording execution close quote canary event {}",
                    event.event_id
                )
            })? {
            ExecutionQuoteCanaryRecordOutcome::Inserted => summary.close_inserted += 1,
            ExecutionQuoteCanaryRecordOutcome::Existing => summary.close_existing += 1,
        }
        Ok(())
    }

    async fn build_entry_quote_event(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryEventInsert> {
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
                return Ok(event);
            }
        };
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
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
                let decimals = resolve_spl_token_decimals(
                    &self.http,
                    &self.config,
                    &signal.token,
                    observed.as_ref().and_then(observed_token_decimals),
                )
                .await;
                if let Some(decimals) = decimals {
                    let quote_in_sol = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), 9);
                    let quote_out_tokens =
                        raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), decimals);
                    event.quote_price_sol = quote_in_sol.and_then(|input| {
                        quote_out_tokens.and_then(|out| price_sol_per_token(input, out))
                    });
                    event.slippage_bps =
                        quote_slippage_bps_for_buy(event.quote_price_sol, event.shadow_price_sol);
                }
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        Ok(event)
    }

    async fn build_close_quote_event(
        &self,
        store: &SqliteStore,
        close: &ExecutionCanaryCloseCandidate,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<ExecutionQuoteCanaryEventInsert> {
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
            return Ok(event);
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
                    let quote_out_sol = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), 9);
                    let quote_in_tokens =
                        raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), decimals);
                    event.quote_price_sol = quote_out_sol.and_then(|out| {
                        quote_in_tokens.and_then(|input| price_sol_per_token(out, input))
                    });
                    event.slippage_bps =
                        quote_slippage_bps_for_sell(event.quote_price_sol, event.shadow_price_sol);
                }
            }
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        Ok(event)
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

fn apply_decision_summary(
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
