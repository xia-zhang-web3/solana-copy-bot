use super::quote_fetch::fetch_quotes;
use super::{
    append_parallel_provider_samples, buy_quote_price_and_slippage, sell_quote_price_and_slippage,
    ExecutionQuoteCanaryRunner, QuoteEventBundle,
};
use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::quote_price_sanity::raw_amount_mismatch_error;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{
    native_buy, ExecutionCanaryCloseCandidate, ExecutionQuoteCanaryEventInsert, SqliteStore,
};

impl ExecutionQuoteCanaryRunner {
    fn initial_entry_quote_event(
        &self, store: &SqliteStore, signal: &CopySignalRow, now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<(ExecutionQuoteCanaryEventInsert, Option<u8>)> {
        let native_source = if signal.status == native_buy::STATUS {
            let signature = signal.signal_id.strip_prefix("native-buy-v1:")
                .ok_or_else(|| anyhow::anyhow!("native_buy_signal_identity"))?;
            let decision_id = format!("native-buy-decision-v1:{signature}");
            Some(store.native_buy_source_amounts(
                &signal.signal_id, &decision_id, Utc::now(),
                self.config.canary_max_signal_age_seconds,
            )?.ok_or_else(|| anyhow::anyhow!("native_buy_source_amounts_unavailable"))?)
        } else { None };
        let observed = if native_source.is_some() { None }
            else { load_matching_observed_entry_leg(store, signal)? };
        let native_price = native_source.as_ref().and_then(|source| {
            let sol = raw_amount_to_ui(Some(&source.amount_in_lamports.to_string()), 9)?;
            let tokens = raw_amount_to_ui(Some(&source.amount_out_raw.to_string()),
                source.amount_out_decimals)?;
            price_sol_per_token(sol, tokens)
        });
        let mut event = ExecutionQuoteCanaryEventInsert {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            event_id: entry_quote_event_id(&signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: SIDE_BUY.to_string(),
            quote_status: QUOTE_STATUS_SKIPPED.to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: None,
            quote_latency_ms: None,
            leader_notional_sol: observed
                .as_ref()
                .map(|value| value.sol_notional)
                .or(Some(signal.notional_sol)),
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_response_json: None,
            quote_price_sol: None,
            shadow_price_sol: native_price.or_else(|| observed
                .as_ref()
                .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty))),
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
        let token_decimals = native_source.as_ref().map(|source| source.amount_out_decimals)
            .or_else(|| observed.as_ref().and_then(observed_token_decimals));
        Ok((event, token_decimals))

    }

    #[cfg(test)]
    pub(super) fn build_entry_quote_event_with_mock_external_quote(
        &self, store: &SqliteStore, signal: &CopySignalRow, now: DateTime<Utc>,
        quote: QuoteSample, priority: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        let (mut event, token_decimals) = self.initial_entry_quote_event(
            store, signal, now, priority,
        )?;
        let decimals = token_decimals.ok_or_else(|| anyhow::anyhow!("source_decimals_missing"))?;
        apply_quote_sample_to_event(&mut event, quote);
        (event.quote_price_sol, event.slippage_bps) =
            buy_quote_price_and_slippage(&event, decimals);
        Ok(QuoteEventBundle::event_only(event))
    }

    pub(super) async fn build_entry_quote_event(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        self.build_entry_quote_event_inner(
            store, signal, now, priority_fee_sample, #[cfg(test)] None,
        ).await
    }

    #[cfg(test)]
    pub(super) async fn build_entry_quote_event_with_external_quote(
        &self, store: &SqliteStore, signal: &CopySignalRow, now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>, quote: QuoteSample,
    ) -> Result<QuoteEventBundle> {
        self.build_entry_quote_event_inner(store, signal, now, priority_fee_sample, Some(quote)).await
    }

    async fn build_entry_quote_event_inner(
        &self, store: &SqliteStore, signal: &CopySignalRow, now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
        #[cfg(test)] external_quote: Option<QuoteSample>,
    ) -> Result<QuoteEventBundle> {
        let (mut event, mut token_decimals) = self.initial_entry_quote_event(
            store, signal, now, priority_fee_sample,
        )?;

        let amount = match sol_to_lamports_raw(self.config.quote_canary_buy_size_sol) {
            Ok(value) => value,
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.quote_response_available_ts = None;
                event.error = Some(short_error(&error));
                return Ok(QuoteEventBundle::event_only(event));
            }
        };
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_BUY);
        #[cfg(test)]
        let (generic, pump) = if let Some(quote) = external_quote {
            (Ok(quote), None)
        } else {
            fetch_quotes(
                &self.http, &self.config, &event, SOL_MINT, &signal.token,
                &amount, limit_bps,
            ).await
        };
        #[cfg(not(test))]
        let (generic, pump) = fetch_quotes(
            &self.http,
            &self.config,
            &event,
            SOL_MINT,
            &signal.token,
            &amount,
            limit_bps,
        )
        .await;
        match generic {
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
                crate::execution_quote_timing::apply_error_timing(&mut event, &error);
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.quote_response_available_ts = None;
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
            pump,
            token_decimals,
            limit_bps,
        )
        .await;
        Ok(bundle)
    }

    pub(super) async fn build_close_quote_event(
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
        if let Some(error) = raw_amount_mismatch_error(&amount, decimals, close.qty, "close quote")
        {
            event.error = Some(error);
            return Ok(QuoteEventBundle::event_only(event));
        }
        event.quote_in_amount_raw = Some(amount.clone());
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        let (generic, pump) = fetch_quotes(
            &self.http,
            &self.config,
            &event,
            &close.token,
            SOL_MINT,
            &amount,
            limit_bps,
        )
        .await;
        match generic {
            Ok(quote) => {
                apply_quote_sample_to_event(&mut event, quote);
                if let Some(decimals) = decimals {
                    let (price, slippage) = sell_quote_price_and_slippage(&event, decimals);
                    event.quote_price_sol = price;
                    event.slippage_bps = slippage;
                }
            }
            Err(error) => {
                crate::execution_quote_timing::apply_error_timing(&mut event, &error);
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.quote_response_available_ts = None;
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
            pump,
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
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: close_quote_event_id(close.id),
        signal_id: Some(close.signal_id.clone()),
        shadow_closed_trade_id: Some(close.id),
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts: Some(close.closed_ts),
        decision_delay_ms: None,
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
