use super::{
    build_pump_fun_provider_sample, buy_quote_price_and_slippage, ExecutionQuoteCanaryRunner,
    ExecutionQuoteCanaryTickSummary, QuoteEventBundle,
};
use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};

impl ExecutionQuoteCanaryRunner {
    pub(crate) async fn process_hot_observed_buy_swap(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        now: DateTime<Utc>,
    ) -> Result<ExecutionQuoteCanaryTickSummary> {
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        if !self.is_enabled() || !observed_swap_is_buy(swap) {
            return Ok(summary);
        }
        let signal_id = observed_buy_signal_id(swap);
        summary.entry_candidates = 1;
        if self.record_existing_entry_event_if_present(store, &signal_id, &mut summary)? {
            return Ok(summary);
        }
        let mut bundle = match self
            .build_hot_observed_buy_quote_event(&signal_id, swap, now, None)
            .await
        {
            Ok(bundle) => bundle,
            Err(error) => QuoteEventBundle::event_only(hot_observed_buy_error_event(
                &signal_id, swap, now, &error,
            )),
        };
        let mut priority_fee_sample = None;
        let priority = self
            .priority_fee_sample_if_needed(&mut priority_fee_sample)
            .await;
        attach_priority_fee(&mut bundle.event, priority);
        self.record_entry_event(store, bundle, &mut summary)?;
        Ok(summary)
    }

    async fn build_hot_observed_buy_quote_event(
        &self,
        signal_id: &str,
        swap: &SwapEvent,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        let mut event = hot_observed_buy_base_event(signal_id, swap, now);
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
        let mut token_decimals = observed_buy_token_decimals(swap);
        match fetch_quote_sample(
            &self.http,
            &self.config,
            SOL_MINT,
            &swap.token_out,
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
                    &swap.token_out,
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
        if let Some(sample) = build_pump_fun_provider_sample(
            &self.http,
            &self.config,
            &bundle.event,
            &amount,
            token_decimals,
            limit_bps,
        )
        .await
        {
            bundle.provider_samples.push(sample);
        }
        Ok(bundle)
    }
}

fn observed_swap_is_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT
        && swap.token_out != SOL_MINT
        && swap.amount_in > 0.0
        && swap.amount_out > 0.0
}

fn observed_buy_signal_id(swap: &SwapEvent) -> String {
    format!(
        "shadow:{}:{}:{}:{}",
        swap.signature, swap.wallet, SIDE_BUY, swap.token_out
    )
}

fn hot_observed_buy_base_event(
    signal_id: &str,
    swap: &SwapEvent,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: entry_quote_event_id(signal_id),
        signal_id: Some(signal_id.to_string()),
        shadow_closed_trade_id: None,
        wallet_id: swap.wallet.clone(),
        token: swap.token_out.clone(),
        side: SIDE_BUY.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts: Some(swap.ts_utc),
        decision_delay_ms: duration_ms_between(swap.ts_utc, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(swap.amount_in),
        quote_in_amount_raw: None,
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(swap.amount_in, swap.amount_out),
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

fn hot_observed_buy_error_event(
    signal_id: &str,
    swap: &SwapEvent,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    let mut event = hot_observed_buy_base_event(signal_id, swap, now);
    event.quote_status = QUOTE_STATUS_ERROR.to_string();
    event.decision_status = Some(DECISION_UNKNOWN.to_string());
    event.decision_reason = Some("quote_error".to_string());
    event.error = Some(short_error(error));
    event
}

fn observed_buy_token_decimals(swap: &SwapEvent) -> Option<u8> {
    swap.exact_amounts
        .as_ref()
        .and_then(|exact| exact.amount_out_quantity().ok())
        .map(|amount| amount.decimals())
        .or_else(|| {
            let exact = swap.exact_amounts.as_ref()?;
            infer_decimals_from_raw_and_ui(&exact.amount_out_raw, swap.amount_out)
        })
}
