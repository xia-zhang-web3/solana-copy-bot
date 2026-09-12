use super::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary, QuoteEventBundle};
use crate::execution_quote_canary_helpers::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;
use copybot_storage_core::SqliteStore;

// Synchronous compatibility seam; runtime ingress uses owned quote jobs.
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
        if self.entry_pending(&signal_id) {
            return Ok(summary);
        }
        summary.entry_candidates = 1;
        let mut priority_fee_sample = None;
        if self
            .record_existing_entry_event_if_present(
                store,
                &signal_id,
                &mut priority_fee_sample,
                &mut summary,
            )
            .await?
        {
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
        priority: Option<&PriorityFeeSample>,
    ) -> Result<QuoteEventBundle> {
        super::hot_network::build_hot_observed_buy_quote_event(
            &self.http,
            &self.config,
            signal_id,
            swap,
            now,
            priority,
        )
        .await
    }
}

pub(super) fn observed_swap_is_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT
        && swap.token_out != SOL_MINT
        && swap.amount_in > 0.0
        && swap.amount_out > 0.0
}

pub(super) fn observed_buy_signal_id(swap: &SwapEvent) -> String {
    format!(
        "shadow:{}:{}:{}:{}",
        swap.signature, swap.wallet, SIDE_BUY, swap.token_out
    )
}

pub(super) fn hot_observed_buy_base_event(
    signal_id: &str,
    swap: &SwapEvent,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: entry_quote_event_id(signal_id),
        signal_id: Some(signal_id.to_string()),
        shadow_closed_trade_id: None,
        wallet_id: swap.wallet.clone(),
        token: swap.token_out.clone(),
        side: SIDE_BUY.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts: Some(swap.ts_utc),
        decision_delay_ms: None,
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

pub(super) fn hot_observed_buy_error_event(
    signal_id: &str,
    swap: &SwapEvent,
    now: DateTime<Utc>,
    error: &anyhow::Error,
) -> ExecutionQuoteCanaryEventInsert {
    let mut event = hot_observed_buy_base_event(signal_id, swap, now);
    event.quote_status = QUOTE_STATUS_ERROR.to_string();
    event.quote_response_available_ts = None;
    event.decision_status = Some(DECISION_UNKNOWN.to_string());
    event.decision_reason = Some("quote_error".to_string());
    event.error = Some(short_error(error));
    event
}

pub(super) fn observed_buy_token_decimals(swap: &SwapEvent) -> Option<u8> {
    swap.exact_amounts
        .as_ref()
        .and_then(|exact| exact.amount_out_quantity().ok())
        .map(|amount| amount.decimals())
        .or_else(|| {
            let exact = swap.exact_amounts.as_ref()?;
            infer_decimals_from_raw_and_ui(&exact.amount_out_raw, swap.amount_out)
        })
}
