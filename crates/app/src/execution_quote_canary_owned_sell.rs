use super::parallel_samples::append_guarded_parallel_provider_samples;
use super::provider_compare::{sell_quote_price_and_slippage, QuoteEventBundle};
use super::quote_fetch::fetch_guarded_quotes;
use super::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary};
use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, attach_priority_fee, load_matching_observed_leg_for_signal,
    observed_token_decimals, price_sol_per_token, quote_canary_slippage_limit_bps, short_error,
    ui_amount_to_raw_string, PriorityFeeSample, QUOTE_STATUS_ERROR, QUOTE_STATUS_SKIPPED,
    SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_source_sell_guard as source_guard;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionCanaryOwnedPosition, ExecutionQuoteCanaryEventInsert,
    SqliteStore,
};

const OWNED_SELL_SIGNAL_LOOKBACK_SECONDS: i64 = 24 * 60 * 60;
const OWNED_STALE_CLOSE_LOOKBACK_SECONDS: i64 = 24 * 60 * 60;

impl ExecutionQuoteCanaryRunner {
    pub(super) async fn process_owned_sell_signal_candidates(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let since = now - Duration::seconds(OWNED_SELL_SIGNAL_LOOKBACK_SECONDS);
        let signal_ids = store.list_execution_quote_canary_owned_sell_signal_candidate_ids(
            copy_signal_status,
            since,
            batch_limit,
        )?;
        summary.close_candidates += signal_ids.len();
        for signal_id in signal_ids {
            store.advance_execution_owned_sell_cursor(&signal_id)?;
            let before_attempt = summary.clone();
            let result: Result<()> = async {
                let Some(signal) = store.load_copy_signal_by_signal_id(&signal_id)? else {
                    return Ok(());
                };
                self.process_owned_sell_signal(store, &signal, now, priority_fee_sample, summary)
                    .await
            }
            .await;
            if let Err(error) = result {
                if copybot_storage_core::is_fatal_sqlite_anyhow_error(&error)
                    || source_guard::global_storage_error(&error)
                {
                    return Err(error);
                }
                // A recorder may update its in-memory summary before its write fails.
                *summary = before_attempt;
                summary.close_errors += 1;
                summary.decision_unknown += 1;
                let reason =
                    short_error(&error.context(format!("owned sell recovery {signal_id}")));
                summary.last_owned_sell_recovery_error = Some(
                    crate::telemetry::OwnedSellRecoveryError::from_bounded_error(
                        &signal_id, &reason,
                    ),
                );
                summary.last_error = Some(reason);
            }
        }
        self.process_owned_stale_close_candidates(
            store,
            copy_signal_status,
            now,
            batch_limit,
            priority_fee_sample,
            summary,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn process_owned_stale_close_candidates(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        now: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let since = now - Duration::seconds(OWNED_STALE_CLOSE_LOOKBACK_SECONDS);
        let closes =
            store.list_execution_quote_canary_owned_stale_close_candidates(since, batch_limit)?;
        summary.close_candidates += closes.len();
        for close in closes {
            self.process_owned_stale_close(
                store,
                copy_signal_status,
                &close,
                now,
                priority_fee_sample,
                summary,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn process_owned_sell_signal(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let result = self
            .try_process_owned_sell_signal(store, signal, now, priority_fee_sample, summary)
            .await;
        source_guard::quote_result(result, summary)?;
        Ok(())
    }

    async fn try_process_owned_sell_signal(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let source = source_guard::signal(store, &signal.signal_id)?;
        let signal = &source.signal;
        if !signal.side.eq_ignore_ascii_case(SIDE_SELL) {
            return Ok(());
        }
        if signal.status == copybot_storage_core::EXECUTION_SELL_INTENT_STATUS
            && store
                .load_execution_canary_order_by_signal(&signal.signal_id)?
                .is_some()
        {
            return Ok(());
        }
        let Some(position) = store.load_execution_canary_open_position(&signal.token)? else {
            return Ok(());
        };
        if signal.ts < position.opened_ts {
            return Ok(());
        }
        let priority = self
            .priority_fee_sample_if_needed(priority_fee_sample)
            .await;
        source.recheck(store)?;
        let bundle = self
            .build_owned_sell_quote_event(store, signal, &position, now, priority, None)
            .await;
        source.recheck(store)?;
        let bundle = bundle?;
        self.record_close_event(store, bundle, summary)
    }

    async fn process_owned_stale_close(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        close: &ExecutionCanaryCloseCandidate,
        now: DateTime<Utc>,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let Some(position) = store.load_execution_canary_open_position(&close.token)? else {
            return Ok(());
        };
        if close.closed_ts < position.opened_ts {
            return Ok(());
        }
        let signal = stale_close_copy_signal(close, copy_signal_status);
        store.insert_copy_signal(&signal)?;
        let priority = self
            .priority_fee_sample_if_needed(priority_fee_sample)
            .await;
        let bundle = self
            .build_owned_sell_quote_event(store, &signal, &position, now, priority, Some(close))
            .await?;
        self.record_close_event(store, bundle, summary)
    }

    async fn build_owned_sell_quote_event(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        position: &ExecutionCanaryOwnedPosition,
        now: DateTime<Utc>,
        priority_fee_sample: Option<&PriorityFeeSample>,
        shadow_close: Option<&ExecutionCanaryCloseCandidate>,
    ) -> Result<QuoteEventBundle> {
        let source = source_guard::signal(store, &signal.signal_id)?;
        let observed =
            load_matching_observed_leg_for_signal(store, &signal.signal_id, &signal.token)?;
        let mut event =
            owned_sell_quote_event(signal, position, now, observed.as_ref(), shadow_close);
        attach_priority_fee(&mut event, priority_fee_sample);
        let decimals = position
            .qty_exact
            .map(|qty| qty.decimals())
            .or_else(|| observed.as_ref().and_then(observed_token_decimals));
        let decimals =
            resolve_spl_token_decimals(&self.http, &self.config, &signal.token, decimals).await;
        source.recheck(store)?;
        let amount = position
            .qty_exact
            .map(|qty| qty.raw().to_string())
            .or_else(|| decimals.and_then(|value| ui_amount_to_raw_string(position.qty, value)));
        let Some(amount) = amount else {
            event.quote_status = QUOTE_STATUS_ERROR.to_string();
            event.quote_response_available_ts = None;
            event.error = Some("missing owned sell qty_raw and inferred decimals".to_string());
            return Ok(QuoteEventBundle::event_only(event));
        };
        event.quote_in_amount_raw = Some(amount.clone());
        let limit_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        let (quote, pump) = fetch_guarded_quotes(
            &self.http,
            &self.config,
            &event,
            &signal.token,
            SOL_MINT,
            &amount,
            limit_bps,
            || source.recheck(store),
        )
        .await?;
        match quote {
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
        append_guarded_parallel_provider_samples(
            &mut bundle,
            &self.http,
            &self.config,
            &signal.token,
            SOL_MINT,
            pump,
            decimals,
            limit_bps,
            || source.recheck(store),
        )
        .await?;
        Ok(bundle)
    }
}

fn owned_sell_quote_event(
    signal: &CopySignalRow,
    position: &ExecutionCanaryOwnedPosition,
    now: DateTime<Utc>,
    observed: Option<&copybot_storage_core::ExecutionCanaryObservedLeg>,
    shadow_close: Option<&ExecutionCanaryCloseCandidate>,
) -> ExecutionQuoteCanaryEventInsert {
    let signal_ts = shadow_close
        .map(|close| close.closed_ts)
        .or(Some(signal.ts));
    ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: owned_sell_quote_event_id(signal, shadow_close),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: shadow_close.map(|close| close.id),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_SKIPPED.to_string(),
        request_ts: now,
        signal_ts,
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: observed
            .map(|value| value.sol_notional)
            .or_else(|| shadow_close.map(|close| close.exit_value_sol))
            .or(Some(signal.notional_sol)),
        quote_in_amount_raw: position.qty_exact.map(|qty| qty.raw().to_string()),
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: observed
            .and_then(|value| price_sol_per_token(value.sol_notional, value.token_qty))
            .or_else(|| {
                shadow_close.and_then(|close| price_sol_per_token(close.exit_value_sol, close.qty))
            }),
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

fn stale_close_copy_signal(close: &ExecutionCanaryCloseCandidate, status: &str) -> CopySignalRow {
    CopySignalRow {
        signal_id: close.signal_id.clone(),
        wallet_id: close.wallet_id.clone(),
        side: SIDE_SELL.to_string(),
        token: close.token.clone(),
        notional_sol: close.exit_value_sol,
        notional_lamports: None,
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts: close.closed_ts,
        status: status.to_string(),
    }
}

fn owned_sell_quote_event_id(
    signal: &CopySignalRow,
    shadow_close: Option<&ExecutionCanaryCloseCandidate>,
) -> String {
    match shadow_close {
        Some(close) => format!("quote:owned-stale-close:{}", close.id),
        None => format!("quote:owned-close:{}", signal.signal_id),
    }
}
