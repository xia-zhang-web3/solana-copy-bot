//! Network-only hot quote construction. No store, signer, or execution runner.
use super::hot_observed::{hot_observed_buy_base_event, observed_buy_token_decimals};
use super::quote_fetch::fetch_quotes;
use super::{append_parallel_provider_samples, buy_quote_price_and_slippage, QuoteEventBundle};
use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
pub(super) async fn build_hot_observed_buy_quote_event(
    http: &reqwest::Client,
    config: &copybot_config::ExecutionConfig,
    signal_id: &str,
    swap: &SwapEvent,
    now: DateTime<Utc>,
    priority_fee_sample: Option<&PriorityFeeSample>,
) -> Result<QuoteEventBundle> {
    let mut event = hot_observed_buy_base_event(signal_id, swap, now);
    attach_priority_fee(&mut event, priority_fee_sample);

    let amount = match sol_to_lamports_raw(config.quote_canary_buy_size_sol) {
        Ok(value) => value,
        Err(error) => {
            event.quote_status = QUOTE_STATUS_ERROR.to_string();
            event.quote_response_available_ts = None;
            event.error = Some(short_error(&error));
            return Ok(QuoteEventBundle::event_only(event));
        }
    };
    let limit_bps = quote_canary_slippage_limit_bps(config, SIDE_BUY);
    let mut token_decimals = observed_buy_token_decimals(swap);
    let (generic, pump) = fetch_quotes(
        http,
        config,
        &event,
        SOL_MINT,
        &swap.token_out,
        &amount,
        limit_bps,
    )
    .await;
    match generic {
        Ok(quote) => {
            apply_quote_sample_to_event(&mut event, quote);
            token_decimals =
                resolve_spl_token_decimals(http, config, &swap.token_out, token_decimals).await;
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
        http,
        config,
        SOL_MINT,
        &swap.token_out,
        pump,
        token_decimals,
        limit_bps,
    )
    .await;
    Ok(bundle)
}
