use super::provider_compare::{
    buy_quote_price_and_slippage, provider_error_sample, provider_sample_from_event,
    sell_quote_price_and_slippage,
};
use super::pump_fun_quote_http::fetch_pump_fun_quote_sample;
use crate::execution_quote_canary_helpers::{apply_quote_sample_to_event, SIDE_BUY, SIDE_SELL};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert,
    PROVIDER_PUMP_FUN_PAID,
};

pub(crate) async fn build_pump_fun_provider_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    event: &ExecutionQuoteCanaryEventInsert,
    amount_raw: &str,
    token_decimals: Option<u8>,
    max_slippage_bps: u64,
) -> Option<ExecutionQuoteCanaryProviderSampleInsert> {
    if !config.quote_canary_pump_fun_parallel_enabled {
        return None;
    }
    let quote = match fetch_pump_fun_quote_sample(
        http,
        config,
        &event.side,
        &event.token,
        amount_raw,
    )
    .await
    {
        Ok(quote) => quote,
        Err(error) => {
            return Some(provider_error_sample(
                event,
                PROVIDER_PUMP_FUN_PAID,
                &error,
                max_slippage_bps,
            ));
        }
    };
    let decimals = match event.side.as_str() {
        SIDE_BUY => quote.out_decimals.or(token_decimals),
        SIDE_SELL => quote.in_decimals.or(token_decimals),
        _ => token_decimals,
    };
    let mut sample_event = event.clone();
    apply_quote_sample_to_event(&mut sample_event, quote);
    sample_event.error = None;
    if let Some(decimals) = decimals {
        let (price, slippage) = match event.side.as_str() {
            SIDE_BUY => buy_quote_price_and_slippage(&sample_event, decimals),
            SIDE_SELL => sell_quote_price_and_slippage(&sample_event, decimals),
            _ => (None, None),
        };
        sample_event.quote_price_sol = price;
        sample_event.slippage_bps = slippage;
    }
    Some(provider_sample_from_event(
        &sample_event,
        PROVIDER_PUMP_FUN_PAID,
        max_slippage_bps,
    ))
}
