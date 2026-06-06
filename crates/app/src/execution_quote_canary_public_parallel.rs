use super::provider_compare::{
    buy_quote_price_and_slippage, provider_error_sample, provider_sample_from_event,
    sell_quote_price_and_slippage,
};
use crate::execution_quote_canary_helpers::{apply_quote_sample_to_event, SIDE_BUY, SIDE_SELL};
use crate::execution_quote_http::fetch_quote_sample_from_base_url;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert,
    PROVIDER_GENERIC_PUBLIC,
};

pub(crate) async fn build_public_generic_provider_sample(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    event: &ExecutionQuoteCanaryEventInsert,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    token_decimals: Option<u8>,
    max_slippage_bps: u64,
) -> Option<ExecutionQuoteCanaryProviderSampleInsert> {
    if !config.quote_canary_public_parallel_enabled {
        return None;
    }
    let quote = match fetch_quote_sample_from_base_url(
        http,
        &config.quote_canary_public_base_url,
        "",
        config.quote_canary_timeout_ms,
        input_mint,
        output_mint,
        amount_raw,
        max_slippage_bps,
    )
    .await
    {
        Ok(quote) => quote,
        Err(error) => {
            return Some(provider_error_sample(
                event,
                PROVIDER_GENERIC_PUBLIC,
                &error,
                max_slippage_bps,
            ));
        }
    };
    let mut sample_event = event.clone();
    apply_quote_sample_to_event(&mut sample_event, quote);
    sample_event.error = None;
    if let Some(decimals) = token_decimals {
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
        PROVIDER_GENERIC_PUBLIC,
        max_slippage_bps,
    ))
}
