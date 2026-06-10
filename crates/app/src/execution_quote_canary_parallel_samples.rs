use super::provider_compare::{
    generic_provider_sample, select_usable_provider_for_event, QuoteEventBundle,
};
use super::pump_fun_parallel::build_pump_fun_provider_sample;
use crate::execution_quote_canary_helpers::{SIDE_BUY, SIDE_SELL};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use copybot_config::ExecutionConfig;

pub(crate) async fn append_parallel_provider_samples(
    bundle: &mut QuoteEventBundle,
    http: &reqwest::Client,
    config: &ExecutionConfig,
    input_mint: &str,
    output_mint: &str,
    amount_raw: &str,
    token_decimals: Option<u8>,
    limit_bps: u64,
) {
    let token_decimals = resolve_parallel_token_decimals(
        http,
        config,
        &bundle.event.side,
        input_mint,
        output_mint,
        token_decimals,
    )
    .await;
    bundle
        .provider_samples
        .push(generic_provider_sample(&bundle.event, limit_bps));
    if let Some(sample) = build_pump_fun_provider_sample(
        http,
        config,
        &bundle.event,
        amount_raw,
        token_decimals,
        limit_bps,
    )
    .await
    {
        bundle.provider_samples.push(sample);
    }
    select_usable_provider_for_event(&mut bundle.event, &bundle.provider_samples);
}

async fn resolve_parallel_token_decimals(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    side: &str,
    input_mint: &str,
    output_mint: &str,
    token_decimals: Option<u8>,
) -> Option<u8> {
    if token_decimals.is_some() {
        return token_decimals;
    }
    if !config.quote_canary_pump_fun_parallel_enabled {
        return token_decimals;
    }
    let token_mint = match side {
        SIDE_BUY => output_mint,
        SIDE_SELL => input_mint,
        _ => return token_decimals,
    };
    resolve_spl_token_decimals(http, config, token_mint, token_decimals).await
}
