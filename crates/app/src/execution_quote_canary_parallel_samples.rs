use super::provider_compare::QuoteEventBundle;
use super::public_parallel::build_public_generic_provider_sample;
use super::pump_fun_parallel::build_pump_fun_provider_sample;
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
    if let Some(sample) = build_public_generic_provider_sample(
        http,
        config,
        &bundle.event,
        input_mint,
        output_mint,
        amount_raw,
        token_decimals,
        limit_bps,
    )
    .await
    {
        bundle.provider_samples.push(sample);
    }
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
}
