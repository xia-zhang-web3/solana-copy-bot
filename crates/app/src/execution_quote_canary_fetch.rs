//! Only the two read-only quote waits overlap. Conversion and writes stay with the caller.
use super::pump_fun_quote_http::fetch_pump_fun_quote_sample;
use crate::execution_quote_http::fetch_quote_sample;
use crate::execution_quote_timing::QuoteAttemptResult;
use copybot_config::ExecutionConfig;
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;

type Quotes = (QuoteAttemptResult, Option<QuoteAttemptResult>);

pub(super) async fn fetch_quotes(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    event: &ExecutionQuoteCanaryEventInsert,
    input_mint: &str,
    output_mint: &str,
    amount: &str,
    limit_bps: u64,
) -> Quotes {
    match fetch_guarded_quotes(
        http,
        config,
        event,
        input_mint,
        output_mint,
        amount,
        limit_bps,
        || Ok::<(), std::convert::Infallible>(()),
    )
    .await
    {
        Ok(quotes) => quotes,
        Err(never) => match never {},
    }
}

pub(super) async fn fetch_guarded_quotes<E, F: Fn() -> Result<(), E>>(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    event: &ExecutionQuoteCanaryEventInsert,
    input_mint: &str,
    output_mint: &str,
    amount: &str,
    limit_bps: u64,
    check: F,
) -> Result<Quotes, E> {
    check()?;
    let generic = async {
        check()?;
        let result =
            fetch_quote_sample(http, config, input_mint, output_mint, amount, limit_bps).await;
        check()?;
        Ok::<_, E>(result)
    };
    let pump = async {
        if !config.quote_canary_pump_fun_parallel_enabled {
            return Ok(None);
        }
        check()?;
        let result =
            fetch_pump_fun_quote_sample(http, config, &event.side, &event.token, amount).await;
        check()?;
        Ok(Some(result))
    };
    // Only authority errors short-circuit and cancel the sibling. HTTP failures are inner
    // QuoteAttemptResults, so both providers finish normally and keep their own provenance.
    // Poll generic first: an immediately known refusal must not start the other request.
    tokio::try_join!(biased; generic, pump)
}
