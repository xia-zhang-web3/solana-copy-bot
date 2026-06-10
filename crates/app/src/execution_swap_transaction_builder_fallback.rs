use super::{swap_transaction_response_summary, SwapTransactionDryRunResult};
use crate::execution_swap_http_request::{
    disable_shared_accounts, is_missing_account_simulation_error,
    metis_fallback_swap_builder_endpoint, post_no_shared_skip_user_accounts_json_with_retry,
    public_fallback_swap_builder_endpoint,
};
use crate::execution_swap_http_retry::post_swap_json_with_retry;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::time::Duration as StdDuration;

pub(super) async fn retry_public_swap_transaction_builder(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    body: &Value,
    timeout: StdDuration,
    endpoint_name: &str,
) -> Result<Option<SwapTransactionDryRunResult>> {
    retry_swap_transaction_builder(
        http,
        body,
        timeout,
        public_fallback_swap_builder_endpoint(config, endpoint_name, "public swap")?,
        "public",
    )
    .await
}

pub(super) async fn retry_metis_swap_transaction_builder(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    body: &Value,
    timeout: StdDuration,
    endpoint_name: &str,
) -> Result<Option<SwapTransactionDryRunResult>> {
    retry_swap_transaction_builder(
        http,
        body,
        timeout,
        metis_fallback_swap_builder_endpoint(config, endpoint_name, "metis swap")?,
        "metis",
    )
    .await
}

async fn retry_swap_transaction_builder(
    http: &reqwest::Client,
    body: &Value,
    timeout: StdDuration,
    fallback: crate::execution_swap_http_request::SwapBuilderEndpoint,
    label: &str,
) -> Result<Option<SwapTransactionDryRunResult>> {
    let response = post_swap_json_with_retry(
        http,
        fallback.url.clone(),
        &fallback.api_key,
        body,
        timeout,
        &format!("{label} swap transaction dry-run fallback"),
    )
    .await?;
    if !is_missing_account_simulation_error(&response.value) {
        return Ok(Some(swap_transaction_response_summary(
            response.value,
            response.elapsed_ms,
            response.attempts,
            fallback.source,
            false,
            false,
        )?));
    }

    let mut no_shared_body = body.clone();
    disable_shared_accounts(&mut no_shared_body);
    let retry = post_swap_json_with_retry(
        http,
        fallback.url.clone(),
        &fallback.api_key,
        &no_shared_body,
        timeout,
        &format!("{label} swap transaction dry-run no-shared-accounts fallback"),
    )
    .await?;
    if !is_missing_account_simulation_error(&retry.value) {
        return Ok(Some(swap_transaction_response_summary(
            retry.value,
            retry.elapsed_ms,
            retry.attempts,
            fallback.source,
            true,
            false,
        )?));
    }

    let skip_retry = post_no_shared_skip_user_accounts_json_with_retry(
        http,
        &fallback.url,
        &fallback.api_key,
        body,
        timeout,
        &format!("{label} swap transaction dry-run no-shared skip-user-accounts fallback"),
    )
    .await?;
    Ok(Some(swap_transaction_response_summary(
        skip_retry.value,
        skip_retry.elapsed_ms,
        skip_retry.attempts,
        fallback.source,
        true,
        true,
    )?))
}
