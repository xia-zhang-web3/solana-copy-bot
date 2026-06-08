use super::{ExecutionSubmitTransportAttempt, ExecutionSubmitTransportOutcome};
use crate::execution_quote_canary_helpers::truncate_for_log;
use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) const RPC_SUBMIT_ERROR_REASON: &str = "rpc_send_transaction_error";
pub(crate) const RPC_SUBMIT_MISSING_SIGNATURE_REASON: &str =
    "rpc_send_transaction_missing_signature";

#[derive(Debug, Clone)]
pub(crate) struct RpcExecutionSubmitTransport {
    http: reqwest::Client,
    rpc_url: String,
    options: RpcExecutionSubmitOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RpcExecutionSubmitOptions {
    pub(crate) skip_preflight: bool,
    pub(crate) preflight_commitment: String,
    pub(crate) max_retries: u64,
}

impl Default for RpcExecutionSubmitOptions {
    fn default() -> Self {
        Self {
            skip_preflight: false,
            preflight_commitment: "confirmed".to_string(),
            max_retries: 0,
        }
    }
}

impl RpcExecutionSubmitTransport {
    pub(crate) fn new(rpc_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            rpc_url,
            options: RpcExecutionSubmitOptions::default(),
        }
    }

    pub(crate) fn with_client(
        http: reqwest::Client,
        rpc_url: String,
        options: RpcExecutionSubmitOptions,
    ) -> Self {
        Self {
            http,
            rpc_url,
            options,
        }
    }

    pub(crate) async fn submit(
        &self,
        attempt: &ExecutionSubmitTransportAttempt,
    ) -> Result<ExecutionSubmitTransportOutcome> {
        validate_rpc_submit(attempt, &self.rpc_url, &self.options)?;
        let response = match self
            .http
            .post(self.rpc_url.trim())
            .timeout(Duration::from_millis(attempt.timeout_ms.max(1)))
            .json(&rpc_send_transaction_request(attempt, &self.options))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_timeout() => {
                return Ok(ExecutionSubmitTransportOutcome::SubmittedUnknown {
                    idempotency_key: attempt.idempotency_key.clone(),
                    tx_signature: attempt.tx_signature_hint.clone(),
                });
            }
            Err(error) => return Err(anyhow!("submit RPC request failed: {error}")),
        };
        let status = response.status();
        let body = response
            .text()
            .await
            .context("submit RPC body read failed")?;
        if !status.is_success() {
            return Ok(not_sent(
                attempt,
                format!(
                    "{RPC_SUBMIT_ERROR_REASON}:http_{status}:{}",
                    truncate_for_log(&body, 180)
                ),
            ));
        }
        let value: Value = serde_json::from_str(&body).context("submit RPC JSON decode failed")?;
        rpc_submit_outcome_from_json(attempt, value)
    }
}

pub(crate) fn rpc_send_transaction_request(
    attempt: &ExecutionSubmitTransportAttempt,
    options: &RpcExecutionSubmitOptions,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "execution-submit",
        "method": "sendTransaction",
        "params": [
            attempt.signed_transaction_base64,
            {
                "encoding": "base64",
                "skipPreflight": options.skip_preflight,
                "preflightCommitment": options.preflight_commitment,
                "maxRetries": options.max_retries,
            }
        ],
    })
}

pub(crate) fn rpc_submit_outcome_from_json(
    attempt: &ExecutionSubmitTransportAttempt,
    value: Value,
) -> Result<ExecutionSubmitTransportOutcome> {
    if let Some(error) = value.get("error") {
        return Ok(not_sent(
            attempt,
            format!(
                "{RPC_SUBMIT_ERROR_REASON}:{}",
                truncate_for_log(&error.to_string(), 220)
            ),
        ));
    }
    let Some(signature) = value.get("result").and_then(Value::as_str) else {
        return Ok(not_sent(
            attempt,
            RPC_SUBMIT_MISSING_SIGNATURE_REASON.to_string(),
        ));
    };
    if signature.trim().is_empty() {
        return Ok(not_sent(
            attempt,
            RPC_SUBMIT_MISSING_SIGNATURE_REASON.to_string(),
        ));
    }
    Ok(ExecutionSubmitTransportOutcome::SubmittedUnknown {
        idempotency_key: attempt.idempotency_key.clone(),
        tx_signature: Some(signature.to_string()),
    })
}

fn validate_rpc_submit(
    attempt: &ExecutionSubmitTransportAttempt,
    rpc_url: &str,
    options: &RpcExecutionSubmitOptions,
) -> Result<()> {
    if rpc_url.trim().is_empty() {
        anyhow::bail!("submit RPC URL must be non-empty");
    }
    if attempt.idempotency_key.trim().is_empty() {
        anyhow::bail!("submit transport idempotency key must be non-empty");
    }
    if attempt.signed_transaction_base64.trim().is_empty() {
        anyhow::bail!("submit transport signed transaction must be non-empty");
    }
    if attempt.timeout_ms == 0 {
        anyhow::bail!("submit transport timeout must be positive");
    }
    if options.preflight_commitment.trim().is_empty() {
        anyhow::bail!("submit RPC preflight commitment must be non-empty");
    }
    Ok(())
}

fn not_sent(
    attempt: &ExecutionSubmitTransportAttempt,
    reason: String,
) -> ExecutionSubmitTransportOutcome {
    ExecutionSubmitTransportOutcome::NotSent {
        idempotency_key: attempt.idempotency_key.clone(),
        reason,
    }
}
