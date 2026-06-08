use super::{
    ExecutionConfirmationProof, ExecutionConfirmationRequest, ExecutionConfirmationTrackerOutcome,
};
use crate::execution_quote_canary_helpers::truncate_for_log;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) const RPC_CONFIRMATION_PENDING_STATUS_MISSING: &str = "rpc_signature_status_missing";
pub(crate) const RPC_CONFIRMATION_PENDING_STATUS_PROCESSED: &str =
    "rpc_confirmation_pending_processed";
pub(crate) const RPC_CONFIRMATION_PENDING_STATUS_UNKNOWN: &str = "rpc_confirmation_pending_unknown";

pub(crate) async fn fetch_rpc_signature_confirmation(
    http: &reqwest::Client,
    rpc_url: &str,
    request: &ExecutionConfirmationRequest,
    confirmed_at: DateTime<Utc>,
    timeout_ms: u64,
) -> Result<ExecutionConfirmationTrackerOutcome> {
    validate_rpc_confirmation_request(rpc_url, request)?;
    let response = http
        .post(rpc_url.trim())
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .json(&rpc_signature_status_request(&request.tx_signature))
        .send()
        .await
        .context("confirmation RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("confirmation RPC body read failed")?;
    if !status.is_success() {
        return Err(anyhow!(
            "confirmation RPC returned HTTP {status}: {}",
            truncate_for_log(&body, 240)
        ));
    }
    let value = serde_json::from_str(&body).context("confirmation RPC JSON decode failed")?;
    rpc_signature_confirmation_from_json(&request.tx_signature, confirmed_at, value)
}

pub(crate) fn rpc_signature_status_request(tx_signature: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "execution-confirmation",
        "method": "getSignatureStatuses",
        "params": [[tx_signature], {"searchTransactionHistory": true}],
    })
}

pub(crate) fn rpc_signature_confirmation_from_json(
    tx_signature: &str,
    confirmed_at: DateTime<Utc>,
    value: Value,
) -> Result<ExecutionConfirmationTrackerOutcome> {
    if let Some(error) = value.get("error") {
        return Err(anyhow!(
            "confirmation RPC error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let result = value
        .get("result")
        .ok_or_else(|| anyhow!("confirmation RPC response missing result"))?;
    let Some(status) = result
        .get("value")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
    else {
        return Ok(pending(
            tx_signature,
            RPC_CONFIRMATION_PENDING_STATUS_MISSING,
        ));
    };
    if status.is_null() {
        return Ok(pending(
            tx_signature,
            RPC_CONFIRMATION_PENDING_STATUS_MISSING,
        ));
    }
    if let Some(error) = status.get("err").filter(|error| !error.is_null()) {
        return Err(anyhow!(
            "confirmation RPC transaction_error: {}",
            truncate_for_log(&error.to_string(), 240)
        ));
    }
    let confirmation_status = status
        .get("confirmationStatus")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match confirmation_status {
        "confirmed" | "finalized" => Ok(ExecutionConfirmationTrackerOutcome::Confirmed(
            ExecutionConfirmationProof {
                tx_signature: tx_signature.to_string(),
                confirmation_status: confirmation_status.to_string(),
                slot: status_slot(result, status),
                confirmed_at,
            },
        )),
        "processed" => Ok(pending(
            tx_signature,
            RPC_CONFIRMATION_PENDING_STATUS_PROCESSED,
        )),
        _ => Ok(pending(
            tx_signature,
            RPC_CONFIRMATION_PENDING_STATUS_UNKNOWN,
        )),
    }
}

fn validate_rpc_confirmation_request(
    rpc_url: &str,
    request: &ExecutionConfirmationRequest,
) -> Result<()> {
    if rpc_url.trim().is_empty() {
        anyhow::bail!("confirmation RPC URL must be non-empty");
    }
    if request.order_id.trim().is_empty() {
        anyhow::bail!("confirmation request order_id must be non-empty");
    }
    if request.tx_signature.trim().is_empty() {
        anyhow::bail!("confirmation request tx_signature must be non-empty");
    }
    Ok(())
}

fn status_slot(result: &Value, status: &Value) -> Option<u64> {
    status
        .get("slot")
        .and_then(Value::as_u64)
        .or_else(|| result.pointer("/context/slot").and_then(Value::as_u64))
}

fn pending(tx_signature: &str, reason: &str) -> ExecutionConfirmationTrackerOutcome {
    ExecutionConfirmationTrackerOutcome::Pending {
        tx_signature: tx_signature.to_string(),
        reason: reason.to_string(),
    }
}
