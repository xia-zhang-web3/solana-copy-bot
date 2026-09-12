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

#[derive(Debug)]
pub(super) struct SignatureTransactionFailed {
    pub slot: Option<u64>,
    pub commitment: String,
    pub error: Value,
}
impl std::fmt::Display for SignatureTransactionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("confirmation RPC transaction_error")
    }
}
impl std::error::Error for SignatureTransactionFailed {}

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
    let Some(err) = status.get("err") else {
        return Ok(pending(
            tx_signature,
            RPC_CONFIRMATION_PENDING_STATUS_MISSING,
        ));
    };
    if !err.is_null() {
        anyhow::ensure!(
            valid_transaction_error(err),
            "confirmation RPC malformed transaction error"
        );
        let commitment = status
            .get("confirmationStatus")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(commitment, "confirmed" | "finalized")
            || !super::rpc_failed_expense::proven_failure(err)
        {
            return Ok(pending(
                tx_signature,
                RPC_CONFIRMATION_PENDING_STATUS_UNKNOWN,
            ));
        }
        return Err(SignatureTransactionFailed {
            slot: status.get("slot").and_then(Value::as_u64),
            commitment: commitment.into(),
            error: err.clone(),
        }
        .into());
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
                slot: status.get("slot").and_then(Value::as_u64),
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

fn pending(tx_signature: &str, reason: &str) -> ExecutionConfirmationTrackerOutcome {
    ExecutionConfirmationTrackerOutcome::Pending {
        tx_signature: tx_signature.to_string(),
        reason: reason.to_string(),
    }
}

// RPC errors outside this supported transaction-error shape are unavailable proof.
pub(super) fn valid_transaction_error(error: &Value) -> bool {
    if let Some(name) = error.as_str() {
        return !name.is_empty();
    }
    let Some(object) = error.as_object().filter(|o| o.len() == 1) else {
        return false;
    };
    let Some(instruction) = object
        .get("InstructionError")
        .and_then(Value::as_array)
        .filter(|a| a.len() == 2 && a[0].as_u64().is_some())
    else {
        return false;
    };
    if let Some(name) = instruction[1].as_str() {
        return !name.is_empty();
    }
    instruction[1]
        .as_object()
        .filter(|o| o.len() == 1)
        .and_then(|o| o.get("Custom"))
        .and_then(Value::as_u64)
        .is_some_and(|code| u32::try_from(code).is_ok())
}
