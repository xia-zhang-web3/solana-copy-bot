use super::rpc_receipt_balances::{token_delta, wallet_native_balances};
use anyhow::{anyhow, ensure, Result};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::{
    ExecutionCanaryReceiptFacts, ExecutionCanaryReceiptProof, ReceiptDecomposition,
    ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptTokenDelta, ReceiptWsolCoverage,
};
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Debug)]
pub(super) struct ReceiptTransactionFailed {
    pub receipt: Value,
    pub slot: u64,
}
impl std::fmt::Display for ReceiptTransactionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("receipt_transaction_failed")
    }
}
impl std::error::Error for ReceiptTransactionFailed {}

pub(super) async fn fetch_confirmed_receipt_facts(
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    proof: &ExecutionCanaryReceiptProof,
    timeout_ms: u64,
) -> Result<copybot_storage_core::ReceiptObservationBundle> {
    ensure!(!rpc_url.trim().is_empty(), "receipt_rpc_url_missing");
    let response = http.post(rpc_url)
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .json(&json!({"jsonrpc":"2.0","id":"execution-confirmed-fill","method":"getTransaction",
            "params":[proof.tx_signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}]}))
        .send().await.map_err(|e| anyhow!(if e.is_timeout() {"receipt_rpc_timeout"} else {"receipt_rpc_transport_error"}))?;
    ensure!(response.status().is_success(), "receipt_rpc_http_error");
    let value = response.json::<Value>().await.map_err(|e| {
        anyhow!(if e.is_timeout() {
            "receipt_rpc_timeout"
        } else {
            "receipt_rpc_json_invalid"
        })
    })?;
    facts_from_transaction_json(order_id, proof, &value)
}

pub(super) fn facts_from_transaction_json(
    order_id: &str,
    proof: &ExecutionCanaryReceiptProof,
    value: &Value,
) -> Result<copybot_storage_core::ReceiptObservationBundle> {
    ensure!(
        value.get("error").is_none_or(Value::is_null),
        "receipt_rpc_error"
    );
    let result = value
        .get("result")
        .filter(|v| !v.is_null())
        .ok_or_else(|| anyhow!("receipt_not_available"))?;
    let signatures = result
        .pointer("/transaction/signatures")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("receipt_signatures_missing"))?;
    ensure!(
        signatures.first().and_then(Value::as_str) == Some(proof.tx_signature.as_str()),
        "receipt_signature_mismatch"
    );
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("receipt_slot_missing"))?;
    ensure!(
        proof.slot.is_none_or(|expected| expected == slot),
        "receipt_slot_mismatch"
    );
    let err = result
        .pointer("/meta/err")
        .ok_or_else(|| anyhow!("receipt_meta_err_missing"))?;
    if !err.is_null() {
        ensure!(
            super::rpc_failed_expense::proven_failure(err),
            "receipt_meta_err_invalid"
        );
        return Err(ReceiptTransactionFailed {
            receipt: value.clone(),
            slot,
        }
        .into());
    }
    // Successful receipts retain their existing strict account/balance contract.
    let (pre, post, keys) = wallet_native_balances(result, &proof.wallet_pubkey)?;
    let block_time = match result.get("blockTime") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_i64()
                .filter(|t| chrono::DateTime::from_timestamp(*t, 0).is_some())
                .ok_or_else(|| anyhow!("receipt_block_time_invalid"))?,
        ),
    };
    let (token_delta, token_coverage, token_coverage_reason) =
        match token_delta(result, &proof.wallet_pubkey, &proof.token, keys) {
            Ok((raw, decimals, lifecycle)) => (
                Some(ReceiptTokenDelta { raw, decimals }),
                if lifecycle {
                    ReceiptTokenCoverage::ProvenLifecycle
                } else {
                    ReceiptTokenCoverage::PairedBalances
                },
                None,
            ),
            Err(error) => {
                let reason = error.to_string();
                // A known contradictory identity is not a partial success for the requested mint.
                ensure!(
                    reason != "receipt_token_account_identity_changed"
                        && !only_foreign_token_rows(result, &proof.wallet_pubkey, &proof.token),
                    "receipt_token_identity_mismatch"
                );
                (None, ReceiptTokenCoverage::Unresolved, Some(reason))
            }
        };
    let (transaction_fee, fee_coverage) = match result.pointer("/meta/fee") {
        None | Some(Value::Null) => (None, ReceiptFeeCoverage::Missing),
        Some(v) => match v.as_u64() {
            Some(fee) => (Some(Lamports::new(fee)), ReceiptFeeCoverage::Known),
            None => (None, ReceiptFeeCoverage::Invalid),
        },
    };
    let payer = &result["transaction"]["message"]["accountKeys"][0];
    let fee_payer = if payer["signer"] == true && payer["writable"] == true {
        payer["pubkey"].as_str().map(str::to_owned)
    } else {
        None
    };
    let wsol =
        token_rows(result).any(|row| row["mint"] == "So11111111111111111111111111111111111111112");
    let facts = ExecutionCanaryReceiptFacts {
        order_id: order_id.into(),
        tx_signature: proof.tx_signature.clone(),
        wallet_pubkey: proof.wallet_pubkey.clone(),
        token: proof.token.clone(),
        side: proof.side.clone(),
        slot,
        wallet_native_pre: Lamports::new(pre),
        wallet_native_post: Lamports::new(post),
        wallet_native_delta: SignedLamports::new(i128::from(post) - i128::from(pre)),
        transaction_fee,
        fee_coverage,
        fee_payer,
        token_delta,
        token_coverage,
        token_coverage_reason,
        wsol_coverage: if wsol {
            ReceiptWsolCoverage::Observed
        } else {
            ReceiptWsolCoverage::Unresolved
        },
        block_time,
        decomposition: ReceiptDecomposition::Unresolved,
    };
    let native = super::rpc_native_accounts::collect(result, &facts);
    native.validate()?;
    Ok(copybot_storage_core::ReceiptObservationBundle { facts, native })
}

fn token_rows(result: &Value) -> impl Iterator<Item = &Value> {
    ["preTokenBalances", "postTokenBalances"]
        .into_iter()
        .filter_map(|name| result["meta"][name].as_array())
        .flatten()
}

fn only_foreign_token_rows(result: &Value, wallet: &str, mint: &str) -> bool {
    !token_rows(result).any(|row| row["owner"] == wallet && row["mint"] == mint)
        && token_rows(result).any(|row| {
            row["owner"].as_str().is_some_and(|s| !s.is_empty())
                && row["mint"].as_str().is_some_and(|s| !s.is_empty())
        })
}
