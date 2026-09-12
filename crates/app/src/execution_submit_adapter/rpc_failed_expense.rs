use anyhow::{anyhow, ensure, Result};
use copybot_storage_core::{
    FailedExpenseCoverage as Coverage, FailedExpenseTask, FailedTransactionFacts,
};
use serde_json::{json, Value};
use std::time::Duration;

pub(super) use copybot_storage_core::proven_transaction_failure as proven_failure;
pub(super) async fn fetch(
    http: &reqwest::Client,
    url: &str,
    signature: &str,
    timeout_ms: u64,
) -> Result<Value> {
    let response=http.post(url).timeout(Duration::from_millis(timeout_ms.max(1)))
        .json(&json!({"jsonrpc":"2.0","id":"execution-failed-expense","method":"getTransaction","params":[signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}]}))
        .send().await.map_err(|_|anyhow!("failed_receipt_rpc_unavailable"))?;
    ensure!(
        response.status().is_success(),
        "failed_receipt_rpc_http_error"
    );
    response
        .json()
        .await
        .map_err(|_| anyhow!("failed_receipt_rpc_json_invalid"))
}
pub(super) fn parse(task: &FailedExpenseTask, value: &Value) -> Result<FailedTransactionFacts> {
    ensure!(
        value.get("error").is_none_or(Value::is_null),
        "failed_receipt_rpc_error"
    );
    let result = value
        .get("result")
        .filter(|r| !r.is_null())
        .ok_or_else(|| anyhow!("failed_receipt_unavailable"))?;
    ensure!(
        result
            .pointer("/transaction/signatures")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(Value::as_str)
            == Some(task.tx_signature.as_str()),
        "failed_receipt_signature_conflict"
    );
    let slot = result["slot"]
        .as_u64()
        .ok_or_else(|| anyhow!("failed_receipt_slot_missing"))?;
    ensure!(
        task.slot.is_none_or(|s| s == slot),
        "failed_receipt_slot_conflict"
    );
    let err = result
        .pointer("/meta/err")
        .ok_or_else(|| anyhow!("failed_receipt_meta_err_missing"))?;
    ensure!(!err.is_null(), "failed_receipt_success_conflict");
    ensure!(proven_failure(err), "failed_receipt_meta_err_invalid");
    let (fee, fee_coverage) = match result.pointer("/meta/fee") {
        None | Some(Value::Null) => (None, Coverage::Missing),
        Some(v) => match v.as_u64() {
            Some(f) => (Some(f.to_string()), Coverage::Known),
            None => (None, Coverage::Invalid),
        },
    };
    let mut facts = FailedTransactionFacts {
        tx_signature: task.tx_signature.clone(),
        wallet: task.wallet.clone(),
        slot,
        commitment: "confirmed".into(),
        transaction_error: err.clone(),
        transaction_fee_lamports: fee,
        fee_coverage,
        payer: None,
        payer_coverage: Coverage::Unsupported,
        wallet_native_pre_lamports: None,
        wallet_native_post_lamports: None,
        native_coverage: Coverage::Unsupported,
    };
    let keys = result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array);
    // This bounded parser supports parsed static accounts only. No raw header/ALT guesses.
    if let Some(keys) = keys.filter(|keys| !keys.is_empty() && keys.iter().all(|k| k.is_object())) {
        let no_alt = result
            .get("version")
            .is_none_or(|v| v == "legacy" || v.as_u64() == Some(0))
            && result
                .pointer("/transaction/message/addressTableLookups")
                .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty))
            && result.pointer("/meta/loadedAddresses").is_none_or(|v| {
                ["writable", "readonly"]
                    .iter()
                    .all(|k| v[*k].as_array().is_some_and(Vec::is_empty))
            })
            && keys
                .iter()
                .all(|k| k.get("source").is_none_or(|v| v == "transaction"));
        let mut seen = std::collections::HashSet::new();
        let valid_keys = keys.iter().all(|k| {
            k["pubkey"]
                .as_str()
                .is_some_and(|p| !p.is_empty() && seen.insert(p))
                && k["signer"].is_boolean()
                && k["writable"].is_boolean()
        });
        if no_alt && valid_keys {
            let matches: Vec<_> = keys
                .iter()
                .enumerate()
                .filter(|(_, k)| k["pubkey"] == task.wallet)
                .collect();
            ensure!(matches.len() == 1, "failed_receipt_wallet_conflict");
            let (index, _key) = matches[0];
            {
                let signers = keys.iter().take_while(|k| k["signer"] == true).count();
                let signature_layout = result
                    .pointer("/transaction/signatures")
                    .and_then(Value::as_array)
                    .is_some_and(|s| {
                        s.len() == signers
                            && s.iter().all(|v| v.as_str().is_some_and(|v| !v.is_empty()))
                    })
                    && keys.iter().skip(signers).all(|k| k["signer"] == false);
                if signature_layout && keys[0]["signer"] == true && keys[0]["writable"] == true {
                    facts.payer = keys[0]["pubkey"].as_str().map(str::to_owned);
                    facts.payer_coverage = Coverage::Known;
                }
                let arrays = result
                    .pointer("/meta/preBalances")
                    .zip(result.pointer("/meta/postBalances"));
                facts.native_coverage = Coverage::Missing;
                if let Some((pre, post)) = arrays {
                    facts.native_coverage = Coverage::Invalid;
                    if let Some((pre, post)) =
                        pre.as_array().zip(post.as_array()).filter(|(pre, post)| {
                            pre.len() == keys.len()
                                && post.len() == keys.len()
                                && pre.iter().chain(post.iter()).all(|v| v.as_u64().is_some())
                        })
                    {
                        facts.wallet_native_pre_lamports =
                            Some(pre[index].as_u64().unwrap().to_string());
                        facts.wallet_native_post_lamports =
                            Some(post[index].as_u64().unwrap().to_string());
                        facts.native_coverage = Coverage::Known;
                    }
                }
            }
        }
    }
    facts.validate()?;
    Ok(facts)
}
