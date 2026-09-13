//! Deterministic non-DEX peer. Receipt balance deltas are synthetic, not on-chain evidence.
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
pub(super) fn response(r: &Value, calls: &[Value], fault: &str) -> Result<Option<Value>> {
    let method = r["method"].as_str().unwrap_or("");
    let sent = calls.iter().find(|v| v["method"] == "sendTransaction");
    let signed =
        |v: &Value| -> Result<(String, crate::execution_transaction_wire::DecodedMessage)> {
            let payload = v["params"][0].as_str().unwrap();
            let bytes = STANDARD.decode(payload)?;
            let m = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
            ensure!(bytes[0] == 1, "synthetic signer count");
            let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65])?;
            ed25519_dalek::VerifyingKey::from_bytes(&m.binding.accounts[0].pubkey)?
                .verify_strict(&m.binding.message_bytes, &signature)?;
            Ok((bs58::encode(signature.to_bytes()).into_string(), m))
        };
    let result = match method {
        "isBlockhashValid" => {
            json!({"context":{"slot":if fault=="blockhash_stale" {1} else {152}},"value":if fault=="blockhash_null" {Value::Null} else {json!(fault!="blockhash_false")}})
        }
        "getFeeForMessage" if fault.starts_with("fee_") => {
            let after = calls
                .iter()
                .filter(|v| v["method"] == "getFeeForMessage")
                .count()
                > 1;
            if fault.ends_with("after") && !after {
                return Ok(None);
            }
            json!({"context":{"slot":if fault.contains("stale") {1} else {151}},"value":if fault.contains("null") {Value::Null} else if fault.contains("over") {json!(100001)} else {json!(19000)}})
        }
        "sendTransaction" => {
            let (sig, _) = signed(r)?;
            if fault == "blockhash_not_found" {
                return Ok(Some(
                    json!({"jsonrpc":"2.0","id":r["id"],"error":{"code":-32002,"message":"BlockhashNotFound"}}),
                ));
            }
            if fault == "timeout" {
                return Ok(Some(json!({"jsonrpc":"2.0","id":r["id"],"result":null})));
            }
            json!(sig)
        }
        "getSignatureStatuses" => {
            json!({"context":{"slot":152},"value":[if matches!(fault,"timeout"|"unknown"|"blockhash_not_found") {Value::Null} else {json!({"slot":152,"confirmations":null,"confirmationStatus":"finalized","err":if fault.starts_with("failed") {json!({"InstructionError":[0,"InvalidArgument"]})} else {Value::Null}})}]})
        }
        "getTransaction" if sent.is_some() && r["params"][0] == signed(sent.unwrap())?.0 => {
            if fault == "receipt_unknown" {
                Value::Null
            } else {
                let (sig, m) = signed(sent.unwrap())?;
                let wallet = bs58::encode(m.binding.accounts[0].pubkey).into_string();
                let meta: Value = serde_json::from_slice(&std::fs::read(
                    super::b136_fixture::inputs().join("chain.json"),
                )?)?;
                let mint = meta["our"]["token_out"].as_str().unwrap();
                let keys:Vec<_>=m.binding.accounts.iter().map(|k|json!({"pubkey":bs58::encode(k.pubkey).into_string(),"signer":k.is_signer,"writable":k.is_writable})).collect();
                let mut pre = vec![2_039_280u64; keys.len()];
                pre[0] = 100_000_000;
                let mut post = pre.clone();
                post[0] = if fault.starts_with("failed") {
                    99_981_000
                } else {
                    100_981_000
                };
                let token = |raw: &str| json!({"accountIndex":1,"mint":mint,"owner":wallet,"programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","uiTokenAmount":{"amount":raw,"decimals":3}});
                json!({"slot":152,"blockTime":null,"version":"legacy","transaction":{"signatures":[sig],"message":{"accountKeys":keys,"instructions":[],"recentBlockhash":bs58::encode(m.recent_blockhash).into_string()}},"meta":{"err":if fault.starts_with("failed") {json!({"InstructionError":[0,"InvalidArgument"]})} else {Value::Null},"fee":if fault.ends_with("unknown_fee") {Value::Null} else {json!(19000)},"preBalances":pre,"postBalances":post,"preTokenBalances":[token("7000")],"postTokenBalances":[token(if fault.starts_with("failed") {"7000"} else {"0"})],"innerInstructions":[],"logMessages":[]}})
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(json!({"jsonrpc":"2.0","id":r["id"],"result":result})))
}
