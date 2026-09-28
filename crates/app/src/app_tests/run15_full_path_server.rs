//! Loopback external-boundary model. SELL bundle/receipt are not DEX evidence.
use super::{run15_full_path_fixture as f, run15_rpc_proof_fixture as p};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_storage_core::ordered_sell_quote::fractional::inventory::Evidence;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) struct Server {
    pub url: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
    pub unknown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn new(
        e: Evidence,
        buy: Value,
        unknown: bool,
        db: std::path::PathBuf,
    ) -> Result<Self> {
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", socket.local_addr()?);
        let calls = Arc::new(Mutex::new(vec![]));
        let record = calls.clone();
        let unknown = Arc::new(AtomicBool::new(unknown));
        let fault = unknown.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut s, _) = socket.accept().await?;
                let mut raw = vec![];
                let (head, offset, len) = loop {
                    let mut buffer = [0; 4096];
                    let n = s.read(&mut buffer).await?;
                    ensure!(n > 0 && raw.len() + n < 1 << 20, "bounded model request");
                    raw.extend(&buffer[..n]);
                    if let Some(at) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
                        let head = String::from_utf8(raw[..at].to_vec())?;
                        let len = head
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if raw.len() >= at + 4 + len {
                            break (head, at + 4, len);
                        }
                    }
                };
                let request: Value = if head.starts_with("GET ") {
                    let path = head.split_whitespace().nth(1).unwrap();
                    let query: std::collections::HashMap<_, _> =
                        reqwest::Url::parse(&format!("http://localhost{path}"))?
                            .query_pairs()
                            .into_owned()
                            .collect();
                    json!({"method":"quote","params":query})
                } else {
                    serde_json::from_slice(&raw[offset..offset + len])?
                };
                record.lock().unwrap().push(request.clone());
                if matches!(
                    request["method"].as_str(),
                    Some("simulateTransaction" | "sendTransaction")
                ) {
                    let before = parent_count(&db)?;
                    let after = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                        loop {
                            let after = parent_count(&db)?;
                            if after > before {
                                break Ok::<_, anyhow::Error>(after);
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    })
                    .await??;
                    let mut calls = record.lock().unwrap();
                    let last = calls.last_mut().unwrap();
                    last["model_parent_before"] = json!(before);
                    last["model_parent_after"] = json!(after);
                }
                let result = response(
                    &request,
                    &e,
                    &buy,
                    &record.lock().unwrap(),
                    fault.load(Ordering::SeqCst),
                )?;
                let body = result.to_string();
                s.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
            }
        });
        Ok(Self {
            url,
            calls,
            unknown,
            task,
        })
    }
    pub fn sends(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "sendTransaction")
            .count()
    }
    pub fn healthy(&self) -> Result<()> {
        ensure!(!self.task.is_finished(), "model RPC peer exited");
        Ok(())
    }
}
fn parent_count(path: &std::path::Path) -> Result<i64> {
    let db = rusqlite::Connection::open(path)?;
    ensure!(
        db.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r
            .get::<_, i64>(0))?
            > 0,
        "SELL parent overlap must follow durable handoff"
    );
    Ok(
        db.query_row("SELECT count(*) FROM association_parent_blocks", [], |r| {
            r.get(0)
        })?,
    )
}
fn signed(r: &Value) -> Result<(String, crate::execution_transaction_wire::DecodedMessage)> {
    let payload = r["params"][0].as_str().unwrap();
    let bytes = STANDARD.decode(payload)?;
    let wire = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65])?;
    ed25519_dalek::VerifyingKey::from_bytes(&wire.binding.accounts[0].pubkey)?
        .verify_strict(&wire.binding.message_bytes, &signature)?;
    Ok((bs58::encode(signature.to_bytes()).into_string(), wire))
}
fn response(r: &Value, e: &Evidence, buy: &Value, calls: &[Value], unknown: bool) -> Result<Value> {
    let result = match r["method"].as_str() {
        Some("getGenesisHash") => json!("11111111111111111111111111111111"),
        Some("getTransaction") if r["params"][0] == buy["transaction"]["signatures"][0] => {
            buy.clone()
        }
        Some("getTransaction")
            if r["params"][0] == p::source_sell()["transaction"]["signatures"][0] =>
        {
            p::source_sell()
        }
        Some("getBlock") if r["params"][0] == e.slot => e.block.clone(),
        Some("getBlock") => e.parent.clone(),
        Some("getTokenAccountsByOwnerAtSlot") => e
            .pages
            .iter()
            .find(|p| r["params"][1]["programId"] == p.program)
            .unwrap()
            .response
            .clone(),
        Some("getTokenAccountsByOwner") => e.execution_accounts.clone(),
        Some("getMinimumBalanceForRentExemption") => json!(2_039_280),
        Some("getMultipleAccounts") => {
            let balance = if calls.iter().any(|r| {
                r["method"] == "getTransaction"
                    && r["params"][0] == buy["transaction"]["signatures"][0]
            }) {
                buy["meta"]["postBalances"][0].as_u64().unwrap()
            } else {
                1_000_000_000
            };
            json!({"context":{"slot":451313059},"value":r["params"][0].as_array().unwrap().iter().enumerate()
            .map(|(i,_)|if i==0 {super::initial_sol_rpc_fixture::system(balance)} else {Value::Null}).collect::<Vec<_>>()})
        }
        Some("getFeeForMessage") => json!({"context":{"slot":451313059},"value":19000}),
        Some("simulateTransaction") => {
            json!({"context":{"slot":451313059},"value":{"err":null,"logs":[],"unitsConsumed":100000}})
        }
        Some("isBlockhashValid") => json!({"context":{"slot":451313060},"value":true}),
        Some("sendTransaction") => {
            if unknown {
                Value::Null
            } else {
                json!(signed(r)?.0)
            }
        }
        Some("getSignatureStatuses") => {
            json!({"context":{"slot":451313060},"value":[if unknown {Value::Null} else {
            json!({"slot":451313060,"confirmationStatus":"finalized","confirmations":null,"err":null})}]})
        }
        Some("getTransaction") => {
            if unknown {
                Value::Null
            } else {
                let sent = calls
                    .iter()
                    .find(|v| v["method"] == "sendTransaction")
                    .expect("SELL sent");
                let (signature, wire) = signed(sent)?;
                ensure!(r["params"][0] == signature, "receipt signature");
                let wallet = bs58::encode(wire.binding.accounts[0].pubkey).into_string();
                let keys: Vec<_> = wire
                    .binding
                    .accounts
                    .iter()
                    .map(|a| {
                        json!({"pubkey":bs58::encode(a.pubkey).into_string(),
                    "signer":a.is_signer,"writable":a.is_writable})
                    })
                    .collect();
                let mut pre = vec![0u64; keys.len()];
                pre[0] = buy["meta"]["postBalances"][0].as_u64().unwrap();
                // The newly created target ATA remains open after this partial SELL.
                pre[1] = 2_039_280;
                let mut post = pre.clone();
                post[0] += 981000;
                let token = |amount: u64| {
                    json!({"accountIndex":1,"mint":p::MINT,"owner":wallet,"programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                    "uiTokenAmount":{"amount":amount.to_string(),"decimals":9}})
                };
                json!({"slot":451313060,"blockTime":null,"version":"legacy","transaction":{"signatures":[signature],
                    "message":{"accountKeys":keys,"recentBlockhash":bs58::encode(wire.recent_blockhash).into_string(),"instructions":[]}},
                    "meta":{"err":null,"fee":19000,"preBalances":pre,"postBalances":post,"preTokenBalances":[token(f::OWNED)],
                    "postTokenBalances":[token(f::OWNED-f::SOLD)],"innerInstructions":[],"logMessages":[]}})
            }
        }
        Some("quote") => {
            return Ok(
                json!({"inputMint":r["params"]["inputMint"],"outputMint":r["params"]["outputMint"],
            "inAmount":r["params"]["amount"],"outAmount":"1000000","otherAmountThreshold":"995000","swapMode":"ExactIn","slippageBps":50,
            "routePlan":[{"swapInfo":{"label":"model SELL boundary"}}]}),
            )
        }
        None if r.get("quoteResponse").is_some() => {
            let payer = crate::execution_pumpswap_accounts::parse_pubkey(
                r["userPublicKey"].as_str().unwrap(),
                "model",
            )?;
            let mut bundle = super::generic_sell_synthetic_fixture::bundle(payer, 1_400_000, 10000);
            let mint = crate::execution_pumpswap_accounts::parse_pubkey(p::MINT, "model mint")?;
            let ata = crate::execution_pumpswap_accounts::associated_token_address(
                &payer,
                &mint,
                &crate::execution_pumpswap_accounts::token_program_id(),
            );
            bundle["swapInstruction"]["accounts"].as_array_mut().unwrap().push(json!({"pubkey":bs58::encode(ata).into_string(),"isSigner":false,"isWritable":true}));
            bundle["blockhashWithMetadata"]["lastValidBlockHeight"] = json!(1000);
            return Ok(bundle);
        }
        other => anyhow::bail!("unexpected model RPC: {other:?}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
