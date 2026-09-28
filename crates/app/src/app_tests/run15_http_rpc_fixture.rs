//! Complete loopback JSON projection for modeled headers/follower facts.
//! Captured SELL RPC body is served separately; this is not production conversion.
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yellowstone_grpc_proto::prelude::*;

pub(super) struct Http {
    pub url: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Drop for Http {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Http {
    pub async fn start(
        fixture: super::run15_reconnect_tonic_fixture::Fixture,
        corpus: &std::path::Path,
        directory: std::path::PathBuf,
        bad_anchor: bool,
    ) -> Result<Self> {
        std::fs::create_dir(&directory)?;
        let captured: Value =
            serde_json::from_slice(&std::fs::read(corpus.join("response-725.json"))?)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/rpc", listener.local_addr()?);
        let calls = Arc::new(Mutex::new(vec![]));
        let record = calls.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = vec![];
                let request = loop {
                    let mut part = [0; 4096];
                    let n = socket.read(&mut part).await?;
                    ensure!(n > 0 && bytes.len() + n < 65536, "HTTP model request bound");
                    bytes.extend_from_slice(&part[..n]);
                    if let Some(at) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..at])?;
                        let size = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|s| s.trim().parse::<usize>().unwrap())
                            })
                            .context("body length")?;
                        if bytes.len() >= at + 4 + size {
                            break serde_json::from_slice::<Value>(&bytes[at + 4..at + 4 + size])?;
                        }
                    }
                };
                let index = {
                    let mut calls = record.lock().unwrap();
                    calls.push(request.clone());
                    calls.len()
                };
                ensure!(
                    request["params"][if request["method"] == "getBlocks" {
                        2
                    } else {
                        1
                    }]["commitment"]
                        == "confirmed",
                    "HTTP confirmed request"
                );
                let blocks = fixture.blocks();
                let mut result = match request["method"].as_str() {
                    Some("getBlocks") => {
                        let start = request["params"][0].as_u64().unwrap();
                        let end = request["params"][1].as_u64().unwrap();
                        json!(blocks
                            .iter()
                            .filter(|b| b.slot >= start && b.slot <= end)
                            .map(|b| b.slot)
                            .collect::<Vec<_>>())
                    }
                    Some("getBlock") => {
                        ensure!(
                            request["params"][1]["maxSupportedTransactionVersion"] == 1
                                && request["params"][1]["encoding"] == "json",
                            "HTTP full version1"
                        );
                        let slot = request["params"][0].as_u64().unwrap();
                        // HTTP is deliberately slower than one live block: producer continues.
                        tokio::time::sleep(std::time::Duration::from_millis(35)).await;
                        let b = blocks
                            .iter()
                            .find(|b| b.slot == slot)
                            .context("requested model block absent")?;
                        if slot == super::run15_full_path_frames::SELL_SLOT {
                            let mut raw = captured["result"].clone();
                            raw["rewards"] = json!([]);
                            raw["numRewardPartitions"] = Value::Null;
                            raw
                        } else {
                            block(b)?
                        }
                    }
                    other => anyhow::bail!("unexpected recovery method {other:?}"),
                };
                if bad_anchor && request["method"] == "getBlock"
                    && request["params"][0] == super::run15_full_path_frames::SELL_SLOT + 1 {
                    result["blockhash"] = json!(bs58::encode([77;32]).into_string());
                }
                let response = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                let raw = serde_json::to_vec(&response)?;
                // Immutable complete raw evidence before response, analogous to common broker.
                std::fs::write(directory.join(format!("response-{index:04}.json")), &raw)?;
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",raw.len()).as_bytes()).await?;
                socket.write_all(&raw).await?;
            }
        });
        Ok(Self { url, calls, task })
    }
}
fn b58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}
fn reward(r: &Reward) -> Value {
    let kind = match r.reward_type {
        1 => Some("fee"),
        2 => Some("rent"),
        3 => Some("staking"),
        4 => Some("voting"),
        5 => Some("deactivatedStake"),
        _ => None,
    };
    json!({"pubkey":r.pubkey,"lamports":r.lamports,"postBalance":r.post_balance,"rewardType":kind,"commission":r.commission.parse::<u32>().ok(),"commissionBps":r.commission_bps.parse::<u32>().ok()})
}
fn token(b: &TokenBalance) -> Value {
    json!({"accountIndex":b.account_index,"mint":b.mint,"owner":b.owner,"programId":b.program_id,"uiTokenAmount":b.ui_token_amount.as_ref().map(|u|json!({"amount":u.amount,"decimals":u.decimals,"uiAmount":u.ui_amount,"uiAmountString":u.ui_amount_string}))})
}
fn transaction(i: &SubscribeUpdateTransactionInfo) -> Result<Value> {
    let t = i
        .transaction
        .as_ref()
        .context("complete model transaction")?;
    let m = t.message.as_ref().context("message")?;
    let h = m.header.as_ref().context("header")?;
    let meta = i.meta.as_ref().context("meta")?;
    let mut message = json!({"header":{"numRequiredSignatures":h.num_required_signatures,"numReadonlySignedAccounts":h.num_readonly_signed_accounts,"numReadonlyUnsignedAccounts":h.num_readonly_unsigned_accounts},"accountKeys":m.account_keys.iter().map(|k|b58(k)).collect::<Vec<_>>(),"recentBlockhash":b58(&m.recent_blockhash),"instructions":m.instructions.iter().map(|x|json!({"programIdIndex":x.program_id_index,"accounts":x.accounts,"data":b58(&x.data)})).collect::<Vec<_>>()});
    if m.versioned {
        message["addressTableLookups"]=json!(m.address_table_lookups.iter().map(|a|json!({"accountKey":b58(&a.account_key),"writableIndexes":a.writable_indexes,"readonlyIndexes":a.readonly_indexes})).collect::<Vec<_>>());
    }
    if let Some(c) = &m.config {
        message["transactionConfig"] = json!({"priorityFee":c.priority_fee,"computeUnitLimit":c.compute_unit_limit,"loadedAccountsDataSizeLimit":c.loaded_accounts_data_size_limit,"heapSize":c.heap_size});
    }
    let inner=meta.inner_instructions.iter().map(|g|json!({"index":g.index,"instructions":g.instructions.iter().map(|x|json!({"programIdIndex":x.program_id_index,"accounts":x.accounts,"data":b58(&x.data),"stackHeight":x.stack_height})).collect::<Vec<_>>()})).collect::<Vec<_>>();
    Ok(
        json!({"version":if m.config.is_some(){json!(1)}else if m.versioned {json!(0)}else{json!("legacy")},"transaction":{"signatures":t.signatures.iter().map(|s|b58(s)).collect::<Vec<_>>(),"message":message},"meta":{"err":meta.err.as_ref().map(|e|serde_json::from_slice::<Value>(&e.err).expect("only modeled JSON error bytes")),"fee":meta.fee,"preBalances":meta.pre_balances,"postBalances":meta.post_balances,"innerInstructions":if meta.inner_instructions_none {Value::Null}else{json!(inner)},"logMessages":if meta.log_messages_none {Value::Null}else{json!(meta.log_messages)},"preTokenBalances":meta.pre_token_balances.iter().map(token).collect::<Vec<_>>(),"postTokenBalances":meta.post_token_balances.iter().map(token).collect::<Vec<_>>(),"rewards":meta.rewards.iter().map(reward).collect::<Vec<_>>(),"loadedAddresses":{"writable":meta.loaded_writable_addresses.iter().map(|a|b58(a)).collect::<Vec<_>>(),"readonly":meta.loaded_readonly_addresses.iter().map(|a|b58(a)).collect::<Vec<_>>()},"returnData":meta.return_data.as_ref().map(|d|json!({"programId":b58(&d.program_id),"data":[STANDARD.encode(&d.data),"base64"]})),"computeUnitsConsumed":meta.compute_units_consumed,"costUnits":meta.cost_units}}),
    )
}
pub(super) fn block(b: &SubscribeUpdateBlock) -> Result<Value> {
    Ok(
        json!({"blockhash":b.blockhash,"previousBlockhash":b.parent_blockhash,"parentSlot":b.parent_slot,"blockTime":b.block_time.as_ref().map(|t|t.timestamp),"blockHeight":b.block_height.as_ref().map(|h|h.block_height),"rewards":b.rewards.as_ref().map(|r|r.rewards.iter().map(reward).collect::<Vec<_>>()).unwrap_or_default(),"numRewardPartitions":b.rewards.as_ref().and_then(|r|r.num_partitions.as_ref().map(|p|p.num_partitions)),"transactions":b.transactions.iter().map(transaction).collect::<Result<Vec<_>>>()?}),
    )
}
