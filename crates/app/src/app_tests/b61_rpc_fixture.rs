use super::b61_receipt_fixture::*;
use super::{open_risk_sell_task_fixture::RpcTask, rpc_simulation_http_fixture::read};
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

#[derive(Default)]
pub(super) struct Control {
    pub calls: Vec<(String, Value)>,
    pub sent: Option<(String, Value)>,
    pub receipt_enabled: bool,
    completed_responses: usize,
}
pub(super) struct Server {
    pub url: String,
    pub state: Arc<Mutex<Control>>,
    task: RpcTask,
}
impl Server {
    pub async fn new(path: PathBuf, payer: [u8; 32]) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(Control::default()));
        let capture = state.clone();
        let task = RpcTask::new(tokio::spawn(async move {
            loop {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(15), listener.accept()).await??;
                let (path_url, request) =
                    tokio::time::timeout(Duration::from_secs(3), read(&mut stream)).await??;
                let method = if path_url.starts_with("/quote?") {
                    if path_url.contains("inputMint=So11111111111111111111111111111111111111112") {
                        "quote_buy"
                    } else {
                        "quote_sell"
                    }
                } else if path_url == "/swap-instructions" {
                    "instructions"
                } else if path_url == "/swap" {
                    "swap"
                } else {
                    request["method"].as_str().context("expected RPC method")?
                };
                let response = {
                    let mut state = capture.lock().unwrap();
                    ensure!(state.calls.len() < 100, "bounded B61 RPC budget");
                    state.calls.push((
                        method.into(),
                        if method.starts_with("quote_") {
                            json!({"url":path_url})
                        } else {
                            request.clone()
                        },
                    ));
                    match method {
                        "quote_buy" | "quote_sell" => {
                            let url = reqwest::Url::parse(&format!("http://localhost{path_url}"))?;
                            let q: std::collections::HashMap<_, _> =
                                url.query_pairs().into_owned().collect();
                            let out = if method == "quote_sell" {
                                ensure!(
                                    q["inputMint"] == key(MINT)
                                        && q["outputMint"]
                                            == "So11111111111111111111111111111111111111112"
                                );
                                ensure!(
                                    q["amount"] == RAW.to_string(),
                                    "full exit must match wallet raw"
                                );
                                GROSS
                            } else {
                                ensure!(q["outputMint"] == key(MINT));
                                RAW // Diagnostic BUY quote only; entry submit remains disabled.
                            };
                            json!({"inputMint":q["inputMint"],"outputMint":q["outputMint"],"inAmount":q["amount"],
                                "outAmount":out.to_string(),"otherAmountThreshold":(out*9/10).to_string(),"swapMode":"ExactIn",
                                "slippageBps":q["slippageBps"].parse::<u64>()?,"priceImpactPct":"0","routePlan":[{"swapInfo":{"label":"Metis"}}]})
                        }
                        "instructions" => bundle(payer)?,
                        "swap" => {
                            json!({"swapTransaction":transaction(payer)?,"simulationError":null})
                        }
                        _ => {
                            let result = match method {
                                "qn_estimatePriorityFees" => json!({"recommended":100000}),
                                "getFeeForMessage" => json!({"context":{"slot":4241},"value":FEE}),
                                "getTokenAccountsByOwner" => {
                                    ensure!(request["params"][0] == key(payer));
                                    if request["params"][1]["programId"]
                                        == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
                                    {
                                        json!({"context":{"slot":4241},"value":[]})
                                    } else {
                                        let raw = if state.sent.is_some() { 0 } else { RAW };
                                        json!({"context":{"slot":4241},"value":[{"pubkey":key([63;32]),"account":{
                                            "owner":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","data":{"parsed":{"info":{
                                            "mint":key(MINT),"owner":key(payer),"tokenAmount":{"amount":raw.to_string(),"decimals":3}}}}}}]})
                                    }
                                }
                                "getTokenSupply" => {
                                    json!({"context":{"slot":4241},"value":{"amount":"17000","decimals":3}})
                                }
                                "simulateTransaction" => {
                                    let payload = request["params"][0]
                                        .as_str()
                                        .context("simulation bytes")?;
                                    let actual = crate::execution_transaction_wire::decode_message(
                                        payload,
                                        |_| Ok(()),
                                    )?;
                                    let expected =
                                        crate::execution_transaction_wire::decode_message(
                                            &transaction(payer)?,
                                            |_| Ok(()),
                                        )?;
                                    ensure!(
                                        actual.binding.message_bytes
                                            == expected.binding.message_bytes
                                    );
                                    json!({"context":{"slot":4241},"value":{"err":null,"logs":[]}})
                                }
                                "sendTransaction" => {
                                    ensure!(state.sent.is_none(), "duplicate send");
                                    let sent = accept_send(
                                        &path,
                                        payer,
                                        request["params"][0].as_str().context("send bytes")?,
                                        chrono::Utc::now().timestamp(),
                                    )?;
                                    let signature = sent.0.clone();
                                    state.sent = Some(sent);
                                    json!(signature)
                                }
                                "getSignatureStatuses" => {
                                    let sent = state
                                        .sent
                                        .as_ref()
                                        .context("status without verified send")?;
                                    ensure!(request["params"][0] == json!([sent.0]));
                                    json!({"context":{"slot":4242},"value":[{"slot":4242,"confirmations":1,
                                        "confirmationStatus":"confirmed","err":null,"status":{"Ok":null}}]})
                                }
                                "getTransaction" => {
                                    let sent = state
                                        .sent
                                        .as_ref()
                                        .context("receipt without verified send")?;
                                    ensure!(request["params"][0] == sent.0);
                                    if state.receipt_enabled {
                                        sent.1.clone()
                                    } else {
                                        Value::Null
                                    }
                                }
                                _ => bail!("unexpected B61 RPC {method}"),
                            };
                            json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                        }
                    }
                };
                let body = response.to_string();
                tokio::time::timeout(Duration::from_secs(3),stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes())).await??;
                capture.lock().unwrap().completed_responses += 1;
            }
        }));
        Ok(Self { url, state, task })
    }
    pub fn count(&self, method: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(m, _)| m == method)
            .count()
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.task.finish().await?;
        let s = self.state.lock().unwrap();
        ensure!(
            s.calls.len() == s.completed_responses,
            "unfinished RPC handler"
        );
        Ok(())
    }
}
