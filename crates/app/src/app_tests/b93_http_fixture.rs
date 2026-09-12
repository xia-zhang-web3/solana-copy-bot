use super::open_risk_sell_task_fixture::RpcTask;
use super::rpc_simulation_http_fixture::read;
use super::source_write_off_fixture::snapshot;
use anyhow::{bail, ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::io::AsyncWriteExt;

pub(super) type Snapshot = BTreeMap<String, Vec<String>>;
#[derive(Default)]
pub(super) struct Control {
    pub mutate_at: Option<&'static str>,
    pub mutation: Option<Box<dyn FnOnce() -> Result<()> + Send>>,
    pub wallet_raw: u64,
    pub receipt: Option<Value>,
    pub after_wallet_raw: Option<u64>,
    pub wallet_decimals: u8,
    pub error_at: Option<&'static str>,
    pub after_mutation: Option<Snapshot>,
    pub calls: Vec<(String, Value)>,
    pub responses: Vec<(String, Value)>,
}
pub(super) struct Server {
    pub url: String,
    pub state: Arc<Mutex<Control>>,
    task: RpcTask,
}
impl Server {
    pub async fn new(path: PathBuf, _now: DateTime<Utc>, payer: [u8; 32]) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(Control {
            wallet_raw: 7000,
            wallet_decimals: 3,
            ..Default::default()
        }));
        let capture = state.clone();
        let task = RpcTask::new(tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await?;
                let (path_url, request) = read(&mut stream).await?;
                let method = if path_url.starts_with("/quote?") {
                    "quote"
                } else if path_url.contains("/pump-fun/") {
                    "fallback"
                } else if path_url == "/swap-instructions" {
                    "instructions"
                } else if path_url == "/swap" {
                    "swap"
                } else {
                    request["method"].as_str().unwrap_or("unknown")
                };
                let error = {
                    let mut state = capture.lock().unwrap();
                    ensure!(state.calls.len() < 200, "source guard RPC budget exceeded");
                    state
                        .calls
                        .push((method.into(), json!({"url":path_url,"body":request})));
                    if state.mutate_at == Some(method) {
                        state.mutate_at = None;
                        if let Some(mutation) = state.mutation.take() {
                            mutation()?;
                        }
                        if let Some(raw) = state.after_wallet_raw.take() {
                            state.wallet_raw = raw;
                        }
                        state.after_mutation =
                            Some(snapshot(&rusqlite::Connection::open(&path)?, &[])?);
                    }
                    state.error_at == Some(method)
                };
                let response = if error {
                    if method == "simulateTransaction" {
                        json!({"jsonrpc":"2.0","id":request["id"],"result":{"value":{"err":{"InstructionError":[0,"Custom"]}}}})
                    } else {
                        json!({"error":"NO_ROUTES_FOUND"})
                    }
                } else {
                    match method {
                        "quote" => {
                            let url = reqwest::Url::parse(&format!("http://localhost{path_url}"))?;
                            let query: std::collections::HashMap<_, _> =
                                url.query_pairs().into_owned().collect();
                            json!({"inputMint":query["inputMint"],"outputMint":query["outputMint"],
                                "inAmount":query["amount"],"outAmount":"100000000","otherAmountThreshold":"90000000",
                                "swapMode":"ExactIn","slippageBps":query["slippageBps"].parse::<u64>()?,"priceImpactPct":"0",
                                "routePlan":[{"swapInfo":{"label":"Metis"}}]})
                        }
                        "fallback" => json!({"error":"NO_ROUTES_FOUND"}),
                        "instructions" => {
                            super::generic_sell_synthetic_fixture::bundle(payer, 200_000, 100_000)
                        }
                        "swap" => {
                            json!({"swapTransaction":super::priority_fee_fixture::transaction(payer,200000,100000),"simulationError":null})
                        }
                        _ => {
                            let result = match method {
                                "qn_estimatePriorityFees" => json!({"recommended":100000}),
                                "getTokenAccountsByOwner" => {
                                    let (raw, decimals) = {
                                        let c = capture.lock().unwrap();
                                        (c.wallet_raw, c.wallet_decimals)
                                    };
                                    json!({"value":[{"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":raw.to_string(),"decimals":decimals}}}}}}]})
                                }
                                "getTokenSupply" => {
                                    json!({"context":{"slot":1},"value":{"amount":"1000000","decimals":3}})
                                }
                                "simulateTransaction" => {
                                    json!({"context":{"slot":42},"value":{"err":null,"logs":[]}})
                                }
                                "sendTransaction" => {
                                    bail!("B92 boundary: submit forbidden even on loopback")
                                }
                                "getSignatureStatuses" => json!({"value":[null]}),
                                "getTransaction" => capture
                                    .lock()
                                    .unwrap()
                                    .receipt
                                    .clone()
                                    .unwrap_or(Value::Null),
                                _ => bail!("unexpected source guard fixture request {method}"),
                            };
                            json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                        }
                    }
                };
                capture
                    .lock()
                    .unwrap()
                    .responses
                    .push((method.into(), response.clone()));
                let body = response.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await?;
            }
        }));
        Ok(Self { url, state, task })
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.task.finish().await
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
}
