use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) struct Server {
    pub url: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
    pub fault: Arc<Mutex<String>>,
    pause: Arc<Mutex<Option<Pause>>>,
    task: tokio::task::JoinHandle<Result<()>>,
    terminal: Arc<Mutex<Option<String>>>,
}
struct Pause {
    method: String,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort()
    }
}
impl Server {
    pub async fn new() -> Result<Self> {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", l.local_addr()?);
        let calls = Arc::new(Mutex::new(vec![]));
        let captured = calls.clone();
        let fault = Arc::new(Mutex::new(String::new()));
        let errors = fault.clone();
        let pause = Arc::new(Mutex::new(None::<Pause>));
        let held = pause.clone();
        let terminal = Arc::new(Mutex::new(None));
        let finished = terminal.clone();
        let task = tokio::spawn(async move {
            let result: Result<()> = async {
            'accept: loop {
                let (mut s, _) = l.accept().await?;
                let mut data = vec![];
                let (header, length) = loop {
                    let mut b = [0; 4096];
                    let n = s.read(&mut b).await?;
                        if n == 0 {
                            // A cancelled request may close before its full body arrives.
                            continue 'accept;
                        }
                        ensure!(data.len() + n <= 1 << 20, "bounded request");
                    data.extend_from_slice(&b[..n]);
                    if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let h = String::from_utf8(data[..i].to_vec())?;
                        let n = h
                            .lines()
                            .find_map(|s| {
                                s.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if data.len() >= i + 4 + n {
                            break (h, n);
                        }
                    }
                };
                let req: Value = if header.starts_with("GET ") {
                    let path = header.split_whitespace().nth(1).unwrap();
                    let params: std::collections::HashMap<_, _> =
                        reqwest::Url::parse(&format!("http://localhost{path}"))?
                            .query_pairs()
                            .into_owned()
                            .collect();
                    json!({"method":"quote","params":params})
                } else {
                    serde_json::from_slice(&data[data.len() - length..])?
                };
                captured.lock().unwrap().push(req.clone());
                let wait = {
                    let mut hold = held.lock().unwrap();
                    if hold.as_ref().is_some_and(|p| {
                        Some(p.method.as_str()) == req["method"].as_str()
                            || p.method
                                == format!(
                                    "{}:{}",
                                    req["method"].as_str().unwrap_or(""),
                                    captured
                                        .lock()
                                        .unwrap()
                                        .iter()
                                        .filter(|v| v["method"] == req["method"])
                                        .count()
                                )
                            || (p.method == "instructions" && req.get("quoteResponse").is_some())
                    }) {
                        hold.take()
                    } else {
                        None
                    }
                };
                if let Some(p) = wait {
                    let _ = p.reached.send(());
                    p.release.await?;
                }
                let fault = errors.lock().unwrap().clone();
                let custom = super::b136_rpc::response(&req, &captured.lock().unwrap(), &fault)?;
                let mut result = match custom {
                    Some(v) => v,
                    None => respond(&req)?,
                };
                fault_response(&fault, &req, &mut result);
                let wire = result.to_string();
                if fault == "endpoint" && req["method"] == "getGenesisHash" {
                    s.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
                    continue;
                }
                if let Err(error) = s.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{wire}",wire.len()).as_bytes()).await {
                    match error.kind() {
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset => continue 'accept,
                        _ => return Err(error.into()),
                    }
                }
            }
            }.await;
            if let Err(error) = &result {
                *finished.lock().unwrap() = Some(format!("{error:#}"));
            }
            result
        });
        Ok(Self {
            url,
            calls,
            task,
            fault,
            pause,
            terminal,
        })
    }
    pub fn hold(
        &self,
        method: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (sent, reached) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self.pause.lock().unwrap() = Some(Pause {
            method: method.into(),
            reached: sent,
            release: wait,
        });
        (reached, release)
    }
    pub fn healthy(&self) {
        assert!(
            !self.task.is_finished(),
            "loopback peer ended: {:?}",
            *self.terminal.lock().unwrap()
        );
    }
    pub fn terminal(&self) -> Option<String> {
        self.terminal.lock().unwrap().clone()
    }
    pub fn check(&self) -> Result<()> {
        ensure!(
            !self.task.is_finished(),
            "loopback peer terminated: {:?}",
            self.terminal()
        );
        Ok(())
    }
}
fn fault_response(f: &str, req: &Value, r: &mut Value) {
    if req["method"] == "getGenesisHash" {
        match f {
            "genesis" => r["result"] = json!("other"),
            "request" => r["id"] = json!("wrong-id"),
            "jsonrpc" => r["jsonrpc"] = json!("1.0"),
            _ => {}
        }
    }
    if req["method"] == "getTransaction" {
        let sell = r["result"]["slot"] == 150;
        match f {
            "null" => r["result"] = Value::Null,
            "meta" => r["result"]["meta"] = Value::Null,
            "err" => r["result"]["meta"]["err"] = json!({"InstructionError":[0,"InvalidArgument"]}),
            "version" => r["result"]["version"] = json!(1),
            "signature" => r["result"]["transaction"]["signatures"][0] = json!("wrong"),
            "buy_receipt" if !sell => r["result"]["meta"]["postBalances"][0] = json!(1),
            "buy_wallet" if !sell => {
                r["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                    json!("11111111111111111111111111111111")
            }
            "same" if sell => r["result"]["slot"] = json!(120),
            "earlier" if sell => r["result"]["slot"] = json!(119),
            "sell_wallet" if sell => {
                r["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                    json!("11111111111111111111111111111111")
            }
            "sell_no_instruction" if sell => {
                r["result"]["transaction"]["message"]["instructions"] = json!([])
            }
            "sell_wrong_instruction" if sell => {
                r["result"]["transaction"]["message"]["instructions"][0]["data"] =
                    json!(bs58::encode([0; 24]).into_string())
            }
            "conflict" if sell => {
                for field in ["preTokenBalances", "postTokenBalances"] {
                    let mut row = r["result"]["meta"][field][0].clone();
                    row["accountIndex"] = json!(2);
                    row["uiTokenAmount"]["amount"] = json!(if field == "preTokenBalances" {
                        "0"
                    } else {
                        "1"
                    });
                    r["result"]["meta"][field].as_array_mut().unwrap().push(row);
                }
            }
            "sell_amount" if sell => {
                r["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("1")
            }
            "sell_mint" if sell => {
                r["result"]["transaction"]["message"]["instructions"][0]["accounts"][3] =
                    json!("11111111111111111111111111111111")
            }
            _ => {}
        }
    }
    if f == "total_fee" && req["method"] == "getFeeForMessage" {
        r["result"]["value"] = json!(100001);
    }
}
fn respond(r: &Value) -> Result<Value> {
    let result = match r["method"].as_str() {
        Some("quote") => {
            return Ok(
                json!({"inputMint":r["params"]["inputMint"],"outputMint":r["params"]["outputMint"],"inAmount":r["params"]["amount"],"outAmount":"1000000","otherAmountThreshold":"950000","swapMode":"ExactIn","slippageBps":r["params"]["slippageBps"].as_str().unwrap().parse::<u64>()?,"routePlan":[{"swapInfo":{"label":"Jupiter"}}]}),
            )
        }
        Some("getGenesisHash") => json!("11111111111111111111111111111111"),
        Some("getTransaction") => {
            let root = super::b136_fixture::inputs();
            let meta: Value = serde_json::from_slice(&std::fs::read(root.join("chain.json"))?)?;
            let which = if r["params"][0] == meta["our"]["signature"] {
                "our"
            } else if r["params"][0] == meta["sell"]["signature"] {
                "sell"
            } else {
                anyhow::bail!("unexpected signature")
            };
            serde_json::from_slice(&std::fs::read(root.join(format!("{which}-rpc.json")))?)?
        }
        Some("simulateTransaction") => {
            json!({"context":{"slot":151},"value":{"err":null,"logs":[],"unitsConsumed":100000}})
        }
        Some("getFeeForMessage") => json!({"context":{"slot":151},"value":19000}),
        None if r.get("quoteResponse").is_some() => {
            let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
                r["userPublicKey"].as_str().unwrap(),
                "fixture",
            )?;
            let mut bundle =
                super::generic_sell_synthetic_fixture::bundle(wallet, 1_400_000, 10000);
            bundle["blockhashWithMetadata"]["lastValidBlockHeight"] = json!(1000);
            return Ok(bundle);
        }
        _ => anyhow::bail!("unexpected transport method"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
