//! LOCAL external provider only. The daemon owns every order, signature and send.
//! System/SPL effects and AMM CPI use the accepted instruction model, not a VM.
use super::{owner_buy_wire_fixture as buy, owner_exit_test_fixture as sell};
use super::{wsol_lifecycle_fixture as wire, wsol_lifecycle_oracle as model};
use crate::execution_pumpswap_accounts::*;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) type History = Arc<Mutex<BTreeMap<String, (String, Value)>>>;
pub(super) struct State {
    pub bank: model::Bank,
    pub calls: Vec<String>,
    pub requests: Vec<Value>,
    pub sends: Vec<String>,
    pub unknown_sell: bool,
    pub confirmation_status: &'static str,
    pub genesis: String,
    buy_raw: u64,
    buy_slot: u64,
    sell_slot: u64,
    history: History,
}
pub(super) struct Peer {
    pub url: String,
    pub state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<Result<()>>,
    failure: Arc<Mutex<Option<String>>>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    pub async fn new(wallet: [u8; 32], raw: u64, slot: u64, history: History) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(State {
            bank: model::Bank::new(wallet)?,
            calls: vec![],
            requests: vec![],
            sends: vec![],
            unknown_sell: false,
            confirmation_status: "confirmed",
            genesis: super::owner_buy_fixture::GENESIS.into(),
            buy_raw: raw,
            buy_slot: slot,
            sell_slot: slot + 2,
            history,
        }));
        let shared = state.clone();
        let failure = Arc::new(Mutex::new(None));
        let finished = failure.clone();
        let task = tokio::spawn(async move {
            let result = async {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let Some(request) = read(&mut socket).await.context("LOCAL request read")? else {
                    continue; // A dropped recovery future may leave no complete request.
                };
                let response = respond(&request, &mut shared.lock().unwrap())
                    .with_context(|| format!("LOCAL response method={}", request["method"]))?;
                let body = serde_json::to_vec(&response)?;
                let written: std::io::Result<()> = async {
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await?;
                socket.write_all(&body).await
                }.await;
                if let Err(error) = written {
                    if disconnected(error.kind()) { continue; }
                    return Err(error).context("LOCAL response write");
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
            state,
            task,
            failure,
        })
    }
    pub fn healthy(&self) -> Result<()> {
        ensure!(
            !self.task.is_finished(),
            "LOCAL copy cycle provider terminated: {}",
            self.failure
                .lock()
                .unwrap()
                .as_deref()
                .unwrap_or("task panicked or aborted")
        );
        Ok(())
    }
    pub fn sends(&self) -> usize {
        self.state.lock().unwrap().sends.len()
    }
    pub fn artifact(&self, side: usize) -> Result<(String, Value)> {
        let state = self.state.lock().unwrap();
        let sig = state.sends.get(side).context("LOCAL send absent")?;
        let found = state.history.lock().unwrap().get(sig).cloned();
        found.context("LOCAL receipt absent")
    }
}
fn disconnected(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}
async fn read_chunk(
    socket: &mut tokio::net::TcpStream,
    bytes: &mut [u8],
) -> std::io::Result<usize> {
    match socket.read(bytes).await {
        Err(error) if disconnected(error.kind()) => Ok(0),
        other => other,
    }
}
async fn read(socket: &mut tokio::net::TcpStream) -> Result<Option<Value>> {
    let mut bytes = vec![];
    let (end, len) = loop {
        let mut chunk = [0; 4096];
        let n = read_chunk(socket, &mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        ensure!(bytes.len() < 1 << 20, "LOCAL request bound");
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(at) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..at]);
            if header.starts_with("GET ") {
                let url = reqwest::Url::parse(&format!(
                    "http://localhost{}",
                    header.split_whitespace().nth(1).unwrap()
                ))?;
                let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
                return Ok(Some(json!({"method":"quote","params":params})));
            }
            let len = header
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .context("LOCAL content length")?;
            break (at + 4, len);
        }
    };
    while bytes.len() < end + len {
        let mut chunk = [0; 4096];
        let n = read_chunk(socket, &mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    Ok(Some(serde_json::from_slice(&bytes[end..end + len])?))
}
fn account(s: &State, key: &str) -> Result<Value> {
    let raw = parse_pubkey(key, "LOCAL account")?;
    if key == buy::USDC {
        let mut data = [0; 82];
        data[44] = 6;
        data[45] = 1;
        return Ok(
            json!({"lamports":model::RENT,"owner":bs58::encode(token_program_id()).into_string(),"executable":false,"data":[STANDARD.encode(data),"base64"]}),
        );
    }
    let Some(a) = s.bank.accounts.get(&raw) else {
        return Ok(Value::Null);
    };
    let (owner, data) = if let Some((mint, wallet, amount)) = a.token {
        let mut data = vec![0; 165];
        data[..32].copy_from_slice(&mint);
        data[32..64].copy_from_slice(&wallet);
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        data[108] = 1;
        (token_program_id(), data)
    } else {
        (system_program_id(), vec![])
    };
    Ok(
        json!({"lamports":a.lamports,"owner":bs58::encode(owner).into_string(),"executable":false,"data":[STANDARD.encode(data),"base64"]}),
    )
}
fn inventory(s: &State, slot: u64) -> Result<Value> {
    let usdc = parse_pubkey(buy::USDC, "LOCAL usdc")?;
    let rows: Vec<_> = s.bank.accounts.iter().filter_map(|(key,a)| {
        let (mint, wallet, raw) = a.token?;
        (mint == usdc && wallet == s.bank.wallet).then(|| json!({"pubkey":bs58::encode(key).into_string(),"account":{
            "owner":bs58::encode(token_program_id()).into_string(),"data":{"parsed":{"type":"account","info":{
            "owner":bs58::encode(wallet).into_string(),"mint":buy::USDC,"tokenAmount":{"amount":raw.to_string(),"decimals":6}}}}}}))
    }).collect();
    Ok(json!({"context":{"slot":slot},"value":rows}))
}
fn quote(r: &Value, s: &State) -> Result<Value> {
    let buying = r["params"]["inputMint"] != buy::USDC;
    let mut q = if buying { buy::quote() } else { sell::quote() };
    let input = r["params"]["amount"]
        .as_str()
        .context("LOCAL quote amount")?;
    let output = if buying { s.buy_raw } else { sell::OUT };
    q["inAmount"] = json!(input);
    q["outAmount"] = json!(output.to_string());
    q["otherAmountThreshold"] = json!((output * 9950 / 10000).to_string());
    q["routePlan"][0]["swapInfo"]["inAmount"] = json!(input);
    q["routePlan"][0]["swapInfo"]["outAmount"] = json!(output.to_string());
    Ok(q)
}
fn instructions(r: &Value, s: &State) -> Result<Value> {
    let buying = r["quoteResponse"]["inputMint"] != buy::USDC;
    let mut ix = if buying {
        buy::instructions(s.bank.wallet)?
    } else {
        sell::instructions(s.bank.wallet)?
    };
    let jup = parse_pubkey(crate::execution_owner_buy_wire::JUPITER, "LOCAL jupiter")?;
    let route = ix.iter_mut().find(|i| i.program_id == jup).unwrap();
    route.data[16..24].copy_from_slice(
        &r["quoteResponse"]["inAmount"]
            .as_str()
            .unwrap()
            .parse::<u64>()?
            .to_le_bytes(),
    );
    route.data[24..32].copy_from_slice(
        &r["quoteResponse"]["outAmount"]
            .as_str()
            .unwrap()
            .parse::<u64>()?
            .to_le_bytes(),
    );
    if !buying {
        route.accounts[4] = crate::execution_solana_tx::SolanaAccountMeta::readonly(jup);
    }
    // The accepted chain model emits AMM-v2 opcode16. Choose the matching,
    // already-supported compact Jupiter leg105 before the daemon builds/signs.
    // A classic leg7 with opcode16 is contradictory external fixture evidence.
    wire::bundle(&wire::compact_buy(&ix))
}
fn respond(r: &Value, s: &mut State) -> Result<Value> {
    let method = r["method"].as_str().unwrap_or("instructions");
    s.calls.push(method.into());
    s.requests.push(r.clone());
    if method == "quote" {
        return quote(r, s);
    }
    if method == "instructions" {
        return instructions(r, s);
    }
    let slot = s.sell_slot;
    let result = match method {
        "getGenesisHash" => json!(s.genesis),
        "getSlot" => json!(119),
        "getBalance" => {
            json!({"context":{"slot":slot},"value":s.bank.accounts[&s.bank.wallet].lamports})
        }
        "getAccountInfo" => {
            json!({"context":{"slot":slot},"value":account(s,r["params"][0].as_str().unwrap())?})
        }
        "getMultipleAccounts" => {
            json!({"context":{"slot":slot},"value":r["params"][0].as_array().unwrap().iter().map(|k|account(s,k.as_str().unwrap())).collect::<Result<Vec<_>>>()?})
        }
        "getTokenAccountsByOwner" => inventory(s, slot)?,
        "getMinimumBalanceForRentExemption" => json!(model::RENT),
        "getFeeForMessage" => json!({"context":{"slot":slot},"value":model::FEE}),
        "qn_estimatePriorityFees" => json!({"recommended":0}),
        "simulateTransaction" => {
            json!({"context":{"slot":slot},"value":{"err":null,"logs":[],"unitsConsumed":100000}})
        }
        "isBlockhashValid" => json!({"context":{"slot":slot},"value":true}),
        "getBlockHeight" => json!(0),
        "sendTransaction" => {
            ensure!(s.sends.len() < 2, "LOCAL duplicate external send");
            let payload = r["params"][0].as_str().unwrap();
            let bytes = STANDARD.decode(payload)?;
            let sig = bs58::encode(&bytes[1..65]).into_string();
            let mut receipt = s.bank.execute(payload)?;
            receipt["result"]["slot"] = json!(if s.sends.is_empty() {
                s.buy_slot
            } else {
                s.sell_slot
            });
            receipt["result"]["transaction"]["signatures"] = json!([sig]);
            s.history
                .lock()
                .unwrap()
                .insert(sig.clone(), (payload.into(), receipt));
            s.sends.push(sig.clone());
            json!(sig)
        }
        "getSignatureStatuses" => {
            if s.unknown_sell && s.sends.len() == 2 {
                json!({"context":{"slot":slot},"value":[null]})
            } else {
                json!({"context":{"slot":slot},"value":[{"err":null,"slot":if s.sends.len()==1{s.buy_slot}else{s.sell_slot},"confirmationStatus":s.confirmation_status}]})
            }
        }
        "getTransaction" => {
            let sig = r["params"][0].as_str().unwrap();
            if s.unknown_sell && s.sends.get(1).is_some_and(|v| v == sig) {
                Value::Null
            } else {
                s.history
                    .lock()
                    .unwrap()
                    .get(sig)
                    .context("LOCAL unknown signature")?
                    .1["result"]
                    .clone()
            }
        }
        other => anyhow::bail!("LOCAL unexpected provider method {other}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
