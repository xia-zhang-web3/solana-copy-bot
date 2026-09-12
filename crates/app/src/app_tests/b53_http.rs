use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Notify},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Send {
    #[default]
    Pending,
    Hold,
    BadBody,
    BadJson,
    HttpError,
    MissingResult,
    RpcError,
    Disconnect,
    EmptyResult,
    Mismatch,
}
#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Chain {
    #[default]
    Pending,
    MissingReceipt,
    Settled,
    Failed,
}
#[derive(Default)]
pub(super) struct State {
    pub mode: Send,
    pub chain: HashMap<String, Chain>,
    pub requests: Vec<Value>,
    pub sends: Vec<Value>,
    pub completions: usize,
}
pub(super) struct Rpc {
    pub url: String,
    pub state: Arc<Mutex<State>>,
    pub received: Arc<Notify>,
    pub release: Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}
impl Rpc {
    pub async fn new(upstream: String, wallet: String) -> Result<Self> {
        ensure!(upstream.starts_with("http://127.0.0.1:"));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(State::default()));
        let received = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (stop, mut stopping) = oneshot::channel();
        let (s, got, go) = (state.clone(), received.clone(), release.clone());
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopping => break,
                    socket = listener.accept() => {
                        let (socket, _) = socket?;
                        let (s, got, go, endpoint, payer) = (s.clone(), got.clone(), go.clone(), upstream.clone(), wallet.clone());
                        tasks.spawn(async move {
                            tokio::time::timeout(Duration::from_secs(3), handle(socket, endpoint, payer, s, got, go)).await?
                        });
                    }
                }
            }
            while let Some(done) = tasks.join_next().await {
                done??;
            }
            Ok(())
        });
        Ok(Self {
            url,
            state,
            received,
            release,
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub async fn finish(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(4), task).await???;
        }
        let s = self.state.lock().unwrap();
        ensure!(
            s.completions == s.requests.len(),
            "all full requests completed: {}/{}",
            s.completions,
            s.requests.len()
        );
        Ok(())
    }
}
impl Drop for Rpc {
    fn drop(&mut self) {
        if let Some(t) = &self.task {
            t.abort();
        }
    }
}

async fn handle(
    mut socket: TcpStream,
    upstream: String,
    wallet: String,
    state: Arc<Mutex<State>>,
    got: Arc<Notify>,
    go: Arc<Notify>,
) -> Result<()> {
    let (path, body) = read(&mut socket).await?;
    state
        .lock()
        .unwrap()
        .requests
        .push(json!({"path":path,"body":body}));
    let method = body["method"].as_str().unwrap_or("");
    let mut code = 200;
    let text = if method == "sendTransaction" {
        let wire = body["params"][0].as_str().unwrap();
        let bytes = STANDARD.decode(wire)?;
        ensure!(bytes[0] == 1);
        let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65])?;
        let payer: [u8; 32] = bs58::decode(&wallet).into_vec()?.try_into().unwrap();
        ed25519_dalek::VerifyingKey::from_bytes(&payer)?.verify_strict(&bytes[65..], &signature)?;
        let binding = crate::execution_transaction_wire::decode_message(wire, |_| Ok(()))?.binding;
        let sig = bs58::encode(signature.to_bytes()).into_string();
        let mode = {
            let mut s = state.lock().unwrap();
            s.sends.push(json!({"signature":sig,"message_sha256":binding.message_sha256,"transaction_sha256":binding.transaction_sha256,"complete_request":true,"valid_signature":true}));
            std::mem::take(&mut s.mode)
        };
        got.notify_one();
        match mode {
            Send::Hold => { go.notified().await; state.lock().unwrap().completions += 1; return Ok(()); }
            Send::BadBody => {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{").await?;
                state.lock().unwrap().completions += 1; return Ok(());
            }
            Send::Disconnect => { state.lock().unwrap().completions += 1; return Ok(()); }
            Send::EmptyResult => json!({"jsonrpc":"2.0","id":body["id"],"result":""}).to_string(),
            Send::Mismatch => json!({"jsonrpc":"2.0","id":body["id"],"result":"untrusted-other-signature"}).to_string(),
            Send::BadJson => "{broken-json".to_owned(),
            Send::HttpError => { code = 503; "upstream outcome unavailable".to_owned() }
            Send::MissingResult => json!({"jsonrpc":"2.0","id":body["id"],"result":null}).to_string(),
            Send::RpcError => json!({"jsonrpc":"2.0","id":body["id"],"error":{"code":-32002,"message":"synthetic not sent"}}).to_string(),
            Send::Pending => json!({"jsonrpc":"2.0","id":body["id"],"result":sig}).to_string(),
        }
    } else if matches!(method, "getSignatureStatuses" | "getTransaction") {
        let sig = if method == "getSignatureStatuses" {
            body["params"][0][0].as_str()
        } else {
            body["params"][0].as_str()
        }
        .unwrap();
        let chain = state
            .lock()
            .unwrap()
            .chain
            .get(sig)
            .copied()
            .unwrap_or_default();
        let result = if method == "getSignatureStatuses" {
            let status = match chain {
                Chain::Pending => Value::Null,
                _ => json!({"slot":42,"confirmations":null,"confirmationStatus":"finalized",
                    "err":if matches!(chain, Chain::Failed) { json!({"InstructionError":[1,{"Custom":7}]}) } else { Value::Null }}),
            };
            json!({"context":{"slot":42},"value":[status]})
        } else {
            match chain {
                Chain::Pending | Chain::MissingReceipt => Value::Null,
                _ => receipt(&wallet, sig, matches!(chain, Chain::Failed)),
            }
        };
        json!({"jsonrpc":"2.0","id":body["id"],"result":result}).to_string()
    } else {
        let http = reqwest::Client::new();
        let request = if path.starts_with("GET ") {
            http.get(format!(
                "{}{}",
                upstream,
                path.split_whitespace().nth(1).unwrap()
            ))
        } else {
            http.post(format!(
                "{}{}",
                upstream,
                path.split_whitespace().nth(1).unwrap()
            ))
            .json(&body)
        };
        request
            .timeout(Duration::from_millis(1000))
            .send()
            .await?
            .text()
            .await?
    };
    socket.write_all(format!("HTTP/1.1 {code} Synthetic\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}", text.len()).as_bytes()).await?;
    state.lock().unwrap().completions += 1;
    Ok(())
}

fn receipt(wallet: &str, sig: &str, failed: bool) -> Value {
    let row = |amount: &str| json!({"accountIndex":1,"owner":wallet,"mint":super::priority_fee_route_fixture::TOKEN,"uiTokenAmount":{"amount":amount,"decimals":0}});
    json!({"slot":42,"blockTime":chrono::Utc::now().timestamp(),
        "transaction":{"signatures":[sig],"message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},{"pubkey":"token-account","signer":false,"writable":true}]}},
        "meta":{"err":if failed { json!({"InstructionError":[1,{"Custom":7}]}) } else { Value::Null },"fee":7000,
        "preBalances":[80000000,2039280],"postBalances":[if failed {79993000} else {69993000},2039280],
        "preTokenBalances":[row("0")],"postTokenBalances":[row(if failed {"0"} else {"123456"})]}})
}
async fn read(socket: &mut TcpStream) -> Result<(String, Value)> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 8192];
        let n = socket.read(&mut buf).await?;
        ensure!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        ensure!(bytes.len() <= 65536);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return Ok((
                    headers.lines().next().unwrap().to_owned(),
                    if length == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes[end + 4..end + 4 + length])?
                    },
                ));
            }
        }
    }
}
