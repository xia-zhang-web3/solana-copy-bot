use super::buy_retry_queue_fixture::{PENDING_TOKEN, SELL_TOKEN};
use super::execution_build_plan_refresh_contract::write_http_status_json;
use super::execution_state_machine_tiny_submit_route::{
    serialized_legacy_transaction, tiny_route_keypair,
};
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::{receipt, SIGNATURE};
use anyhow::Result;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

pub(super) struct QueueRpc {
    pub calls: Arc<Mutex<Vec<String>>>,
    pub ordinary_pending: Arc<Mutex<bool>>,
    pub pending_receipt: Arc<Mutex<bool>>,
    task: tokio::task::JoinHandle<()>,
}
impl QueueRpc {
    pub async fn new(f: &mut RuntimeFixture, missing: bool) -> Result<Self> {
        Self::with_simulation(f, missing, false, false).await
    }
    pub async fn with_simulation(
        f: &mut RuntimeFixture,
        missing: bool,
        malformed_buy: bool,
        malformed_sell: bool,
    ) -> Result<Self> {
        Self::start(f, missing, malformed_buy, malformed_sell, None, None).await
    }
    pub async fn with_initial_sol_failure(f: &mut RuntimeFixture, missing: bool) -> Result<Self> {
        Self::start(
            f,
            missing,
            false,
            false,
            Some(super::initial_sol_rpc_fixture::FundingRpc {
                balance: 0,
                ..Default::default()
            }),
            None,
        )
        .await
    }
    pub async fn with_postawait_change(f: &mut RuntimeFixture, missing: bool) -> Result<Self> {
        let change = (f.db_path.clone(), f.signal.signal_id.clone());
        Self::start(
            f,
            missing,
            false,
            false,
            Some(Default::default()),
            Some(change),
        )
        .await
    }
    async fn start(
        f: &mut RuntimeFixture,
        missing: bool,
        malformed_buy: bool,
        malformed_sell: bool,
        funding: Option<super::initial_sol_rpc_fixture::FundingRpc>,
        change_signal: Option<(std::path::PathBuf, String)>,
    ) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        f.config.quote_canary_base_url = url.clone();
        f.config.submit_adapter_http_url = url;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let pending_receipt = Arc::new(Mutex::new(missing));
        let ordinary_pending = Arc::new(Mutex::new(false));
        let ordinary = ordinary_pending.clone();
        let (log, pending) = (calls.clone(), pending_receipt.clone());
        let key = tiny_route_keypair(81);
        let transaction = serialized_legacy_transaction(key.public_key);
        let buy_transaction =
            super::tiny_transport_fixture::buy(key.public_key, [9; 32], 10_000_000);
        let allow_buy = malformed_buy || funding.is_some();
        let task = tokio::spawn(async move {
            let mut buying = false;
            loop {
                let mut socket = listener.accept().await.unwrap().0;
                let text = read_request(&mut socket).await;
                let path = text.lines().next().unwrap();
                if path.starts_with("GET ") {
                    assert!(allow_buy, "blocked BUY requested fresh quote");
                    assert!(path.starts_with("GET /quote?") && path.contains("amount=10000000&"));
                    log.lock().unwrap().push("buy-quote".into());
                    let quote = super::fresh_buy_size_fixture::quote("10000000", "100");
                    write_http_status_json(&mut socket, 200, &quote.to_string()).await;
                    continue;
                }
                let body: Value =
                    serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
                if body["id"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("native-funding-"))
                {
                    assert!(
                        buying || body["method"] == "getFeeForMessage",
                        "SELL must not request BUY account/rent RPC"
                    );
                    if body["method"] == "getFeeForMessage" {
                        if let Some((path, signal_id)) = &change_signal {
                            rusqlite::Connection::open(path).unwrap().execute(
                                "UPDATE copy_signals SET status='r1-current-state' WHERE signal_id=?1", [signal_id]).unwrap();
                        }
                    }
                    let mut funding = funding
                        .clone()
                        .unwrap_or_else(super::tiny_transport_fixture::funding);
                    funding.fee = Some(100_000);
                    let response = funding.reply(&body);
                    log.lock()
                        .unwrap()
                        .push(format!("funding:{}", body["method"].as_str().unwrap()));
                    write_http_status_json(&mut socket, 200, &response.to_string()).await;
                    continue;
                }
                let (label, result) = if path.starts_with("POST /swap") {
                    buying = body["quoteResponse"]["inputMint"]
                        == crate::execution_quote_canary_helpers::SOL_MINT;
                    assert!(!buying || allow_buy, "unexpected BUY build");
                    assert_eq!(
                        body["quoteResponse"]["inputMint"],
                        if buying {
                            crate::execution_quote_canary_helpers::SOL_MINT
                        } else {
                            SELL_TOKEN
                        }
                    );
                    assert_eq!(
                        body["quoteResponse"]["inAmount"],
                        if buying { "10000000" } else { "100" }
                    );
                    assert_eq!(body["prioritizationFeeLamports"], 22_000);
                    if path.starts_with("POST /swap-instructions ") {
                        (
                            format!("{}-build-instructions", if buying { "buy" } else { "sell" }),
                            if buying {
                                super::tiny_transport_fixture::bundle(
                                    key.public_key,
                                    [9; 32],
                                    10_000_000,
                                )
                            } else {
                                super::generic_sell_synthetic_fixture::bundle(
                                    key.public_key,
                                    200_000,
                                    10_000,
                                )
                            },
                        )
                    } else {
                        (
                            format!("{}-build-transaction", if buying { "buy" } else { "sell" }),
                            json!({"swapTransaction":if buying && funding.is_some() { &buy_transaction } else { &transaction },"simulationError":null}),
                        )
                    }
                } else {
                    let method = body["method"].as_str().unwrap();
                    let signature = if method == "getSignatureStatuses" {
                        body["params"][0][0].as_str()
                    } else {
                        body["params"][0].as_str()
                    };
                    let result = match method {
                        "simulateTransaction" => {
                            if (buying && malformed_buy) || malformed_sell {
                                json!({"context":{"slot":42},"value":{}})
                            } else {
                                json!({"context":{"slot":42},"value":{"err":null,"logs":[]}})
                            }
                        }
                        "sendTransaction" => {
                            assert!(!buying && !malformed_sell, "malformed payload was sent");
                            use base64::Engine as _;
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(body["params"][0].as_str().unwrap())
                                .unwrap();
                            assert_eq!(bytes[0], 1);
                            let sig = ed25519_dalek::Signature::from_slice(&bytes[1..65]).unwrap();
                            ed25519_dalek::VerifyingKey::from_bytes(&key.public_key)
                                .unwrap()
                                .verify_strict(&bytes[65..], &sig)
                                .unwrap();
                            json!("b12-sell-signature")
                        }
                        "getSignatureStatuses" => {
                            if signature == Some(SIGNATURE) && *ordinary.lock().unwrap() {
                                json!({"value":[null]})
                            } else {
                                json!({"value":[{"slot":42,"err":null,"confirmations":null,"confirmationStatus":"finalized"}]})
                            }
                        }
                        "getTransaction" => {
                            if signature == Some(SIGNATURE) && *pending.lock().unwrap() {
                                Value::Null
                            } else {
                                receipt_for(&key.pubkey, signature.unwrap())
                            }
                        }
                        _ => panic!("unexpected queue RPC {method}"),
                    };
                    (
                        format!(
                            "{method}:{}",
                            if method.starts_with("get") {
                                signature.unwrap_or("")
                            } else if buying {
                                "buy"
                            } else {
                                "sell"
                            }
                        ),
                        json!({"jsonrpc":"2.0","id":body["id"],"result":result}),
                    )
                };
                log.lock().unwrap().push(label);
                write_http_status_json(&mut socket, 200, &result.to_string()).await;
            }
        });
        Ok(Self {
            calls,
            pending_receipt,
            ordinary_pending,
            task,
        })
    }
    pub fn trace(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.task.abort();
        match (&mut self.task).await {
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(e.into()),
            Ok(()) => Ok(()),
        }
    }
}
impl Drop for QueueRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn receipt_for(wallet: &str, signature: &str) -> Value {
    let buy = signature == SIGNATURE;
    let mut value = receipt(
        if buy { "buy" } else { "sell" },
        if buy { -10_007_000 } else { 10_000_000 },
    )["result"]
        .clone();
    value["transaction"]["signatures"] = json!([signature]);
    value["transaction"]["message"]["accountKeys"][0]["pubkey"] = wallet.into();
    let token = if buy { PENDING_TOKEN } else { SELL_TOKEN };
    let row = |amount| json!({"accountIndex":1,"owner":wallet,"mint":token,"uiTokenAmount":{"amount":amount,"decimals":0}});
    value["meta"]["preTokenBalances"] = json!([row(if buy { "0" } else { "100" })]);
    value["meta"]["postTokenBalances"] = json!([row(if buy { "100" } else { "0" })]);
    value["meta"]["fee"] = 7000.into();
    value
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 8192];
        let count = socket.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
        assert!(bytes.len() <= 65536);
        let text = String::from_utf8_lossy(&bytes);
        if let Some((headers, body)) = text.split_once("\r\n\r\n") {
            let size = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if body.len() >= size {
                return text.into_owned();
            }
        }
    }
}
