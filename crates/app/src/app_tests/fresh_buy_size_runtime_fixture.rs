use super::execution_build_plan_refresh_contract::write_http_status_json;
use super::execution_state_machine_tiny_submit_route::*;
use super::execution_state_machine_tiny_submit_timeout_route::{
    mark_tiny_timeout_simulated, record_tiny_timeout_build_metadata,
};
use super::fresh_buy_size_fixture::quote;
use super::*;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

pub(super) struct RuntimeFixture {
    pub store: SqliteStore,
    pub config: ExecutionConfig,
    pub signal: copybot_core_types::CopySignalRow,
    pub now: chrono::DateTime<Utc>,
    pub db_path: PathBuf,
    key_path: PathBuf,
    server: tokio::task::JoinHandle<()>,
    calls: Arc<Mutex<Vec<String>>>,
    submitted_signature: Arc<Mutex<Option<String>>>,
    pub simulation_result: Arc<Mutex<Option<Value>>>,
}

impl RuntimeFixture {
    pub async fn new(
        name: &str,
        old_input: u64,
        old_output: u64,
        fresh_input: u64,
        fresh_output: u64,
        retry: bool,
    ) -> Result<Self> {
        let (store, db_path) = make_test_store(name)?;
        // Recorded fixture timestamps precede the refresh's actual Utc::now().
        let now = Utc::now() - chrono::Duration::seconds(10);
        let signal = tiny_route_signal(name, now);
        store.insert_copy_signal(&signal)?;
        let key = tiny_route_keypair(81);
        let key_path = write_tiny_route_keypair_file(name, &key.bytes)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut config = tiny_route_config(&key.pubkey, &key_path, &url, &url);
        config.quote_canary_enabled = !retry; // hot runner must reuse the persisted entry quote
        config.canary_buy_size_sol = fresh_input as f64 / 1e9;
        config.quote_canary_buy_size_sol = old_input as f64 / 1e9;
        config.max_submit_attempts = 2;
        if retry {
            let order_id = mark_tiny_timeout_simulated(&store, &signal, now)?;
            record_tiny_timeout_build_metadata(&store, &order_id, &signal, now)?;
            store.mark_execution_canary_retry_after_submit_not_sent(
                &order_id,
                now + chrono::Duration::seconds(3),
                "retry_after_rpc_submit_not_sent:rpc_send_transaction_error",
            )?;
        } else {
            record_tiny_route_quote(&store, &signal, now)?;
        }
        let table = if retry {
            "execution_canary_build_plan_metadata"
        } else {
            "execution_quote_canary_events"
        };
        let conn = Connection::open(&db_path)?;
        assert_eq!(
            conn.execute(
                &format!(
                    "UPDATE {table} SET quote_in_amount_raw=?1,
            quote_out_amount_raw=?2, quote_price_sol=0.0001, slippage_bps=0,
            quote_response_json='{{\"meta\":{{\"outDecimals\":0}}}}'"
                ),
                params![old_input.to_string(), old_output.to_string()]
            )?,
            1
        );
        drop(conn);
        drop(store);
        let store = SqliteStore::open(&db_path)?;
        if retry {
            assert!(store
                .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
                .is_none());
            let order = store
                .load_execution_canary_order_by_signal(&signal.signal_id)?
                .unwrap();
            let durable = store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
            assert_eq!(
                durable.quote_in_amount_raw.as_deref(),
                Some(old_input.to_string().as_str())
            );
            assert_eq!(
                durable.quote_out_amount_raw.as_deref(),
                Some(old_output.to_string().as_str())
            );
            assert_eq!(order.attempt, 2);
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let server_calls = calls.clone();
        let submitted_signature = Arc::new(Mutex::new(None));
        let server_signature = submitted_signature.clone();
        // B25: valid synthetic provider payload includes the mandatory BUY guard.
        let tx = super::priority_fee_fixture::guarded_transaction(key.public_key, 200_000, 10_000);
        let simulation_result = Arc::new(Mutex::new(None::<Value>));
        let server_simulation = simulation_result.clone();
        let server = tokio::spawn(async move {
            loop {
                let mut socket = listener.accept().await.unwrap().0;
                let mut buf = vec![0; 32768];
                let n = socket.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let (label, mut response) = respond(
                    &request,
                    &tx,
                    &key.pubkey,
                    fresh_input,
                    fresh_output,
                    &server_signature,
                );
                if label == "simulateTransaction" {
                    if let Some(result) = server_simulation.lock().unwrap().as_ref() {
                        response["result"] = result.clone();
                    }
                }
                server_calls.lock().unwrap().push(label);
                write_http_status_json(&mut socket, 200, &response.to_string()).await;
            }
        });
        Ok(Self {
            store,
            config,
            signal,
            now,
            db_path,
            key_path,
            server,
            calls,
            simulation_result,
            submitted_signature,
        })
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.server.abort();
        match (&mut self.server).await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    }

    pub fn submitted_signature(&self) -> Option<String> {
        self.submitted_signature.lock().unwrap().clone()
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub async fn hot(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        ExecutionCanaryRunner::new(self.config.clone())
            .process_recorded_shadow_signal(
                &self.store,
                &copybot_shadow::ShadowSignalResult {
                    signal_id: self.signal.signal_id.clone(),
                    wallet_id: self.signal.wallet_id.clone(),
                    side: "buy".into(),
                    token: self.signal.token.clone(),
                    notional_sol: 0.2,
                    latency_ms: 10,
                    closed_qty: 0.0,
                    realized_pnl_sol: 0.0,
                    has_open_lots_after_signal: Some(true),
                },
                self.now + chrono::Duration::seconds(4),
            )
            .await
    }

    pub async fn sweep(
        &self,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        Ok(super::entry_risk_clock_fixture::or_at(
            self.now + chrono::Duration::seconds(5),
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &self.config,
                &self.store,
                self.now + chrono::Duration::seconds(4),
            ),
        )
        .await?
        .expect("retry sweep"))
    }
}

impl Drop for RuntimeFixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.key_path);
        let _ = std::fs::remove_file(&self.db_path);
    }
}

fn respond(
    request: &str,
    transaction: &str,
    wallet: &str,
    input: u64,
    output: u64,
    submitted_signature: &Mutex<Option<String>>,
) -> (String, Value) {
    if request.starts_with("GET /quote?") {
        assert!(request.contains(&format!("amount={input}&")));
        return (
            "quote".into(),
            quote(&input.to_string(), &output.to_string()),
        );
    }
    let body: Value = serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    if request.starts_with("POST /swap-instructions ") || request.starts_with("POST /swap ") {
        assert_eq!(
            body.pointer("/quoteResponse/inAmount")
                .and_then(Value::as_str),
            Some(input.to_string().as_str())
        );
        assert_eq!(
            body.pointer("/quoteResponse/outAmount")
                .and_then(Value::as_str),
            Some(output.to_string().as_str())
        );
        assert_eq!(body["prioritizationFeeLamports"], 22_000);
        if request.starts_with("POST /swap-instructions ") {
            return (
                "build-instructions".into(),
                json!({"computeBudgetInstructions":[],
                "setupInstructions":[],"swapInstruction":{},"cleanupInstruction":null,
                "otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null}),
            );
        }
        return (
            "build-transaction".into(),
            json!({"swapTransaction":transaction,"simulationError":null}),
        );
    }
    let method = body["method"].as_str().unwrap();
    if body["id"]
        .as_str()
        .is_some_and(|id| id.starts_with("native-funding-"))
    {
        return (
            method.into(),
            super::initial_sol_rpc_fixture::FundingRpc::default().reply(&body),
        );
    }
    if matches!(method, "getSignatureStatuses" | "getTransaction") {
        let supplied = if method == "getTransaction" {
            &body["params"][0]
        } else {
            &body["params"][0][0]
        };
        assert_eq!(
            supplied.as_str(),
            submitted_signature.lock().unwrap().as_deref()
        );
    }
    let result = match method {
        "simulateTransaction" => json!({"context":{"slot":42},"value":{"err":null,"logs":[]}}),
        "sendTransaction" => {
            use base64::Engine as _;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(body["params"][0].as_str().unwrap())
                .unwrap();
            assert_eq!(bytes[0], 1, "one real test signer");
            let signature = ed25519_dalek::Signature::from_slice(&bytes[1..65]).unwrap();
            let key: [u8; 32] = bs58::decode(wallet).into_vec().unwrap().try_into().unwrap();
            ed25519_dalek::VerifyingKey::from_bytes(&key)
                .unwrap()
                .verify_strict(&bytes[65..], &signature)
                .expect("valid signed test transaction");
            let actual = bs58::encode(signature.to_bytes()).into_string();
            assert!(
                submitted_signature
                    .lock()
                    .unwrap()
                    .replace(actual.clone())
                    .is_none(),
                "exactly one signed submission"
            );
            json!(actual)
        }
        "getSignatureStatuses" => json!({"value":[{"slot":42,"confirmations":null,
            "err":null,"confirmationStatus":"finalized"}]}),
        "getTransaction" => {
            let mut value = receipt(wallet, input, output);
            value["transaction"]["signatures"] = json!([body["params"][0]]);
            value
        }
        _ => panic!("unexpected local request: {method}"),
    };
    (
        method.into(),
        json!({"jsonrpc":"2.0","id":body["id"],"result":result}),
    )
}

fn receipt(wallet: &str, input: u64, output: u64) -> Value {
    let row = |raw: u64| {
        json!({"accountIndex":1,"owner":wallet,"mint":"TokenMint",
        "uiTokenAmount":{"amount":raw.to_string(),"decimals":0}})
    };
    json!({"slot":42,"transaction":{"signatures":["tx-fresh-size"],
        "message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},
        {"pubkey":"token-account","signer":false,"writable":true}]}},
        "meta":{"err":null,"fee":7000,"preBalances":[2_000_000_000_u64,2_039_280],
        "postBalances":[2_000_000_000_u64-input-7000,2_039_280],
        "preTokenBalances":[row(0)],"postTokenBalances":[row(output)]}})
}
