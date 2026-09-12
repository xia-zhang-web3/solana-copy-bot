// Independent B13 same-tick probe. Import as a sibling app_tests module.
// No project wiring or Cargo execution performed by the subagent.
use super::buy_retry_safety_fixture::reopen;
use super::execution_state_machine_tiny_submit_route::tiny_route_keypair;
use super::failed_expense_runtime_tests::failure;
use super::fresh_buy_size_fixture::quote;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::ExecutionCanaryRunner;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::Duration;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn b13_auditor_same_tick_failed_fee_stops_second_new_buy() -> Result<()> {
    let mut f = RuntimeFixture::new(
        "b13-same-tick-first",
        10_000_000,
        100,
        10_000_000,
        100,
        false,
    )
    .await?;
    f.finish().await?; // replace only the fixture HTTP server
    f.config.quote_canary_enabled = false; // persisted quotes remain eligible
    f.config.canary_batch_limit = 2;
    f.config.canary_max_open_positions = 10;
    // 5,000 base + 2,000 priority lamports: a concrete small positive cap.
    f.config.canary_max_daily_loss_sol = 0.000007;
    let tick_now = f.now + Duration::seconds(4);
    let mut second = f.signal.clone();
    second.signal_id = format!("{}:second", f.signal.signal_id);
    second.ts = f.now + Duration::seconds(1);
    f.store.insert_copy_signal(&second)?;
    let mut second_quote = f
        .store
        .load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?
        .unwrap();
    second_quote.event_id = format!("quote:entry:{}", second.signal_id);
    second_quote.signal_id = Some(second.signal_id.clone());
    second_quote.signal_ts = Some(second.ts);
    second_quote.request_ts = second.ts;
    f.store.record_execution_quote_canary_event(&second_quote)?;
    assert!(f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .is_none());
    assert!(f
        .store
        .load_execution_canary_order_by_signal(&second.signal_id)?
        .is_none());
    assert!(
        !f.store
            .execution_canary_entry_cost(tick_now)?
            .check_cap(0.000007)?
            .exhausted
    );
    let key = tiny_route_keypair(81);
    let mut rpc = TickRpc::new(key.public_key, key.pubkey).await?;
    f.config.quote_canary_base_url = rpc.url.clone();
    f.config.submit_adapter_http_url = rpc.url.clone();
    reopen(&mut f)?;
    let result = ExecutionCanaryRunner::new(f.config.clone())
        .process_tick(&f.store, tick_now)
        .await;
    rpc.finish().await?;
    let result = result?;
    eprintln!(
        "ROOT TICK DEBUG {result:?}, calls={:?}",
        rpc.calls.lock().unwrap()
    );
    let first = f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .unwrap();
    let second_order = f
        .store
        .load_execution_canary_order_by_signal(&second.signal_id)?;
    let task = f.store.load_failed_expense_task(&first.order_id)?.unwrap();
    assert_eq!(
        result.candidates, 2,
        "must run both new BUY candidates: {result:?}"
    );
    assert!(
        first.submit_ts > tick_now,
        "durable claim records its authoritative decision instant"
    );
    assert_eq!(
        first.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(task.status, "complete");
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&task.operation_at)?.with_timezone(&chrono::Utc),
        first.submit_ts
    );
    assert_eq!(
        f.store
            .load_failed_transaction_facts(&first.order_id)?
            .unwrap()
            .wallet_fee()?
            .unwrap()
            .as_u64(),
        7_000
    );
    assert!(!f.store.execution_canary_accounting_pending()?);
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    let conn = rusqlite::Connection::open(&f.db_path)?;
    let first_recorded: String = conn.query_row(
        "SELECT wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",
        [&first.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(first_recorded, "7000");
    let at_boundary = f.store.execution_canary_entry_cost(tick_now)?;
    let after_boundary = f
        .store
        .execution_canary_entry_cost(first.submit_ts + Duration::nanoseconds(1))?;
    let calls = rpc.calls.lock().unwrap().clone();
    let sends = calls
        .iter()
        .filter(|v| v.as_str() == "sendTransaction")
        .count();
    eprintln!("B13 SAME TICK candidates={}, sends={sends}, first_submit={}, first_fee={first_recorded}, cost_at={}, cost_plus_1ns={}, second={:?}, trace={calls:?}", result.candidates, first.submit_ts, at_boundary.known_total_lamports.as_deref().unwrap(), after_boundary.known_total_lamports.as_deref().unwrap(), second_order.as_ref().map(|o| (&o.status, o.submit_ts)));
    // Historical report remains half-open. +1ns is a control that the actual fee exists.
    assert_eq!(at_boundary.known_total_lamports.as_deref().unwrap(), "0");
    assert_eq!(
        after_boundary.known_total_lamports.as_deref().unwrap(),
        (7_000 * sends).to_string()
    );
    assert!(after_boundary.check_cap(0.000007)?.exhausted);
    // Desired risk behavior: durable fee from the earlier work in this tick must stop BUY #2.
    assert_eq!(
        sends, 1,
        "second BUY crossed cap using the frozen tick timestamp"
    );
    assert!(
        second_order.is_none(),
        "blocked second BUY must not reserve/build/sign/send"
    );
    assert_eq!(result.state_machine_skipped_reason, Some("max_daily_loss"));
    super::initial_sol_rpc_fixture::assert_funded_buy_trace(
        &calls,
        &[
            "quote",
            "build-instructions",
            "simulateTransaction",
            "sendTransaction",
            "getSignatureStatuses",
            "getTransaction",
        ],
    );
    Ok(())
}

pub(super) struct TickRpc {
    pub(super) url: String,
    pub(super) calls: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl TickRpc {
    pub(super) async fn new(public_key: [u8; 32], wallet: String) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let output = calls.clone();
        let task = tokio::spawn(async move {
            let mut built = 0_u8;
            let mut signatures = Vec::new();
            loop {
                let mut stream = listener.accept().await?.0;
                let text = request(&mut stream).await?;
                let path = text.lines().next().unwrap();
                let (label, response) = if path.starts_with("GET /quote?") {
                    ensure!(
                        path.contains("amount=10000000&"),
                        "unexpected quote size {path}"
                    );
                    ("quote", quote("10000000", "100"))
                } else {
                    let body: Value = serde_json::from_str(text.split_once("\r\n\r\n").unwrap().1)?;
                    if path.starts_with("POST /swap") {
                        assert_eq!(body["quoteResponse"]["inAmount"], "10000000");
                        assert_eq!(body["quoteResponse"]["outAmount"], "100");
                        assert_eq!(body["prioritizationFeeLamports"], 22_000);
                        if path.starts_with("POST /swap-instructions ") {
                            built += 1;
                            (
                                "build-instructions",
                                super::tiny_transport_fixture::bundle_with_price(
                                    public_key,
                                    [built; 32],
                                    10_000_000,
                                    10_000,
                                ),
                            )
                        } else {
                            anyhow::bail!("guarded BUY must assemble locally, no opaque /swap");
                        }
                    } else if body["id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("native-funding-"))
                    {
                        let method = match body["method"].as_str().unwrap() {
                            "getFeeForMessage" => "getFeeForMessage",
                            "getMultipleAccounts" => "getMultipleAccounts",
                            "getMinimumBalanceForRentExemption" => {
                                "getMinimumBalanceForRentExemption"
                            }
                            _ => unreachable!(),
                        };
                        (
                            method,
                            super::tiny_transport_fixture::funding().reply(&body),
                        )
                    } else {
                        let method = body["method"].as_str().unwrap();
                        let response = match method {
                            "simulateTransaction" => {
                                json!({"context":{"slot":42},"value":{"err":null,"logs":[]}})
                            }
                            "sendTransaction" => {
                                let bytes = STANDARD.decode(body["params"][0].as_str().unwrap())?;
                                assert_eq!(bytes[0], 1);
                                let sig = ed25519_dalek::Signature::from_slice(&bytes[1..65])?;
                                ed25519_dalek::VerifyingKey::from_bytes(&public_key)?
                                    .verify_strict(&bytes[65..], &sig)?;
                                let signature = bs58::encode(&bytes[1..65]).into_string();
                                assert!(!signatures.contains(&signature));
                                signatures.push(signature.clone());
                                json!(signature)
                            }
                            "getSignatureStatuses" => {
                                let signature = body["params"][0][0].as_str().unwrap();
                                assert!(signatures.iter().any(|s| s == signature));
                                json!({"value":[{"slot":42,"confirmationStatus":"confirmed","err":failure()}]})
                            }
                            "getTransaction" => {
                                let signature = body["params"][0].as_str().unwrap();
                                assert!(signatures.iter().any(|s| s == signature));
                                json!({"slot":42,"transaction":{"signatures":[signature],"message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},{"pubkey":"ComputeBudget111111111111111111111111111111","signer":false,"writable":false}]}},"meta":{"err":failure(),"fee":7000,"preBalances":[1000000000,0],"postBalances":[999993000,0],"preTokenBalances":[],"postTokenBalances":[]}})
                            }
                            _ => anyhow::bail!("unexpected same-tick RPC {method}"),
                        };
                        let label = match method {
                            "simulateTransaction" => "simulateTransaction",
                            "sendTransaction" => "sendTransaction",
                            "getSignatureStatuses" => "getSignatureStatuses",
                            "getTransaction" => "getTransaction",
                            _ => unreachable!(),
                        };
                        (
                            label,
                            json!({"jsonrpc":"2.0","id":body["id"],"result":response}),
                        )
                    }
                };
                output.lock().unwrap().push(label.to_string());
                let body = response.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await?;
            }
        });
        Ok(Self { url, calls, task })
    }
    pub(super) async fn finish(&mut self) -> Result<()> {
        self.task.abort();
        match (&mut self.task).await {
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(e.into()),
            Ok(v) => v,
        }
    }
}
impl Drop for TickRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn request(stream: &mut tokio::net::TcpStream) -> Result<String> {
    let mut bytes = Vec::new();
    loop {
        let mut b = [0; 8192];
        let n = stream.read(&mut b).await?;
        ensure!(n > 0 && bytes.len() + n <= 65536, "bad HTTP request");
        bytes.extend_from_slice(&b[..n]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
            let size = headers
                .lines()
                .find_map(|v| v.strip_prefix("content-length:"))
                .map(|v| v.trim().parse::<usize>())
                .transpose()?
                .unwrap_or(0);
            if bytes.len() >= end + 4 + size {
                return Ok(String::from_utf8(bytes)?);
            }
        }
    }
}
