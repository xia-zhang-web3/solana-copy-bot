// Signature-routed loopback fixture for actual daemon ticks under continuous arrivals.
use super::receipt_cash_facts_fixture::money_snapshot;
use super::receipt_reconciliation_fixture::{
    add_order, config, receipt, Fixture, SIGNATURE, TOKEN, WALLET,
};
use anyhow::Result;
use chrono::Duration;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RoutedRpc {
    url: String,
    receipts: Arc<Mutex<HashMap<String, Value>>>,
    calls: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl RoutedRpc {
    async fn new() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let receipts = Arc::new(Mutex::new(HashMap::<String, Value>::new()));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (r, c) = (receipts.clone(), calls.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (r, c) = (r.clone(), c.clone());
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let body_start = loop {
                        let mut buf = [0; 4096];
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buf[..n]);
                        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                            let len: usize = headers
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .unwrap()
                                .trim()
                                .parse()
                                .unwrap();
                            if bytes.len() >= end + 4 + len {
                                break end + 4;
                            }
                        }
                    };
                    let request: Value = serde_json::from_slice(&bytes[body_start..]).unwrap();
                    let method = request["method"].as_str().unwrap().to_string();
                    let signature = request["params"][0].as_str().unwrap_or("").to_string();
                    c.lock().unwrap().push((method.clone(), signature.clone()));
                    let value = match method.as_str() {
                        "getSignatureStatuses" => json!({"result":{"value":[{
                            "err":{"InstructionError":[0,{"Custom":7}]},
                            "slot":42,"confirmationStatus":"confirmed"}]}}),
                        "getTransaction" => r
                            .lock()
                            .unwrap()
                            .get(&signature)
                            .cloned()
                            .unwrap_or_else(|| json!({"result":null})),
                        "getTokenAccountsByOwner" => json!({"result":{"value":[]}}),
                        _ => {
                            json!({"error":{"code":-32000,"message":"unexpected review RPC method"}})
                        }
                    };
                    let body = value.to_string();
                    let response = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body);
                    stream.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        Ok(Self {
            url,
            receipts,
            calls,
            task,
        })
    }
    fn available(&self, signature: &str) {
        let mut value = receipt("sell", -5000);
        value["result"]["transaction"]["signatures"][0] = json!(signature);
        value["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":7}]});
        value["result"]["meta"]["fee"] = json!(5000);
        self.receipts
            .lock()
            .unwrap()
            .insert(signature.to_string(), value);
    }
}
impl Drop for RoutedRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn task_status(f: &Fixture, id: &str) -> Result<String> {
    Ok(f.store.load_failed_expense_task(id)?.unwrap().status)
}
fn add_sent_b(f: &Fixture, index: usize) -> Result<(String, String)> {
    let signature = format!("arrival-signature-{index}");
    let id = add_order(
        &f.store,
        &format!("arrival-b-{index}"),
        "buy",
        TOKEN,
        f.now,
        false,
    )?;
    f.store
        .mark_execution_canary_submitted(&id, f.now, &signature)?;
    Ok((id, signature))
}

#[tokio::test]
async fn failed_expense_actual_ticks_recover_ready_a_while_new_b_keep_arriving() -> Result<()> {
    let mut f = Fixture::new("sell")?;
    let rpc = RoutedRpc::new().await?;
    let mut cfg = config(&rpc.url);
    cfg.canary_enabled = true;
    cfg.canary_dry_run = true;
    cfg.canary_tiny_submit_enabled = true;
    cfg.canary_batch_limit = 1;
    cfg.canary_max_signal_age_seconds = 3600;
    cfg.max_confirm_seconds = 1;
    cfg.quote_canary_enabled = false;
    cfg.execution_signer_pubkey = WALLET.into();
    cfg.execution_signer_keypair_path = "/nonexistent/failed-expense-no-key".into();
    cfg.swap_transaction_dry_run_enabled = true;
    cfg.priority_fee_canary_rpc_url.clear();
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(cfg);
    let now = f.now + Duration::seconds(60);
    let initial_money = money_snapshot(&f)?;

    runner.process_tick(&f.store, now).await?; // Initial A: unavailable.
    assert_eq!(task_status(&f, &f.order_id)?, "pending");
    assert_eq!(
        rpc.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, s)| m == "getTransaction" && s == SIGNATURE)
            .count(),
        1
    );
    let (_, b0_signature) = add_sent_b(&f, 0)?;
    runner.process_tick(&f.store, now).await?; // Retry A, then detect B0.
    rpc.available(SIGNATURE);
    rpc.available(&b0_signature);

    let mut a_requests = 0;
    for index in 1..=4 {
        let (new_b, signature) = add_sent_b(&f, index)?;
        f.reopen()?;
        rpc.calls.lock().unwrap().clear();
        runner.process_tick(&f.store, now).await?;
        let calls = rpc.calls.lock().unwrap().clone();
        let receipts: Vec<_> = calls
            .iter()
            .filter(|(m, _)| m == "getTransaction")
            .collect();
        // One bounded sweep slot plus the ordinary detection's initial attempt.
        assert_eq!(receipts.len(), 2, "{calls:?}");
        assert_eq!(receipts.iter().filter(|(_, s)| s == &signature).count(), 1);
        a_requests += receipts.iter().filter(|(_, s)| s == SIGNATURE).count();
        assert!(!calls.iter().any(|(m, _)| m == "sendTransaction"));
        let new = f.store.load_failed_expense_task(&new_b)?.unwrap();
        assert_eq!(new.status, "pending");
        assert_eq!(new.reason, "failed_receipt_unavailable");
        assert_eq!(new.tx_signature, signature);
        assert_eq!(new.wallet, WALLET);
        assert_eq!(new.token, TOKEN);
        assert_eq!(new.attempt, 1);
        assert_eq!(money_snapshot(&f)?, initial_money);
        rpc.available(&signature);
    }
    // No drain tick: ready A must already have completed during the arrivals.
    assert_eq!(
        task_status(&f, &f.order_id)?,
        "complete",
        "ready A starved by new B"
    );
    assert_eq!(
        a_requests, 1,
        "A receives exactly one request after becoming ready"
    );
    let facts = f.store.load_failed_transaction_facts(&f.order_id)?.unwrap();
    assert_eq!(facts.tx_signature, SIGNATURE);
    assert_eq!(facts.wallet_fee()?.unwrap().as_u64(), 5000);
    assert_eq!(facts.native_delta()?.unwrap().as_i128(), -5000);
    assert_eq!(
        facts.fee_coverage,
        copybot_storage_core::FailedExpenseCoverage::Known
    );
    let (count, fee): (u64, String) = f.conn()?.query_row(
        "SELECT count(*),wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",
        [&f.order_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    assert_eq!((count, fee), (1, "5000".into()));
    assert_eq!(money_snapshot(&f)?, initial_money);
    Ok(())
}
