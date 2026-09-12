use super::failed_expense_runtime_tests::{failed_receipt, failure};
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::{add_order, SIGNATURE};
use anyhow::{ensure, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQuantity;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) fn as_of(f: &RuntimeFixture) -> DateTime<Utc> {
    f.now + Duration::seconds(4)
}

pub(super) fn closed_loss(f: &RuntimeFixture, loss: i64) -> Result<()> {
    f.store.record_execution_canary_open_position(
        "b13-closed",
        "ClosedMint",
        1.0,
        Some(TokenQuantity::new(1, 0)),
        0.03,
        as_of(f),
    )?;
    Connection::open(&f.db_path)?.execute(
        "UPDATE positions SET state='closed', closed_ts=?1, pnl_lamports=?2, pnl_sol=?3 WHERE token='ClosedMint'",
        params![as_of(f).to_rfc3339(), -loss, -(loss as f64) / 1e9],
    )?;
    Ok(())
}

/// Actual failed confirmation boundary writes the existing task/facts/ledger.
/// The fixed two-request mock is awaited and propagates every assertion.
pub(super) async fn failed_fee(f: &RuntimeFixture, fee: u64) -> Result<String> {
    let id = add_order(
        &f.store,
        "b13-network-failure",
        "sell",
        "FeeMint",
        as_of(f) - Duration::nanoseconds(1),
        true,
    )?;
    let mut receipt = failed_receipt("sell", fee);
    receipt["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
        json!(f.config.canary_wallet_pubkey);
    let (url, task) = receipt_server(receipt, true).await?;
    let result = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &f.store,
        &f.config,
        &id,
        &reqwest::Client::new(),
        &url,
        as_of(f),
        200,
    )
    .await;
    let calls = task.await??;
    assert_eq!(result?.confirmation_failed, 1);
    assert_eq!(calls, ["getSignatureStatuses", "getTransaction"]);
    let facts = f.store.load_failed_transaction_facts(&id)?.unwrap();
    assert_eq!(facts.wallet_fee()?.unwrap().as_u64(), fee);
    assert_eq!(
        f.store.load_failed_expense_task(&id)?.unwrap().status,
        "complete"
    );
    let recorded: String = Connection::open(&f.db_path)?.query_row(
        "SELECT wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",
        [&id],
        |r| r.get(0),
    )?;
    assert_eq!(recorded, fee.to_string());
    assert!(!f.store.execution_canary_fill_exists(&id)?);
    Ok(id)
}

pub(super) async fn receipt_server(
    receipt: Value,
    failed: bool,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let mut calls = Vec::new();
        for expected in ["getSignatureStatuses", "getTransaction"] {
            let mut stream =
                tokio::time::timeout(std::time::Duration::from_secs(2), listener.accept())
                    .await??
                    .0;
            let request = read_request(&mut stream).await?;
            assert_eq!(request["method"], expected);
            let signature = if expected == "getSignatureStatuses" {
                &request["params"][0][0]
            } else {
                &request["params"][0]
            };
            assert_eq!(signature, SIGNATURE);
            calls.push(expected.into());
            let response = if expected == "getSignatureStatuses" {
                json!({"result":{"value":[{"slot":42,"confirmationStatus":"confirmed","err":if failed { failure() } else { Value::Null }}]}})
            } else {
                receipt.clone()
            };
            let body = response.to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
        }
        Ok(calls)
    });
    Ok((url, task))
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0_u8; 4096];
        let n = stream.read(&mut buf).await?;
        ensure!(
            n > 0 && bytes.len() + n < 65536,
            "invalid mock HTTP request"
        );
        bytes.extend_from_slice(&buf[..n]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|v| v.strip_prefix("content-length:"))
                .unwrap()
                .trim()
                .parse()?;
            if bytes.len() >= end + 4 + length {
                return Ok(serde_json::from_slice(&bytes[end + 4..end + 4 + length])?);
            }
        }
    }
}
