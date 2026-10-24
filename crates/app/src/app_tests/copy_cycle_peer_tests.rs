//! A dropped recovery RPC connection must not stop the LOCAL provider listener.
use super::copy_cycle_peer_fixture::Peer;
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

async fn cancelled_connection_then_rpc(partial_header: bool) -> Result<()> {
    let peer = Peer::new(
        [41; 32],
        1_700_007,
        120,
        Arc::new(Mutex::new(BTreeMap::new())),
    )
    .await?;
    let mut abandoned =
        tokio::net::TcpStream::connect(peer.url.strip_prefix("http://").unwrap()).await?;
    if partial_header {
        abandoned
            .write_all(b"POST / HTTP/1.1\r\nContent-Length: 100\r\n")
            .await?;
    }
    drop(abandoned); // Mirrors a recovery future cancelled during runner drop.
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()?
        .post(&peer.url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getGenesisHash","params":[]}))
        .send()
        .await
        .with_context(|| format!("RPC after cancelled connection; peer={:?}", peer.healthy()))?;
    let value: serde_json::Value = response.json().await?;
    assert_eq!(value["result"], super::owner_buy_fixture::GENESIS);
    peer.healthy()?;
    let state = peer.state.lock().unwrap();
    assert_eq!(state.calls, ["getGenesisHash"]);
    assert!(
        state.sends.is_empty(),
        "cancelled input cannot apply a transaction"
    );
    Ok(())
}

#[tokio::test]
async fn copy_cycle_peer_empty_cancel_keeps_listener() -> Result<()> {
    cancelled_connection_then_rpc(false).await
}

#[tokio::test]
async fn copy_cycle_peer_partial_cancel_keeps_listener() -> Result<()> {
    cancelled_connection_then_rpc(true).await
}

async fn fatal_response(
    method: &str,
    params: serde_json::Value,
    duplicate: bool,
    reason: &str,
) -> Result<()> {
    let history = Arc::new(Mutex::new(BTreeMap::new()));
    let peer = Peer::new([41; 32], 1_700_007, 120, history.clone()).await?;
    if duplicate {
        // Negative guard control only; no wire, receipt or financial action is created.
        peer.state.lock().unwrap().sends = vec!["prior-one".into(), "prior-two".into()];
    }
    let before = peer.state.lock().unwrap().bank.clone();
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()?
        .post(&peer.url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await;
    assert!(
        response.is_err(),
        "semantic/model failure cannot produce success"
    );
    let failure = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Err(error) = peer.healthy() {
                break error;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        format!("{failure:#}").contains(reason),
        "original fatal cause lost: {failure:#}"
    );
    let state = peer.state.lock().unwrap();
    assert_eq!(state.bank, before);
    assert_eq!(state.sends.len(), if duplicate { 2 } else { 0 });
    assert!(history.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn copy_cycle_peer_semantic_error_stays_fatal() -> Result<()> {
    fatal_response(
        "unsupported-local-method",
        json!([]),
        false,
        "LOCAL unexpected provider method",
    )
    .await
}

#[tokio::test]
async fn copy_cycle_peer_model_error_stays_fatal() -> Result<()> {
    fatal_response(
        "sendTransaction",
        json!([STANDARD.encode([0; 80])]),
        false,
        "priority_fee_missing_signatures",
    )
    .await
}

#[tokio::test]
async fn copy_cycle_peer_duplicate_send_stays_fatal() -> Result<()> {
    fatal_response(
        "sendTransaction",
        json!(["NEVER_EXECUTABLE"]),
        true,
        "LOCAL duplicate external send",
    )
    .await
}
